package lease

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"os/user"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
	"unicode/utf8"
)

const maxFrameBytes = 65536

type Server struct {
	stateDir          string
	socketPath        string
	ownerUID          int64
	clock             Clock
	store             *Store
	listener          *net.UnixListener
	lockFile          *os.File
	socketInfo        os.FileInfo
	staleSocketBackup staleSocketBackup
	mu                sync.Mutex
	active            map[net.Conn]struct{}
	workers           sync.WaitGroup
	closing           bool
	closeOnce         sync.Once
	doneOnce          sync.Once
	done              chan struct{}
	closeErr          error
}

// POSIX access(2) permission bits; syscall does not export these on Darwin.
const (
	accessExecute = 0x01
	accessWrite   = 0x02
)

func Start(config Config) (*Server, error) {
	return startWithSocketRemoval(config, os.Remove)
}

func startWithSocketRemoval(config Config, removeSocket func(string) error) (*Server, error) {
	uid := os.Geteuid()
	if uid == 0 {
		return nil, errUnsafePath
	}
	clock := config.Clock
	if clock == nil {
		clock = systemClock{}
	}
	if _, err := readClockSample(clock); err != nil {
		return nil, errClockUnavailable
	}
	stateDir := config.StateDir
	if stateDir == "" {
		home, err := homeForUID(uid)
		if err != nil {
			return nil, errUnsafePath
		}
		stateDir = filepath.Join(home, ".swarma-lease")
	}
	stateDir, err := filepath.Abs(stateDir)
	if err != nil {
		return nil, errUnsafePath
	}
	socketPath := config.SocketPath
	if socketPath == "" {
		socketPath = filepath.Join(stateDir, "service.sock")
	}
	socketPath, err = filepath.Abs(socketPath)
	if err != nil {
		return nil, errUnsafePath
	}
	if socketPathTooLong(socketPath) {
		return nil, errSocketPathTooLong
	}
	if err := checkDirectoryComponents(stateDir, true); err != nil {
		return nil, err
	}
	socketParent := filepath.Dir(socketPath)
	if socketParent != stateDir {
		if err := checkDirectoryComponents(socketParent, false); err != nil {
			return nil, err
		}
	}
	if err := ensureStateDir(stateDir, uint32(uid)); err != nil {
		return nil, err
	}
	lockPath := filepath.Join(stateDir, "service.lock")
	lockFile, err := openAndLock(lockPath, uint32(uid))
	if err != nil {
		return nil, err
	}
	var staleSocketInfo os.FileInfo
	var staleBackup staleSocketBackup
	if info, err := os.Lstat(socketPath); err == nil {
		if info.Mode()&os.ModeSymlink != 0 || info.Mode()&os.ModeSocket == 0 {
			unlockClose(lockFile)
			return nil, errUnsafePath
		}
		stat, ok := info.Sys().(*syscall.Stat_t)
		if !ok || stat.Uid != uint32(uid) {
			unlockClose(lockFile)
			return nil, errUnsafePath
		}
		if info.Mode().Perm() != 0600 {
			unlockClose(lockFile)
			return nil, errBadMode
		}
		if err := proveStaleSocket(socketPath, info, uint32(uid)); err != nil {
			unlockClose(lockFile)
			return nil, err
		}
		staleSocketInfo = info
	} else if !errors.Is(err, os.ErrNotExist) {
		unlockClose(lockFile)
		return nil, errUnsafePath
	}
	if socketParent != stateDir {
		if err := syscall.Access(socketParent, accessWrite|accessExecute); err != nil {
			unlockClose(lockFile)
			return nil, errUnsafePath
		}
	}
	if staleSocketInfo != nil {
		if err := preflightStoreBeforeStaleSocketRemoval(stateDir, int64(uid)); err != nil {
			unlockClose(lockFile)
			return nil, err
		}
		backup, err := backupProvenStaleSocket(socketPath, staleSocketInfo, uint32(uid))
		if err != nil {
			unlockClose(lockFile)
			return nil, err
		}
		staleBackup = backup
		if err := removeProvenStaleSocket(socketPath, staleSocketInfo, uint32(uid), removeSocket); err != nil {
			if cleanupErr := cleanupStaleSocketBackup(staleBackup, uint32(uid)); cleanupErr != nil {
				unlockClose(lockFile)
				return nil, errUnsafePath
			}
			unlockClose(lockFile)
			return nil, err
		}
	}
	store, err := openStore(stateDir, int64(uid), clock)
	if err != nil {
		if err := restoreStaleSocketBackup(socketPath, staleBackup, uint32(uid)); err != nil {
			unlockClose(lockFile)
			return nil, errUnsafePath
		}
		unlockClose(lockFile)
		return nil, err
	}
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: socketPath, Net: "unix"})
	if err != nil {
		_ = store.Close()
		if restoreErr := restoreStaleSocketBackup(socketPath, staleBackup, uint32(uid)); restoreErr != nil {
			unlockClose(lockFile)
			return nil, errUnsafePath
		}
		unlockClose(lockFile)
		return nil, errUnsafePath
	}
	listener.SetUnlinkOnClose(false)
	boundInfo, err := os.Lstat(socketPath)
	if err != nil || boundInfo.Mode()&os.ModeSocket == 0 || boundInfo.Mode()&os.ModeSymlink != 0 {
		if err == nil {
			_ = removeOwnedSocket(socketPath, boundInfo, uint32(uid))
		}
		_ = listener.Close()
		_ = store.Close()
		if restoreErr := restoreStaleSocketBackup(socketPath, staleBackup, uint32(uid)); restoreErr != nil {
			unlockClose(lockFile)
			return nil, errUnsafePath
		}
		unlockClose(lockFile)
		return nil, errUnsafePath
	}
	if err := os.Chmod(socketPath, 0600); err != nil {
		_ = removeOwnedSocket(socketPath, boundInfo, uint32(uid))
		_ = listener.Close()
		_ = store.Close()
		if restoreErr := restoreStaleSocketBackup(socketPath, staleBackup, uint32(uid)); restoreErr != nil {
			unlockClose(lockFile)
			return nil, errUnsafePath
		}
		unlockClose(lockFile)
		return nil, errBadMode
	}
	socketInfo, err := os.Lstat(socketPath)
	if err != nil || !os.SameFile(boundInfo, socketInfo) || socketInfo.Mode()&os.ModeSocket == 0 || socketInfo.Mode()&os.ModeSymlink != 0 {
		_ = removeOwnedSocket(socketPath, boundInfo, uint32(uid))
		_ = listener.Close()
		_ = store.Close()
		if restoreErr := restoreStaleSocketBackup(socketPath, staleBackup, uint32(uid)); restoreErr != nil {
			unlockClose(lockFile)
			return nil, errUnsafePath
		}
		unlockClose(lockFile)
		return nil, errUnsafePath
	}
	stat, ok := socketInfo.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != uint32(uid) {
		_ = removeOwnedSocket(socketPath, boundInfo, uint32(uid))
		_ = listener.Close()
		_ = store.Close()
		if restoreErr := restoreStaleSocketBackup(socketPath, staleBackup, uint32(uid)); restoreErr != nil {
			unlockClose(lockFile)
			return nil, errUnsafePath
		}
		unlockClose(lockFile)
		return nil, errUnsafePath
	}
	if socketInfo.Mode().Perm() != 0600 {
		_ = removeOwnedSocket(socketPath, boundInfo, uint32(uid))
		_ = listener.Close()
		_ = store.Close()
		if restoreErr := restoreStaleSocketBackup(socketPath, staleBackup, uint32(uid)); restoreErr != nil {
			unlockClose(lockFile)
			return nil, errUnsafePath
		}
		unlockClose(lockFile)
		return nil, errBadMode
	}
	server := &Server{stateDir: stateDir, socketPath: socketPath, ownerUID: int64(uid), clock: clock, store: store, listener: listener, lockFile: lockFile, socketInfo: socketInfo, staleSocketBackup: staleBackup, active: make(map[net.Conn]struct{}), done: make(chan struct{})}
	if err := cleanupStaleSocketBackup(server.staleSocketBackup, uint32(uid)); err == nil {
		server.staleSocketBackup = staleSocketBackup{}
	}
	return server, nil
}

func proveStaleSocket(path string, original os.FileInfo, uid uint32) error {
	conn, err := net.DialTimeout("unix", path, time.Second)
	if err == nil {
		_ = conn.Close()
		return errors.New("socket_stale_uncertain")
	}
	if !errors.Is(err, syscall.ECONNREFUSED) {
		return errors.New("socket_stale_uncertain")
	}
	current, err := os.Lstat(path)
	if err != nil || !os.SameFile(original, current) || current.Mode()&os.ModeSocket == 0 || current.Mode()&os.ModeSymlink != 0 {
		return errors.New("socket_stale_uncertain")
	}
	stat, ok := current.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != uid || current.Mode().Perm() != 0600 {
		return errors.New("socket_stale_uncertain")
	}
	return nil
}

func removeProvenStaleSocket(path string, original os.FileInfo, uid uint32, removeSocket func(string) error) error {
	if err := proveStaleSocket(path, original, uid); err != nil {
		return err
	}
	if err := removeSocket(path); err != nil {
		return errors.New("socket_stale_uncertain")
	}
	return nil
}

const staleSocketBackupDirPrefix = ".swarma-lease-stale-"

type staleSocketBackup struct {
	dir     string
	dirInfo os.FileInfo
	path    string
	info    os.FileInfo
}

func backupProvenStaleSocket(path string, original os.FileInfo, uid uint32) (staleSocketBackup, error) {
	dir, err := os.MkdirTemp(filepath.Dir(path), staleSocketBackupDirPrefix)
	if err != nil {
		return staleSocketBackup{}, errors.New("socket_stale_uncertain")
	}
	if err := os.Chmod(dir, 0700); err != nil {
		_ = os.Remove(dir)
		return staleSocketBackup{}, errors.New("socket_stale_uncertain")
	}
	dirInfo, err := os.Lstat(dir)
	dirStat, ok := (*syscall.Stat_t)(nil), false
	if err == nil {
		dirStat, ok = dirInfo.Sys().(*syscall.Stat_t)
	}
	if err != nil || !dirInfo.IsDir() || dirInfo.Mode()&os.ModeSymlink != 0 || dirInfo.Mode().Perm() != 0700 || !ok || dirStat.Uid != uid {
		_ = os.Remove(dir)
		return staleSocketBackup{}, errUnsafePath
	}

	backupPath := filepath.Join(dir, "service.sock")
	if err := os.Link(path, backupPath); err != nil {
		_ = os.Remove(dir)
		return staleSocketBackup{}, errors.New("socket_stale_uncertain")
	}
	backupInfo, err := os.Lstat(backupPath)
	currentInfo, currentErr := os.Lstat(path)
	if err != nil || currentErr != nil || !sameOwnedSocket(backupInfo, original, uid) || !sameOwnedSocket(currentInfo, original, uid) {
		if err == nil && sameOwnedSocket(backupInfo, original, uid) {
			_ = os.Remove(backupPath)
		}
		_ = os.Remove(dir)
		return staleSocketBackup{}, errors.New("socket_stale_uncertain")
	}
	return staleSocketBackup{dir: dir, dirInfo: dirInfo, path: backupPath, info: backupInfo}, nil
}

func sameOwnedSocket(current, expected os.FileInfo, uid uint32) bool {
	if current == nil || expected == nil || !os.SameFile(expected, current) || current.Mode()&os.ModeSocket == 0 || current.Mode()&os.ModeSymlink != 0 || current.Mode().Perm() != 0600 {
		return false
	}
	stat, ok := current.Sys().(*syscall.Stat_t)
	return ok && stat.Uid == uid
}

func restoreStaleSocketBackup(socketPath string, backup staleSocketBackup, uid uint32) error {
	if backup.path == "" {
		return nil
	}
	if !sameOwnedStaleSocketBackupDir(backup.dir, backup.dirInfo, uid) {
		return errUnsafePath
	}
	backupInfo, err := os.Lstat(backup.path)
	if err != nil || !sameOwnedSocket(backupInfo, backup.info, uid) {
		return errUnsafePath
	}
	if _, err := os.Lstat(socketPath); err == nil || !errors.Is(err, os.ErrNotExist) {
		return errUnsafePath
	}
	if err := os.Link(backup.path, socketPath); err != nil {
		return errUnsafePath
	}
	restoredInfo, err := os.Lstat(socketPath)
	if err != nil || !sameOwnedSocket(restoredInfo, backup.info, uid) {
		return errUnsafePath
	}
	return cleanupStaleSocketBackup(backup, uid)
}

func cleanupStaleSocketBackup(backup staleSocketBackup, uid uint32) error {
	if backup.path == "" {
		return nil
	}
	_, dirErr := os.Lstat(backup.dir)
	if errors.Is(dirErr, os.ErrNotExist) {
		if _, err := os.Lstat(backup.path); errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return errUnsafePath
	}
	if dirErr != nil || !sameOwnedStaleSocketBackupDir(backup.dir, backup.dirInfo, uid) {
		return errUnsafePath
	}
	current, err := os.Lstat(backup.path)
	if err == nil {
		if !sameOwnedSocket(current, backup.info, uid) {
			return errUnsafePath
		}
		if err := os.Remove(backup.path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return errUnsafePath
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return errUnsafePath
	}
	if err := os.Remove(backup.dir); err != nil && !errors.Is(err, os.ErrNotExist) {
		return errUnsafePath
	}
	return nil
}

func sameOwnedStaleSocketBackupDir(path string, expected os.FileInfo, uid uint32) bool {
	if expected == nil {
		return false
	}
	current, err := os.Lstat(path)
	if err != nil || !os.SameFile(expected, current) || !current.IsDir() || current.Mode()&os.ModeSymlink != 0 || current.Mode().Perm() != 0700 {
		return false
	}
	stat, ok := current.Sys().(*syscall.Stat_t)
	return ok && stat.Uid == uid
}

func removeOwnedSocket(path string, expected os.FileInfo, uid uint32) error {
	if expected == nil {
		return nil
	}
	current, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil || !os.SameFile(expected, current) || current.Mode()&os.ModeSocket == 0 || current.Mode()&os.ModeSymlink != 0 {
		return nil
	}
	stat, ok := current.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != uid {
		return nil
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return errUnsafePath
	}
	return nil
}

func RunDefault(ctx context.Context) error {
	server, err := Start(Config{})
	if err != nil {
		return err
	}
	defer server.Close()
	return server.Serve(ctx)
}

func DefaultSocketPath() (string, error) {
	uid := os.Geteuid()
	if uid == 0 {
		return "", errUnsafePath
	}
	home, err := homeForUID(uid)
	if err != nil {
		return "", err
	}
	path := defaultSocketPath(home)
	if socketPathTooLong(path) {
		return "", errSocketPathTooLong
	}
	return path, nil
}

func defaultSocketPath(home string) string {
	return filepath.Join(home, ".swarma-lease", "service.sock")
}

func socketPathCapacity() int {
	switch runtime.GOOS {
	case "darwin":
		return 104
	case "linux":
		return 108
	default:
		return 0
	}
}

func socketPathTooLong(path string) bool {
	capacity := socketPathCapacity()
	return capacity == 0 || len(path)+1 > capacity
}

func checkDirectoryComponents(path string, allowMissingLeaf bool) error {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return errUnsafePath
	}
	cleaned := filepath.Clean(absolute)
	volume := filepath.VolumeName(cleaned)
	current := volume + string(os.PathSeparator)
	if volume == "" {
		current = string(os.PathSeparator)
	}
	relative := strings.TrimPrefix(cleaned, current)
	if relative == "" {
		return nil
	}
	components := strings.Split(relative, string(os.PathSeparator))
	for index, component := range components {
		current = filepath.Join(current, component)
		info, err := os.Lstat(current)
		if errors.Is(err, os.ErrNotExist) && allowMissingLeaf && index == len(components)-1 {
			return nil
		}
		if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
			return errUnsafePath
		}
	}
	return nil
}

func homeForUID(uid int) (string, error) {
	account, err := user.LookupId(strconv.Itoa(uid))
	if err != nil || account.HomeDir == "" {
		return "", errUnsafePath
	}
	return account.HomeDir, nil
}

func ensureStateDir(path string, uid uint32) error {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		if err := os.Mkdir(path, 0700); err != nil && !errors.Is(err, os.ErrExist) {
			return errUnsafePath
		}
		info, err = os.Lstat(path)
		if err != nil {
			return errUnsafePath
		}
		stat, ok := info.Sys().(*syscall.Stat_t)
		if !ok || stat.Uid != uid || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return errUnsafePath
		}
		if err := os.Chmod(path, 0700); err != nil {
			return errBadMode
		}
		info, err = os.Lstat(path)
	} else if err != nil {
		return errUnsafePath
	}
	if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return errUnsafePath
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != uid {
		return errUnsafePath
	}
	if info.Mode().Perm() != 0700 {
		return errBadMode
	}
	return nil
}

func openAndLock(path string, uid uint32) (*os.File, error) {
	fd, err := syscall.Open(path, syscall.O_CREAT|syscall.O_EXCL|syscall.O_RDWR|syscall.O_NOFOLLOW, 0600)
	created := err == nil
	if err != nil {
		if !errors.Is(err, syscall.EEXIST) {
			return nil, errUnsafePath
		}
		info, statErr := os.Lstat(path)
		if statErr != nil {
			return nil, errUnsafePath
		}
		if err := checkOwnedFile(path, info, uid, 0600); err != nil {
			return nil, err
		}
		fd, err = syscall.Open(path, syscall.O_RDWR|syscall.O_NOFOLLOW, 0)
		if err != nil {
			return nil, errUnsafePath
		}
	}
	file := os.NewFile(uintptr(fd), path)
	if created {
		if err := file.Chmod(0600); err != nil {
			_ = file.Close()
			return nil, errBadMode
		}
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, errUnsafePath
	}
	if err := checkOwnedFile(path, info, uid, 0600); err != nil {
		_ = file.Close()
		return nil, err
	}
	if err := syscall.Flock(fd, syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = file.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
			return nil, errors.New("instance_running")
		}
		return nil, errUnsafePath
	}
	return file, nil
}

func unlockClose(file *os.File) {
	if file == nil {
		return
	}
	_ = syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
	_ = file.Close()
}

func (s *Server) Serve(ctx context.Context) error {
	stop := context.AfterFunc(ctx, func() {
		s.stopServing()
	})
	defer stop()
	s.workers.Add(1)
	go func() {
		defer s.workers.Done()
		s.runScheduler(ctx)
	}()
	for {
		conn, err := s.listener.AcceptUnix()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				s.stopServing()
				s.workers.Wait()
				return nil
			}
			s.stopServing()
			s.workers.Wait()
			return errors.New("daemon_unavailable")
		}
		s.mu.Lock()
		if s.closing {
			s.mu.Unlock()
			_ = conn.Close()
			continue
		}
		s.active[conn] = struct{}{}
		s.workers.Add(1)
		s.mu.Unlock()
		go func(connection *net.UnixConn) {
			defer s.workers.Done()
			defer func() {
				s.mu.Lock()
				delete(s.active, connection)
				s.mu.Unlock()
				_ = connection.Close()
			}()
			s.serveConn(connection)
		}(conn)
	}
}

func (s *Server) serveConn(conn *net.UnixConn) {
	uid, err := peerUID(conn)
	if err != nil || int64(uid) != s.ownerUID {
		_ = writeResponse(conn, response("protocol", "", "error", "permissions_invalid", emptyData()))
		return
	}
	reader := bufio.NewReaderSize(conn, maxFrameBytes+2)
	for {
		frame, err := reader.ReadSlice('\n')
		if errors.Is(err, bufio.ErrBufferFull) {
			_ = writeResponse(conn, response("protocol", "", "error", "frame_too_large", emptyData()))
			return
		}
		if errors.Is(err, os.ErrClosed) || errors.Is(err, net.ErrClosed) {
			return
		}
		if err != nil {
			if len(frame) != 0 {
				_ = writeResponse(conn, response("protocol", "", "error", "invalid_json", emptyData()))
			}
			return
		}
		frame = frame[:len(frame)-1]
		if len(frame) > 0 && frame[len(frame)-1] == '\r' {
			frame = frame[:len(frame)-1]
		}
		if len(frame) > maxFrameBytes {
			_ = writeResponse(conn, response("protocol", "", "error", "frame_too_large", emptyData()))
			return
		}
		result, malformed := s.handleBytes(frame, uid)
		if result.State == "queued" && (result.Op == "lease.acquire" || result.Op == "lease.wait") {
			if s.serveWait(conn, result) != nil {
				return
			}
		} else if writeResponse(conn, result) != nil {
			return
		}
		if malformed {
			return
		}
	}
}

func (s *Server) serveWait(conn net.Conn, queued Response) error {
	subscription, err := s.store.subscribeRequest(queued.RequestID)
	if err != nil {
		return writeResponse(conn, response(queued.Op, queued.RequestID, "error", "result_unknown", emptyData()))
	}
	defer subscription.cancel()
	if err := writeResponse(conn, queued); err != nil {
		return err
	}
	event, ok := subscription.wait(s.done)
	if !ok {
		return net.ErrClosed
	}
	return writeTerminalEvent(conn, event)
}

func (s *Server) runScheduler(ctx context.Context) {
	for {
		changed := s.store.changes()
		if _, code := s.store.RunScheduler(); code != "" {
			if !s.waitSchedulerRetry(ctx, changed) {
				return
			}
			continue
		}
		if ctx.Err() != nil {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-s.done:
			return
		case <-changed:
			continue
		default:
		}

		sample, err := readClockSample(s.clock)
		if err != nil {
			if !s.waitSchedulerRetry(ctx, changed) {
				return
			}
			continue
		}
		delay, hasDeadline, err := s.store.nextWakeDelay(sample)
		if err != nil {
			if !s.waitSchedulerRetry(ctx, changed) {
				return
			}
			continue
		}
		if !hasDeadline {
			select {
			case <-ctx.Done():
				return
			case <-s.done:
				return
			case <-changed:
				continue
			}
		}
		if delay <= 0 {
			delay = 100 * time.Millisecond
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-s.done:
			timer.Stop()
			return
		case <-changed:
			timer.Stop()
		case <-timer.C:
			continue
		}
	}
}

func (s *Server) waitSchedulerRetry(ctx context.Context, changed <-chan struct{}) bool {
	timer := time.NewTimer(100 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-s.done:
		return false
	case <-changed:
		return true
	case <-timer.C:
		return true
	}
}

func writeResponse(conn net.Conn, result Response) error {
	encoded, err := json.Marshal(result)
	if err != nil {
		return err
	}
	encoded = append(encoded, '\n')
	for len(encoded) > 0 {
		n, err := conn.Write(encoded)
		if err != nil {
			return err
		}
		if n == 0 {
			return errors.New("short write")
		}
		encoded = encoded[n:]
	}
	return nil
}

func writeTerminalEvent(conn net.Conn, event TerminalEvent) error {
	encoded, err := json.Marshal(event)
	if err != nil {
		return err
	}
	encoded = append(encoded, '\n')
	for len(encoded) > 0 {
		n, err := conn.Write(encoded)
		if err != nil {
			return err
		}
		if n == 0 {
			return errors.New("short write")
		}
		encoded = encoded[n:]
	}
	return nil
}

func (s *Server) stopServing() {
	s.mu.Lock()
	if !s.closing {
		s.closing = true
		s.doneOnce.Do(func() { close(s.done) })
	}
	if s.listener != nil {
		_ = s.listener.Close()
	}
	for conn := range s.active {
		_ = conn.Close()
	}
	s.mu.Unlock()
}

func (s *Server) Close() error {
	s.closeOnce.Do(func() {
		s.stopServing()
		s.workers.Wait()
		if err := removeOwnedSocket(s.socketPath, s.socketInfo, uint32(s.ownerUID)); err != nil {
			s.closeErr = errors.New("unsafe_state_path")
		}
		if err := cleanupStaleSocketBackup(s.staleSocketBackup, uint32(s.ownerUID)); err != nil && s.closeErr == nil {
			s.closeErr = errors.New("unsafe_state_path")
		}
		if s.store != nil {
			if err := s.store.Close(); err != nil && s.closeErr == nil {
				s.closeErr = errors.New("storage_unavailable")
			}
		}
		unlockClose(s.lockFile)
	})
	return s.closeErr
}

func (s *Server) handleBytes(raw []byte, uid uint32) (Response, bool) {
	if int64(uid) != s.ownerUID {
		return response("protocol", "", "error", "permissions_invalid", emptyData()), true
	}
	if len(raw) > maxFrameBytes {
		return response("protocol", "", "error", "frame_too_large", emptyData()), true
	}
	if !utf8.Valid(raw) || !jsonObjectKeysUnique(raw) {
		return response("protocol", "", "error", "invalid_json", emptyData()), true
	}
	return s.handleJSON(raw)
}
