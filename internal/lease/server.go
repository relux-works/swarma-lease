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
	stateDir   string
	socketPath string
	ownerUID   int64
	clock      Clock
	store      *Store
	listener   *net.UnixListener
	lockFile   *os.File
	socketInfo os.FileInfo
	mu         sync.Mutex
	active     map[net.Conn]struct{}
	workers    sync.WaitGroup
	closing    bool
	closeOnce  sync.Once
	closeErr   error
}

func Start(config Config) (*Server, error) {
	uid := os.Geteuid()
	if uid == 0 {
		return nil, errUnsafePath
	}
	clock := config.Clock
	if clock == nil {
		clock = systemClock{}
	}
	sample, err := clock.Read()
	if err != nil || !validUUID(sample.BootSessionID) || sample.Wall.IsZero() || sample.MonotonicNS < 0 {
		return nil, errors.New("clock_unavailable")
	}
	stateDir := config.StateDir
	if stateDir == "" {
		home, err := homeForUID(uid)
		if err != nil {
			return nil, errUnsafePath
		}
		stateDir = filepath.Join(home, ".swarma-lease")
	}
	stateDir, err = filepath.Abs(stateDir)
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
	store, err := openStore(stateDir, int64(uid), sample)
	if err != nil {
		unlockClose(lockFile)
		return nil, err
	}
	if info, err := os.Lstat(socketPath); err == nil {
		if info.Mode()&os.ModeSymlink != 0 || info.Mode()&os.ModeSocket == 0 {
			_ = store.Close()
			unlockClose(lockFile)
			return nil, errUnsafePath
		}
		stat, ok := info.Sys().(*syscall.Stat_t)
		if !ok || stat.Uid != uint32(uid) {
			_ = store.Close()
			unlockClose(lockFile)
			return nil, errUnsafePath
		}
		if info.Mode().Perm() != 0600 {
			_ = store.Close()
			unlockClose(lockFile)
			return nil, errBadMode
		}
		if err := removeProvenStaleSocket(socketPath, info, uint32(uid)); err != nil {
			_ = store.Close()
			unlockClose(lockFile)
			return nil, err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		_ = store.Close()
		unlockClose(lockFile)
		return nil, errUnsafePath
	}
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: socketPath, Net: "unix"})
	if err != nil {
		_ = store.Close()
		unlockClose(lockFile)
		return nil, errUnsafePath
	}
	if err := os.Chmod(socketPath, 0600); err != nil {
		_ = listener.Close()
		_ = store.Close()
		unlockClose(lockFile)
		return nil, errBadMode
	}
	socketInfo, err := os.Lstat(socketPath)
	if err != nil || socketInfo.Mode()&os.ModeSocket == 0 {
		_ = listener.Close()
		_ = store.Close()
		unlockClose(lockFile)
		return nil, errUnsafePath
	}
	stat, ok := socketInfo.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != uint32(uid) {
		_ = listener.Close()
		_ = store.Close()
		unlockClose(lockFile)
		return nil, errUnsafePath
	}
	if socketInfo.Mode().Perm() != 0600 {
		_ = listener.Close()
		_ = store.Close()
		unlockClose(lockFile)
		return nil, errBadMode
	}
	return &Server{stateDir: stateDir, socketPath: socketPath, ownerUID: int64(uid), clock: clock, store: store, listener: listener, lockFile: lockFile, socketInfo: socketInfo, active: make(map[net.Conn]struct{})}, nil
}

func removeProvenStaleSocket(path string, original os.FileInfo, uid uint32) error {
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
	if err := os.Remove(path); err != nil {
		return errors.New("socket_stale_uncertain")
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
	for {
		conn, err := s.listener.AcceptUnix()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				s.workers.Wait()
				return nil
			}
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
		if writeResponse(conn, result) != nil {
			return
		}
		if malformed {
			return
		}
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

func (s *Server) stopServing() {
	s.mu.Lock()
	s.closing = true
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
		if info, err := os.Lstat(s.socketPath); err == nil && os.SameFile(info, s.socketInfo) && info.Mode()&os.ModeSocket != 0 {
			stat, ok := info.Sys().(*syscall.Stat_t)
			if ok && stat.Uid == uint32(s.ownerUID) && info.Mode().Perm() == 0600 {
				if err := os.Remove(s.socketPath); err != nil {
					s.closeErr = errors.New("unsafe_state_path")
				}
			}
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
