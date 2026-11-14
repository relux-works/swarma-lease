//go:build darwin || linux

package lease

/*
#cgo CFLAGS: -std=c11
#if !defined(__APPLE__)
#define _GNU_SOURCE
#endif
#include <stdint.h>
#include <stddef.h>
#include <time.h>
#include <sys/types.h>
#include <sys/socket.h>
#if defined(__APPLE__)
#include <sys/sysctl.h>
#include <sys/un.h>
#include <sys/ucred.h>
#include <mach/mach_time.h>
#else
#include <sys/un.h>
#include <unistd.h>
#include <fcntl.h>
#endif

static int swarma_read_clocks(int64_t *realtime_ns, int64_t *monotonic_ns) {
	struct timespec wall;
	if (clock_gettime(CLOCK_REALTIME, &wall) != 0) return -1;
	*realtime_ns = (int64_t)wall.tv_sec * 1000000000LL + (int64_t)wall.tv_nsec;
#if defined(__APPLE__)
	mach_timebase_info_data_t info;
	if (mach_timebase_info(&info) != KERN_SUCCESS || info.denom == 0) return -1;
	__uint128_t value = (__uint128_t)mach_continuous_time() * info.numer / info.denom;
	if (value > INT64_MAX) return -1;
	*monotonic_ns = (int64_t)value;
#else
	struct timespec up;
	if (clock_gettime(CLOCK_BOOTTIME, &up) != 0) return -1;
	*monotonic_ns = (int64_t)up.tv_sec * 1000000000LL + (int64_t)up.tv_nsec;
#endif
	return 0;
}

static int swarma_boot_session_id(char *buffer, size_t size) {
#if defined(__APPLE__)
	size_t length = size;
	if (sysctlbyname("kern.bootsessionuuid", buffer, &length, NULL, 0) != 0) return -1;
	if (length == 0 || length >= size) return -1;
	buffer[length] = '\0';
	return 0;
#else
	int fd = open("/proc/sys/kernel/random/boot_id", O_RDONLY | O_CLOEXEC);
	if (fd < 0) return -1;
	ssize_t length = read(fd, buffer, size - 1);
	close(fd);
	if (length <= 0 || (size_t)length >= size) return -1;
	while (length > 0 && (buffer[length - 1] == '\n' || buffer[length - 1] == '\r')) length--;
	buffer[length] = '\0';
	return 0;
#endif
}

static int64_t swarma_peer_uid(int fd) {
#if defined(__APPLE__)
	struct xucred credentials;
	socklen_t length = sizeof(credentials);
	if (getsockopt(fd, SOL_LOCAL, LOCAL_PEERCRED, &credentials, &length) != 0) return -1;
	if (length < sizeof(credentials)) return -1;
	return (int64_t)credentials.cr_uid;
#else
	struct ucred credentials;
	socklen_t length = sizeof(credentials);
	if (getsockopt(fd, SOL_SOCKET, SO_PEERCRED, &credentials, &length) != 0) return -1;
	if (length < sizeof(credentials)) return -1;
	return (int64_t)credentials.uid;
#endif
}
*/
import "C"

import (
	"errors"
	"net"
	"runtime"
	"strings"
	"time"
)

type Sample struct {
	Wall          time.Time
	MonotonicNS   int64
	BootSessionID string
}

type Clock interface {
	Read() (Sample, error)
}

func readClockSample(clock Clock) (Sample, error) {
	if clock == nil {
		return Sample{}, errClockUnavailable
	}
	sample, err := clock.Read()
	if err != nil || !validUUID(sample.BootSessionID) || sample.Wall.IsZero() || sample.MonotonicNS < 0 {
		return Sample{}, errClockUnavailable
	}
	return sample, nil
}

type systemClock struct{}

func (systemClock) Read() (Sample, error) {
	var wallNS, monoNS C.int64_t
	if C.swarma_read_clocks(&wallNS, &monoNS) != 0 {
		return Sample{}, errors.New("clock unavailable")
	}
	var boot [64]C.char
	if C.swarma_boot_session_id(&boot[0], C.size_t(len(boot))) != 0 {
		return Sample{}, errors.New("clock unavailable")
	}
	bootID, valid := normalizeBootSessionID(C.GoString(&boot[0]))
	if !valid {
		return Sample{}, errors.New("clock unavailable")
	}
	return Sample{
		Wall:          time.Unix(0, int64(wallNS)).UTC(),
		MonotonicNS:   int64(monoNS),
		BootSessionID: bootID,
	}, nil
}

func normalizeBootSessionID(value string) (string, bool) {
	canonical := strings.ToLower(value)
	if !validUUID(canonical) {
		return "", false
	}
	return canonical, true
}

func peerUID(conn net.Conn) (uint32, error) {
	unixConn, ok := conn.(*net.UnixConn)
	if !ok {
		return 0, errors.New("not a unix socket")
	}
	raw, err := unixConn.SyscallConn()
	if err != nil {
		return 0, err
	}
	var value int64 = -1
	err = raw.Control(func(fd uintptr) {
		value = int64(C.swarma_peer_uid(C.int(fd)))
	})
	runtime.KeepAlive(unixConn)
	if err != nil || value < 0 || value > int64(^uint32(0)) {
		return 0, errors.New("peer credentials unavailable")
	}
	return uint32(value), nil
}
