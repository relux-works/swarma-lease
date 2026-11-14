package lease

import (
	"errors"

	"github.com/relux-works/swarma-lease/internal/sqlite"
)

// ErrorCode maps internal startup/runtime failures to the stable public code set.
func ErrorCode(err error) string {
	if err == nil {
		return ""
	}
	switch {
	case errors.Is(err, errSocketPathTooLong):
		return "socket_path_too_long"
	case errors.Is(err, errUnsafePath):
		return "unsafe_state_path"
	case errors.Is(err, errBadMode):
		return "permissions_invalid"
	case errors.Is(err, errCorrupt):
		return "state_corrupt"
	case errors.Is(err, errIncompatible):
		return "state_incompatible"
	case errors.Is(err, sqlite.ErrOutcomeUnknown):
		return "result_unknown"
	case errors.Is(err, errStorage), errors.Is(err, sqlite.ErrStorage), errors.Is(err, sqlite.ErrClosed):
		return "storage_unavailable"
	}
	switch err.Error() {
	case "clock_unavailable", "instance_running", "socket_stale_uncertain", "daemon_unavailable":
		return err.Error()
	default:
		return "storage_unavailable"
	}
}
