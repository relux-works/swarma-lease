package sqlite

/*
#cgo CFLAGS: -std=c11
#cgo darwin LDFLAGS: -lsqlite3
#cgo linux LDFLAGS: -lsqlite3
#include <sqlite3.h>
#include <stdlib.h>

static int swarma_bind_text(sqlite3_stmt *stmt, int index, const char *value, int length) {
	return sqlite3_bind_text(stmt, index, value, length, SQLITE_TRANSIENT);
}

static int swarma_bind_blob(sqlite3_stmt *stmt, int index, const void *value, int length) {
	return sqlite3_bind_blob(stmt, index, value, length, SQLITE_TRANSIENT);
}
*/
import "C"

import (
	"errors"
	"fmt"
	"sync"
	"unsafe"
)

var (
	ErrStorage        = errors.New("sqlite operation failed")
	ErrOutcomeUnknown = errors.New("sqlite transaction outcome is unknown")
	ErrClosed         = errors.New("sqlite connection is closed")
)

type Row map[string]any

type Result struct {
	Changes      int64
	LastInsertID int64
}

type DB struct {
	mu     sync.Mutex
	conn   *C.sqlite3
	closed bool
}

type Tx struct {
	db *DB
}

func Open(path string) (*DB, error) {
	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))

	var conn *C.sqlite3
	flags := C.int(C.SQLITE_OPEN_READWRITE | C.SQLITE_OPEN_FULLMUTEX)
	if rc := C.sqlite3_open_v2(cpath, &conn, flags, nil); rc != C.SQLITE_OK {
		if conn != nil {
			C.sqlite3_close_v2(conn)
		}
		return nil, ErrStorage
	}
	db := &DB{conn: conn}
	if err := db.ExecScript("PRAGMA foreign_keys=ON; PRAGMA busy_timeout=5000;"); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return nil
	}
	if C.sqlite3_close_v2(db.conn) != C.SQLITE_OK {
		return ErrStorage
	}
	db.closed = true
	db.conn = nil
	return nil
}

func (db *DB) ExecScript(script string) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrClosed
	}
	cscript := C.CString(script)
	defer C.free(unsafe.Pointer(cscript))
	if rc := C.sqlite3_exec(db.conn, cscript, nil, nil, nil); rc != C.SQLITE_OK {
		return ErrStorage
	}
	return nil
}

func (db *DB) Exec(query string, args ...any) (Result, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return Result{}, ErrClosed
	}
	return execLocked(db.conn, query, args...)
}

func (db *DB) Query(query string, args ...any) ([]Row, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return nil, ErrClosed
	}
	return queryLocked(db.conn, query, args...)
}

func (db *DB) Transaction(fn func(*Tx) error) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrClosed
	}
	if _, err := execLocked(db.conn, "BEGIN IMMEDIATE"); err != nil {
		return err
	}
	tx := &Tx{db: db}
	if err := fn(tx); err != nil {
		if _, rollbackErr := execLocked(db.conn, "ROLLBACK"); rollbackErr != nil {
			return fmt.Errorf("%w: rollback failed", ErrOutcomeUnknown)
		}
		return err
	}
	if _, err := execLocked(db.conn, "COMMIT"); err != nil {
		if _, rollbackErr := execLocked(db.conn, "ROLLBACK"); rollbackErr != nil {
			return fmt.Errorf("%w: commit outcome uncertain", ErrOutcomeUnknown)
		}
		return err
	}
	return nil
}

// View runs a consistent snapshot callback while serializing access to the connection.
func (db *DB) View(fn func(*Tx) error) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrClosed
	}
	if _, err := execLocked(db.conn, "BEGIN"); err != nil {
		return err
	}
	tx := &Tx{db: db}
	if err := fn(tx); err != nil {
		if _, rollbackErr := execLocked(db.conn, "ROLLBACK"); rollbackErr != nil {
			return fmt.Errorf("%w: read rollback failed", ErrStorage)
		}
		return err
	}
	if _, err := execLocked(db.conn, "COMMIT"); err != nil {
		if _, rollbackErr := execLocked(db.conn, "ROLLBACK"); rollbackErr != nil {
			return fmt.Errorf("%w: read commit uncertain", ErrOutcomeUnknown)
		}
		return err
	}
	return nil
}

func (tx *Tx) ExecScript(script string) error {
	if tx == nil || tx.db == nil || tx.db.closed {
		return ErrClosed
	}
	cscript := C.CString(script)
	defer C.free(unsafe.Pointer(cscript))
	if rc := C.sqlite3_exec(tx.db.conn, cscript, nil, nil, nil); rc != C.SQLITE_OK {
		return ErrStorage
	}
	return nil
}

func (tx *Tx) Exec(query string, args ...any) (Result, error) {
	if tx == nil || tx.db == nil || tx.db.closed {
		return Result{}, ErrClosed
	}
	return execLocked(tx.db.conn, query, args...)
}

func (tx *Tx) Query(query string, args ...any) ([]Row, error) {
	if tx == nil || tx.db == nil || tx.db.closed {
		return nil, ErrClosed
	}
	return queryLocked(tx.db.conn, query, args...)
}

func execLocked(conn *C.sqlite3, query string, args ...any) (Result, error) {
	stmt, err := prepare(conn, query, args...)
	if err != nil {
		return Result{}, err
	}
	defer C.sqlite3_finalize(stmt)
	for {
		rc := C.sqlite3_step(stmt)
		switch rc {
		case C.SQLITE_ROW:
			continue
		case C.SQLITE_DONE:
			return Result{Changes: int64(C.sqlite3_changes(conn)), LastInsertID: int64(C.sqlite3_last_insert_rowid(conn))}, nil
		default:
			return Result{}, ErrStorage
		}
	}
}

func queryLocked(conn *C.sqlite3, query string, args ...any) ([]Row, error) {
	stmt, err := prepare(conn, query, args...)
	if err != nil {
		return nil, err
	}
	defer C.sqlite3_finalize(stmt)
	columnCount := int(C.sqlite3_column_count(stmt))
	rows := make([]Row, 0)
	for {
		rc := C.sqlite3_step(stmt)
		if rc == C.SQLITE_DONE {
			return rows, nil
		}
		if rc != C.SQLITE_ROW {
			return nil, ErrStorage
		}
		row := make(Row, columnCount)
		for i := 0; i < columnCount; i++ {
			name := C.GoString(C.sqlite3_column_name(stmt, C.int(i)))
			switch C.sqlite3_column_type(stmt, C.int(i)) {
			case C.SQLITE_INTEGER:
				row[name] = int64(C.sqlite3_column_int64(stmt, C.int(i)))
			case C.SQLITE_FLOAT:
				row[name] = float64(C.sqlite3_column_double(stmt, C.int(i)))
			case C.SQLITE_TEXT:
				ptr := C.sqlite3_column_text(stmt, C.int(i))
				length := C.sqlite3_column_bytes(stmt, C.int(i))
				row[name] = C.GoStringN((*C.char)(unsafe.Pointer(ptr)), length)
			case C.SQLITE_BLOB:
				ptr := C.sqlite3_column_blob(stmt, C.int(i))
				length := C.sqlite3_column_bytes(stmt, C.int(i))
				row[name] = C.GoBytes(ptr, length)
			default:
				row[name] = nil
			}
		}
		rows = append(rows, row)
	}
}

func prepare(conn *C.sqlite3, query string, args ...any) (*C.sqlite3_stmt, error) {
	cquery := C.CString(query)
	defer C.free(unsafe.Pointer(cquery))
	var stmt *C.sqlite3_stmt
	if rc := C.sqlite3_prepare_v2(conn, cquery, -1, &stmt, nil); rc != C.SQLITE_OK {
		return nil, ErrStorage
	}
	for i, arg := range args {
		if err := bind(stmt, C.int(i+1), arg); err != nil {
			C.sqlite3_finalize(stmt)
			return nil, err
		}
	}
	return stmt, nil
}

func bind(stmt *C.sqlite3_stmt, index C.int, value any) error {
	var rc C.int
	switch v := value.(type) {
	case nil:
		rc = C.sqlite3_bind_null(stmt, index)
	case int:
		rc = C.sqlite3_bind_int64(stmt, index, C.sqlite3_int64(v))
	case int64:
		rc = C.sqlite3_bind_int64(stmt, index, C.sqlite3_int64(v))
	case uint32:
		rc = C.sqlite3_bind_int64(stmt, index, C.sqlite3_int64(v))
	case bool:
		if v {
			rc = C.sqlite3_bind_int64(stmt, index, 1)
		} else {
			rc = C.sqlite3_bind_int64(stmt, index, 0)
		}
	case string:
		cvalue := C.CString(v)
		rc = C.swarma_bind_text(stmt, index, cvalue, C.int(len(v)))
		C.free(unsafe.Pointer(cvalue))
	case []byte:
		if len(v) == 0 {
			rc = C.sqlite3_bind_zeroblob(stmt, index, 0)
		} else {
			ptr := C.CBytes(v)
			rc = C.swarma_bind_blob(stmt, index, ptr, C.int(len(v)))
			C.free(ptr)
		}
	default:
		return fmt.Errorf("unsupported sqlite value type %T", value)
	}
	if rc != C.SQLITE_OK {
		return ErrStorage
	}
	return nil
}
