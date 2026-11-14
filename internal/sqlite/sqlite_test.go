package sqlite

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// TestOpenReadOnlyAllowsInspectionAndRejectsWrites proves a read-only connection can inspect committed WAL-backed data without changing the database or WAL, rejects SQL writes, and does not create a missing database; it does not exercise crash recovery of a WAL.
func TestOpenReadOnlyAllowsInspectionAndRejectsWrites(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "state.sqlite")
	fixture, err := os.OpenFile(dbPath, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil {
		t.Fatalf("create writer fixture file: %v", err)
	}
	if err := fixture.Close(); err != nil {
		t.Fatalf("close writer fixture file: %v", err)
	}
	writer, err := Open(dbPath)
	if err != nil {
		t.Fatalf("open writer fixture: %v", err)
	}
	if err := writer.ExecScript("PRAGMA journal_mode=WAL; CREATE TABLE sample (value TEXT NOT NULL); INSERT INTO sample VALUES ('kept');"); err != nil {
		_ = writer.Close()
		t.Fatalf("create WAL fixture: %v", err)
	}
	beforeDB, err := os.ReadFile(dbPath)
	if err != nil {
		_ = writer.Close()
		t.Fatalf("read fixture database: %v", err)
	}
	beforeWAL, err := os.ReadFile(dbPath + "-wal")
	if err != nil {
		_ = writer.Close()
		t.Fatalf("read fixture WAL: %v", err)
	}

	readOnly, err := OpenReadOnly(dbPath)
	if err != nil {
		_ = writer.Close()
		t.Fatalf("open read-only connection: %v", err)
	}
	rows, err := readOnly.Query("SELECT value FROM sample")
	if err != nil || len(rows) != 1 || rows[0]["value"] != "kept" {
		_ = readOnly.Close()
		_ = writer.Close()
		t.Fatalf("read-only query = (%v, %v), want committed row kept", rows, err)
	}
	if _, err := readOnly.Exec("INSERT INTO sample VALUES ('changed')"); !errors.Is(err, ErrStorage) {
		_ = readOnly.Close()
		_ = writer.Close()
		t.Fatalf("read-only insert error = %v, want ErrStorage", err)
	}
	if err := readOnly.Close(); err != nil {
		_ = writer.Close()
		t.Fatalf("close read-only connection: %v", err)
	}
	afterDB, err := os.ReadFile(dbPath)
	if err != nil {
		_ = writer.Close()
		t.Fatalf("read database after read-only connection: %v", err)
	}
	afterWAL, err := os.ReadFile(dbPath + "-wal")
	if err != nil {
		_ = writer.Close()
		t.Fatalf("read WAL after read-only connection: %v", err)
	}
	if !bytes.Equal(beforeDB, afterDB) || !bytes.Equal(beforeWAL, afterWAL) {
		_ = writer.Close()
		t.Fatal("read-only inspection changed database or WAL bytes")
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close writer fixture: %v", err)
	}

	missingPath := filepath.Join(t.TempDir(), "missing.sqlite")
	if readOnly, err := OpenReadOnly(missingPath); err == nil {
		_ = readOnly.Close()
		t.Fatal("read-only open unexpectedly created a missing database")
	}
	if _, err := os.Stat(missingPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing database stat error = %v, want not-exist", err)
	}
}
