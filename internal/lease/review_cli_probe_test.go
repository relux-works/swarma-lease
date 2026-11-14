package lease_test

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/relux-works/swarma-lease/internal/cli"
)

// Proves documented history forms parse and reach transport, while an unknown flag refuses locally.
func TestReviewHistoryCLI(t *testing.T) {
	root, err := os.MkdirTemp("", "rh-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(root)
	path := filepath.Join(root, "absent.sock")
	for _, tc := range []struct {
		name string
		args []string
		want int
	}{
		{"default", []string{"history", "--json"}, 69},
		{"limit", []string{"history", "--limit", "50", "--json"}, 69},
		{"cursor", []string{"history", "--limit", "50", "--before-sequence", "100", "--json"}, 69},
		{"unknown-flag", []string{"history", "--invalid", "--json"}, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			exit := cli.RunAtSocket(tc.args, &stdout, &stderr, path)
			var result map[string]any
			if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
				t.Fatal(err)
			}
			if exit != tc.want {
				t.Fatalf("exit=%d code=%v, want %d", exit, result["code"], tc.want)
			}
		})
	}
}
