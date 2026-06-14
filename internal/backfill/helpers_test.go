package backfill

import (
	"os"
	"testing"
)

// mustCloseT closes c or fails the test (keeps errcheck happy under check-blank).
func mustCloseT(t *testing.T, c interface{ Close() error }) {
	t.Helper()
	if err := c.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

// mustRead reads path or fails the test.
func mustRead(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path) //nolint:gosec // test path
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return data
}
