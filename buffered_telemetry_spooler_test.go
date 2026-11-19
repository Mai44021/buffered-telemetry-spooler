package bufferedtelemetryspooler

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func newTestSpool(t *testing.T, max int) (*Spool, string) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "spool.dat")
	s, err := New(path, max)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s, path
}

func TestPushPopAck(t *testing.T) {
	s, _ := newTestSpool(t, 10)
	if err := s.Push([]byte("alpha")); err != nil {
		t.Fatalf("Push: %v", err)
	}
	if err := s.Push([]byte("beta")); err != nil {
		t.Fatalf("Push: %v", err)
	}
	if got := s.Len(); got != 2 {
		t.Fatalf("Len = %d, want 2", got)
	}
	got, err := s.Pop()
	if err != nil {
		t.Fatalf("Pop: %v", err)
	}
	if !bytes.Equal(got, []byte("alpha")) {
		t.Fatalf("Pop = %q, want alpha", got)
	}
	// Pop does not remove; Len stays 2.
	if got := s.Len(); got != 2 {
		t.Fatalf("Len after Pop = %d, want 2", got)
	}
	if err := s.Ack(); err != nil {
		t.Fatalf("Ack: %v", err)
	}
	if got := s.Len(); got != 1 {
		t.Fatalf("Len after Ack = %d, want 1", got)
	}
	got, err = s.Pop()
	if err != nil {
		t.Fatalf("Pop: %v", err)
	}
	if !bytes.Equal(got, []byte("beta")) {
		t.Fatalf("Pop = %q, want beta", got)
	}
}

func TestPopEmpty(t *testing.T) {
	s, _ := newTestSpool(t, 4)
	_, err := s.Pop()
	if !errors.Is(err, ErrEmpty) {
		t.Fatalf("Pop empty err = %v, want ErrEmpty", err)
	}
	if err := s.Ack(); !errors.Is(err, ErrEmpty) {
		t.Fatalf("Ack empty err = %v, want ErrEmpty", err)
	}
}

func TestBoundedDropsOldest(t *testing.T) {
	s, _ := newTestSpool(t, 2)
	for _, v := range []string{"one", "two", "three"} {
		if err := s.Push([]byte(v)); err != nil {
			t.Fatalf("Push %s: %v", v, err)
		}
	}
	if got := s.Len(); got != 2 {
		t.Fatalf("Len = %d, want 2", got)
	}
	got, err := s.Pop()
	if err != nil {
		t.Fatalf("Pop: %v", err)
	}
	if !bytes.Equal(got, []byte("two")) {
		t.Fatalf("Pop = %q, want two (oldest dropped)", got)
	}
}

func TestPersistenceAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "spool.dat")
	s, err := New(path, 8)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := s.Push([]byte("keep1")); err != nil {
		t.Fatalf("Push: %v", err)
	}
	if err := s.Push([]byte("keep2")); err != nil {
		t.Fatalf("Push: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	s2, err := New(path, 8)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()
	if got := s2.Len(); got != 2 {
		t.Fatalf("Len after reopen = %d, want 2", got)
	}
	got, err := s2.Pop()
	if err != nil {
		t.Fatalf("Pop: %v", err)
	}
	if !bytes.Equal(got, []byte("keep1")) {
		t.Fatalf("Pop = %q, want keep1", got)
	}
}

func TestTruncatedTailIsDiscarded(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "spool.dat")
	s, err := New(path, 8)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := s.Push([]byte("complete")); err != nil {
		t.Fatalf("Push: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Append a partial record: header claims 100 bytes but only 3 follow.
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatalf("open append: %v", err)
	}
	partial := []byte{0, 0, 0, 100, 'a', 'b', 'c'}
	if _, err := f.Write(partial); err != nil {
		t.Fatalf("write partial: %v", err)
	}
	f.Close()

	s2, err := New(path, 8)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()
	if got := s2.Len(); got != 1 {
		t.Fatalf("Len = %d, want 1 (partial discarded)", got)
	}
	got, err := s2.Pop()
	if err != nil {
		t.Fatalf("Pop: %v", err)
	}
	if !bytes.Equal(got, []byte("complete")) {
		t.Fatalf("Pop = %q, want complete", got)
	}
}

func TestClosedSpool(t *testing.T) {
	s, _ := newTestSpool(t, 4)
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if err := s.Push([]byte("x")); !errors.Is(err, ErrClosed) {
		t.Fatalf("Push after close err = %v, want ErrClosed", err)
	}
	if _, err := s.Pop(); !errors.Is(err, ErrClosed) {
		t.Fatalf("Pop after close err = %v, want ErrClosed", err)
	}
	if err := s.Ack(); !errors.Is(err, ErrClosed) {
		t.Fatalf("Ack after close err = %v, want ErrClosed", err)
	}
}

func TestEmptyPayload(t *testing.T) {
	s, _ := newTestSpool(t, 4)
	if err := s.Push(nil); err != nil {
		t.Fatalf("Push nil: %v", err)
	}
	got, err := s.Pop()
	if err != nil {
		t.Fatalf("Pop: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("Pop = %q, want empty", got)
	}
}

func TestCapacityFloor(t *testing.T) {
	s, _ := newTestSpool(t, 0)
	if got := s.Cap(); got != 1 {
		t.Fatalf("Cap = %d, want 1", got)
	}
	if err := s.Push([]byte("a")); err != nil {
		t.Fatalf("Push: %v", err)
	}
	if err := s.Push([]byte("b")); err != nil {
		t.Fatalf("Push: %v", err)
	}
	if got := s.Len(); got != 1 {
		t.Fatalf("Len = %d, want 1", got)
	}
	got, err := s.Pop()
	if err != nil {
		t.Fatalf("Pop: %v", err)
	}
	if !bytes.Equal(got, []byte("b")) {
		t.Fatalf("Pop = %q, want b", got)
	}
}

func TestPath(t *testing.T) {
	s, path := newTestSpool(t, 4)
	if got := s.Path(); got != path {
		t.Fatalf("Path = %q, want %q", got, path)
	}
}
