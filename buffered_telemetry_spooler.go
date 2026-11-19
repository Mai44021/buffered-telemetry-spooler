// Package bufferedtelemetryspooler provides a bounded, disk-backed FIFO queue
// that accepts telemetry events even when the downstream consumer is slow or
// unavailable. Events are appended to a single spool file and only removed from
// the front once they have been successfully consumed.
//
// The design is deliberately simple: a single file, a monotonically increasing
// write offset, and a read offset that advances only after successful delivery.
// This trades throughput for durability and bounded disk usage. When the queue is
// full, the oldest events are dropped to make room for new ones, because losing
// stale telemetry is preferable to unbounded disk growth or backpressure on the
// producer.
package bufferedtelemetryspooler

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"sync"
)

// ErrEmpty is returned by Pop when the spool has no events to deliver.
var ErrEmpty = errors.New("spool is empty")

// ErrClosed is returned by operations on a closed spool.
var ErrClosed = errors.New("spool is closed")

// recordHeader is the fixed-size prefix on every record in the spool file.
// Layout: uint32 length of the payload, followed by the payload bytes.
const recordHeaderSize = 4

// Spool is a bounded, disk-backed FIFO queue of byte slices.
//
// The zero value is not usable; construct one with New.
// Spool is safe for concurrent use by multiple goroutines.
type Spool struct {
	mu       sync.Mutex
	file     *os.File
	path     string
	writeOff int64 // offset where the next record will be written
	readOff  int64 // offset of the next record to be read
	count    int   // number of records currently in the spool
	maxCount int   // maximum number of records retained
	closed   bool
}

// New creates or opens a spool backed by the file at path.
//
// maxCount is the maximum number of records retained. When the spool is full,
// Push drops the oldest records until room exists. A maxCount <= 0 is treated as
// 1 so the spool always has room for at least the newest record.
//
// If the file already exists it is opened for reading and appending; existing
// records are preserved and become readable via Pop. The file is created if it
// does not exist.
func New(path string, maxCount int) (*Spool, error) {
	if maxCount <= 0 {
		maxCount = 1
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}
	s := &Spool{
		file:     f,
		path:     path,
		maxCount: maxCount,
	}
	if err := s.scan(); err != nil {
		f.Close()
		return nil, err
	}
	return s, nil
}

// scan reads the file from the beginning and locates the first intact record.
// Any trailing partial record (from a crash mid-write) is discarded by
// truncating the file to the end of the last complete record. This makes the
// spool self-healing after an unclean shutdown.
func (s *Spool) scan() error {
	if _, err := s.file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	r := bufio.NewReader(s.file)
	var off int64
	var count int
	for {
		var hdr [recordHeaderSize]byte
		n, err := io.ReadFull(r, hdr[:])
		if err == io.EOF {
			break
		}
		if err == io.ErrUnexpectedEOF {
			// Partial header at the end; truncate it away.
			if err := s.file.Truncate(off); err != nil {
				return err
			}
			break
		}
		if err != nil {
			return err
		}
		length := binary.BigEndian.Uint32(hdr[:])
		payload := make([]byte, length)
		if _, err := io.ReadFull(r, payload); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				if err := s.file.Truncate(off); err != nil {
					return err
				}
				break
			}
			return err
		}
		off += int64(recordHeaderSize) + int64(length)
		count++
		_ = n
	}
	s.readOff = 0
	s.writeOff = off
	s.count = count
	return nil
}

// Push appends data to the spool. If the spool is at capacity the oldest
// records are dropped until there is room for the new record.
//
// The data slice is copied; the caller may reuse it after Push returns.
func (s *Spool) Push(data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	for s.count >= s.maxCount {
		if err := s.dropOldestLocked(); err != nil {
			return err
		}
	}
	var hdr [recordHeaderSize]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(data)))
	if _, err := s.file.Seek(s.writeOff, io.SeekStart); err != nil {
		return err
	}
	if _, err := s.file.Write(hdr[:]); err != nil {
		return err
	}
	if _, err := s.file.Write(data); err != nil {
		return err
	}
	if err := s.file.Sync(); err != nil {
		return err
	}
	s.writeOff += int64(recordHeaderSize) + int64(len(data))
	s.count++
	return nil
}

// Pop returns the oldest record in the spool without removing it. The returned
// slice is a copy and is safe for the caller to retain. If the spool is empty it
// returns ErrEmpty.
func (s *Spool) Pop() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, ErrClosed
	}
	if s.count == 0 {
		return nil, ErrEmpty
	}
	return s.readAtLocked(s.readOff)
}

// readAtLocked reads the record starting at off. Caller must hold s.mu.
func (s *Spool) readAtLocked(off int64) ([]byte, error) {
	if _, err := s.file.Seek(off, io.SeekStart); err != nil {
		return nil, err
	}
	var hdr [recordHeaderSize]byte
	if _, err := io.ReadFull(s.file, hdr[:]); err != nil {
		return nil, err
	}
	length := binary.BigEndian.Uint32(hdr[:])
	payload := make([]byte, length)
	if _, err := io.ReadFull(s.file, payload); err != nil {
		return nil, err
	}
	return payload, nil
}

// Ack removes the oldest record from the spool, advancing the read offset. It is
// intended to be called after the record returned by Pop has been delivered.
// If the spool is empty it returns ErrEmpty.
func (s *Spool) Ack() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	if s.count == 0 {
		return ErrEmpty
	}
	return s.dropOldestLocked()
}

// dropOldestLocked advances readOff past the oldest record and decrements count.
// Caller must hold s.mu.
func (s *Spool) dropOldestLocked() error {
	if _, err := s.file.Seek(s.readOff, io.SeekStart); err != nil {
		return err
	}
	var hdr [recordHeaderSize]byte
	if _, err := io.ReadFull(s.file, hdr[:]); err != nil {
		return err
	}
	length := binary.BigEndian.Uint32(hdr[:])
	s.readOff += int64(recordHeaderSize) + int64(length)
	s.count--
	if s.count == 0 {
		// Compact: the spool is empty, so reset both offsets and truncate the
		// file. This keeps the file from growing without bound across many
		// push/ack cycles.
		if err := s.file.Truncate(0); err != nil {
			return err
		}
		s.readOff = 0
		s.writeOff = 0
	}
	return nil
}

// Len returns the number of records currently in the spool.
func (s *Spool) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.count
}

// Cap returns the configured maximum number of records.
func (s *Spool) Cap() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.maxCount
}

// Close releases the underlying file. After Close, Push, Pop, and Ack return
// ErrClosed. Close is idempotent.
func (s *Spool) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	return s.file.Close()
}

// Path returns the filesystem path backing the spool.
func (s *Spool) Path() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.path
}
