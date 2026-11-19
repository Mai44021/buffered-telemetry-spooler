# buffered-telemetry-spooler

A bounded, disk-backed FIFO spool that accepts telemetry events even when the
downstream consumer is slow or unavailable, dropping the oldest events when the
configured capacity is reached.

## Usage

```go
package main

import (
	"errors"
	"fmt"
	"log"

	spool "github.com/example/buffered-telemetry-spooler"
)

func main() {
	s, err := spool.New("/var/lib/telemetry/spool.dat", 10000)
	if err != nil {
		log.Fatal(err)
	}
	defer s.Close()

	// Producer side: never blocks, never fails because the consumer is down.
	if err := s.Push([]byte(`{"span":"db.query","ms":12}`)); err != nil {
		log.Fatal(err)
	}

	// Consumer side: read the oldest event, deliver it, then acknowledge.
	event, err := s.Pop()
	if errors.Is(err, spool.ErrEmpty) {
		return
	}
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(event))
	if err := s.Ack(); err != nil {
		log.Fatal(err)
	}
}
```

## Why this exists

Telemetry producers run in the hot path of the application. When the collector
they forward to goes away, the usual choices are to block the producer, buffer in
memory until the process runs out of it, or drop events on the floor. This library
takes a fourth option: write events to a single append-only file so they survive a
restart, and cap the number of retained events so the file cannot grow without
bound. The trade-off is that when the spool is full the oldest events are
discarded to make room for the newest ones. That is a deliberate choice: recent
telemetry is more useful than stale telemetry, and a bounded file is easier to
reason about than an unbounded one.

## API

- `New(path string, maxCount int) (*Spool, error)` opens or creates the spool
  file. `maxCount` is the maximum number of retained records; values `<= 0` are
  treated as `1`.
- `(*Spool) Push(data []byte) error` appends a record, dropping the oldest
  records if the spool is at capacity. The data is copied.
- `(*Spool) Pop() ([]byte, error)` returns a copy of the oldest record without
  removing it, or `ErrEmpty`.
- `(*Spool) Ack() error` removes the oldest record. Call it after a successful
  delivery.
- `(*Spool) Len() int` and `(*Spool) Cap() int` report the current count and the
  configured maximum.
- `(*Spool) Close() error` releases the file. It is idempotent.
- `(*Spool) Path() string` returns the backing file path.
- `ErrEmpty` and `ErrClosed` are sentinel errors.

## The awkward edge

The spool is a single file with a read offset and a write offset. If the process
is killed mid-write, the file ends with a partial record. `New` detects that on
open and truncates the file back to the last complete record, so a crashed spool
reopens cleanly with all intact events preserved. The cost is that the last
in-flight event is lost; the alternative, refusing to open the file, would leave
the producer unable to recover without manual intervention.

`Pop` does not remove the record. Delivery and removal are separate so a crash
between reading and delivering does not silently drop an event. The caller must
call `Ack` once delivery succeeds. Calling `Pop` twice without `Ack` returns the
same record both times.
