package fastzip

import "sync"

// writeSerializer enforces that zip entries are committed to the archive in the
// order they were enumerated (sorted by name), regardless of the order their
// concurrent compression finishes. This makes the archive output
// deterministic: the same set of input files always produces the same bytes,
// which lets callers rely on a stable archive checksum across repeated runs.
//
// It also provides the single-writer guarantee for the underlying zip.Writer,
// so callers using it don't additionally lock Archiver.m.
type writeSerializer struct {
	mu      sync.Mutex
	cond    *sync.Cond
	next    int
	aborted bool
}

func newWriteSerializer() *writeSerializer {
	s := &writeSerializer{}
	s.cond = sync.NewCond(&s.mu)
	return s
}

// do blocks until it is idx's turn to write, runs fn while holding the write
// turn, then releases the turn to idx+1. Entries must present a contiguous
// range of indices starting at 0; a missing index would stall every later
// write.
//
// If fn returns an error, or the serializer has been aborted, all outstanding
// and future writes are unblocked. A write cancelled by an abort returns nil,
// because the entry itself didn't fail and the whole archive is discarded on
// abort regardless; returning nil avoids masking the error that triggered the
// teardown.
func (s *writeSerializer) do(idx int, fn func() error) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for s.next != idx && !s.aborted {
		s.cond.Wait()
	}

	if s.aborted {
		return nil
	}

	err := fn()
	if err != nil {
		s.aborted = true
	}
	s.next++
	s.cond.Broadcast()

	return err
}

// abort wakes every waiter so they return instead of blocking forever when the
// archive is torn down before every entry has been dispatched (for example, an
// error encountered while enumerating entries).
func (s *writeSerializer) abort() {
	s.mu.Lock()
	s.aborted = true
	s.cond.Broadcast()
	s.mu.Unlock()
}
