package fastzip

import (
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWriteSerializerOrdersWrites(t *testing.T) {
	s := newWriteSerializer()

	var got []int
	record := func(i int) func() error {
		return func() error {
			got = append(got, i)
			return nil
		}
	}

	// Queued entries wait for their predecessor and are drained, in order, by
	// the call that completes it.
	require.NoError(t, s.enqueue(1, record(1)))
	require.NoError(t, s.enqueue(2, record(2)))
	require.Empty(t, got)

	require.NoError(t, s.do(0, record(0)))
	require.Equal(t, []int{0, 1, 2}, got)

	// An entry whose turn has already come runs immediately.
	require.NoError(t, s.enqueue(3, record(3)))
	require.Equal(t, []int{0, 1, 2, 3}, got)

	// Blocked do calls resume in index order once the gap is filled.
	var wg sync.WaitGroup
	for _, i := range []int{6, 5} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			require.NoError(t, s.do(i, record(i)))
		}()
	}
	require.NoError(t, s.enqueue(7, record(7)))
	require.NoError(t, s.do(4, record(4)))
	wg.Wait()
	require.Equal(t, []int{0, 1, 2, 3, 4, 5, 6, 7}, got)
}

func TestWriteSerializerQueuedErrorAborts(t *testing.T) {
	s := newWriteSerializer()
	boom := errors.New("boom")

	require.NoError(t, s.enqueue(1, func() error { return boom }))

	ran := false
	require.NoError(t, s.enqueue(2, func() error { ran = true; return nil }))

	// The queued failure surfaces from the call that drained it, and everything
	// after it is skipped.
	require.ErrorIs(t, s.do(0, func() error { return nil }), boom)
	require.False(t, ran)
	require.NoError(t, s.do(3, func() error { t.Fatal("ran after abort"); return nil }))
	require.NoError(t, s.enqueue(4, func() error { t.Fatal("ran after abort"); return nil }))
}

func TestWriteSerializerAbortReleasesWaiters(t *testing.T) {
	s := newWriteSerializer()

	ran := false
	done := make(chan error)
	go func() { done <- s.do(1, func() error { ran = true; return nil }) }()
	require.NoError(t, s.enqueue(2, func() error { ran = true; return nil }))

	s.abort()
	require.NoError(t, <-done)
	require.False(t, ran)
}
