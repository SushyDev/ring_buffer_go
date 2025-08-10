package ring_buffer_go_test

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	rb "github.com/sushydev/ring_buffer_go"
)

// This suite focuses on pointer math, off-by-one, and wrap boundaries.
func TestPointerMath_EmptyAndSingleByte(t *testing.T) {
	t.Parallel()
	buf := rb.NewLockingRingBuffer(1, 10) // cap=1

	// Nothing available yet
	assert.False(t, buf.IsPositionAvailable(10))

	// Write one byte
	n, err := buf.Write([]byte{0xAB})
	require.NoError(t, err)
	require.Equal(t, 1, n)

	// Availability semantics are inclusive at upper bound
	assert.True(t, buf.IsPositionAvailable(10)) // first byte
	assert.True(t, buf.IsPositionAvailable(11)) // boundary (end index)
	assert.False(t, buf.IsPositionAvailable(9))
	assert.False(t, buf.IsPositionAvailable(12))

	// Read exactly one byte
	b := make([]byte, 1)
	n, err = buf.ReadAt(b, 10)
	require.Equal(t, 1, n)
	// Given ReaderAt semantics in this impl, EOF is returned when fewer than requested available OR EOF set.
	// We requested 1, got 1, so no EOF required here; current impl may return nil.
	assert.True(t, err == nil || err == io.EOF)
	assert.Equal(t, []byte{0xAB}, b)

	// After read, lower positions should be out of range/consumed
	assert.False(t, buf.IsPositionAvailable(10))
}

func TestOffByOne_ReadAtBoundaries(t *testing.T) {
	t.Parallel()
	buf := rb.NewLockingRingBuffer(8, 100)  // cap=8
	_, err := buf.Write([]byte("ABCDEFGH")) // fill capacity
	require.NoError(t, err)

	// read at earliest (100)
	p := make([]byte, 3)
	n, err := buf.ReadAt(p, 100)
	require.NoError(t, err)
	require.Equal(t, 3, n)
	assert.Equal(t, []byte("ABC"), p)

	// read exactly at current lastReadPosition (103)
	p = make([]byte, 0)
	n, err = buf.ReadAt(p, 103)
	// Zero-size read should succeed with 0, EOF per current impl returns EOF only when n==0 too; allow either
	assert.Equal(t, 0, n)
	assert.True(t, err == nil || err == io.EOF)

	// read at lastWritePosition (108) should return EOF and 0 bytes
	n, err = buf.ReadAt(make([]byte, 1), 108)
	assert.Equal(t, 0, n)
	assert.Equal(t, io.EOF, err)

	// attempting to read behind lastReadPosition should be ErrOutOfRange
	_, err = buf.ReadAt(make([]byte, 1), 101)
	assert.ErrorIs(t, err, rb.ErrOutOfRange)
}

func TestWrapWritesAndReads(t *testing.T) {
	t.Parallel()
	buf := rb.NewLockingRingBuffer(8, 0) // cap=8

	_, err := buf.Write([]byte("12345"))
	require.NoError(t, err)
	// consume two to advance earliest and free space
	b := make([]byte, 2)
	n, err := buf.ReadAt(b, 0)
	require.NoError(t, err)
	require.Equal(t, 2, n)
	assert.Equal(t, "12", string(b))

	// Now write across boundary
	_, err = buf.Write([]byte("67890"))
	require.NoError(t, err)

	// Read remaining sequence from pos 2
	out := make([]byte, 8)
	n, err = buf.ReadAt(out, 2)
	require.NoError(t, err)
	require.Equal(t, 8, n)
	assert.Equal(t, "34567890", string(out))
}

func TestAvailabilityWindowMovement(t *testing.T) {
	t.Parallel()
	buf := rb.NewLockingRingBuffer(16, 1000)
	_, err := buf.Write([]byte("abcdef")) // 1000..1005
	require.NoError(t, err)

	// initial availability inclusive of endpoint
	assert.True(t, buf.IsPositionAvailable(1000))
	assert.True(t, buf.IsPositionAvailable(1006)) // boundary
	assert.False(t, buf.IsPositionAvailable(999))

	// Read 4 bytes -> earliest moves to 1004
	p := make([]byte, 4)
	n, err := buf.ReadAt(p, 1000)
	require.NoError(t, err)
	require.Equal(t, 4, n)
	assert.Equal(t, []byte("abcd"), p)

	assert.False(t, buf.IsPositionAvailable(1000))
	assert.False(t, buf.IsPositionAvailable(1003))
	assert.True(t, buf.IsPositionAvailable(1004)) // next unread
	assert.True(t, buf.IsPositionAvailable(1006)) // boundary
}

func TestWaitForPosition_EOFAndOverwritten(t *testing.T) {
	t.Parallel()
	buf := rb.NewLockingRingBuffer(4, 0)

	// Writer: write 3 bytes then EOF
	go func() {
		_, _ = buf.Write([]byte("xyz"))
		_, _ = buf.Write(rb.EOFMarker)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	assert.True(t, buf.WaitForPosition(ctx, 2))  // last byte index
	assert.True(t, buf.WaitForPosition(ctx, 3))  // boundary
	assert.False(t, buf.WaitForPosition(ctx, 4)) // beyond, EOF reached
}

func TestWriteBlocksUntilReadFreesSpace(t *testing.T) {
	t.Parallel()
	buf := rb.NewLockingRingBuffer(4, 0) // cap=4
	_, err := buf.Write([]byte("1234"))
	require.NoError(t, err)

	unblocked := make(chan struct{})
	go func() {
		_, _ = buf.Write([]byte("A")) // should block
		close(unblocked)
	}()

	// ensure it blocks
	select {
	case <-unblocked:
		t.Fatal("write did not block")
	case <-time.After(50 * time.Millisecond):
	}

	// Free one byte by reading earliest
	p := make([]byte, 1)
	_, err = buf.ReadAt(p, 0)
	require.NoError(t, err)

	// Now write should have unblocked soon
	select {
	case <-unblocked:
		// ok
	case <-time.After(200 * time.Millisecond):
		t.Fatal("blocked write did not unblock after read")
	}
}

func TestResetClearsPointersAndWindow(t *testing.T) {
	t.Parallel()
	buf := rb.NewLockingRingBuffer(8, 50)
	_, _ = buf.Write([]byte("abcd"))
	buf.ResetToPosition(100)

	// No availability at old or new start without new writes
	assert.False(t, buf.IsPositionAvailable(50))
	assert.False(t, buf.IsPositionAvailable(100))

	// Writes resume relative to new start
	_, err := buf.Write([]byte("xy"))
	require.NoError(t, err)
	assert.True(t, buf.IsPositionAvailable(100))
	assert.True(t, buf.IsPositionAvailable(102)) // boundary
}
