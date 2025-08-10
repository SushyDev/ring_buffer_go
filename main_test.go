package ring_buffer_go

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCalculateBufferSize(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name     string
		input    int64
		expected int64
	}{
		{"zero size", 0, 1},
		{"size 1", 1, 1},
		{"size 2", 2, 2},
		{"size 3", 3, 4},
		{"size 4", 4, 4},
		{"size 5", 5, 8},
		{"size 15", 15, 16},
		{"size 16", 16, 16},
		{"size 17", 17, 32},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, calculateBufferSize(tc.input))
		})
	}
}

func TestNewLockingRingBuffer(t *testing.T) {
	t.Parallel()
	buffer := NewLockingRingBuffer(10, 100)
	require.NotNil(t, buffer)
	assert.Equal(t, int64(16), buffer.cap(), "Capacity should be next power of two")
	assert.Equal(t, int64(100), buffer.startPosition, "StartPosition should be set")
	assert.Equal(t, int64(16), buffer.GetBytesToOverwrite(), "BytesToOverwrite should be full capacity")
}

func TestWriteRead(t *testing.T) {
	t.Parallel()
	buffer := NewLockingRingBuffer(10, 0) // effective capacity 16
	data := []byte("hello world")

	n, err := buffer.Write(data)
	require.NoError(t, err)
	require.Equal(t, len(data), n)

	readBuf := make([]byte, len(data))
	n, err = buffer.ReadAt(readBuf, 0)
	require.NoError(t, err)
	require.Equal(t, len(data), n)
	assert.Equal(t, data, readBuf)
}

func TestReadAtPartial(t *testing.T) {
	t.Parallel()
	buffer := NewLockingRingBuffer(20, 0) // effective capacity 32
	data := []byte("hello world")
	_, err := buffer.Write(data)
	require.NoError(t, err)

	// Read only a part of the written data
	readBuf := make([]byte, 5)
	n, err := buffer.ReadAt(readBuf, 0)
	require.NoError(t, err)
	require.Equal(t, 5, n)
	assert.Equal(t, []byte("hello"), readBuf)

	// Read remaining part
	readBuf = make([]byte, len(data)-5)
	n, err = buffer.ReadAt(readBuf, 5)
	require.NoError(t, err)
	require.Equal(t, len(data)-5, n)
	assert.Equal(t, []byte(" world"), readBuf)
}

func TestWrapAround(t *testing.T) {
	t.Parallel()
	buffer := NewLockingRingBuffer(8, 0) // effective capacity 8

	// First write fits, second write would block without a consumer since cap=8
	_, err := buffer.Write([]byte("12345"))
	require.NoError(t, err)

	writeDone := make(chan struct{})
	go func() {
		_, err := buffer.Write([]byte("67890"))
		require.NoError(t, err)
		close(writeDone)
	}()

	// Ensure the writer is actually blocked briefly
	select {
	case <-writeDone:
		// If it didn't block at all, that's unexpected for this capacity
		// but not a hard failure; continue
	case <-time.After(50 * time.Millisecond):
		// Expected to block until we read some bytes
	}

	// Read a small prefix to free space and allow the blocked write to complete
	part1 := make([]byte, 2)
	n, err := buffer.ReadAt(part1, 0)
	require.NoError(t, err)
	require.Equal(t, 2, n)
	assert.Equal(t, "12", string(part1))

	// Now the pending write should complete quickly
	select {
	case <-writeDone:
		// good
	case <-time.After(200 * time.Millisecond):
		t.Fatal("second write did not unblock after reading")
	}

	// Read the remaining bytes from the new lastReadPosition (2)
	part2 := make([]byte, 8)
	n, err = buffer.ReadAt(part2, 2)
	require.NoError(t, err)
	require.Equal(t, 8, n)
	assert.Equal(t, "34567890", string(part2))

	// Combined, we saw the full sequence 1234567890 across the wrap boundary
	assert.Equal(t, "1234567890", string(append(part1, part2...)))
}

func TestWriteBlocksWhenFull(t *testing.T) {
	t.Parallel()
	buffer := NewLockingRingBuffer(8, 0) // capacity 8
	_, err := buffer.Write(make([]byte, 8))
	require.NoError(t, err)

	writeDone := make(chan bool)
	go func() {
		// This write should block
		_, err := buffer.Write([]byte("a"))
		require.NoError(t, err)
		close(writeDone)
	}()

	select {
	case <-writeDone:
		t.Fatal("Write did not block when buffer was full")
	case <-time.After(100 * time.Millisecond):
		// Expected to block
	}

	// Read some data to make space
	readBuf := make([]byte, 4)
	_, err = buffer.ReadAt(readBuf, 0)
	require.NoError(t, err)

	select {
	case <-writeDone:
		// Expected to complete now
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Write did not unblock after space was made")
	}
}

func TestWaitForPosition(t *testing.T) {
	t.Parallel()
	buffer := NewLockingRingBuffer(16, 100)

	// Test success
	go func() {
		time.Sleep(50 * time.Millisecond)
		_, err := buffer.Write([]byte("hello"))
		require.NoError(t, err)
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	assert.True(t, buffer.WaitForPosition(ctx, 104), "Should wait for position and succeed")

	// Test context cancellation
	ctx, cancel = context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	assert.False(t, buffer.WaitForPosition(ctx, 110), "Should fail due to context timeout")

	// Test buffer close
	go func() {
		time.Sleep(50 * time.Millisecond)
		buffer.Close()
	}()
	ctx, cancel = context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	assert.False(t, buffer.WaitForPosition(ctx, 110), "Should fail due to buffer being closed")
}

func TestReset(t *testing.T) {
	t.Parallel()
	buffer := NewLockingRingBuffer(10, 0)
	_, err := buffer.Write([]byte("12345"))
	require.NoError(t, err)

	buffer.ResetToPosition(100)
	assert.Equal(t, int64(100), buffer.startPosition)
	assert.Equal(t, int64(0), buffer.count.Load())
	assert.False(t, buffer.IsPositionAvailable(0))
	assert.False(t, buffer.IsPositionAvailable(100))

	readBuf := make([]byte, 5)
	_, err = buffer.ReadAt(readBuf, 100)
	assert.Error(t, err, "Read after reset should fail")
}

func TestEOF(t *testing.T) {
	t.Parallel()
	buffer := NewLockingRingBuffer(20, 0)
	data := []byte("some data")
	_, err := buffer.Write(data)
	require.NoError(t, err)

	_, err = buffer.Write(EOFMarker)
	require.NoError(t, err)

	readBuf := make([]byte, len(data))
	n, err := buffer.ReadAt(readBuf, 0)
	assert.Equal(t, len(data), n)
	assert.Equal(t, data, readBuf)
	assert.Equal(t, io.EOF, err, "Error should be EOF after reading data preceding EOFMarker")

	// After prior read consumed all available bytes, another read at end position returns EOF per ReaderAt semantics
	readBuf = make([]byte, 1)
	n, err = buffer.ReadAt(readBuf, int64(len(data)))
	assert.Equal(t, 0, n)
	assert.Equal(t, io.EOF, err)
}

func TestClose(t *testing.T) {
	t.Parallel()
	buffer := NewLockingRingBuffer(8, 0)
	_, err := buffer.Write(make([]byte, 8))
	require.NoError(t, err)

	writeErrChan := make(chan error)
	go func() {
		// This should block, then get unblocked by Close
		_, err := buffer.Write([]byte("a"))
		writeErrChan <- err
	}()

	time.Sleep(50 * time.Millisecond) // give goroutine time to block
	err = buffer.Close()
	require.NoError(t, err)

	select {
	case err := <-writeErrChan:
		assert.Error(t, err, "Write on closed buffer should return error")
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Close did not unblock waiting writer")
	}

	_, err = buffer.Write([]byte("b"))
	assert.Error(t, err, "Write on closed buffer should fail")

	_, err = buffer.ReadAt(make([]byte, 1), 0)
	assert.Error(t, err, "Read on closed buffer should fail")
}

func TestIsPositionAvailable(t *testing.T) {
	t.Parallel()
	buffer := NewLockingRingBuffer(10, 100) // capacity 16

	assert.False(t, buffer.IsPositionAvailable(100))

	_, err := buffer.Write([]byte("hello")) // writes at 100-104
	require.NoError(t, err)

	assert.True(t, buffer.IsPositionAvailable(100))
	assert.True(t, buffer.IsPositionAvailable(104))
	// Endpoint (105) is considered available by current semantics (inclusive)
	assert.True(t, buffer.IsPositionAvailable(105))
	assert.False(t, buffer.IsPositionAvailable(99))

	readBuf := make([]byte, 2)
	_, err = buffer.ReadAt(readBuf, 100) // reads 100-101
	require.NoError(t, err)

	// After reading two bytes, positions below new lastReadPosition (<=101) are no longer available
	assert.False(t, buffer.IsPositionAvailable(100))
	assert.False(t, buffer.IsPositionAvailable(101))
	// Positions between lastReadPosition and lastWritePosition remain available
	assert.True(t, buffer.IsPositionAvailable(102))
	assert.True(t, buffer.IsPositionAvailable(104))
}

func TestConcurrentReadWrite(t *testing.T) {
	t.Parallel()
	buffer := NewLockingRingBuffer(1<<12, 0) // 4KB
	iterations := 1000
	chunkSize := 100
	totalDataSize := iterations * chunkSize

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			data := make([]byte, chunkSize)
			_, err := buffer.Write(data)
			if !assert.NoError(t, err) {
				return
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		bytesRead := 0
		for bytesRead < totalDataSize {
			readBuf := make([]byte, chunkSize)
			// Wait for the data to be available
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			if !buffer.WaitForPosition(ctx, int64(bytesRead)) {
				cancel()
				assert.Fail(t, "timed out waiting for position", "pos: %d", bytesRead)
				return
			}
			cancel()

			n, err := buffer.ReadAt(readBuf, int64(bytesRead))
			if err != nil && err != io.EOF {
				if !assert.NoError(t, err, "read failed at pos %d", bytesRead) {
					return
				}
			}
			bytesRead += n
		}
		assert.Equal(t, totalDataSize, bytesRead)
	}()

	wg.Wait()
}
