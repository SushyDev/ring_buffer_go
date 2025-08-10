// Package ring_buffer_go provides a concurrency-safe ring buffer with absolute
// position addressing, blocking writes when full, and bounded waiting for data
// to appear at a given absolute position.
package ring_buffer_go

import (
	"bytes"
	"context"
	"io"
	"os"
	"sync"
	"sync/atomic"
)

// LockingRingBuffer is a concurrency-safe ring buffer.
//
// Internal state:
//   - data: underlying circular storage of size cap(data)
//   - startPosition: absolute base used to compute normalized offsets
//   - lastReadPosition: normalized offset after the last read byte
//   - lastWritePosition: normalized offset after the last written byte
//   - count: number of unread bytes currently stored
//   - notFull/available: condition variables for writers/readers
//   - eof/closed: flags for logical end-of-stream and closure
//
// All fields are mutated under mu except the atomic flags/counters which are
// still accessed under mu for consistent snapshots.
type LockingRingBuffer struct {
	data          []byte
	startPosition int64

	lastReadPosition  int64
	lastWritePosition int64

	count atomic.Int64
	mu    sync.Mutex

	notFull   *sync.Cond
	available *sync.Cond

	eof    atomic.Bool
	closed atomic.Bool
}

// EOFMarker signals logical end-of-stream when written via Write.
var EOFMarker = []byte("__EOF__")

// NewLockingRingBuffer constructs a new ring buffer with a capacity rounded up
// to the next power of two, starting at startPosition.
func NewLockingRingBuffer(size int64, startPosition int64) *LockingRingBuffer {
	bufferSize := calculateBufferSize(size)
	ringBuffer := &LockingRingBuffer{
		data:          make([]byte, bufferSize),
		startPosition: startPosition,
	}
	ringBuffer.notFull = sync.NewCond(&ringBuffer.mu)
	ringBuffer.available = sync.NewCond(&ringBuffer.mu)
	return ringBuffer
}

// GetCapacity returns the fixed capacity of the buffer.
func (ringBuffer *LockingRingBuffer) GetCapacity() int64 {
	ringBuffer.mu.Lock()
	defer ringBuffer.mu.Unlock()
	return ringBuffer.cap()
}

// GetSize returns the current number of unread bytes.
func (ringBuffer *LockingRingBuffer) GetSize() int64 {
	ringBuffer.mu.Lock()
	defer ringBuffer.mu.Unlock()
	return ringBuffer.count.Load()
}

// GetStartPosition returns the absolute start position.
func (ringBuffer *LockingRingBuffer) GetStartPosition() int64 {
	ringBuffer.mu.Lock()
	defer ringBuffer.mu.Unlock()
	return ringBuffer.startPosition
}

// Write appends p to the buffer, blocking if needed to avoid overwriting unread
// data. If p equals EOFMarker, Write marks logical EOF, notifies waiters, and
// returns (0, nil).
func (ringBuffer *LockingRingBuffer) Write(p []byte) (n int, err error) {
	ringBuffer.mu.Lock()
	defer ringBuffer.mu.Unlock()

	if ringBuffer.closed.Load() {
		return 0, os.ErrClosed
	}
	if bytes.Equal(p, EOFMarker) {
		ringBuffer.eof.Store(true)
		ringBuffer.available.Broadcast()
		return 0, nil
	}

	capacity := ringBuffer.cap()
	requested := int64(len(p))
	if requested > capacity {
		return 0, io.ErrShortWrite
	}
	for requested > ringBuffer.GetBytesToOverwrite() {
		if ringBuffer.closed.Load() {
			return 0, os.ErrClosed
		}
		ringBuffer.notFull.Wait()
	}

	writePosition := ringBuffer.getBufferPosition(ringBuffer.lastWritePosition)
	var bytesWritten int
	if writePosition+requested <= capacity {
		bytesWritten = copy(ringBuffer.data[writePosition:], p)
	} else {
		firstSegment := capacity - writePosition
		firstCopy := copy(ringBuffer.data[writePosition:], p[:firstSegment])
		secondCopy := copy(ringBuffer.data[0:], p[firstSegment:])
		bytesWritten = firstCopy + secondCopy
	}

	ringBuffer.lastWritePosition += int64(bytesWritten)
	ringBuffer.count.Add(int64(bytesWritten))
	ringBuffer.available.Broadcast()
	ringBuffer.notFull.Broadcast()
	return bytesWritten, nil
}

// ReadAt reads from the absolute position into p. It returns ErrOutOfRange if
// the position is behind the read cursor, and io.EOF if the position is beyond
// the last written offset. When fewer bytes than requested are available, it may
// return (n > 0, io.EOF), including when EOFMarker was set.
func (ringBuffer *LockingRingBuffer) ReadAt(p []byte, position int64) (int, error) {
	ringBuffer.mu.Lock()
	defer ringBuffer.mu.Unlock()

	if ringBuffer.closed.Load() {
		return 0, os.ErrClosed
	}
	if ringBuffer.count.Load() <= 0 {
		return 0, io.EOF
	}

	normalized := ringBuffer.getNormalizedPosition(position)
	if normalized < ringBuffer.lastReadPosition {
		return 0, ErrOutOfRange
	}
	if normalized > ringBuffer.lastWritePosition {
		return 0, io.EOF
	}

	bufferPosition := ringBuffer.getBufferPosition(normalized)
	capacity := ringBuffer.cap()
	requested := int64(len(p))
	readSize := min(ringBuffer.lastWritePosition-normalized, requested)
	if readSize <= 0 {
		return 0, io.EOF
	}

	var bytesRead int
	if bufferPosition+readSize <= capacity {
		bytesRead = copy(p, ringBuffer.data[bufferPosition:bufferPosition+readSize])
	} else {
		firstSegment := capacity - bufferPosition
		firstCopy := copy(p, ringBuffer.data[bufferPosition:])
		secondCopy := copy(p[firstSegment:], ringBuffer.data[0:readSize-firstSegment])
		bytesRead = firstCopy + secondCopy
	}

	ringBuffer.lastReadPosition = normalized + int64(bytesRead)
	ringBuffer.count.Add(-int64(bytesRead))
	ringBuffer.notFull.Broadcast()

	var err error
	if bytesRead == 0 || ringBuffer.eof.Load() || int64(bytesRead) < requested {
		err = io.EOF
	}
	return bytesRead, err
}

// IsPositionAvailable reports whether the absolute position lies inside the
// current readable window (inclusive at the write boundary).
func (ringBuffer *LockingRingBuffer) IsPositionAvailable(position int64) bool {
	ringBuffer.mu.Lock()
	defer ringBuffer.mu.Unlock()
	return ringBuffer.isPositionAvailableLocked(position)
}

// IsPositionInCapacity reports whether an absolute position can be represented
// within one capacity span of the current buffer window, allowing a tolerance
// on both sides. This does not assert that data is present in the buffer.
func (ringBuffer *LockingRingBuffer) IsPositionInCapacity(position int64, tolerance int64) bool {
	ringBuffer.mu.Lock()
	defer ringBuffer.mu.Unlock()
	if ringBuffer.closed.Load() {
		return false
	}
	normalized := ringBuffer.getNormalizedPosition(position)
	if normalized < 0 {
		return false
	}
	capacity := ringBuffer.cap()
	lowerBound := ringBuffer.lastReadPosition - tolerance
	upperBound := ringBuffer.lastWritePosition + tolerance + capacity
	return normalized >= lowerBound && normalized < upperBound
}

// GetBytesToOverwrite returns the number of free bytes available for writes
// without overwriting unread data.
func (ringBuffer *LockingRingBuffer) GetBytesToOverwrite() int64 {
	return ringBuffer.cap() - ringBuffer.count.Load()
}

// WaitForPosition blocks until the position becomes available, the buffer is
// closed, EOF guarantees the position will never be written, or the context is
// canceled. It returns true only if the position became available.
func (ringBuffer *LockingRingBuffer) WaitForPosition(ctx context.Context, position int64) bool {
	if ringBuffer.closed.Load() {
		return false
	}
	ringBuffer.mu.Lock()
	defer ringBuffer.mu.Unlock()
	if ringBuffer.isPositionAvailableLocked(position) {
		return true
	}
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			ringBuffer.mu.Lock()
			ringBuffer.available.Broadcast()
			ringBuffer.mu.Unlock()
		case <-done:
		}
	}()
	defer close(done)
	for !ringBuffer.closed.Load() && !ringBuffer.isPositionAvailableLocked(position) {
		if ctx.Err() != nil {
			return false
		}
		normalized := ringBuffer.getNormalizedPosition(position)
		if normalized < ringBuffer.lastReadPosition {
			return false
		}
		if ringBuffer.eof.Load() && normalized > ringBuffer.lastWritePosition {
			return false
		}
		ringBuffer.available.Wait()
	}
	if ringBuffer.closed.Load() {
		return false
	}
	return ringBuffer.isPositionAvailableLocked(position)
}

// ResetToPosition clears the buffer and resets all internal cursors to start
// from the provided absolute position.
func (ringBuffer *LockingRingBuffer) ResetToPosition(position int64) {
	ringBuffer.mu.Lock()
	defer ringBuffer.mu.Unlock()
	ringBuffer.startPosition = position
	ringBuffer.lastReadPosition = 0
	ringBuffer.lastWritePosition = 0
	ringBuffer.count.Store(0)
	ringBuffer.eof.Store(false)
	for i := range ringBuffer.data {
		ringBuffer.data[i] = 0
	}
	ringBuffer.notFull.Broadcast()
	ringBuffer.available.Broadcast()
}

// Close marks the buffer closed, releases all waiting readers/writers, and
// clears the underlying storage reference. Further operations fail with
// os.ErrClosed.
func (ringBuffer *LockingRingBuffer) Close() error {
	ringBuffer.closed.Store(true)
	ringBuffer.mu.Lock()
	defer ringBuffer.mu.Unlock()
	ringBuffer.data = nil
	ringBuffer.notFull.Broadcast()
	ringBuffer.available.Broadcast()
	return nil
}

// =========================
// Implementation helpers
// =========================

// calculateBufferSize returns the smallest power of two >= size.
func calculateBufferSize(size int64) int64 {
	if size > 0 && (size&(size-1)) == 0 {
		return size
	}
	power := int64(1)
	for power < size {
		power *= 2
	}
	return power
}

// cap returns the capacity as an int64.
func (ringBuffer *LockingRingBuffer) cap() int64 { return int64(cap(ringBuffer.data)) }

// earliest returns the normalized offset of the earliest unread byte, or -1 if
// the buffer is empty.
func (ringBuffer *LockingRingBuffer) earliest() int64 {
	if ringBuffer.count.Load() == 0 {
		return -1
	}
	return ringBuffer.lastWritePosition - ringBuffer.count.Load()
}

// getNormalizedPosition converts an absolute position into the linear,
// monotonically increasing normalized offset used by the buffer.
func (ringBuffer *LockingRingBuffer) getNormalizedPosition(position int64) int64 {
	return position - ringBuffer.startPosition
}

// getBufferPosition maps a normalized offset to a physical index in the
// underlying storage using a power-of-two mask.
func (ringBuffer *LockingRingBuffer) getBufferPosition(normalized int64) int64 {
	capacity := ringBuffer.cap()
	return normalized & (capacity - 1)
}

// isPositionAvailableLocked reports whether the absolute position lies inside
// the current readable window (inclusive at the upper boundary). Caller holds
// ringBuffer.mu.
func (ringBuffer *LockingRingBuffer) isPositionAvailableLocked(position int64) bool {
	if ringBuffer.count.Load() == 0 {
		return false
	}
	normalized := ringBuffer.getNormalizedPosition(position)
	if normalized < 0 {
		return false
	}
	earliest := ringBuffer.earliest()
	return normalized >= earliest && normalized <= ringBuffer.lastWritePosition
}
