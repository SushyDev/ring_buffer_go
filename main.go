// Package ring_buffer_go implements a locking ring buffer with read/write capabilities.
package ring_buffer_go

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"sync"
	"sync/atomic"
)

type LockingRingBufferInterface interface {
	GetCapacity() int64
	GetSize() int64
	GetStartPosition() int64

	// Reads from the buffer at the given absolute position.
	ReadAt(p []byte, position int64) (n int, err error)

	// Appends data to the buffer and returns the number of bytes written.
	Write(p []byte) (n int, err error)

	// Returns the number of bytes that can be written to the buffer without overwriting any data.
	GetBytesToOverwrite() int64

	// Calculates if the given absolute position is in the buffer.
	IsPositionAvailable(position int64) bool

	// Checks if the given absolute position is within the buffer's capacity.
	IsPositionInCapacity(position int64, tolerance int64) bool

	// Waits until the given absolute position is in the buffer.
	WaitForPosition(ctx context.Context, position int64) bool

	// Resets the buffer to the given absolute position.
	ResetToPosition(position int64)

	// Closes the buffer.
	Close() error
}

var _ LockingRingBufferInterface = &LockingRingBuffer{}
var _ io.WriteCloser = &LockingRingBuffer{}
var _ io.ReaderAt = &LockingRingBuffer{}

// ErrOutOfRange indicates the requested position is no longer available in the buffer window.
var ErrOutOfRange = errors.New("ringbuffer: position out of range")

type LockingRingBuffer struct {
	data          []byte
	startPosition int64

	lastReadPosition  int64 // Stores normalized position (page * cap + position)
	lastWritePosition int64 // Stores normalized position (page * cap + position)

	count atomic.Int64
	mu    sync.Mutex

	notFull   *sync.Cond
	available *sync.Cond

	eof    atomic.Bool
	closed atomic.Bool
}

var EOFMarker = []byte("__EOF__")

// Calculates the nearest bigger power of 2 for the given size.
func calculateBufferSize(size int64) int64 {
	// Check if already a power of two
	if size > 0 && (size&(size-1)) == 0 {
		return size
	}

	// If not, find the next power of two
	power := int64(1)
	for power < size {
		power *= 2
	}

	return power
}

func NewLockingRingBuffer(size int64, startPosition int64) *LockingRingBuffer {
	bufferSize := calculateBufferSize(size)

	lockingRingBuffer := &LockingRingBuffer{
		data:          make([]byte, bufferSize),
		startPosition: startPosition,
	}

	lockingRingBuffer.notFull = sync.NewCond(&lockingRingBuffer.mu)
	lockingRingBuffer.available = sync.NewCond(&lockingRingBuffer.mu)

	return lockingRingBuffer
}

func (buffer *LockingRingBuffer) earliest() int64 {
	if buffer.count.Load() == 0 {
		return -1
	}

	return buffer.lastWritePosition - buffer.count.Load()
}

func (buffer *LockingRingBuffer) cap() int64 {
	return int64(cap(buffer.data))
}

func (buffer *LockingRingBuffer) GetCapacity() int64 {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()
	return buffer.cap()
}

func (buffer *LockingRingBuffer) GetSize() int64 {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()
	return buffer.count.Load()
}

func (buffer *LockingRingBuffer) GetStartPosition() int64 {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()
	return buffer.startPosition
}

func (buffer *LockingRingBuffer) getNormalizedPosition(position int64) int64 {
	return position - buffer.startPosition
}

func (buffer *LockingRingBuffer) getBufferPosition(normalizedPosition int64) int64 {
	cap := buffer.cap()
	return normalizedPosition & (cap - 1)
}

func (buffer *LockingRingBuffer) isPositionAvailableLocked(position int64) bool {
	if buffer.count.Load() == 0 {
		return false
	}

	normalizedPosition := buffer.getNormalizedPosition(position)
	if normalizedPosition < 0 {
		return false
	}

	earliest := buffer.earliest()

	return normalizedPosition >= earliest && normalizedPosition <= buffer.lastWritePosition
}

func (buffer *LockingRingBuffer) IsPositionAvailable(position int64) bool {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()
	return buffer.isPositionAvailableLocked(position)
}

func (buffer *LockingRingBuffer) Write(p []byte) (n int, err error) {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()

	if buffer.closed.Load() {
		return 0, os.ErrClosed
	}

	if bytes.Equal(p, EOFMarker) {
		buffer.eof.Store(true)
		// wake any readers waiting for EOF boundary
		buffer.available.Broadcast()
		return 0, nil
	}

	bufferCap := buffer.cap()
	requestedSize := int64(len(p))

	if requestedSize > bufferCap {
		return 0, io.ErrShortWrite
	}

	for requestedSize > buffer.GetBytesToOverwrite() {
		if buffer.closed.Load() {
			return 0, os.ErrClosed
		}
		buffer.notFull.Wait()
	}

	bufferWritePos := buffer.getBufferPosition(buffer.lastWritePosition)

	var bytesWritten int
	if bufferWritePos+requestedSize <= bufferCap {
		bytesWritten = copy(buffer.data[bufferWritePos:], p)
	} else {
		firstPart := bufferCap - bufferWritePos
		a := copy(buffer.data[bufferWritePos:], p[:firstPart])
		b := copy(buffer.data[0:], p[firstPart:])
		bytesWritten = a + b
	}

	buffer.lastWritePosition += int64(bytesWritten)
	buffer.count.Add(int64(bytesWritten))

	// Notify readers that data is available and writers that space might have changed
	buffer.available.Broadcast()
	buffer.notFull.Broadcast()

	return bytesWritten, nil
}

func (buffer *LockingRingBuffer) ReadAt(p []byte, position int64) (int, error) {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()

	if buffer.closed.Load() {
		return 0, os.ErrClosed
	}

	if buffer.count.Load() <= 0 {
		return 0, io.EOF
	}

	normalizedPosition := buffer.getNormalizedPosition(position)

	if normalizedPosition < buffer.lastReadPosition {
		return 0, ErrOutOfRange
	}

	if normalizedPosition > buffer.lastWritePosition {
		return 0, io.EOF
	}

	// if normalizedPosition > buffer.lastWritePosition {
	// 	return 0, io.EOF
	// }

	bufferPosition := buffer.getBufferPosition(normalizedPosition)
	bufferCap := buffer.cap()
	requestedSize := int64(len(p))

	readSize := min(buffer.lastWritePosition-normalizedPosition, requestedSize)
	if readSize <= 0 {
		return 0, io.EOF
	}

	var bytesRead int
	if bufferPosition+readSize <= bufferCap {
		bytesRead = copy(p, buffer.data[bufferPosition:bufferPosition+readSize])
	} else {
		firstPart := bufferCap - bufferPosition
		a := copy(p, buffer.data[bufferPosition:])
		b := copy(p[firstPart:], buffer.data[0:readSize-firstPart])
		bytesRead = a + b
	}

	buffer.lastReadPosition = normalizedPosition + int64(bytesRead)
	// Decrease count by bytesRead; safe under lock, but use atomic Add for clarity
	buffer.count.Add(-int64(bytesRead))

	// Wake any writers that might be waiting for space
	buffer.notFull.Broadcast()

	// Per io.ReaderAt, it's allowed to return EOF with n>0 when fewer bytes than requested are available.
	var err error
	if bytesRead == 0 || buffer.eof.Load() || int64(bytesRead) < requestedSize {
		err = io.EOF
	}

	return bytesRead, err
}

func (buffer *LockingRingBuffer) GetBytesToOverwrite() int64 {
	return buffer.cap() - buffer.count.Load()
}

// IsPositionInCapacity detects wether absolute position is within the buffer's capacity, considering a tolerance
func (buffer *LockingRingBuffer) IsPositionInCapacity(position int64, tolerance int64) bool {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()

	if buffer.closed.Load() {
		return false
	}

	norm := buffer.getNormalizedPosition(position)
	cap := buffer.cap()

	if norm < 0 {
		return false
	}

	// Define a capacity window centered on the current buffer window,
	// allowing tolerance on both sides but not exceeding one capacity span.
	lower := buffer.lastReadPosition - tolerance
	upper := buffer.lastWritePosition + tolerance + cap

	return norm >= lower && norm < upper
}

func (buffer *LockingRingBuffer) WaitForPosition(ctx context.Context, position int64) bool {
	// Fast path: if already closed, nothing to wait for
	if buffer.closed.Load() {
		return false
	}

	buffer.mu.Lock()
	defer buffer.mu.Unlock()

	// If already available, return immediately
	if buffer.isPositionAvailableLocked(position) {
		return true
	}

	// Arrange for context cancellation to wake waiters.
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			buffer.mu.Lock()
			buffer.available.Broadcast()
			buffer.mu.Unlock()
		case <-done:
		}
	}()
	defer close(done)

	for !buffer.closed.Load() && !buffer.isPositionAvailableLocked(position) {
		// Abort on context cancellation
		if ctx.Err() != nil {
			return false
		}

		// If the requested position has been overwritten already, it can never become available again.
		normalizedPosition := buffer.getNormalizedPosition(position)
		if normalizedPosition < buffer.lastReadPosition {
			return false
		}
		// If EOF reached and the requested position is beyond last written, it will never appear.
		if buffer.eof.Load() && normalizedPosition > buffer.lastWritePosition {
			return false
		}

		// Wait for more data or state change.
		buffer.available.Wait()
	}

	if buffer.closed.Load() {
		return false
	}

	return buffer.isPositionAvailableLocked(position)
}

func (buffer *LockingRingBuffer) ResetToPosition(position int64) {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()

	buffer.startPosition = position
	buffer.lastReadPosition = 0
	buffer.lastWritePosition = 0
	buffer.count.Store(0)
	buffer.eof.Store(false)

	// Clear buffer data
	for i := range buffer.data {
		buffer.data[i] = 0
	}

	buffer.notFull.Broadcast()
	buffer.available.Broadcast()
}

func (buffer *LockingRingBuffer) Close() error {
	buffer.closed.Store(true)

	buffer.mu.Lock()
	defer buffer.mu.Unlock()

	buffer.data = nil

	buffer.notFull.Broadcast()
	buffer.available.Broadcast()

	return nil
}
