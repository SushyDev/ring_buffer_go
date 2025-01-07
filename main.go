package ring_buffer_go

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"
)

type LockingRingBufferInterface interface {
	// Reads from the buffer at the given absolute position.
	ReadAt(p []byte, position int64) (n int, err error)

	// Appends data to the buffer and returns the number of bytes written.
	Write(p []byte) (n int, err error)

	// Returns the number of bytes that can be written to the buffer without overwriting any data.
	GetBytesToOverwrite() int64

	// Calculates if the given absolute position is in the buffer.
	IsPositionAvailable(position int64) bool

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

type LockingRingBuffer struct {
	data          []byte
	startPosition int64

	lastReadPosition  int64 // Stores normalized position (page * cap + position)
	lastWritePosition int64 // Stores normalized position (page * cap + position)

	count  int64
	mu     sync.Mutex
	closed atomic.Bool
}

func NewLockingRingBuffer(size int64, startPosition int64) *LockingRingBuffer {
	return &LockingRingBuffer{
		data:          make([]byte, size),
		startPosition: startPosition,
	}
}

func (buffer *LockingRingBuffer) cap() int64 {
	return int64(cap(buffer.data))
}

func (buffer *LockingRingBuffer) getNormalizedPosition(position int64) int64 {
	return position - buffer.startPosition
}

func (buffer *LockingRingBuffer) getBufferPosition(normalizedPosition int64) int64 {
	// Handle negative positions correctly
	cap := buffer.cap()
	pos := normalizedPosition % cap
	if pos < 0 {
		pos += cap
	}
	return pos
}

func (buffer *LockingRingBuffer) IsPositionAvailable(position int64) bool {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()

	normalizedPosition := buffer.getNormalizedPosition(position)
	if normalizedPosition < 0 {
		return false
	}

	if buffer.count == 0 {
		return false
	}

	return normalizedPosition >= buffer.lastReadPosition && normalizedPosition <= buffer.lastWritePosition
}

func (buffer *LockingRingBuffer) Write(p []byte) (n int, err error) {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()

	if buffer.closed.Load() {
		return 0, context.Canceled
	}

	bufferCap := buffer.cap()
	requestedSize := int64(len(p))

	if requestedSize > bufferCap {
		return 0, fmt.Errorf("Write data exceeds buffer size: %d", requestedSize)
	}

	for {
		availableSpace := buffer.GetBytesToOverwrite()
		if requestedSize <= availableSpace {
			break
		}

		if buffer.closed.Load() {
			return 0, errors.New("Buffer is closed")
		}

		buffer.mu.Unlock()
		time.Sleep(250 * time.Millisecond)
		buffer.mu.Lock()
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
	buffer.count += int64(bytesWritten)

	return bytesWritten, nil
}

func (buffer *LockingRingBuffer) ReadAt(p []byte, position int64) (n int, err error) {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()

	if buffer.closed.Load() {
		return 0, errors.New("Buffer is closed")
	}

	if buffer.count <= 0 {
		return 0, errors.New("Buffer is empty")
	}

	normalizedPosition := buffer.getNormalizedPosition(position)
	if normalizedPosition < buffer.lastReadPosition || normalizedPosition > buffer.lastWritePosition {
		return 0, fmt.Errorf("Position %d is not in buffer", position)
	}

	bufferPosition := buffer.getBufferPosition(normalizedPosition)
	bufferCap := buffer.cap()
	requestedSize := int64(len(p))

	var readSize int64
	if buffer.lastWritePosition-normalizedPosition >= requestedSize {
		readSize = requestedSize
	} else {
		readSize = buffer.lastWritePosition - normalizedPosition
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
	buffer.count -= int64(bytesRead)

	return bytesRead, nil
}

func (buffer *LockingRingBuffer) GetBytesToOverwrite() int64 {
	return buffer.cap() - buffer.count
}

func (buffer *LockingRingBuffer) WaitForPosition(ctx context.Context, position int64) bool {
	for {
		if buffer.closed.Load() {
			return false
		}

		if buffer.IsPositionAvailable(position) {
			return true
		}

		select {
		case <-ctx.Done():
			return false
		default:
		case <-time.After(100 * time.Microsecond):
		}
	}
}

func (buffer *LockingRingBuffer) ResetToPosition(position int64) {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()

	buffer.startPosition = position
	buffer.lastReadPosition = 0
	buffer.lastWritePosition = 0
	buffer.count = 0
	buffer.data = make([]byte, buffer.cap())
}

func (buffer *LockingRingBuffer) Close() error {
	buffer.closed.Store(true)
	buffer.mu.Lock()
	defer buffer.mu.Unlock()
	buffer.data = nil
	return nil
}
