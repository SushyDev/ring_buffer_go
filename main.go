package ring_buffer_go

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"
)

// LockingRingBufferInterface is an interface for a locking ring buffer.
// Features:
// - Fixed size buffer.
// - Unread data cannot be overwritten.
// - Read from absolute position.
// - Absolute positions get checked if they are in the buffer.

type LockingRingBufferInterface interface {
	// Reads from the buffer at the given absolute position.
	ReadAt(p []byte, absolutePosition uint64) (n int, err error)

	// Appends data to the buffer and returns the number of bytes written.
	Write(p []byte) (n int, err error)

	// Returns the number of bytes that can be written to the buffer without overwriting any data.
	GetBytesToOverwrite() uint64

	// Calculates if the given absolute position is in the buffer.
	IsPositionAvailable(absolutePosition uint64) bool

	// Waits until the given absolute position is in the buffer.
	WaitForPosition(ctx context.Context, absolutePosition uint64) bool

	// Resets the buffer to the given absolute position.
	ResetToPosition(absolutePosition uint64)
}

var _ LockingRingBufferInterface = &LockingRingBuffer{}

type LockingRingBuffer struct {
	data          []byte
	startPosition atomic.Uint64

	lastReadPosition  atomic.Uint64
	lastWritePosition atomic.Uint64

	count atomic.Uint64

	readPage  atomic.Uint64
	writePage atomic.Uint64

	closed atomic.Bool
}

func NewLockingRingBuffer(size uint64, startPosition uint64) *LockingRingBuffer {
	buffer := &LockingRingBuffer{
		data: make([]byte, size),
	}

	buffer.setStartPosition(startPosition)

	return buffer
}

func (buffer *LockingRingBuffer) cap() uint64 {
	return uint64(cap(buffer.data))
}

// Reads from the buffer at the given absolute position.
func (buffer *LockingRingBuffer) ReadAt(p []byte, absolutePosition uint64) (n int, err error) {
	if buffer.closed.Load() {
		return 0, errors.New("buffer is closed")
	}

	bufferCount := buffer.count.Load()
	if bufferCount <= 0 {
		return 0, errors.New("buffer is empty")
	}

	if !buffer.IsPositionAvailable(absolutePosition) {
		return 0, fmt.Errorf("position %d is not in buffer", absolutePosition)
	}

	bufferCap := buffer.cap()
	relativePosition := buffer.getRelativePosition(absolutePosition)
	bufferPosition := relativePosition % bufferCap

	lastReadPosition := buffer.lastReadPosition.Load()
	lastWritePosition := buffer.lastWritePosition.Load()

	requestedSize := uint64(len(p))

	var readSize uint64
	if bufferCount == bufferCap && lastReadPosition == lastWritePosition {
		readSize = min(requestedSize, bufferCap)
	} else if lastWritePosition >= bufferPosition {
		readSize = min(requestedSize, lastWritePosition-bufferPosition)
	} else {
		readSize = min(requestedSize, bufferCap-bufferPosition+lastWritePosition)
	}

	if bufferPosition+readSize <= bufferCap {
		copy(p, buffer.data[bufferPosition:bufferPosition+readSize])
	} else {
		firstPart := bufferCap - bufferPosition
		copy(p, buffer.data[bufferPosition:bufferCap])
		copy(p[firstPart:], buffer.data[0:readSize-firstPart])
	}

	newReadPosition := (bufferPosition + readSize) % bufferCap
	if newReadPosition <= lastReadPosition {
		buffer.readPage.Add(1)
	}

	buffer.lastReadPosition.Store(newReadPosition)
	buffer.count.Store(bufferCount - readSize)

	return int(readSize), nil
}

// Appends data to the buffer and returns the number of bytes written.
func (buffer *LockingRingBuffer) Write(p []byte) (n int, err error) {
	if buffer.closed.Load() {
		return 0, errors.New("buffer is closed")
	}

	bufferCap := buffer.cap()

	requestedSize := uint64(len(p))
	if requestedSize > bufferCap {
		return 0, fmt.Errorf("write data exceeds buffer size: %d", requestedSize)
	}

	availableSpace := buffer.GetBytesToOverwrite()
	if requestedSize > availableSpace {
		return 0, fmt.Errorf("not enough space in buffer: %d/%d", requestedSize, availableSpace)
	}

	lastWritePosition := buffer.lastWritePosition.Load()

	if lastWritePosition+requestedSize <= bufferCap {
		copy(buffer.data[lastWritePosition:], p)
	} else {
		firstPart := bufferCap - lastWritePosition
		copy(buffer.data[lastWritePosition:], p[:firstPart])
		copy(buffer.data[0:], p[firstPart:])
	}

	newWritePosition := (lastWritePosition + requestedSize) % bufferCap
	if newWritePosition <= lastWritePosition {
		buffer.writePage.Add(1)
	}

	buffer.lastWritePosition.Store(newWritePosition)
	buffer.count.Add(requestedSize)

	return int(requestedSize), nil
}

func (buffer *LockingRingBuffer) setStartPosition(absolutePosition uint64) {
	buffer.startPosition.Store(absolutePosition)
}

func (buffer *LockingRingBuffer) getStartPosition() uint64 {
	return buffer.startPosition.Load()
}

func (buffer *LockingRingBuffer) getRelativePosition(absolutePosition uint64) uint64 {
	return absolutePosition - buffer.startPosition.Load()
}

// Calculates if the given absolute position is in the locking ring buffer.
func (buffer *LockingRingBuffer) IsPositionAvailable(absolutePosition uint64) bool {
	relativePosition := buffer.getRelativePosition(absolutePosition)
	if relativePosition < 0 {
		return false
	}

	bufferCap := buffer.cap()
	if bufferCap == 0 {
		return false
	}

	bufferPosition := relativePosition % bufferCap
	bufferPositionPage := relativePosition / bufferCap

	readPage := buffer.readPage.Load()
	writePage := buffer.writePage.Load()

	lastReadPosition := buffer.lastReadPosition.Load()
	lastWritePosition := buffer.lastWritePosition.Load()

	if readPage == 0 && writePage == 0 && lastReadPosition == 0 && lastWritePosition == 0 {
		// Case 0: The buffer is empty.
		return false
	}

	if readPage == writePage {
		// Case 1: Same page, position must be between readPosition and writePosition.
		return bufferPosition >= lastReadPosition && bufferPosition < lastWritePosition
	}

	if bufferPositionPage == readPage {
		// Case 2: Position is on the read page.
		return bufferPosition >= lastReadPosition
	}

	if bufferPositionPage == writePage {
		// Case 3: Position is on the write page.
		return bufferPosition < lastWritePosition
	}

	// Case 4: Position is in between readPage and writePage when they are not the same.
	return readPage < writePage
}

// Waits until the given absolute position is in the buffer.
func (buffer *LockingRingBuffer) WaitForPosition(ctx context.Context, absolutePosition uint64) bool {
	for {
		if buffer.closed.Load() {
			return false
		}

		if buffer.IsPositionAvailable(absolutePosition) {
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

// Returns the number of bytes that can be written to the buffer without overwriting any data.
func (buffer *LockingRingBuffer) GetBytesToOverwrite() uint64 {
	bufferCap := buffer.cap()
	bufferCount := buffer.count.Load()

	return bufferCap - bufferCount
}

// Resets the buffer to the given absolute position.
func (buffer *LockingRingBuffer) ResetToPosition(absolutePosition uint64) {
	buffer.setStartPosition(absolutePosition)
	buffer.writePage.Store(0)
	buffer.lastWritePosition.Store(0)
	buffer.readPage.Store(0)
	buffer.lastReadPosition.Store(0)
	buffer.count.Store(0)
	buffer.data = make([]byte, buffer.cap())
}

func (buffer *LockingRingBuffer) Close() {
	buffer.closed.Store(true)
	buffer.data = nil
}

