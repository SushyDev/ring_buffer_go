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
	IsPositionInBuffer(absolutePosition uint64) bool

	// Waits until the given absolute position is in the buffer.
	WaitForPositionInBuffer(absolutePosition uint64)

	// Resets the buffer to the given absolute position.
	ResetToPosition(absolutePosition uint64)
}

var _ LockingRingBufferInterface = &LockingRingBuffer{}

type LockingRingBuffer struct {
	data          []byte
	startPosition atomic.Uint64

	readPosition  atomic.Uint64
	writePosition atomic.Uint64

	count atomic.Uint64

	readPage  atomic.Uint64
	writePage atomic.Uint64

	ctx context.Context
}

func NewLockingRingBuffer(ctx context.Context, size uint64, startPosition uint64) *LockingRingBuffer {
	buffer := &LockingRingBuffer{
		data: make([]byte, size),
		ctx:  ctx,
	}

	buffer.setStartPosition(startPosition)

	return buffer
}

func (buffer *LockingRingBuffer) cap() uint64 {
	return uint64(cap(buffer.data))
}

// Reads from the buffer at the given absolute position.
func (buffer *LockingRingBuffer) ReadAt(p []byte, absolutePosition uint64) (n int, err error) {
	if buffer.isClosed() {
		return 0, errors.New("buffer is closed")
	}

	bufferCount := buffer.count.Load()
	if bufferCount <= 0 {
		return 0, errors.New("buffer is empty")
	}

	if !buffer.IsPositionInBuffer(absolutePosition) {
		return 0, fmt.Errorf("position %d is not in buffer", absolutePosition)
	}

	bufferCap := buffer.cap()
	relativePosition := buffer.getRelativePosition(absolutePosition)
	bufferPosition := relativePosition % bufferCap

	readPosition := buffer.readPosition.Load()
	writePosition := buffer.writePosition.Load()

	requestedSize := uint64(len(p))

	var readSize uint64
	if bufferCount == bufferCap && readPosition == writePosition {
		readSize = min(requestedSize, bufferCap)
	} else if writePosition >= bufferPosition {
		readSize = min(requestedSize, writePosition-bufferPosition)
	} else {
		readSize = min(requestedSize, bufferCap-bufferPosition+writePosition)
	}

	if bufferPosition+readSize <= bufferCap {
		copy(p, buffer.data[bufferPosition:bufferPosition+readSize])
	} else {
		firstPart := bufferCap - bufferPosition
		copy(p, buffer.data[bufferPosition:bufferCap])
		copy(p[firstPart:], buffer.data[0:readSize-firstPart])
	}

	newReadPosition := (bufferPosition + readSize) % bufferCap
	if newReadPosition <= readPosition {
		buffer.readPage.Add(1)
	}

	buffer.readPosition.Store(newReadPosition)
	buffer.count.Store(bufferCount - readSize)

	return int(readSize), nil
}

// Appends data to the buffer and returns the number of bytes written.
func (buffer *LockingRingBuffer) Write(p []byte) (n int, err error) {
	if buffer.isClosed() {
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

	writePosition := buffer.writePosition.Load()

	if writePosition+requestedSize <= bufferCap {
		copy(buffer.data[writePosition:], p)
	} else {
		firstPart := bufferCap - writePosition
		copy(buffer.data[writePosition:], p[:firstPart])
		copy(buffer.data[0:], p[firstPart:])
	}

	newWritePosition := (writePosition + requestedSize) % bufferCap
	if newWritePosition <= writePosition {
		buffer.writePage.Add(1)
	}

	buffer.writePosition.Store(newWritePosition)
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

// Calculates if the given absolute position is in the buffer.
func (buffer *LockingRingBuffer) IsPositionInBuffer(absolutePosition uint64) bool {
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
	readPosition := buffer.readPosition.Load()
	writePage := buffer.writePage.Load()
	writePosition := buffer.writePosition.Load()

	if readPage == 0 && writePage == 0 && readPosition == 0 && writePosition == 0 {
		// Case 0: The buffer is empty.
		return false
	}

	if readPage == writePage {
		// Case 1: Same page, position must be between readPosition and writePosition.
		return bufferPosition >= readPosition && bufferPosition < writePosition
	}

	if bufferPositionPage == readPage {
		// Case 2: Position is on the read page.
		return bufferPosition >= readPosition
	}

	if bufferPositionPage == writePage {
		// Case 3: Position is on the write page.
		return bufferPosition < writePosition
	}

	// Case 4: Position is in between readPage and writePage when they are not the same.
	return readPage < writePage
}

// Waits until the given absolute position is in the buffer.
func (buffer *LockingRingBuffer) WaitForPositionInBuffer(absolutePosition uint64) {
	for {
		if buffer.IsPositionInBuffer(absolutePosition) {
			return
		}

		select {
		case <-buffer.ctx.Done():
			return
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
	buffer.writePosition.Store(0)
	buffer.readPage.Store(0)
	buffer.readPosition.Store(0)
	buffer.count.Store(0)
	buffer.data = make([]byte, buffer.cap())
}

func (buffer *LockingRingBuffer) isClosed() bool {
	select {
	case <-buffer.ctx.Done():
		return true
	default:
		return false
	}
}
