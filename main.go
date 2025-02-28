package ring_buffer_go

import (
	"bytes"
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

	count int64
	mu    sync.Mutex

	notFull *sync.Cond

	eof    atomic.Bool
	closed atomic.Bool
}

var EOFMarker = []byte("__EOF__")

// Calculates the nearest bigger power of 2 for the given size.
func calculateBufferSize(size int64) int64 { 
	// Check if already a power of two
	if size > 0 && (size & (size - 1)) == 0 {
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

	mu := sync.Mutex{}
	notFull := sync.NewCond(&mu)

	return &LockingRingBuffer{
		data:          make([]byte, bufferSize),
		startPosition: startPosition,

		lastReadPosition:  0,
		lastWritePosition: 0,

		count: 0,
		mu: mu,

		notFull:  notFull,

		eof:    atomic.Bool{},
		closed: atomic.Bool{},
	}
}

func (buffer *LockingRingBuffer) cap() int64 {
	return int64(cap(buffer.data))
}

func (buffer *LockingRingBuffer) getNormalizedPosition(position int64) int64 {
	return position - buffer.startPosition
}

func (buffer *LockingRingBuffer) getBufferPosition(normalizedPosition int64) int64 {
	cap := buffer.cap()

	return normalizedPosition & (cap - 1)
}

func (buffer *LockingRingBuffer) IsPositionAvailable(position int64) bool {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()

	if buffer.count == 0 {
		return false
	}

	normalizedPosition := buffer.getNormalizedPosition(position)

	if normalizedPosition < 0 {
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

	if bytes.Equal(p, EOFMarker) {
		buffer.eof.Store(true)
		return 0, nil
	}

	bufferCap := buffer.cap()
	requestedSize := int64(len(p))

	if requestedSize > bufferCap {
		return 0, fmt.Errorf("Write data exceeds buffer size: %d", requestedSize)
	}

	for requestedSize > buffer.GetBytesToOverwrite() {
		if buffer.closed.Load() {
			return 0, errors.New("Buffer is closed")
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
	buffer.count += int64(bytesWritten)

	return bytesWritten, nil
}

func (buffer *LockingRingBuffer) ReadAt(p []byte, position int64) (int, error) {
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

	buffer.notFull.Signal()

	var err error
	if bytesRead == 0 || buffer.eof.Load() {
		err = io.EOF
	}

	return bytesRead, err
}

func (buffer *LockingRingBuffer) GetBytesToOverwrite() int64 {
	count := atomic.LoadInt64(&buffer.count)

	return buffer.cap() - count
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
		case <-time.After(200 * time.Millisecond):
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
	buffer.eof.Store(false)

	for i := range buffer.data {
		buffer.data[i] = 0
	}
}

func (buffer *LockingRingBuffer) Close() error {
	buffer.closed.Store(true)

	buffer.mu.Lock()
	defer buffer.mu.Unlock()

	buffer.notFull.Broadcast()

	buffer.data = nil
	return nil
}
