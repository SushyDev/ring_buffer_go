package ring_buffer_go

import (
	"context"
	"errors"
	"io"
)

// LockingRingBufferInterface defines the public API for the ring buffer.
//
// Absolute positions are measured from startPosition; internally the buffer
// operates on normalized offsets (position - startPosition). The buffer stores
// normalized read/write cursors that monotonically increase as data is read and
// written.
//
// Notes on semantics:
//   - IsPositionAvailable reports whether a position lies inside the current
//     readable window [earliest, lastWritePosition], with an inclusive upper
//     bound at the write boundary (the boundary itself is considered
//     "available").
//   - ReadAt advances the shared read cursor; attempts to read behind the
//     cursor return ErrOutOfRange, and attempts to read beyond the last written
//     position return io.EOF.
//   - IsPositionInCapacity indicates whether a position is within a window that
//     can be represented by the current buffer capacity, allowing a tolerance.
//     It does not assert that the data is currently available to read.
//   - Write blocks when there is insufficient space (to avoid overwrite) and
//     unblocks when readers advance.
//   - Writing EOFMarker marks logical end-of-stream and causes subsequent reads
//     to return io.EOF once the available bytes are consumed or when fewer bytes
//     than requested are available.
//
// All methods are safe for concurrent use.
type LockingRingBufferInterface interface {
	GetCapacity() int64
	GetSize() int64
	GetStartPosition() int64
	ReadAt(p []byte, position int64) (n int, err error)
	Write(p []byte) (n int, err error)
	GetBytesToOverwrite() int64
	IsPositionAvailable(position int64) bool
	IsPositionInCapacity(position int64, tolerance int64) bool
	WaitForPosition(ctx context.Context, position int64) bool
	ResetToPosition(position int64)
	Close() error
}

var _ LockingRingBufferInterface = &LockingRingBuffer{}
var _ io.WriteCloser = &LockingRingBuffer{}
var _ io.ReaderAt = &LockingRingBuffer{}

// ErrOutOfRange indicates the requested position is no longer available in the
// buffer window (it is behind the read cursor and cannot be re-read).
var ErrOutOfRange = errors.New("ringbuffer: position out of range")
