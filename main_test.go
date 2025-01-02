package ring_buffer

import (
    "context"
    "testing"
    "time"
)

func TestLockingRingBuffer(t *testing.T) {
    // Helper function to create a new buffer
    newBuffer := func(size uint64, startPosition uint64) *LockingRingBuffer {
        return NewLockingRingBuffer(context.Background(), size, startPosition)
    }

    // Test: Basic Write and Read
    t.Run("Basic Write and Read", func(t *testing.T) {
        buffer := newBuffer(10, 0)

        data := []byte("hello")
        n, err := buffer.Write(data)
        if err != nil || n != len(data) {
            t.Fatalf("expected to write %d bytes, wrote %d, error: %v", len(data), n, err)
        }

        readBuf := make([]byte, len(data))
        n, err = buffer.ReadAt(readBuf, 0)
        if err != nil || n != len(data) || string(readBuf) != "hello" {
            t.Fatalf("expected to read %d bytes, read %d, error: %v, data: %s", len(data), n, err, string(readBuf))
        }
    })

    // Test: Write and Read with Wrap Around
    t.Run("Write and Read with Wrap Around", func(t *testing.T) {
        buffer := newBuffer(5, 0)

        data := []byte("abcdef")
        n, err := buffer.Write(data[:4])
        if err != nil || n != 4 {
            t.Fatalf("expected to write 4 bytes, wrote %d, error: %v", n, err)
        }

        readBuf := make([]byte, 2)
        n, err = buffer.ReadAt(readBuf, 0)
        if err != nil || n != 2 || string(readBuf) != "ab" {
            t.Fatalf("expected to read 2 bytes, read %d, error: %v, data: %s", n, err, string(readBuf))
        }

        n, err = buffer.Write(data[4:])
        if err != nil || n != 2 {
            t.Fatalf("expected to write 2 bytes, wrote %d, error: %v", n, err)
        }

        readBuf = make([]byte, 4)
        n, err = buffer.ReadAt(readBuf, 2)
        if err != nil || n != 4 || string(readBuf) != "cdef" {
            t.Fatalf("expected to read 4 bytes, read %d, error: %v, data: %s", n, err, string(readBuf))
        }
    })

    // Test: Buffer Overwrite Prevention
    t.Run("Buffer Overwrite Prevention", func(t *testing.T) {
        buffer := newBuffer(5, 0)

        data := []byte("abcde")
        n, err := buffer.Write(data)
        if err != nil || n != 5 {
            t.Fatalf("expected to write 5 bytes, wrote %d, error: %v", n, err)
        }

        // Attempt to overwrite unread data
        data = []byte("fgh")
        n, err = buffer.Write(data)
        if err == nil || n != 0 {
            t.Fatalf("expected to prevent overwrite, wrote %d, error: %v", n, err)
        }
    })

    // Test: Buffer Empty
    t.Run("Buffer Empty", func(t *testing.T) {
        buffer := newBuffer(5, 0)

        readBuf := make([]byte, 5)
        n, err := buffer.ReadAt(readBuf, 0)
        if err == nil || n != 0 {
            t.Fatalf("expected to read 0 bytes, read %d, error: %v", n, err)
        }
    })

    // Test: Write Exceeding Buffer Size
    t.Run("Write Exceeding Buffer Size", func(t *testing.T) {
        buffer := newBuffer(5, 0)

        data := []byte("abcdef")
        n, err := buffer.Write(data)
        if err == nil || n != 0 {
            t.Fatalf("expected to write 0 bytes, wrote %d, error: %v", n, err)
        }
    })

    // Test: Reset Buffer
    t.Run("Reset Buffer", func(t *testing.T) {
        buffer := newBuffer(5, 0)

        data := []byte("abcde")
        n, err := buffer.Write(data)
        if err != nil || n != 5 {
            t.Fatalf("expected to write 5 bytes, wrote %d, error: %v", n, err)
        }

        buffer.ResetToPosition(10)

        readBuf := make([]byte, 5)
        n, err = buffer.ReadAt(readBuf, 10)
        if err == nil || n != 0 {
            t.Fatalf("expected to read 0 bytes after reset, read %d, error: %v", n, err)
        }
    })

    // Test: Wait for Position in Buffer
    t.Run("Wait for Position in Buffer", func(t *testing.T) {
        buffer := newBuffer(5, 0)

        data := []byte("abc")
        n, err := buffer.Write(data)
        if err != nil || n != 3 {
            t.Fatalf("expected to write 3 bytes, wrote %d, error: %v", n, err)
        }

        _, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
        defer cancel()

        go func() {
            time.Sleep(50 * time.Millisecond)
            buffer.Write([]byte("d"))
        }()

        buffer.WaitForPositionInBuffer(3)

        if !buffer.IsPositionInBuffer(3) {
            t.Fatalf("expected position 3 to be in buffer")
        }
    })

    // Test: Write and Read Across Multiple Resets
    t.Run("Write and Read Across Multiple Resets", func(t *testing.T) {
        buffer := newBuffer(5, 0)

        data := []byte("abcde")
        n, err := buffer.Write(data)
        if err != nil || n != 5 {
            t.Fatalf("expected to write 5 bytes, wrote %d, error: %v", n, err)
        }

        buffer.ResetToPosition(10)
        n, err = buffer.Write(data)
        if err != nil || n != 5 {
            t.Fatalf("expected to write 5 bytes after reset, wrote %d, error: %v", n, err)
        }

        readBuf := make([]byte, 5)
        n, err = buffer.ReadAt(readBuf, 10)
        if err != nil || n != 5 || string(readBuf) != string(data) {
            t.Fatalf("expected to read 5 bytes after reset, read %d, error: %v, data: %s", n, err, string(readBuf))
        }
    })

    // Test: Read From Out of Bounds Position
    t.Run("Read From Out of Bounds Position", func(t *testing.T) {
        buffer := newBuffer(5, 0)

        data := []byte("abcde")
        n, err := buffer.Write(data)
        if err != nil || n != 5 {
            t.Fatalf("expected to write 5 bytes, wrote %d, error: %v", n, err)
        }

        readBuf := make([]byte, 5)
        n, err = buffer.ReadAt(readBuf, 6)
        if err == nil || n != 0 {
            t.Fatalf("expected error for out of bounds read, read %d, error: %v", n, err)
        }
    })

    // Test: Context Cancellation
    t.Run("Context Cancellation", func(t *testing.T) {
        ctx, cancel := context.WithCancel(context.Background())
        buffer := NewLockingRingBuffer(ctx, 5, 0)

        go func() {
            time.Sleep(50 * time.Millisecond)
            cancel()
        }()

        buffer.WaitForPositionInBuffer(5)

        if buffer.IsPositionInBuffer(5) {
            t.Fatalf("expected position 5 not to be in buffer after context cancellation")
        }
    })
}
