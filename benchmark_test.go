package ring_buffer_go

import (
	"context"
	"sync"
	"testing"
)

func benchmarkProducerConsumer(b *testing.B, bufferSize int64, dataSize int) {
	b.Helper()
	buffer := NewLockingRingBuffer(bufferSize, 0)
	defer buffer.Close()

	data := make([]byte, dataSize)
	b.SetBytes(int64(dataSize))
	b.ReportAllocs()
	b.ResetTimer()

	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Producer
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < b.N; i++ {
			_, err := buffer.Write(data)
			if err != nil {
				cancel()
				return
			}
		}
	}()

	// Consumer
	wg.Add(1)
	go func() {
		defer wg.Done()
		readBuffer := make([]byte, dataSize)
		for i := 0; i < b.N; i++ {
			pos := int64(i * dataSize)
			if !buffer.WaitForPosition(ctx, pos) {
				return // Exit if context is cancelled or buffer is closed
			}
			_, err := buffer.ReadAt(readBuffer, pos)
			if err != nil {
				return
			}
		}
	}()

	wg.Wait()
}

func BenchmarkRingBuffer(b *testing.B) {
	scenarios := []struct {
		name       string
		bufferSize int64
		dataSize   int
	}{
		{"1K_Buffer_64B_Data", 1 << 10, 64},
		{"4K_Buffer_64B_Data", 1 << 12, 64},
		{"4K_Buffer_1K_Data", 1 << 12, 1 << 10},
		{"1M_Buffer_1K_Data", 1 << 20, 1 << 10},
		{"1M_Buffer_64K_Data", 1 << 20, 1 << 16},
		{"16M_Buffer_64K_Data", 1 << 24, 1 << 16},
	}

	for _, s := range scenarios {
		b.Run(s.name, func(b *testing.B) {
			benchmarkProducerConsumer(b, s.bufferSize, s.dataSize)
		})
	}
}
