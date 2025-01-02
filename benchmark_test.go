package ring_buffer_go

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func producer(buffer LockingRingBufferInterface, iterations int, data []byte, wg *sync.WaitGroup, totalBytes *int) {
	defer wg.Done()

	for i := 0; i < iterations; i++ {
		n, err := buffer.Write(data)
		if err != nil {
			continue
		}

		*totalBytes += n
	}
}

func consumer(buffer LockingRingBufferInterface, iterations int, dataSize int, wg *sync.WaitGroup, totalBytes *int) {
	defer wg.Done()

	p := make([]byte, dataSize)

	for i := 0; i < iterations; i++ {
		n, err := buffer.ReadAt(p, uint64(i*dataSize))
		if err != nil {
			continue
		}

		*totalBytes += n
	}
}

func benchmarkLockingRingBuffer(buffer LockingRingBufferInterface, iterations int, dataSize int) {
	var wg sync.WaitGroup

	data := make([]byte, dataSize)

	start := time.Now()

	var bytesWritten, bytesRead int

	wg.Add(2)
	go producer(buffer, iterations, data, &wg, &bytesWritten)
	go consumer(buffer, iterations, dataSize, &wg, &bytesRead)
	wg.Wait()

	elapsed := time.Since(start)
	throughput := float64(iterations) / elapsed.Seconds()

	writeGB := float64(bytesWritten) / (1 << 30)
	readGB := float64(bytesRead) / (1 << 30)

	writeGBPs := writeGB / elapsed.Seconds()
	readGBPs := readGB / elapsed.Seconds()

	fmt.Printf("Throughput: %.2f operations/sec\n", throughput)

	fmt.Printf("Write: %.2f GB/sec\n", writeGBPs)
	fmt.Printf("Read: %.2f GB/sec\n", readGBPs)
}

func BenchmarkLockingRingBuffer(b *testing.B) {
	iterations := b.N
	const dataSize = 1024
	const bufferSize = 1 << 30

	buffer := NewLockingRingBuffer(bufferSize, 0)

	// Benchmark the ring buffer
	benchmarkLockingRingBuffer(buffer, iterations, dataSize)
}
