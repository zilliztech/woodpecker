package pool

import (
	"fmt"
	"testing"
)

func TestGetByteBuffer(t *testing.T) {
	tests := []struct {
		name          string
		size          int
		expectedCap   int
		expectedEmpty bool
	}{
		// Very small buffer tests
		{
			name:          "Get buffer for 2KB",
			size:          2 * 1024,
			expectedCap:   4 * 1024, // Should return 4KB buffer
			expectedEmpty: true,
		},
		{
			name:          "Get buffer for 4KB exactly",
			size:          4 * 1024,
			expectedCap:   4 * 1024, // Should return 4KB buffer
			expectedEmpty: true,
		},
		{
			name:          "Get buffer for 6KB",
			size:          6 * 1024,
			expectedCap:   8 * 1024, // Should return 8KB buffer
			expectedEmpty: true,
		},
		{
			name:          "Get buffer for 8KB exactly",
			size:          8 * 1024,
			expectedCap:   8 * 1024, // Should return 8KB buffer
			expectedEmpty: true,
		},
		{
			name:          "Get buffer for 12KB",
			size:          12 * 1024,
			expectedCap:   16 * 1024, // Should return 16KB buffer
			expectedEmpty: true,
		},
		{
			name:          "Get buffer for 16KB exactly",
			size:          16 * 1024,
			expectedCap:   16 * 1024, // Should return 16KB buffer
			expectedEmpty: true,
		},
		// Small buffer tests
		{
			name:          "Get buffer for 24KB",
			size:          24 * 1024,
			expectedCap:   32 * 1024, // Should return 32KB buffer
			expectedEmpty: true,
		},
		{
			name:          "Get buffer for 32KB exactly",
			size:          32 * 1024,
			expectedCap:   32 * 1024, // Should return 32KB buffer
			expectedEmpty: true,
		},
		{
			name:          "Get buffer for 48KB",
			size:          48 * 1024,
			expectedCap:   64 * 1024, // Should return 64KB buffer
			expectedEmpty: true,
		},
		{
			name:          "Get buffer for 100KB",
			size:          100 * 1024,
			expectedCap:   128 * 1024, // Should return 128KB buffer
			expectedEmpty: true,
		},
		{
			name:          "Get buffer for 200KB",
			size:          200 * 1024,
			expectedCap:   256 * 1024, // Should return 256KB buffer
			expectedEmpty: true,
		},
		{
			name:          "Get buffer for 400KB",
			size:          400 * 1024,
			expectedCap:   512 * 1024, // Should return 512KB buffer
			expectedEmpty: true,
		},
		// Medium buffer tests
		{
			name:          "Get buffer for 600KB",
			size:          600 * 1024,
			expectedCap:   1024 * 1024, // Should return 1MB buffer
			expectedEmpty: true,
		},
		{
			name:          "Get buffer for 1.5MB",
			size:          1536 * 1024,
			expectedCap:   2 * 1024 * 1024, // Should return 2MB buffer
			expectedEmpty: true,
		},
		{
			name:          "Get buffer for 3MB",
			size:          3 * 1024 * 1024,
			expectedCap:   4 * 1024 * 1024, // Should return 4MB buffer
			expectedEmpty: true,
		},
		// Larger buffer tests
		{
			name:          "Get buffer for 1MB",
			size:          1 * 1024 * 1024,
			expectedCap:   1 * 1024 * 1024, // Should return 1MB buffer
			expectedEmpty: true,
		},
		{
			name:          "Get buffer for 8MB exactly",
			size:          8 * 1024 * 1024,
			expectedCap:   8 * 1024 * 1024,
			expectedEmpty: true,
		},
		{
			name:          "Get buffer for 9MB",
			size:          9 * 1024 * 1024,
			expectedCap:   16 * 1024 * 1024, // Should return 16MB buffer
			expectedEmpty: true,
		},
		{
			name:          "Get buffer for large size (2GB)",
			size:          2 * 1024 * 1024 * 1024,
			expectedCap:   2 * 1024 * 1024 * 1024, // Should create exact size buffer
			expectedEmpty: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buffer := GetByteBuffer(tt.size)

			// Check capacity
			if cap(buffer) != tt.expectedCap {
				t.Errorf("GetByteBuffer(%d) capacity = %d, want %d",
					tt.size, cap(buffer), tt.expectedCap)
			}

			// Check length
			if len(buffer) != 0 && tt.expectedEmpty {
				t.Errorf("GetByteBuffer(%d) length = %d, want 0",
					tt.size, len(buffer))
			}

			// Verify we can actually write to the buffer
			if len(buffer) == 0 {
				// Write some data to ensure the buffer is usable
				testData := []byte("test data")
				buffer = append(buffer, testData...)

				if len(buffer) != len(testData) {
					t.Errorf("After append, buffer length = %d, want %d",
						len(buffer), len(testData))
				}
			}

			// Return buffer to pool when done
			PutByteBuffer(buffer)
		})
	}
}

func TestPutByteBuffer(t *testing.T) {
	tests := []struct {
		name       string
		bufferSize int
		shouldPool bool
	}{
		// Very small buffer tests
		{
			name:       "Put buffer with standard size (4KB)",
			bufferSize: 4 * 1024,
			shouldPool: true,
		},
		{
			name:       "Put buffer with standard size (8KB)",
			bufferSize: 8 * 1024,
			shouldPool: true,
		},
		{
			name:       "Put buffer with standard size (16KB)",
			bufferSize: 16 * 1024,
			shouldPool: true,
		},
		// Small buffer tests
		{
			name:       "Put buffer with standard size (32KB)",
			bufferSize: 32 * 1024,
			shouldPool: true,
		},
		{
			name:       "Put buffer with standard size (128KB)",
			bufferSize: 128 * 1024,
			shouldPool: true,
		},
		{
			name:       "Put buffer with standard size (512KB)",
			bufferSize: 512 * 1024,
			shouldPool: true,
		},
		// Medium buffer tests
		{
			name:       "Put buffer with standard size (1MB)",
			bufferSize: 1 * 1024 * 1024,
			shouldPool: true,
		},
		{
			name:       "Put buffer with standard size (4MB)",
			bufferSize: 4 * 1024 * 1024,
			shouldPool: true,
		},
		// Larger buffer tests
		{
			name:       "Put buffer with standard size (8MB)",
			bufferSize: 8 * 1024 * 1024,
			shouldPool: true,
		},
		{
			name:       "Put buffer with standard size (64MB)",
			bufferSize: 64 * 1024 * 1024,
			shouldPool: true,
		},
		// Non-standard size tests
		{
			name:       "Put buffer too small for pooling (2KB)",
			bufferSize: 2 * 1024,
			shouldPool: false,
		},
		{
			name:       "Put buffer with non-standard size (6KB)",
			bufferSize: 6 * 1024,
			shouldPool: false,
		},
		{
			name:       "Put buffer with non-standard size (48KB)",
			bufferSize: 48 * 1024,
			shouldPool: false,
		},
		{
			name:       "Put buffer with non-standard size (3MB)",
			bufferSize: 3 * 1024 * 1024,
			shouldPool: false,
		},
		{
			name:       "Put buffer with non-standard size (10MB)",
			bufferSize: 10 * 1024 * 1024,
			shouldPool: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// For standard sizes, we can verify indirectly by getting a buffer,
			// putting it back, and then getting another one to see if it's reused
			if tt.shouldPool {
				// Get a buffer and fill it with a marker value
				buffer1 := GetByteBuffer(tt.bufferSize)
				originalCap := cap(buffer1)

				// Fill with a specific pattern
				for i := 0; i < 100; i++ {
					buffer1 = append(buffer1, 0xAA)
				}

				// Return to pool
				PutByteBuffer(buffer1)

				// Get another buffer of the same size
				buffer2 := GetByteBuffer(tt.bufferSize)

				// Verify it has the expected capacity
				if cap(buffer2) != originalCap {
					t.Errorf("After PutByteBuffer and GetByteBuffer, cap = %d, want %d",
						cap(buffer2), originalCap)
				}

				// The length should be reset to 0
				if len(buffer2) != 0 {
					t.Errorf("Reused buffer length = %d, want 0", len(buffer2))
				}
			} else {
				// For non-standard sizes, we're essentially testing that the code doesn't crash
				// Create a buffer with the specified size
				buffer := make([]byte, 0, tt.bufferSize)

				// Try to put it back
				PutByteBuffer(buffer)
				// If we reach here without panicking, the test passes
			}
		})
	}
}

func TestReuseBufferContents(t *testing.T) {
	// Test with different sizes
	sizes := []int{
		4 * 1024,         // 4KB
		16 * 1024,        // 16KB
		32 * 1024,        // 32KB
		256 * 1024,       // 256KB
		1 * 1024 * 1024,  // 1MB
		16 * 1024 * 1024, // 16MB
	}

	for _, size := range sizes {
		t.Run(fmt.Sprintf("Size_%s", formatSize(size)), func(t *testing.T) {
			// Get a buffer, fill it with data
			buffer1 := GetByteBuffer(size)
			testData := []byte("test data for buffer reuse")
			buffer1 = append(buffer1, testData...)

			// Return it to the pool
			PutByteBuffer(buffer1)

			// Get another buffer of the same size
			buffer2 := GetByteBuffer(size)

			// Verify it has the correct capacity and is empty
			if cap(buffer2) != size {
				t.Errorf("Reused buffer capacity = %d, want %d",
					cap(buffer2), size)
			}

			if len(buffer2) != 0 {
				t.Errorf("Reused buffer should be empty, got length %d", len(buffer2))
			}

			// Write new data and verify integrity
			newData := []byte("new data for reused buffer")
			buffer2 = append(buffer2, newData...)

			if len(buffer2) != len(newData) {
				t.Errorf("After append to reused buffer, length = %d, want %d",
					len(buffer2), len(newData))
			}

			for i, b := range newData {
				if buffer2[i] != b {
					t.Errorf("Data corruption in reused buffer at index %d: got %d, want %d",
						i, buffer2[i], b)
				}
			}

			// Clean up
			PutByteBuffer(buffer2)
		})
	}
}

// ----------------------------------------------------------------------------
// ----- go test ./server/storage/cache  -bench=Benchmark -benchmem    -----
// ----------------------------------------------------------------------------

func BenchmarkGetPutByteBuffer(b *testing.B) {
	sizes := []int{
		4 * 1024,          // 4KB
		16 * 1024,         // 16KB
		32 * 1024,         // 32KB
		128 * 1024,        // 128KB
		512 * 1024,        // 512KB
		1 * 1024 * 1024,   // 1MB
		4 * 1024 * 1024,   // 4MB
		16 * 1024 * 1024,  // 16MB
		64 * 1024 * 1024,  // 64MB
		512 * 1024 * 1024, // 512MB
	}

	for _, size := range sizes {
		b.Run(formatSize(size), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				buffer := GetByteBuffer(size)
				// Do a minimal write operation
				buffer = append(buffer, 1, 2, 3, 4)
				PutByteBuffer(buffer)
			}
		})
	}
}

// BenchmarkWithoutPool provides a comparison point for allocation without pooling
func BenchmarkWithoutPool(b *testing.B) {
	sizes := []int{
		4 * 1024,          // 4KB
		16 * 1024,         // 16KB
		32 * 1024,         // 32KB
		128 * 1024,        // 128KB
		512 * 1024,        // 512KB
		1 * 1024 * 1024,   // 1MB
		4 * 1024 * 1024,   // 4MB
		16 * 1024 * 1024,  // 16MB
		64 * 1024 * 1024,  // 64MB
		512 * 1024 * 1024, // 512MB
	}

	for _, size := range sizes {
		b.Run(formatSize(size), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				buffer := make([]byte, 0, size)
				// Do the same minimal write operation
				buffer = append(buffer, 1, 2, 3, 4)
				// No pool return - let GC handle it
				_ = buffer
			}
		})
	}
}

// Helper function to format size for benchmark names
func formatSize(bytes int) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}
