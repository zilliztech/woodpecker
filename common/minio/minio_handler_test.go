package minio

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
)

// mockReader is a simple ObjectReader implementation for testing
type mockReader struct {
	data   []byte
	offset int
	err    error
}

func (m *mockReader) Read(p []byte) (n int, err error) {
	if m.err != nil {
		return 0, m.err
	}

	if m.offset >= len(m.data) {
		return 0, io.EOF
	}

	n = copy(p, m.data[m.offset:])
	m.offset += n

	if m.offset >= len(m.data) {
		err = io.EOF
	}

	return
}

func (m *mockReader) Close() error {
	return nil
}

// customSizeReader is a wrapper that limits read size
type customSizeReader struct {
	reader      *mockReader
	maxReadSize int
}

func (c *customSizeReader) Read(p []byte) (n int, err error) {
	// Limit the size of each read
	if len(p) > c.maxReadSize {
		p = p[:c.maxReadSize]
	}
	return c.reader.Read(p)
}

func (c *customSizeReader) Close() error {
	return c.reader.Close()
}

func TestReadObjectFull(t *testing.T) {
	tests := []struct {
		name     string
		input    []byte
		readSize int // Size of each read block
		wantErr  bool
	}{
		{
			name:     "Empty data",
			input:    []byte{},
			readSize: 1024,
			wantErr:  false,
		},
		{
			name:     "Small data read at once",
			input:    []byte("hello world"),
			readSize: 1024,
			wantErr:  false,
		},
		{
			name:     "Large data read in chunks",
			input:    bytes.Repeat([]byte("abcdefgh"), 1000), // 8KB
			readSize: 1024,                                   // Read 1KB each time, requires multiple reads
			wantErr:  false,
		},
		{
			name:     "Very small block read",
			input:    bytes.Repeat([]byte("xyz"), 100), // 300 bytes
			readSize: 7,                                // Not a multiple of 8, testing edge cases
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create base reader
			baseReader := &mockReader{
				data: tt.input,
			}

			// Use wrapper to limit read size
			customReader := &customSizeReader{
				reader:      baseReader,
				maxReadSize: tt.readSize,
			}

			// Call the function being tested
			got, err := ReadObjectFull(customReader, 1024*1024)

			// Verify results
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.input, got, "Read data does not match input")
			}
		})
	}

	// Test read error
	t.Run("Read error", func(t *testing.T) {
		r := &mockReader{
			err: io.ErrClosedPipe, // Simulate a read error
		}

		_, err := ReadObjectFull(r, 1024*1024)
		assert.Error(t, err)
		assert.Equal(t, io.ErrClosedPipe, err)
	})
}
