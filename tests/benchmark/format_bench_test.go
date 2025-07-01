package benchmark

import (
	"bytes"
	"fmt"
	"hash/crc32"
	"testing"

	"github.com/zilliztech/woodpecker/server/storage/codec"
)

// BenchmarkHeaderRecordEncode benchmarks HeaderRecord encoding
func BenchmarkHeaderRecordEncode(b *testing.B) {
	record := &codec.HeaderRecord{
		Version: codec.FormatVersion,
		Flags:   0x1234,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := codec.EncodeRecord(record)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkHeaderRecordDecode benchmarks HeaderRecord decoding
func BenchmarkHeaderRecordDecode(b *testing.B) {
	record := &codec.HeaderRecord{
		Version: codec.FormatVersion,
		Flags:   0x1234,
	}

	encoded, err := codec.EncodeRecord(record)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := codec.DecodeRecord(encoded)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkDataRecordEncode benchmarks DataRecord encoding with different payload sizes
func BenchmarkDataRecordEncode(b *testing.B) {
	sizes := []int{0, 64, 1024, 4096, 64 * 1024, 1024 * 1024, 2 * 1024 * 1024, 4 * 1024 * 1024, 8 * 1024 * 1024, 16 * 1024 * 1024}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			payload := make([]byte, size)
			for i := range payload {
				payload[i] = byte(i % 256)
			}

			record := &codec.DataRecord{Payload: payload}

			b.ResetTimer()
			b.SetBytes(int64(size))
			for i := 0; i < b.N; i++ {
				_, err := codec.EncodeRecord(record)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkDataRecordDecode benchmarks DataRecord decoding with different payload sizes
func BenchmarkDataRecordDecode(b *testing.B) {
	sizes := []int{0, 64, 1024, 4096, 64 * 1024, 1024 * 1024, 2 * 1024 * 1024, 4 * 1024 * 1024, 8 * 1024 * 1024, 16 * 1024 * 1024}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			payload := make([]byte, size)
			for i := range payload {
				payload[i] = byte(i % 256)
			}

			record := &codec.DataRecord{Payload: payload}
			encoded, err := codec.EncodeRecord(record)
			if err != nil {
				b.Fatal(err)
			}

			b.ResetTimer()
			b.SetBytes(int64(size))
			for i := 0; i < b.N; i++ {
				_, err := codec.DecodeRecord(encoded)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkIndexRecordEncode benchmarks IndexRecord encoding with different offset counts
func BenchmarkIndexRecordEncode(b *testing.B) {
	counts := []int{0, 10, 100, 1000, 10000, 100000}

	for _, count := range counts {
		b.Run(fmt.Sprintf("offsets_%d", count), func(b *testing.B) {
			offsets := make([]uint32, count)
			for i := range offsets {
				offsets[i] = uint32(i * 1000)
			}

			record := &codec.IndexRecord{
				Offsets: offsets,
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, err := codec.EncodeRecord(record)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkIndexRecordDecode benchmarks IndexRecord decoding with different offset counts
func BenchmarkIndexRecordDecode(b *testing.B) {
	counts := []int{0, 10, 100, 1000, 10000, 100000}

	for _, count := range counts {
		b.Run(fmt.Sprintf("offsets_%d", count), func(b *testing.B) {
			offsets := make([]uint32, count)
			for i := range offsets {
				offsets[i] = uint32(i * 1000)
			}

			record := &codec.IndexRecord{
				Offsets: offsets,
			}

			encoded, err := codec.EncodeRecord(record)
			if err != nil {
				b.Fatal(err)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, err := codec.DecodeRecord(encoded)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkFooterRecordEncode benchmarks FooterRecord encoding
func BenchmarkFooterRecordEncode(b *testing.B) {
	record := &codec.FooterRecord{
		IndexOffset: 98765,
		IndexLength: 432,
		Count:       123,
		Version:     codec.FormatVersion,
		Flags:       0x5678,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := codec.EncodeRecord(record)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkFooterRecordDecode benchmarks FooterRecord decoding
func BenchmarkFooterRecordDecode(b *testing.B) {
	record := &codec.FooterRecord{
		IndexOffset: 98765,
		IndexLength: 432,
		Count:       123,
		Version:     codec.FormatVersion,
		Flags:       0x5678,
	}

	encoded, err := codec.EncodeRecord(record)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := codec.DecodeRecord(encoded)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkCRCCalculation benchmarks CRC calculation for different data sizes
func BenchmarkCRCCalculation(b *testing.B) {
	sizes := []int{64, 1024, 4096, 64 * 1024, 1024 * 1024, 2 * 1024 * 1024, 4 * 1024 * 1024, 8 * 1024 * 1024, 16 * 1024 * 1024}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			data := bytes.Repeat([]byte("test"), size/4)

			b.ResetTimer()
			b.SetBytes(int64(len(data)))
			for i := 0; i < b.N; i++ {
				_ = crc32.ChecksumIEEE(data)
			}
		})
	}
}

// BenchmarkRecordRoundTrip benchmarks complete encode/decode round trip
func BenchmarkRecordRoundTrip(b *testing.B) {
	records := []struct {
		name   string
		record codec.Record
	}{
		{
			name:   "Header",
			record: &codec.HeaderRecord{Version: codec.FormatVersion, Flags: 0x1234},
		},
		{
			name:   "Data_1KB",
			record: &codec.DataRecord{Payload: bytes.Repeat([]byte("test"), 256)},
		},
		{
			name: "Index_100",
			record: &codec.IndexRecord{
				Offsets: func() []uint32 {
					offsets := make([]uint32, 100)
					for i := range offsets {
						offsets[i] = uint32(i * 1000)
					}
					return offsets
				}(),
			},
		},
		{
			name: "Footer",
			record: &codec.FooterRecord{
				IndexOffset: 98765,
				IndexLength: 432,
				Count:       123,
				Version:     codec.FormatVersion,
				Flags:       0x5678,
			},
		},
	}

	for _, rec := range records {
		b.Run(rec.name, func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// Encoding
				encoded, err := codec.EncodeRecord(rec.record)
				if err != nil {
					b.Fatal(err)
				}

				// Decoding
				_, err = codec.DecodeRecord(encoded)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkMemoryAllocation benchmarks memory allocation patterns
func BenchmarkMemoryAllocation(b *testing.B) {
	record := &codec.DataRecord{
		Payload: bytes.Repeat([]byte("test"), 1024), // 4KB payload
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		encoded, err := codec.EncodeRecord(record)
		if err != nil {
			b.Fatal(err)
		}

		_, err = codec.DecodeRecord(encoded)
		if err != nil {
			b.Fatal(err)
		}
	}
}
