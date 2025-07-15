// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package benchmark

import (
	"hash/crc32"
	"testing"
)

// Generate test data of specified size
func generateTestData(size int) []byte {
	data := make([]byte, size)
	for i := 0; i < size; i++ {
		data[i] = byte(i % 256)
	}
	return data
}

// General benchmark function - can test any size
func benchmarkCRC32(b *testing.B, size int) {
	data := generateTestData(size)
	b.ResetTimer()
	b.SetBytes(int64(size))
	for i := 0; i < b.N; i++ {
		crc32.ChecksumIEEE(data)
	}
}

// CRC32 benchmark - different data sizes
func BenchmarkCRC32_100B(b *testing.B)  { benchmarkCRC32(b, 100) }
func BenchmarkCRC32_500B(b *testing.B)  { benchmarkCRC32(b, 500) }
func BenchmarkCRC32_1KB(b *testing.B)   { benchmarkCRC32(b, 1024) }
func BenchmarkCRC32_2KB(b *testing.B)   { benchmarkCRC32(b, 2*1024) }
func BenchmarkCRC32_4KB(b *testing.B)   { benchmarkCRC32(b, 4*1024) }
func BenchmarkCRC32_8KB(b *testing.B)   { benchmarkCRC32(b, 8*1024) }
func BenchmarkCRC32_16KB(b *testing.B)  { benchmarkCRC32(b, 16*1024) }
func BenchmarkCRC32_32KB(b *testing.B)  { benchmarkCRC32(b, 32*1024) }
func BenchmarkCRC32_64KB(b *testing.B)  { benchmarkCRC32(b, 64*1024) }
func BenchmarkCRC32_128KB(b *testing.B) { benchmarkCRC32(b, 128*1024) }
func BenchmarkCRC32_256KB(b *testing.B) { benchmarkCRC32(b, 256*1024) }
func BenchmarkCRC32_512KB(b *testing.B) { benchmarkCRC32(b, 512*1024) }
func BenchmarkCRC32_1MB(b *testing.B)   { benchmarkCRC32(b, 1024*1024) }
func BenchmarkCRC32_2MB(b *testing.B)   { benchmarkCRC32(b, 2*1024*1024) }
func BenchmarkCRC32_4MB(b *testing.B)   { benchmarkCRC32(b, 4*1024*1024) }
func BenchmarkCRC32_8MB(b *testing.B)   { benchmarkCRC32(b, 8*1024*1024) }
func BenchmarkCRC32_16MB(b *testing.B)  { benchmarkCRC32(b, 16*1024*1024) }
func BenchmarkCRC32_32MB(b *testing.B)  { benchmarkCRC32(b, 32*1024*1024) }
func BenchmarkCRC32_64MB(b *testing.B)  { benchmarkCRC32(b, 64*1024*1024) }
func BenchmarkCRC32_128MB(b *testing.B) { benchmarkCRC32(b, 128*1024*1024) }
func BenchmarkCRC32_256MB(b *testing.B) { benchmarkCRC32(b, 256*1024*1024) }
