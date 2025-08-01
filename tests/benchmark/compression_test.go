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
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"math/rand"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
)

// Generate random floating-point vector data
func generateVectors(num, dim int) []byte {
	data := make([]byte, num*dim*4) // float32 -> 4 bytes
	buf := bytes.NewBuffer(data[:0])
	for i := 0; i < num*dim; i++ {
		// Simulate vector data, randomly generate floating-point numbers between [-1,1],
		// following a normal distribution typical of large model embeddings
		f := float32(rand.NormFloat64())*2 - 1 // Between [-1,1]
		_ = binary.Write(buf, binary.LittleEndian, f)
	}
	return buf.Bytes()
}

// Calculate compression ratio
func compressionRatio(orig, compressed []byte) float64 {
	return float64(len(orig)) / float64(len(compressed))
}

// Test vector data compression effectiveness
func TestVectorCompressionRate(t *testing.T) {
	rand.Seed(time.Now().UnixNano())

	num := 10000 // Number of vectors
	dim := 512   // Vector dimension

	data := generateVectors(num, dim)
	t.Logf("Original data size: %.2f MB", float64(len(data))/1024/1024)

	// LZ4 compression
	var lz4Buf bytes.Buffer
	lz4Writer := lz4.NewWriter(&lz4Buf)
	if _, err := lz4Writer.Write(data); err != nil {
		t.Fatal(err)
	}
	lz4Writer.Close()
	t.Logf("LZ4 compressed size: %.2f MB, compression ratio: %.2fx",
		float64(lz4Buf.Len())/1024/1024, compressionRatio(data, lz4Buf.Bytes()))

	// Zstd compression
	zstdEncoder, _ := zstd.NewWriter(nil)
	zstdCompressed := zstdEncoder.EncodeAll(data, nil)
	t.Logf("Zstd compressed size: %.2f MB, compression ratio: %.2fx",
		float64(len(zstdCompressed))/1024/1024, compressionRatio(data, zstdCompressed))

	// Zlib compression (similar to gzip)
	var zlibBuf bytes.Buffer
	zlibWriter := zlib.NewWriter(&zlibBuf)
	if _, err := zlibWriter.Write(data); err != nil {
		t.Fatal(err)
	}
	zlibWriter.Close()
	t.Logf("Zlib compressed size: %.2f MB, compression ratio: %.2fx",
		float64(zlibBuf.Len())/1024/1024, compressionRatio(data, zlibBuf.Bytes()))
}
