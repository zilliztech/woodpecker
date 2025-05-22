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

package pool

import "sync"

var bytePoolFor128B = sync.Pool{
	New: func() any {
		return make([]byte, 0, 128)
	},
}

var bytePoolFor256B = sync.Pool{
	New: func() any {
		return make([]byte, 0, 256)
	},
}

var bytePoolFor384B = sync.Pool{
	New: func() any {
		return make([]byte, 0, 384)
	},
}

var bytePoolFor512B = sync.Pool{
	New: func() any {
		return make([]byte, 0, 512)
	},
}

var bytePoolFor768B = sync.Pool{
	New: func() any {
		return make([]byte, 0, 768)
	},
}

var bytePoolFor1KB = sync.Pool{
	New: func() any {
		return make([]byte, 0, 1024)
	},
}

var bytePoolFor2KB = sync.Pool{
	New: func() any {
		return make([]byte, 0, 2*1024)
	},
}

var bytePoolFor4KB = sync.Pool{
	New: func() any {
		return make([]byte, 0, 4*1024)
	},
}

var bytePoolFor8KB = sync.Pool{
	New: func() any {
		return make([]byte, 0, 8*1024)
	},
}

var bytePoolFor16KB = sync.Pool{
	New: func() any {
		return make([]byte, 0, 16*1024)
	},
}

var bytePoolFor32KB = sync.Pool{
	New: func() any {
		return make([]byte, 0, 32*1024)
	},
}

var bytePoolFor64KB = sync.Pool{
	New: func() any {
		return make([]byte, 0, 64*1024)
	},
}

var bytePoolFor128KB = sync.Pool{
	New: func() any {
		return make([]byte, 0, 128*1024)
	},
}

var bytePoolFor256KB = sync.Pool{
	New: func() any {
		return make([]byte, 0, 256*1024)
	},
}

var bytePoolFor384KB = sync.Pool{
	New: func() any {
		return make([]byte, 0, 384*1024)
	},
}

var bytePoolFor512KB = sync.Pool{
	New: func() any {
		return make([]byte, 0, 512*1024)
	},
}

var bytePoolFor768KB = sync.Pool{
	New: func() any {
		return make([]byte, 0, 768*1024)
	},
}

var bytePoolFor1MB = sync.Pool{
	New: func() any {
		return make([]byte, 0, 1024*1024)
	},
}

var bytePoolFor2MB = sync.Pool{
	New: func() any {
		return make([]byte, 0, 2*1024*1024)
	},
}

var bytePoolFor4MB = sync.Pool{
	New: func() any {
		return make([]byte, 0, 4*1024*1024)
	},
}

var bytePoolFor8MB = sync.Pool{
	New: func() any {
		return make([]byte, 0, 8*1024*1024)
	},
}

var bytePoolFor16MB = sync.Pool{
	New: func() any {
		return make([]byte, 0, 16*1024*1024)
	},
}

var bytePoolFor32MB = sync.Pool{
	New: func() any {
		return make([]byte, 0, 32*1024*1024)
	},
}

var bytePoolFor64MB = sync.Pool{
	New: func() any {
		return make([]byte, 0, 64*1024*1024)
	},
}

var bytePoolFor128MB = sync.Pool{
	New: func() any {
		return make([]byte, 0, 128*1024*1024)
	},
}

var bytePoolFor256MB = sync.Pool{
	New: func() any {
		return make([]byte, 0, 256*1024*1024)
	},
}

var bytePoolFor512MB = sync.Pool{
	New: func() any {
		return make([]byte, 0, 512*1024*1024)
	},
}

var bytePoolFor1024MB = sync.Pool{
	New: func() any {
		return make([]byte, 0, 1024*1024*1024)
	},
}

var bytePools = []sync.Pool{
	bytePoolFor128B,
	bytePoolFor256B,
	bytePoolFor384B,
	bytePoolFor512B,
	bytePoolFor768B,
	bytePoolFor1KB,
	bytePoolFor2KB,
	bytePoolFor4KB,
	bytePoolFor8KB,
	bytePoolFor16KB,
	bytePoolFor32KB,
	bytePoolFor64KB,
	bytePoolFor128KB,
	bytePoolFor256KB,
	bytePoolFor384KB,
	bytePoolFor512KB,
	bytePoolFor768KB,
	bytePoolFor1MB,
	bytePoolFor2MB,
	bytePoolFor4MB,
	bytePoolFor8MB,
	bytePoolFor16MB,
	bytePoolFor32MB,
	bytePoolFor64MB,
	bytePoolFor128MB,
	bytePoolFor256MB,
	bytePoolFor512MB,
	bytePoolFor1024MB,
}

// List of pool capacities, corresponding to bytePools
var poolCapacities = []int{
	128,                // 128B
	256,                // 256B
	384,                // 384B
	512,                // 512B
	768,                // 768B
	1024,               // 1KB
	2 * 1024,           // 2KB
	4 * 1024,           // 4KB
	8 * 1024,           // 8KB
	16 * 1024,          // 16KB
	32 * 1024,          // 32KB
	64 * 1024,          // 64KB
	128 * 1024,         // 128KB
	256 * 1024,         // 256KB
	384 * 1024,         // 384KB
	512 * 1024,         // 512KB
	768 * 1024,         // 768KB
	1024 * 1024,        // 1MB
	2 * 1024 * 1024,    // 2MB
	4 * 1024 * 1024,    // 4MB
	8 * 1024 * 1024,    // 8MB
	16 * 1024 * 1024,   // 16MB
	32 * 1024 * 1024,   // 32MB
	64 * 1024 * 1024,   // 64MB
	128 * 1024 * 1024,  // 128MB
	256 * 1024 * 1024,  // 256MB
	512 * 1024 * 1024,  // 512MB
	1024 * 1024 * 1024, // 1GB
}

// GetByteBuffer retrieves an appropriate byte buffer based on the required size
// size: size of data to store (in bytes)
// returns: an empty byte slice with capacity sufficient to hold the specified data
// if requested size exceeds maximum pool capacity, creates a new slice directly
func GetByteBuffer(size int) []byte {
	// Iterate through pool capacities to find first pool that can accommodate the required size
	for i, capacity := range poolCapacities {
		if size <= capacity {
			// Found suitable pool, get a buffer from it
			buffer := bytePools[i].Get().([]byte)
			// Reset length while preserving capacity
			return buffer[:0]
		}
	}

	// Required size exceeds all pool capacities, create a new slice directly
	return make([]byte, 0, size)
}

// PutByteBuffer returns a buffer to the appropriate pool when no longer needed
// buffer: buffer to be returned
func PutByteBuffer(buffer []byte) {
	capacity := cap(buffer)

	// Skip pooling for buffers that are too small or too large
	if capacity < poolCapacities[0] {
		// Capacity smaller than minimum pool size, skip recycling
		return
	}

	// Find appropriate pool
	for i, poolCapacity := range poolCapacities {
		if capacity == poolCapacity {
			// Found matching pool, return buffer
			bytePools[i].Put(buffer[:0]) // Reset length before returning
			return
		}
	}

	// For buffers that don't match any standard pool size, skip recycling
	// Let GC handle it
}
