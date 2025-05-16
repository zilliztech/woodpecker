package cache

import "sync"

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

var bytePoolFor512KB = sync.Pool{
	New: func() any {
		return make([]byte, 0, 512*1024)
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
	bytePoolFor32KB,
	bytePoolFor64KB,
	bytePoolFor128KB,
	bytePoolFor256KB,
	bytePoolFor512KB,
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
	32 * 1024,          // 32KB
	64 * 1024,          // 64KB
	128 * 1024,         // 128KB
	256 * 1024,         // 256KB
	512 * 1024,         // 512KB
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
