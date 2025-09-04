// Copyright 2023 StreamNative, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package bitset

import (
	"fmt"
	"math/bits"
	"sync"
)

const MaxBitSetSize = 16

// BitSet
// Simplified and compact bitset.
type BitSet struct {
	mu   sync.RWMutex
	bits uint16
}

func (bs *BitSet) Count() int {
	bs.mu.RLock()
	defer bs.mu.RUnlock()
	return bits.OnesCount16(bs.bits)
}

func (bs *BitSet) Set(idx int) {
	bs.mu.Lock()
	defer bs.mu.Unlock()
	if idx < 0 || idx >= MaxBitSetSize {
		panic(fmt.Sprintf("invalid index: %d", idx))
	}
	bs.bits |= 1 << idx
}

func (bs *BitSet) SetAndCount(idx int) int {
	bs.mu.Lock()
	defer bs.mu.Unlock()
	if idx < 0 || idx >= MaxBitSetSize {
		panic(fmt.Sprintf("invalid index: %d", idx))
	}
	bs.bits |= 1 << idx
	return bits.OnesCount16(bs.bits)
}
