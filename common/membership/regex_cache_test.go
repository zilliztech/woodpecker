// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package membership

import (
	"fmt"
	"regexp"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestRegexCache tests the effectiveness of the regex cache
func TestRegexCache(t *testing.T) {
	sd := NewServiceDiscovery()

	t.Run("Cache functionality", func(t *testing.T) {
		pattern := "test-.*-pattern"

		// First call should compile and cache the regex
		regex1, err1 := sd.getCompiledRegex(pattern)
		assert.NoError(t, err1)
		assert.NotNil(t, regex1)

		// Second call should return the same instance from cache
		regex2, err2 := sd.getCompiledRegex(pattern)
		assert.NoError(t, err2)
		assert.NotNil(t, regex2)

		// Should be the same instance (pointer equality)
		assert.True(t, regex1 == regex2, "Should return the same cached regex instance")
	})

	t.Run("Cache size limit", func(t *testing.T) {
		sd := NewServiceDiscovery()

		// Fill cache to limit (100 + 5)
		for i := 0; i < 105; i++ {
			pattern := fmt.Sprintf("pattern-%d", i)
			_, err := sd.getCompiledRegex(pattern)
			assert.NoError(t, err)
		}

		// LRU cache size should not exceed the limit
		assert.LessOrEqual(t, sd.regexCache.Len(), 100)
	})

	t.Run("Invalid regex handling", func(t *testing.T) {
		invalidPattern := "[invalid"
		regex, err := sd.getCompiledRegex(invalidPattern)

		assert.Error(t, err)
		assert.Nil(t, regex)

		// Invalid regex should not be cached
		_, found := sd.regexCache.Get(invalidPattern)
		assert.False(t, found)
	})

	t.Run("Empty pattern handling", func(t *testing.T) {
		regex, err := sd.getCompiledRegex("")

		assert.NoError(t, err)
		assert.Nil(t, regex)

		// Empty pattern should not be cached
		_, found := sd.regexCache.Get("")
		assert.False(t, found)
	})
}

// BenchmarkRegexCacheVsNonCache compares performance with and without cache
func BenchmarkRegexCacheVsNonCache(b *testing.B) {
	sd := NewServiceDiscovery()
	patterns := []string{
		"az-[0-9]+",
		"rg-prod-.*",
		".*-staging",
		"cluster-[a-z]+",
		"node-[0-9]+-[a-z]+",
	}

	b.Run("WithCache", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			pattern := patterns[i%len(patterns)]
			_, _ = sd.getCompiledRegex(pattern)
		}
	})

	b.Run("WithoutCache", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			pattern := patterns[i%len(patterns)]
			_, _ = regexp.Compile(pattern)
		}
	})
}

// TestLRUEviction tests the LRU cache eviction functionality
func TestLRUEviction(t *testing.T) {
	sd := NewServiceDiscovery()

	// Add more patterns than the cache size
	for i := 0; i < 110; i++ {
		pattern := fmt.Sprintf("pattern-%d", i)
		regex, err := sd.getCompiledRegex(pattern)
		assert.NoError(t, err)
		assert.NotNil(t, regex)
	}

	// Cache size should be limited to 100
	assert.Equal(t, 100, sd.regexCache.Len())

	// Early patterns should be evicted (LRU policy)
	_, found := sd.regexCache.Get("pattern-0")
	assert.False(t, found, "Early pattern should be evicted by LRU")

	// Recent patterns should still be in cache
	_, found = sd.regexCache.Get("pattern-109")
	assert.True(t, found, "Recent pattern should still be in cache")
}

// BenchmarkLRUVsManualCache compares LRU cache vs manual cache management
func BenchmarkLRUVsManualCache(b *testing.B) {
	patterns := make([]string, 50)
	for i := 0; i < 50; i++ {
		patterns[i] = fmt.Sprintf("pattern-%d", i)
	}

	b.Run("LRU-Cache", func(b *testing.B) {
		sd := NewServiceDiscovery()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			pattern := patterns[i%len(patterns)]
			_, _ = sd.getCompiledRegex(pattern)
		}
	})

	b.Run("Manual-Cache-with-Mutex", func(b *testing.B) {
		cache := make(map[string]*regexp.Regexp)
		var mu sync.RWMutex

		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			pattern := patterns[i%len(patterns)]

			mu.RLock()
			regex, exists := cache[pattern]
			mu.RUnlock()

			if !exists {
				compiled, err := regexp.Compile(pattern)
				if err == nil {
					mu.Lock()
					if len(cache) < 100 {
						cache[pattern] = compiled
					}
					mu.Unlock()
				}
				regex = compiled
			}
			_ = regex
		}
	})
}
