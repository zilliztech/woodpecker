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

package generic

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestZero(t *testing.T) {
	assert.Equal(t, 0, Zero[int]())
	assert.Equal(t, "", Zero[string]())
	assert.Equal(t, false, Zero[bool]())
	assert.Equal(t, 0.0, Zero[float64]())
	assert.Nil(t, Zero[*int]())
	assert.Nil(t, Zero[[]byte]())
}

func TestIsZero(t *testing.T) {
	assert.True(t, IsZero(0))
	assert.True(t, IsZero(""))
	assert.True(t, IsZero(false))
	assert.True(t, IsZero[*int](nil))

	assert.False(t, IsZero(1))
	assert.False(t, IsZero("hello"))
	assert.False(t, IsZero(true))
	v := 42
	assert.False(t, IsZero(&v))
}

func TestEqual(t *testing.T) {
	assert.True(t, Equal(1, 1))
	assert.True(t, Equal("a", "a"))
	assert.True(t, Equal(nil, nil))

	assert.False(t, Equal(1, 2))
	assert.False(t, Equal("a", "b"))
	assert.False(t, Equal(1, "1"))
}
