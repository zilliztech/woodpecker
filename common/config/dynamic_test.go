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

package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestDynamic_GetReturnsStaticWhenUnbound(t *testing.T) {
	d := NewDynamic(3)
	assert.Equal(t, 3, d.Get())
}

func TestDynamic_SourceWinsWhenBoundAndPresent(t *testing.T) {
	d := NewDynamic("random")
	d.WithSource(func() (string, bool) { return "cross-region", true })
	assert.Equal(t, "cross-region", d.Get())
}

func TestDynamic_FallsBackWhenSourceReturnsFalse(t *testing.T) {
	d := NewDynamic("random")
	d.WithSource(func() (string, bool) { return "ignored", false })
	assert.Equal(t, "random", d.Get())
}

func TestDynamic_NilSourceClears(t *testing.T) {
	d := NewDynamic(5)
	d.WithSource(func() (int, bool) { return 7, true })
	assert.Equal(t, 7, d.Get())
	d.WithSource(nil)
	assert.Equal(t, 5, d.Get())
}

func TestDynamic_SetOverwritesStatic(t *testing.T) {
	d := NewDynamic(3)
	d.Set(5)
	assert.Equal(t, 5, d.Get())
}

func TestDynamic_YAMLRoundTripLikeBareT(t *testing.T) {
	type holder struct {
		Replicas Dynamic[int]    `yaml:"replicas"`
		Strategy Dynamic[string] `yaml:"strategy"`
	}
	var h holder
	require.NoError(t, yaml.Unmarshal([]byte("replicas: 5\nstrategy: custom\n"), &h))
	assert.Equal(t, 5, h.Replicas.Get())
	assert.Equal(t, "custom", h.Strategy.Get())

	out, err := yaml.Marshal(h)
	require.NoError(t, err)
	assert.Contains(t, string(out), "replicas: 5")
	assert.Contains(t, string(out), "strategy: custom")
}

func TestDynamic_YAMLAbsentKeyKeepsDefault(t *testing.T) {
	type holder struct {
		Replicas Dynamic[int] `yaml:"replicas"`
	}
	h := holder{Replicas: NewDynamic(3)}
	require.NoError(t, yaml.Unmarshal([]byte("other: 1\n"), &h))
	assert.Equal(t, 3, h.Replicas.Get())
}
