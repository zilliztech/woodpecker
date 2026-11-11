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

import "gopkg.in/yaml.v3"

// Dynamic wraps a single config value so it can be read either from its static
// (YAML-parsed) value or from an optional runtime source supplied by the
// embedding application. With no source bound, Get() returns the static value,
// so existing behavior is preserved byte-for-byte.
//
// Only YAML (un)marshaling is supported; JSON is intentionally not implemented
// (nothing in the module JSON-serializes the config). A JSON marshal of a
// Dynamic field would emit an empty object because value/source are unexported.
type Dynamic[T any] struct {
	value  T                // static value from YAML / defaults
	source func() (T, bool) // optional runtime override
}

// NewDynamic builds a Dynamic with a static default (used by the defaults builder).
func NewDynamic[T any](v T) Dynamic[T] { return Dynamic[T]{value: v} }

// Get returns the dynamic value when a source is bound and has an opinion,
// otherwise the static value.
func (d Dynamic[T]) Get() T {
	if d.source != nil {
		if v, ok := d.source(); ok {
			return v
		}
	}
	return d.value
}

// WithSource binds (or, with nil, clears) the dynamic source. Bind at startup,
// before the client is used concurrently.
func (d *Dynamic[T]) WithSource(source func() (T, bool)) { d.source = source }

// Set overwrites the static value (defaults builder, config merge, tests).
func (d *Dynamic[T]) Set(v T) { d.value = v }

// UnmarshalYAML decodes a bare T into the static value, so a Dynamic[T] field
// parses exactly like a plain T on disk.
func (d *Dynamic[T]) UnmarshalYAML(node *yaml.Node) error { return node.Decode(&d.value) }

// MarshalYAML emits the static value, so a Dynamic[T] field serializes like T.
func (d Dynamic[T]) MarshalYAML() (interface{}, error) { return d.value, nil }
