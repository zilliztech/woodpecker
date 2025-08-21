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
	"fmt"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// ByteSize represents a size value that can be parsed from YAML with units (KB, MB, GB, etc.)
type ByteSize int64

// UnmarshalYAML implements yaml.Unmarshaler for ByteSize
func (b *ByteSize) UnmarshalYAML(value *yaml.Node) error {
	var v interface{}
	if err := value.Decode(&v); err != nil {
		return err
	}

	switch val := v.(type) {
	case int:
		*b = ByteSize(val)
		return nil
	case int64:
		*b = ByteSize(val)
		return nil
	case string:
		size, err := parseSize(val)
		if err != nil {
			return err
		}
		*b = ByteSize(size)
		return nil
	default:
		return fmt.Errorf("invalid type for ByteSize: %T", v)
	}
}

// Int64 returns the int64 value of ByteSize
func (b ByteSize) Int64() int64 {
	return int64(b)
}

// Duration represents a duration that can be parsed from YAML with units
// Supports time.ParseDuration format (ns, us, ms, s, m, h) or plain numbers with default unit
type Duration struct {
	duration    time.Duration
	defaultUnit time.Duration
}

// NewDuration creates a new Duration with the specified default unit
func NewDuration(d time.Duration, defaultUnit time.Duration) Duration {
	return Duration{duration: d, defaultUnit: defaultUnit}
}

// UnmarshalYAML implements yaml.Unmarshaler for Duration
func (d *Duration) UnmarshalYAML(value *yaml.Node) error {
	var v interface{}
	if err := value.Decode(&v); err != nil {
		return err
	}

	// If no default unit set, use milliseconds
	if d.defaultUnit == 0 {
		d.defaultUnit = time.Millisecond
	}

	switch val := v.(type) {
	case int:
		d.duration = time.Duration(val) * d.defaultUnit
		return nil
	case int64:
		d.duration = time.Duration(val) * d.defaultUnit
		return nil
	case string:
		duration, err := parseDuration(val, d.defaultUnit)
		if err != nil {
			return err
		}
		d.duration = duration
		return nil
	default:
		return fmt.Errorf("invalid type for Duration: %T", v)
	}
}

// Milliseconds returns the duration in milliseconds
func (d Duration) Milliseconds() int {
	return int(d.duration.Milliseconds())
}

// Seconds returns the duration in seconds
func (d Duration) Seconds() int {
	return int(d.duration.Seconds())
}

// Duration returns the underlying time.Duration
func (d Duration) Duration() time.Duration {
	return d.duration
}

// DurationWithDefault represents a duration with a configurable default unit
// This is useful for fields that need different default units
type DurationWithDefault struct {
	Duration
}

// NewDurationWithDefault creates a Duration with default unit as seconds
func NewDurationWithDefault(defaultUnit time.Duration) DurationWithDefault {
	return DurationWithDefault{
		Duration: Duration{defaultUnit: defaultUnit},
	}
}

// DurationSeconds represents a duration with default unit as seconds
type DurationSeconds struct {
	Duration
}

// UnmarshalYAML implements yaml.Unmarshaler for DurationSeconds
func (d *DurationSeconds) UnmarshalYAML(value *yaml.Node) error {
	d.Duration.defaultUnit = time.Second
	return d.Duration.UnmarshalYAML(value)
}

// DurationMilliseconds represents a duration with default unit as milliseconds
type DurationMilliseconds struct {
	Duration
}

// UnmarshalYAML implements yaml.Unmarshaler for DurationMilliseconds
func (d *DurationMilliseconds) UnmarshalYAML(value *yaml.Node) error {
	d.Duration.defaultUnit = time.Millisecond
	return d.Duration.UnmarshalYAML(value)
}

// Helper functions for creating typed values from primitives

// NewByteSize creates a ByteSize from an int64 value
func NewByteSize(size int64) ByteSize {
	return ByteSize(size)
}

// NewDurationSecondsFromInt creates a DurationSeconds from an int value (seconds)
func NewDurationSecondsFromInt(seconds int) DurationSeconds {
	return DurationSeconds{
		Duration: Duration{
			duration:    time.Duration(seconds) * time.Second,
			defaultUnit: time.Second,
		},
	}
}

// NewDurationMillisecondsFromInt creates a DurationMilliseconds from an int value (milliseconds)
func NewDurationMillisecondsFromInt(milliseconds int) DurationMilliseconds {
	return DurationMilliseconds{
		Duration: Duration{
			duration:    time.Duration(milliseconds) * time.Millisecond,
			defaultUnit: time.Millisecond,
		},
	}
}

// parseSize parses size strings like "256M", "2GB", "1024" (bytes)
// Supports: G/GB, M/MB, K/KB, or plain numbers
func parseSize(sizeStr string) (int64, error) {
	if sizeStr == "" {
		return 0, nil
	}

	valueStr := strings.ToLower(strings.TrimSpace(sizeStr))
	if valueStr == "" {
		return 0, nil
	}

	// Handle GB/G suffix
	if strings.HasSuffix(valueStr, "gb") || strings.HasSuffix(valueStr, "g") {
		numStr := strings.Split(valueStr, "g")[0]
		size, err := strconv.ParseInt(numStr, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid size format '%s': %w", sizeStr, err)
		}
		return size * 1024 * 1024 * 1024, nil
	}

	// Handle MB/M suffix
	if strings.HasSuffix(valueStr, "mb") || strings.HasSuffix(valueStr, "m") {
		numStr := strings.Split(valueStr, "m")[0]
		size, err := strconv.ParseInt(numStr, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid size format '%s': %w", sizeStr, err)
		}
		return size * 1024 * 1024, nil
	}

	// Handle KB/K suffix
	if strings.HasSuffix(valueStr, "kb") || strings.HasSuffix(valueStr, "k") {
		numStr := strings.Split(valueStr, "k")[0]
		size, err := strconv.ParseInt(numStr, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid size format '%s': %w", sizeStr, err)
		}
		return size * 1024, nil
	}

	// Handle TB/T suffix
	if strings.HasSuffix(valueStr, "tb") || strings.HasSuffix(valueStr, "t") {
		numStr := strings.Split(valueStr, "t")[0]
		size, err := strconv.ParseInt(numStr, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid size format '%s': %w", sizeStr, err)
		}
		return size * 1024 * 1024 * 1024 * 1024, nil
	}

	// No unit, parse as plain number (bytes)
	size, err := strconv.ParseInt(valueStr, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid size format '%s': %w", sizeStr, err)
	}
	return size, nil
}

// parseDuration parses duration strings and returns time.Duration
// Uses Go's standard time.ParseDuration for better compatibility
// Also supports plain numbers with a default unit for backward compatibility
func parseDuration(durationStr string, defaultUnit time.Duration) (time.Duration, error) {
	if durationStr == "" {
		return 0, nil
	}

	durationStr = strings.TrimSpace(durationStr)
	if durationStr == "" {
		return 0, nil
	}

	// Try to parse as plain number first (for backward compatibility)
	if value, err := strconv.ParseInt(durationStr, 10, 64); err == nil {
		return time.Duration(value) * defaultUnit, nil
	}

	// Try to parse as Go duration format (supports ns, us, ms, s, m, h)
	duration, err := time.ParseDuration(durationStr)
	if err != nil {
		return 0, fmt.Errorf("invalid duration format '%s': %w", durationStr, err)
	}

	return duration, nil
}
