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

package net

import (
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_GetValidLocalIPNoValid(t *testing.T) {
	addrs := make([]net.Addr, 0, 1)
	addrs = append(addrs, &net.IPNet{IP: net.IPv4(127, 1, 1, 1), Mask: net.IPv4Mask(255, 255, 255, 255)})
	ip := GetValidLocalIP(addrs)
	assert.Equal(t, "", ip)
}

func Test_GetValidLocalIPIPv4(t *testing.T) {
	addrs := make([]net.Addr, 0, 1)
	addrs = append(addrs, &net.IPNet{IP: net.IPv4(100, 1, 1, 1), Mask: net.IPv4Mask(255, 255, 255, 255)})
	ip := GetValidLocalIP(addrs)
	assert.Equal(t, "100.1.1.1", ip)
}

func Test_GetValidLocalIPIPv6(t *testing.T) {
	addrs := make([]net.Addr, 0, 1)
	addrs = append(addrs, &net.IPNet{IP: net.IP{8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, Mask: net.IPMask{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}})
	ip := GetValidLocalIP(addrs)
	assert.Equal(t, "[800::]", ip)
}

func Test_GetValidLocalIPIPv4Priority(t *testing.T) {
	addrs := make([]net.Addr, 0, 1)
	addrs = append(addrs, &net.IPNet{IP: net.IP{8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, Mask: net.IPMask{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}})
	addrs = append(addrs, &net.IPNet{IP: net.IPv4(100, 1, 1, 1), Mask: net.IPv4Mask(255, 255, 255, 255)})
	ip := GetValidLocalIP(addrs)
	assert.Equal(t, "100.1.1.1", ip)
}

func Test_GetLocalIP(t *testing.T) {
	ip := GetLocalIP()
	assert.NotNil(t, ip)
	assert.NotZero(t, len(ip))
}

func Test_GetIP(t *testing.T) {
	t.Run("empty_fallback_auto", func(t *testing.T) {
		ip := GetIP("")
		assert.NotNil(t, ip)
		assert.NotZero(t, len(ip))
	})

	t.Run("valid_ip", func(t *testing.T) {
		assert.NotPanics(t, func() {
			ip := GetIP("8.8.8.8")
			assert.Equal(t, "8.8.8.8", ip)
		})
	})

	t.Run("invalid_ip", func(t *testing.T) {
		assert.NotPanics(t, func() {
			ip := GetIP("null")
			assert.Equal(t, "null", ip)
		}, "non ip format, could be hostname or service name")

		assert.Panics(t, func() {
			GetIP("0.0.0.0")
		}, "input is unspecified ip address, panicking")

		assert.Panics(t, func() {
			GetIP("224.0.0.1")
		}, "input is multicast ip address, panicking")
	})
}
