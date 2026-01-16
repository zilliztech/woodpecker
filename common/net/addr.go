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
	"context"
	"net"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/zilliztech/woodpecker/common/logger"
)

// GetIP return the ip address
func GetIP(ip string) string {
	if len(ip) == 0 {
		return GetLocalIP()
	}
	netIP := net.ParseIP(ip)
	// not a valid ip addr
	if netIP == nil {
		logger.Ctx(context.TODO()).Warn("cannot parse input ip, treat it as hostname/service name", zap.String("ip", ip))
		return ip
	}
	// only localhost or unicast is acceptable
	if netIP.IsUnspecified() {
		panic(errors.Newf(`"%s" in param table is Unspecified IP address and cannot be used`))
	}
	if netIP.IsMulticast() || netIP.IsLinkLocalMulticast() || netIP.IsInterfaceLocalMulticast() {
		panic(errors.Newf(`"%s" in param table is Multicast IP address and cannot be used`))
	}
	return ip
}

// GetLocalIP return the local ip address
func GetLocalIP() string {
	addrs, err := net.InterfaceAddrs()
	if err == nil {
		ip := GetValidLocalIP(addrs)
		if len(ip) != 0 {
			return ip
		}
	}
	return "127.0.0.1"
}

// GetValidLocalIP return the first valid local ip address
func GetValidLocalIP(addrs []net.Addr) string {
	// Search for valid ipv4 addresses
	for _, addr := range addrs {
		ipaddr, ok := addr.(*net.IPNet)
		if ok && ipaddr.IP.IsGlobalUnicast() && ipaddr.IP.To4() != nil {
			return ipaddr.IP.String()
		}
	}
	// Search for valid ipv6 addresses
	for _, addr := range addrs {
		ipaddr, ok := addr.(*net.IPNet)
		if ok && ipaddr.IP.IsGlobalUnicast() && ipaddr.IP.To16() != nil && ipaddr.IP.To4() == nil {
			return "[" + ipaddr.IP.String() + "]"
		}
	}
	return ""
}

// ResolveAdvertiseAddr resolves hostname to IP address if needed
func ResolveAdvertiseAddr(addr string) net.IP {
	if addr == "" {
		return nil
	}

	// Check if it's already an IP address
	if ip := net.ParseIP(addr); ip != nil {
		return ip
	}

	// Try to resolve hostname to IP
	ips, err := net.LookupIP(addr)
	if err != nil {
		return nil
	}

	// Prefer IPv4 address
	for _, ip := range ips {
		if ipv4 := ip.To4(); ipv4 != nil {
			return ipv4
		}
	}

	// Fallback to first IP (could be IPv6)
	if len(ips) > 0 {
		return ips[0]
	}

	return nil
}
