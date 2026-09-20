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

package hardware

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/shirou/gopsutil/v4/disk"
)

// DiskIOCounters holds one block device's cumulative I/O counters, as the kernel has
// reported them since boot.
type DiskIOCounters struct {
	Device     string
	ReadOps    uint64
	WriteOps   uint64
	ReadBytes  uint64
	WriteBytes uint64
}

// pickDevice returns the device whose mountpoint is the longest prefix of abs, or "" when
// nothing backs it. Split out from DeviceForPath so the choice can be tested without the
// host's mount table.
func pickDevice(parts []disk.PartitionStat, abs string) string {
	best, bestLen := "", -1
	for _, p := range parts {
		// Skip overlay, tmpfs and bind mounts: they have no block device to attribute to.
		if !strings.HasPrefix(p.Device, "/dev/") {
			continue
		}
		mp := strings.TrimSuffix(p.Mountpoint, "/")
		if abs != p.Mountpoint && !strings.HasPrefix(abs, mp+"/") {
			continue
		}
		if len(p.Mountpoint) > bestLen {
			best, bestLen = p.Device, len(p.Mountpoint)
		}
	}
	return best
}

// DeviceForPath returns the name of the block device backing path, resolved through the
// mount table rather than through /sys, so it needs no dev_t arithmetic and behaves the
// same everywhere gopsutil runs.
//
// Resolving it at all is what makes the counters attributable inside a container:
// /proc/diskstats reports every device on the host, so a node running two logstore pods
// would otherwise have both pods reporting the whole host and any sum() across pods would
// double count. Each PVC is its own volume with its own mount entry, so the longest
// matching mountpoint picks out exactly this pod's disk.
func DeviceForPath(path string) (string, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	parts, err := disk.Partitions(true)
	if err != nil {
		return "", err
	}
	dev := pickDevice(parts, abs)
	if dev == "" {
		return "", fmt.Errorf("no block device is mounted for %q", path)
	}
	return filepath.Base(dev), nil
}

// GetDeviceIOCounters reads one device's cumulative counters by its bare name, e.g. "nvme1n1".
func GetDeviceIOCounters(device string) (DiskIOCounters, error) {
	stats, err := disk.IOCounters(device)
	if err != nil {
		return DiskIOCounters{}, err
	}
	s, ok := stats[device]
	if !ok {
		return DiskIOCounters{}, fmt.Errorf("no I/O counters reported for device %q", device)
	}
	return DiskIOCounters{
		Device:     device,
		ReadOps:    s.ReadCount,
		WriteOps:   s.WriteCount,
		ReadBytes:  s.ReadBytes,
		WriteBytes: s.WriteBytes,
	}, nil
}
