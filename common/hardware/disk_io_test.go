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
	"testing"

	"github.com/shirou/gopsutil/v4/disk"
	"github.com/stretchr/testify/assert"
)

// pickDevice decides which device a WAL path is attributed to. Getting it wrong is not a
// cosmetic bug: /proc/diskstats reports every device on the host, so attributing to the
// wrong one (or to the host root instead of the PVC) makes a node running two logstore pods
// report the same numbers twice, and any sum() across pods double counts.
func TestPickDevice(t *testing.T) {
	parts := []disk.PartitionStat{
		{Device: "/dev/nvme0n1p1", Mountpoint: "/"},
		{Device: "overlay", Mountpoint: "/var/lib/docker/overlay2/abc/merged"},
		{Device: "tmpfs", Mountpoint: "/run"},
		{Device: "/dev/nvme1n1", Mountpoint: "/var/lib/woodpecker"},
		{Device: "/dev/nvme2n1", Mountpoint: "/var/lib/woodpecker-2"},
	}

	t.Run("the longest matching mountpoint wins over the root", func(t *testing.T) {
		assert.Equal(t, "/dev/nvme1n1", pickDevice(parts, "/var/lib/woodpecker/wal/seg-1"))
	})

	t.Run("two pods on one node resolve to their own volumes", func(t *testing.T) {
		a := pickDevice(parts, "/var/lib/woodpecker/wal")
		b := pickDevice(parts, "/var/lib/woodpecker-2/wal")
		assert.Equal(t, "/dev/nvme1n1", a)
		assert.Equal(t, "/dev/nvme2n1", b)
		assert.NotEqual(t, a, b, "co-located pods must not report the same device")
	})

	t.Run("a sibling prefix is not a match", func(t *testing.T) {
		// "/var/lib/woodpecker" must not swallow "/var/lib/woodpecker-2": prefix matching
		// has to be on path boundaries, not raw string prefixes.
		assert.Equal(t, "/dev/nvme2n1", pickDevice(parts, "/var/lib/woodpecker-2"))
	})

	t.Run("an exact mountpoint matches itself", func(t *testing.T) {
		assert.Equal(t, "/dev/nvme1n1", pickDevice(parts, "/var/lib/woodpecker"))
	})

	t.Run("falls back to the root filesystem", func(t *testing.T) {
		assert.Equal(t, "/dev/nvme0n1p1", pickDevice(parts, "/srv/data"))
	})

	t.Run("mounts with no block device are skipped", func(t *testing.T) {
		assert.Equal(t, "/dev/nvme0n1p1", pickDevice(parts, "/run/secrets"),
			"tmpfs has nothing to attribute I/O to, so the root device is the honest answer")
		assert.Equal(t, "/dev/nvme0n1p1",
			pickDevice(parts, "/var/lib/docker/overlay2/abc/merged/x"))
	})

	t.Run("nothing backing the path yields no device", func(t *testing.T) {
		assert.Empty(t, pickDevice([]disk.PartitionStat{{Device: "tmpfs", Mountpoint: "/"}}, "/data"))
		assert.Empty(t, pickDevice(nil, "/data"))
	})
}
