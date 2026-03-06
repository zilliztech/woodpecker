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

	"github.com/labstack/gommon/log"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func Test_GetCPUCoreCount(t *testing.T) {
	log.Info("TestGetCPUCoreCount",
		zap.Int("physical CPUCoreCount", GetCPUNum()))
}

func Test_GetCPUUsage(t *testing.T) {
	log.Info("TestGetCPUUsage",
		zap.Float64("CPUUsage", GetCPUUsage()))
}

func Test_GetMemoryCount(t *testing.T) {
	log.Info("TestGetMemoryCount",
		zap.Uint64("MemoryCount", GetMemoryCount()))

	assert.NotZero(t, GetMemoryCount())
}

func Test_GetUsedMemoryCount(t *testing.T) {
	log.Info("TestGetUsedMemoryCount",
		zap.Uint64("UsedMemoryCount", GetUsedMemoryCount()))
}

func TestGetDiskUsage(t *testing.T) {
	used, total, err := GetDiskUsage("/")
	assert.NoError(t, err)
	assert.GreaterOrEqual(t, used, 0.0)
	assert.GreaterOrEqual(t, total, 0.0)

	used, total, err = GetDiskUsage("/dir_not_exist")
	assert.NoError(t, err)
	assert.Equal(t, 0.0, used)
	assert.Equal(t, 0.0, total)
}

func TestGetIOWait(t *testing.T) {
	iowait, err := GetIOWait()
	assert.NoError(t, err)
	assert.GreaterOrEqual(t, iowait, 0.0)
}

func Test_GetMemoryUsageRatio(t *testing.T) {
	ratio := GetMemoryUseRatio()
	log.Info("TestGetMemoryUsageRatio",
		zap.Float64("Memory usage ratio", ratio))
	assert.GreaterOrEqual(t, ratio, 0.0)
}

func Test_GetFreeMemoryCount(t *testing.T) {
	free := GetFreeMemoryCount()
	total := GetMemoryCount()
	used := GetUsedMemoryCount()
	// Free = Total - Used, but values come from separate system calls so
	// memory can change between calls. Allow 10 MB tolerance.
	assert.InDelta(t, float64(total-used), float64(free), 10*1024*1024)
}

func Test_InitMaxprocs(t *testing.T) {
	// Should not panic
	assert.NotPanics(t, func() {
		InitMaxprocs("test", nil)
	})
}

func Test_GetContainerMemLimit(t *testing.T) {
	limit, err := getContainerMemLimit()
	if err != nil {
		// On Darwin/Windows, returns 0 with "Not supported" error
		assert.Equal(t, uint64(0), limit)
	}
	// On Linux: err may be nil with limit=0 (no cgroup memory limit set)
	// or limit>0 (cgroup memory limit configured). Both are valid.
}

func Test_GetContainerMemUsed(t *testing.T) {
	used, err := getContainerMemUsed()
	if err != nil {
		// On Darwin/Windows, returns 0 with "Not supported" error
		assert.Equal(t, uint64(0), used)
	}
	// On Linux: err may be nil with any usage value. Both are valid.
}

func Test_GetAllGPUMemoryInfo(t *testing.T) {
	// Non-CUDA build returns error
	info, err := GetAllGPUMemoryInfo()
	assert.Error(t, err)
	assert.Nil(t, info)
	assert.Contains(t, err.Error(), "CUDA not supported")
}

func Test_GetCPUNum_Positive(t *testing.T) {
	n := GetCPUNum()
	assert.Greater(t, n, 0)
}

func Test_GetUsedMemoryCount_Positive(t *testing.T) {
	used := GetUsedMemoryCount()
	assert.Greater(t, used, uint64(0))
}
