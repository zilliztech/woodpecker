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
	"context"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/oserror"
	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/disk"
	"go.uber.org/zap"
	"runtime"

	"github.com/zilliztech/woodpecker/common/logger"
)

// GetCPUNum returns the count of cpu core.
func GetCPUNum() int {
	//nolint
	cur := runtime.GOMAXPROCS(0)
	if cur <= 0 {
		//nolint
		cur = runtime.NumCPU()
	}
	return cur
}

// GetCPUUsage returns the cpu usage in percentage.
func GetCPUUsage() float64 {
	percents, err := cpu.Percent(0, false)
	if err != nil {
		logger.Ctx(context.TODO()).Warn("failed to get cpu usage",
			zap.Error(err))
		return 0
	}

	if len(percents) != 1 {
		logger.Ctx(context.TODO()).Warn("something wrong in cpu.Percent, len(percents) must be equal to 1",
			zap.Int("len(percents)", len(percents)))
		return 0
	}

	return percents[0]
}

// GetDiskUsage Get Disk Usage in GB
func GetDiskUsage(path string) (float64, float64, error) {
	diskStats, err := disk.Usage(path)
	if err != nil {
		// If the path does not exist, ignore the error and return 0.
		if errors.Is(err, oserror.ErrNotExist) {
			return 0, 0, nil
		}
		return 0, 0, err
	}
	usedGB := float64(diskStats.Used) / 1e9
	totalGB := float64(diskStats.Total) / 1e9
	return usedGB, totalGB, nil
}

// GetIOWait Get IO Wait Percentage
func GetIOWait() (float64, error) {
	cpuTimes, err := cpu.Times(false)
	if err != nil {
		return 0, err
	}
	if len(cpuTimes) > 0 {
		return cpuTimes[0].Iowait, nil
	}
	return 0, nil
}
