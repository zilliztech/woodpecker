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

package membership

import "github.com/zilliztech/woodpecker/common/hardware"

// LoadSampler produces a node's current load factor in [0,1], where higher means busier.
type LoadSampler interface {
	Sample() float64
}

// computeRawLoad combines flow resources (CPU, IO wait) as a bottleneck (max),
// and folds memory in only as a high-end penalty above memSoftThreshold so that
// normal caching memory usage does not unfairly de-rank a node. All inputs are
// fractions in [0,1] (cpuFrac, ioWaitFrac, memRatio). Returns load clamped to [0,1].
func computeRawLoad(cpuFrac, ioWaitFrac, memRatio, memSoftThreshold float64) float64 {
	flow := cpuFrac
	if ioWaitFrac > flow {
		flow = ioWaitFrac
	}
	load := flow
	if memSoftThreshold < 1 && memRatio > memSoftThreshold {
		memPenalty := (memRatio - memSoftThreshold) / (1 - memSoftThreshold)
		if memPenalty > load {
			load = memPenalty
		}
	}
	return clamp01(load)
}

func clamp01(v float64) float64 {
	if v < 0 {
		return 0
	}
	if v > 1 {
		return 1
	}
	return v
}

// SystemLoadSampler samples local hardware metrics and EWMA-smooths the result.
// The read* function fields exist for testing; production uses the hardware package.
type SystemLoadSampler struct {
	memSoftThreshold float64
	alpha            float64 // EWMA weight on the newest sample, in (0,1]
	ewma             float64
	seeded           bool

	readCPU      func() float64 // returns CPU usage percent (0..100)
	readIOWait   func() float64 // returns IO wait percent (0..100)
	readMemRatio func() float64 // returns memory used ratio (0..1)
}

// NewSystemLoadSampler builds a sampler backed by the common/hardware package.
func NewSystemLoadSampler(memSoftThreshold, alpha float64) *SystemLoadSampler {
	if memSoftThreshold <= 0 || memSoftThreshold >= 1 {
		memSoftThreshold = 0.85
	}
	if alpha <= 0 || alpha > 1 {
		alpha = 0.5
	}
	return &SystemLoadSampler{
		memSoftThreshold: memSoftThreshold,
		alpha:            alpha,
		readCPU:          hardware.GetCPUUsage,
		readIOWait:       func() float64 { v, _ := hardware.GetIOWait(); return v },
		readMemRatio:     hardware.GetMemoryUseRatio,
	}
}

// Sample reads the hardware signals, computes the raw load, and applies EWMA smoothing.
func (s *SystemLoadSampler) Sample() float64 {
	cpuFrac := clamp01(s.readCPU() / 100)
	ioFrac := clamp01(s.readIOWait() / 100)
	memRatio := clamp01(s.readMemRatio())

	raw := computeRawLoad(cpuFrac, ioFrac, memRatio, s.memSoftThreshold)
	if !s.seeded {
		s.ewma = raw
		s.seeded = true
	} else {
		s.ewma = s.alpha*raw + (1-s.alpha)*s.ewma
	}
	return clamp01(s.ewma)
}
