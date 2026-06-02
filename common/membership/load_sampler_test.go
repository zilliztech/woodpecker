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

import (
	"math"
	"testing"
	"time"
)

func approxEq(a, b float64) bool { return math.Abs(a-b) < 1e-9 }

func TestComputeRawLoad_FlowIsMaxOfCPUAndIOWait(t *testing.T) {
	got := computeRawLoad(0.40, 0.70, 0.20, 0.85)
	if !approxEq(got, 0.70) {
		t.Fatalf("want 0.70, got %v", got)
	}
}

func TestComputeRawLoad_MemoryBelowThresholdDoesNotCount(t *testing.T) {
	got := computeRawLoad(0.30, 0.10, 0.80, 0.85)
	if !approxEq(got, 0.30) {
		t.Fatalf("want 0.30, got %v", got)
	}
}

func TestComputeRawLoad_MemoryPenaltyEscalatesAboveThreshold(t *testing.T) {
	// mem 0.925, soft 0.85 => penalty = (0.925-0.85)/(1-0.85) = 0.5; flow max 0.10 => 0.5
	got := computeRawLoad(0.10, 0.05, 0.925, 0.85)
	if !approxEq(got, 0.5) {
		t.Fatalf("want 0.5, got %v", got)
	}
}

func TestComputeRawLoad_ClampsTo01(t *testing.T) {
	if got := computeRawLoad(1.5, 0, 0, 0.85); !approxEq(got, 1.0) {
		t.Fatalf("want clamp to 1.0, got %v", got)
	}
	if got := computeRawLoad(-0.2, -0.3, 0, 0.85); !approxEq(got, 0.0) {
		t.Fatalf("want clamp to 0.0, got %v", got)
	}
}

func TestSystemLoadSampler_EWMASmoothing(t *testing.T) {
	calls := []float64{1.0, 0.0}
	i := 0
	s := &SystemLoadSampler{
		memSoftThreshold: 0.85,
		alpha:            0.5,
		readCPU:          func() float64 { v := calls[i]; return v * 100 },
		readIOWait:       func() float64 { return 0 },
		readMemRatio:     func() float64 { return 0 },
		cpuNum:           func() int { return 1 },
		now:              func() time.Time { return time.Unix(0, 0) },
	}
	if got := s.Sample(); !approxEq(got, 1.0) {
		t.Fatalf("first sample want 1.0, got %v", got)
	}
	i = 1
	if got := s.Sample(); !approxEq(got, 0.5) {
		t.Fatalf("second sample want 0.5, got %v", got)
	}
}

func TestNewSystemLoadSampler_DefaultsForOutOfRange(t *testing.T) {
	cases := []struct {
		name      string
		memSoft   float64
		alpha     float64
		wantMem   float64
		wantAlpha float64
	}{
		{"both valid", 0.9, 0.3, 0.9, 0.3},
		{"mem too low", 0, 0.3, 0.85, 0.3},
		{"mem too high", 1.0, 0.3, 0.85, 0.3},
		{"alpha zero", 0.9, 0, 0.9, 0.5},
		{"alpha above one", 0.9, 1.5, 0.9, 0.5},
		{"alpha one is valid", 0.9, 1.0, 0.9, 1.0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := NewSystemLoadSampler(tc.memSoft, tc.alpha)
			if !approxEq(s.memSoftThreshold, tc.wantMem) {
				t.Errorf("memSoftThreshold: want %v, got %v", tc.wantMem, s.memSoftThreshold)
			}
			if !approxEq(s.alpha, tc.wantAlpha) {
				t.Errorf("alpha: want %v, got %v", tc.wantAlpha, s.alpha)
			}
		})
	}
}

func TestSystemLoadSampler_IOWaitRate(t *testing.T) {
	// Cumulative iowait counter: 0s then 2s, 1 core, 1s elapsed between samples
	// => second-sample ioFrac = (2-0)/(1*1) = 1.0 (clamped). alpha=1 => ewma=raw.
	iowaitSeq := []float64{0, 2}
	timeSeq := []time.Time{time.Unix(100, 0), time.Unix(101, 0)}
	i := 0
	s := &SystemLoadSampler{
		memSoftThreshold: 0.85,
		alpha:            1.0, // no smoothing, ewma == raw
		readCPU:          func() float64 { return 0 },
		readIOWait:       func() float64 { return iowaitSeq[i] },
		readMemRatio:     func() float64 { return 0 },
		cpuNum:           func() int { return 1 },
		now:              func() time.Time { return timeSeq[i] },
	}
	if got := s.Sample(); got != 0 { // first sample: no prior, ioFrac=0, cpu=0
		t.Fatalf("first sample want 0, got %v", got)
	}
	i = 1
	if got := s.Sample(); !approxEq(got, 1.0) { // (2-0)/(1*1)=1.0
		t.Fatalf("second sample want iowait rate 1.0, got %v", got)
	}
}

func TestSystemLoadSampler_IOWaitRate_HalfBusyTwoCores(t *testing.T) {
	// delta 1s iowait over 1s wall on 2 cores => 1/(1*2)=0.5
	iowaitSeq := []float64{0, 1}
	timeSeq := []time.Time{time.Unix(100, 0), time.Unix(101, 0)}
	i := 0
	s := &SystemLoadSampler{
		memSoftThreshold: 0.85,
		alpha:            1.0,
		readCPU:          func() float64 { return 0 },
		readIOWait:       func() float64 { return iowaitSeq[i] },
		readMemRatio:     func() float64 { return 0 },
		cpuNum:           func() int { return 2 },
		now:              func() time.Time { return timeSeq[i] },
	}
	s.Sample()
	i = 1
	if got := s.Sample(); !approxEq(got, 0.5) {
		t.Fatalf("want 0.5, got %v", got)
	}
}
