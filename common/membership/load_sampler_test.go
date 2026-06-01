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
