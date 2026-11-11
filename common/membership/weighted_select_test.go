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
	"reflect"
	"testing"
)

// seqRand returns a randFloat func that yields the given sequence then repeats the last.
func seqRand(vals ...float64) func() float64 {
	i := 0
	return func() float64 {
		v := vals[i]
		if i < len(vals)-1 {
			i++
		}
		return v
	}
}

func TestWeightedSampleIndices_ZeroWeightNeverChosenWhenOthersPositive(t *testing.T) {
	got := weightedSampleIndices([]float64{0, 0, 1}, 1, seqRand(0.5))
	if !reflect.DeepEqual(got, []int{2}) {
		t.Fatalf("want [2], got %v", got)
	}
}

func TestWeightedSampleIndices_RZeroPicksFirstPositive(t *testing.T) {
	got := weightedSampleIndices([]float64{0.2, 0.8}, 1, seqRand(0.0))
	if !reflect.DeepEqual(got, []int{0}) {
		t.Fatalf("want [0], got %v", got)
	}
}

func TestWeightedSampleIndices_WithoutReplacement(t *testing.T) {
	got := weightedSampleIndices([]float64{1, 1, 1}, 2, seqRand(0.0))
	if len(got) != 2 {
		t.Fatalf("want 2 picks, got %v", got)
	}
	if got[0] == got[1] {
		t.Fatalf("picks must be distinct, got %v", got)
	}
}

func TestWeightedSampleIndices_LimitClampedToLen(t *testing.T) {
	got := weightedSampleIndices([]float64{1, 1}, 5, seqRand(0.0))
	if len(got) != 2 {
		t.Fatalf("want 2 picks (clamped), got %v", got)
	}
}

func TestWeightedSampleIndices_AllZeroFallsBackToUniform(t *testing.T) {
	got := weightedSampleIndices([]float64{0, 0, 0}, 1, seqRand(0.0))
	if len(got) != 1 {
		t.Fatalf("want 1 pick, got %v", got)
	}
}

func TestWeightedSampleIndices_EmptyAndZeroLimit(t *testing.T) {
	if got := weightedSampleIndices(nil, 1, seqRand(0.0)); got != nil {
		t.Fatalf("nil weights should return nil, got %v", got)
	}
	if got := weightedSampleIndices([]float64{1, 1}, 0, seqRand(0.0)); len(got) != 2 {
		t.Fatalf("limit<=0 means all, want 2, got %v", got)
	}
}
