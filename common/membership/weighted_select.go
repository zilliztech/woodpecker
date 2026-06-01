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

// weightedSampleIndices selects up to `limit` distinct indices from `weights`
// with probability proportional to each weight (sampling without replacement).
// randFloat must return a value in [0,1). When all remaining weights are zero,
// it falls back to a uniform pick among the remaining indices. Deterministic
// for a given randFloat, which makes it unit-testable.
func weightedSampleIndices(weights []float64, limit int, randFloat func() float64) []int {
	n := len(weights)
	if n == 0 {
		return nil
	}
	if limit <= 0 || limit > n {
		limit = n
	}

	w := make([]float64, n)
	copy(w, weights)
	idx := make([]int, n)
	for i := range idx {
		idx[i] = i
	}

	result := make([]int, 0, limit)
	remaining := n
	for k := 0; k < limit; k++ {
		total := 0.0
		for i := 0; i < remaining; i++ {
			if w[i] > 0 {
				total += w[i]
			}
		}

		var chosen int
		if total <= 0 {
			j := int(randFloat() * float64(remaining))
			if j >= remaining {
				j = remaining - 1
			}
			chosen = j
		} else {
			r := randFloat() * total
			cum := 0.0
			chosen = remaining - 1
			for i := 0; i < remaining; i++ {
				if w[i] <= 0 {
					continue
				}
				cum += w[i]
				if r < cum {
					chosen = i
					break
				}
			}
		}

		result = append(result, idx[chosen])
		// swap-remove the chosen element
		w[chosen] = w[remaining-1]
		idx[chosen] = idx[remaining-1]
		remaining--
	}
	return result
}
