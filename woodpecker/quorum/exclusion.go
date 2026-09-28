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

package quorum

import (
	"context"

	"github.com/zilliztech/woodpecker/proto"
)

type excludedEndpointsKey struct{}

// WithExcludedEndpoints asks the quorum selection made with the returned
// context to leave out the given service endpoints, typically the replicas the
// previous segment could not reach.
//
// The exclusion is soft and one-off. It applies to this one selection, and it
// is dropped rather than allowed to fail the selection when too few nodes
// remain without it: the membership view may still list those nodes, and they
// may well be back by the time the next segment is selected.
func WithExcludedEndpoints(ctx context.Context, endpoints []string) context.Context {
	if len(endpoints) == 0 {
		return ctx
	}
	return context.WithValue(ctx, excludedEndpointsKey{}, append([]string(nil), endpoints...))
}

func excludedEndpointsFrom(ctx context.Context) []string {
	endpoints, _ := ctx.Value(excludedEndpointsKey{}).([]string)
	return endpoints
}

func setExclusion(filters []*proto.NodeFilter, excluded []string) {
	for _, f := range filters {
		f.ExcludeEndpoints = excluded
	}
}

func isExcluded(endpoint string, excluded []string) bool {
	for _, e := range excluded {
		if e == endpoint {
			return true
		}
	}
	return false
}

// withoutExcluded drops excluded nodes and reports how many were dropped. A
// server that predates ExcludeEndpoints ignores it, so the answer is checked
// here as well.
func withoutExcluded(nodes []*proto.NodeMeta, excluded []string) ([]*proto.NodeMeta, int) {
	if len(excluded) == 0 {
		return nodes, 0
	}
	kept := make([]*proto.NodeMeta, 0, len(nodes))
	for _, n := range nodes {
		if !isExcluded(n.GetEndpoint(), excluded) {
			kept = append(kept, n)
		}
	}
	return kept, len(nodes) - len(kept)
}

// seedsExcludedLast moves excluded seeds to the end: a seed that could not be
// reached as a replica is unlikely to answer a selection either, but it is
// still tried if every other seed fails.
func seedsExcludedLast(seeds []string, excluded []string) []string {
	if len(excluded) == 0 {
		return seeds
	}
	ordered := make([]string, 0, len(seeds))
	var last []string
	for _, s := range seeds {
		if isExcluded(s, excluded) {
			last = append(last, s)
			continue
		}
		ordered = append(ordered, s)
	}
	return append(ordered, last...)
}
