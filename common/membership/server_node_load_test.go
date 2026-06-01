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
	"testing"

	"github.com/zilliztech/woodpecker/proto"
)

type fakeSampler struct{ v float64 }

func (f fakeSampler) Sample() float64 { return f.v }

func TestReportLoadOnce_PublishesLoadToMeta(t *testing.T) {
	meta := &proto.NodeMeta{NodeId: "n1"}
	delegate := NewServerDelegate(meta)
	n := &ServerNode{
		delegate: delegate,
		meta:     meta,
		sampler:  fakeSampler{v: 0.37},
	}
	// publishLoad does the sampler->delegate->meta part without memberlist/discovery.
	n.publishLoad()
	if got := meta.GetLoadFactor(); got != 0.37 {
		t.Fatalf("want load 0.37 published, got %v", got)
	}
	if meta.GetLoadUpdatedAt() == 0 {
		t.Fatalf("load_updated_at should be stamped")
	}
}
