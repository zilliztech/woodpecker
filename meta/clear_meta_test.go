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

package meta

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
)

// resp builds the four Get responses InitIfNecessary inspects, in its key order:
// instance, version, logidgen, quorumidgen. A true entry means the key exists.
func resp(present ...bool) []*etcdserverpb.ResponseOp {
	out := make([]*etcdserverpb.ResponseOp, 0, len(present))
	for _, p := range present {
		r := &etcdserverpb.RangeResponse{}
		if p {
			r.Kvs = []*mvccpb.KeyValue{{Key: []byte("k"), Value: []byte("v")}}
		}
		out = append(out, &etcdserverpb.ResponseOp{
			Response: &etcdserverpb.ResponseOp_ResponseRange{ResponseRange: r},
		})
	}
	return out
}

// TestOnlyLogIdGenSurvives pins the exception that makes ClearMetaExceptLogIdGen usable: a
// cleared instance keeps exactly one of the four instance-level keys, and startup must accept
// that. Every other partial combination stays an error — it means something outside ClearMeta
// wrote or removed these keys, and guessing at the intent is worse than refusing.
func TestOnlyLogIdGenSurvives(t *testing.T) {
	// instance, version, logidgen, quorumidgen
	assert.True(t, onlyLogIdGenSurvives(resp(false, false, true, false)),
		"the state ClearMetaExceptLogIdGen leaves behind must be accepted")

	assert.False(t, onlyLogIdGenSurvives(resp(false, false, false, false)), "nothing present is a fresh cluster")
	assert.False(t, onlyLogIdGenSurvives(resp(true, true, true, true)), "all present is an initialised cluster")
	assert.False(t, onlyLogIdGenSurvives(resp(true, false, true, false)), "instance also present")
	assert.False(t, onlyLogIdGenSurvives(resp(false, true, true, false)), "version also present")
	assert.False(t, onlyLogIdGenSurvives(resp(false, false, true, true)), "quorumidgen also present")
	assert.False(t, onlyLogIdGenSurvives(resp(true, false, false, false)), "only instance present")

	assert.False(t, onlyLogIdGenSurvives(resp(false, false, true)), "short response is not trusted")
	assert.False(t, onlyLogIdGenSurvives(nil), "empty response is not trusted")
}
