//go:build noembedetcd

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

package etcd

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInitEtcdServer_NoEmbed(t *testing.T) {
	err := InitEtcdServer(true, "", t.TempDir(), t.TempDir()+"/etcd.log", "info")
	assert.ErrorIs(t, err, ErrEmbedEtcdNotCompiled)

	err = InitEtcdServer(false, "", "", "", "")
	assert.NoError(t, err)
	assert.False(t, HasServer())
}

func TestStartEtcdServerUnsafe_NoEmbed(t *testing.T) {
	for _, useEmbedEtcd := range []bool{true, false} {
		err := StartEtcdServerUnsafe(useEmbedEtcd, "", t.TempDir(), t.TempDir()+"/etcd.log", "info")
		assert.ErrorIs(t, err, ErrEmbedEtcdNotCompiled, "useEmbedEtcd=%v", useEmbedEtcd)
	}
	assert.False(t, HasServer())
}

func TestGetEmbedEtcdClient_NoEmbed(t *testing.T) {
	client, err := GetEmbedEtcdClient()
	assert.ErrorIs(t, err, ErrEmbedEtcdNotCompiled)
	assert.Nil(t, client)
	assert.False(t, HasServer())
}
