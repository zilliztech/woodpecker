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

package tracer

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/zilliztech/woodpecker/common/config"
)

func TestTracer_Init(t *testing.T) {
	cfg, err := config.NewConfiguration()
	assert.NoError(t, err)
	cfg.Trace.Exporter = "unknown"
	// init failed with unknown exporter
	err = Init(cfg, "TestTracer_Init", 1001)
	assert.Error(t, err)

	cfg.Trace.Exporter = "stdout"
	// init with stdout exporter
	err = Init(cfg, "TestTracer_Init", 1001)
	assert.NoError(t, err)

	cfg.Trace.Exporter = "noop"
	// init with noop exporter
	err = Init(cfg, "TestTracer_Init", 1001)
	assert.NoError(t, err)
}

func TestTracer_CloseProviderFailed(t *testing.T) {
	cfg, err := config.NewConfiguration()
	assert.NoError(t, err)
	cfg.Trace.Exporter = "stdout"
	// init with stdout exporter
	err = Init(cfg, "TestTracer_CloseProviderFailed", 2001)
	assert.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err = CloseTracerProvider(ctx)
	assert.Error(t, err)
}
