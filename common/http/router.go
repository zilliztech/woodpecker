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

package http

// HealthRouterPath is default path for check health state.
const HealthRouterPath = "/healthz"

// MetricsRouterPath is path for Prometheus metrics.
const MetricsRouterPath = "/metrics"

// LogLevelRouterPath is path for Get and Update log level at runtime.
const LogLevelRouterPath = "/log/level"

// Pprof paths are automatically registered by importing net/http/pprof
// Available paths:
// - /debug/pprof/
// - /debug/pprof/cmdline
// - /debug/pprof/profile
// - /debug/pprof/symbol
// - /debug/pprof/trace
// - /debug/pprof/heap
// - /debug/pprof/goroutine
// - /debug/pprof/threadcreate
// - /debug/pprof/block
// - /debug/pprof/mutex
