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

package segment

import "go.opentelemetry.io/otel/trace"

// AppendBatch is an immutable grouping of logical appends, not an executable
// operation. Every replica may retry its entries independently of this grouping.
type AppendBatch struct{ ops []*AppendOp }

func (b *AppendBatch) traceLinks() []trace.Link {
	type identity struct {
		traceID trace.TraceID
		spanID  trace.SpanID
	}
	seen := make(map[identity]bool)
	links := make([]trace.Link, 0, len(b.ops))
	for _, op := range b.ops {
		sc := op.traceParent
		key := identity{sc.TraceID(), sc.SpanID()}
		if sc.IsValid() && !seen[key] {
			seen[key] = true
			links = append(links, trace.Link{SpanContext: sc})
		}
	}
	return links
}
