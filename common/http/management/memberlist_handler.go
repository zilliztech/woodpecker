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

package management

import "net/http"

// NewMemberlistHandler serves GET /admin/memberlist with content negotiation:
// JSON when the request asks for it, human-readable text otherwise.
//
// Either callback may be nil, in which case that representation is not served.
func NewMemberlistHandler(getJSON func() []byte, getText func() string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		accept := r.Header.Get("Accept")
		if accept == "application/json" && getJSON != nil {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(getJSON())
			return
		}
		if getText != nil {
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
			_, _ = w.Write([]byte(getText()))
		}
	}
}
