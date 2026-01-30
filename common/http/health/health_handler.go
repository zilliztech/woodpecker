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

package health

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/labstack/gommon/log"
	"go.uber.org/zap"
	"net/http"
	"sync"
)

const (
	Initializing = "Initializing"
	Healthy      = "Healthy"
	Abnormal     = "Abnormal"
	StandBy      = "StandBy"
	Stopping     = "Stopping"
)

const (
	// ContentTypeHeader is the health check request type header.
	ContentTypeHeader = "Content-Type"
	// ContentTypeText is the health check request type text.
	ContentTypeText = "text/plain"
	// ContentTypeJSON is another health check request type text, which response contains more info.
	ContentTypeJSON = "application/json"
)

type Indicator interface {
	GetName() string
	Health(ctx context.Context) string
}

type IndicatorState struct {
	Name string `json:"name"`
	Code string `json:"code"`
}

type HealthResponse struct {
	State  string            `json:"state"`
	Detail []*IndicatorState `json:"detail"`
}

type HealthHandler struct {
	indicators   []Indicator
	indicatorNum int

	// unregister role when call stop by restful api
	unregisterLock    sync.RWMutex
	unregisteredRoles map[string]struct{}
}

func (handler *HealthHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	resp := &HealthResponse{
		State: "OK",
	}

	unhealthyComponent := make([]string, 0)
	ctx := context.Background()
	for _, in := range handler.indicators {
		handler.unregisterLock.RLock()
		_, unregistered := handler.unregisteredRoles[in.GetName()]
		handler.unregisterLock.RUnlock()
		if unregistered {
			continue
		}
		code := in.Health(ctx)
		resp.Detail = append(resp.Detail, &IndicatorState{
			Name: in.GetName(),
			Code: code,
		})

		if code != Healthy && code != StandBy {
			unhealthyComponent = append(unhealthyComponent, in.GetName())
		}
	}

	if len(unhealthyComponent) > 0 {
		resp.State = fmt.Sprintf("Not all components are healthy, %d/%d", handler.indicatorNum-len(unhealthyComponent), handler.indicatorNum)
		log.Warn("check health failed", zap.Strings("UnhealthyComponent", unhealthyComponent))
	}

	if resp.State == "OK" {
		w.WriteHeader(http.StatusOK)
	} else {
		w.WriteHeader(http.StatusInternalServerError)
	}
	// for compatibility
	if r.Header.Get(ContentTypeHeader) != ContentTypeJSON {
		writeText(w, r, resp.State)
		return
	}

	writeJSON(w, r, resp)
}

func writeJSON(w http.ResponseWriter, r *http.Request, resp *HealthResponse) {
	w.Header().Set(ContentTypeHeader, ContentTypeJSON)
	bs, err := json.Marshal(resp)
	if err != nil {
		log.Warn("failed to send response", zap.Error(err))
	}
	w.Write(bs)
}

func writeText(w http.ResponseWriter, r *http.Request, reason string) {
	w.Header().Set(ContentTypeHeader, ContentTypeText)
	_, err := fmt.Fprint(w, reason)
	if err != nil {
		log.Warn("failed to send response",
			zap.Error(err))
	}
}

var _ http.Handler = (*HealthHandler)(nil)

var defaultHandler = HealthHandler{}

func Handler() *HealthHandler {
	return &defaultHandler
}
