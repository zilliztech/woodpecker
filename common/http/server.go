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

import (
	"context"
	"fmt"
	"net/http"
	_ "net/http/pprof" // automatically registers pprof handlers
	"os"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"

	"github.com/zilliztech/woodpecker/common/config"
	"github.com/zilliztech/woodpecker/common/http/health"
	"github.com/zilliztech/woodpecker/common/http/management"
	"github.com/zilliztech/woodpecker/common/logger"
)

// LogstoreCallbacks holds callbacks for logstore admin endpoints.
type LogstoreCallbacks struct {
	ListSegments func(logID *int64, writable *bool) []any
	GetSegment   func(logID, segmentID int64) (any, error)
	ForceFlush   func(logID, segmentID int64) error
	ForceFence   func(logID, segmentID int64, reason string) error
	ForceCompact func(logID, segmentID int64) error
}

// OpsCallbacks holds callbacks for ops admin endpoints.
type OpsCallbacks struct {
	List  func(params map[string]string) any
	Get   func(opID string) any
	Stats func() any
}

// AdminCallbacks holds callbacks for admin HTTP endpoints.
type AdminCallbacks struct {
	// Phase 1 callbacks
	GetMemberlistStatus     func() string
	GetMemberlistJSON       func() []byte
	GetNodeStatus           func() any
	Decommission            func() error
	GetDecommissionProgress func() any
	CancelDecommission      func() error
	GetConfig               func() any
	GetLogHealth            management.LogHealthCallback

	// Phase 2 callbacks
	Logstore            LogstoreCallbacks
	Ops                 OpsCallbacks
	MarkLogDeleted      func(bucketName, rootPath string, logId int64) error
	MarkInstanceDeleted func(bucketName, rootPath string) error
}

const (
	DefaultListenPort = "9091"
	ListenPortEnvKey  = "METRICS_PORT"

	DefaultPprofEnable = true
	PprofEnableEnvKey  = "PPROF_ENABLE"
)

var (
	metricsServer *http.ServeMux
	server        *http.Server
)

// Provide alias for native http package
// avoiding import alias when using http package

type (
	ResponseWriter = http.ResponseWriter
	Request        = http.Request
)

type Handler struct {
	Path        string
	HandlerFunc http.HandlerFunc
	Handler     http.Handler
}

func Register(h *Handler) {
	if metricsServer == nil {
		pprofEnableKey := os.Getenv(PprofEnableEnvKey)
		if pprofEnableKey == "true" || pprofEnableKey == "" {
			// 'net/http/pprof' will register pprof handler to DefaultServeMux by default
			metricsServer = http.DefaultServeMux
		} else {
			metricsServer = http.NewServeMux()
		}
	}
	if h.HandlerFunc != nil {
		metricsServer.HandleFunc(h.Path, h.HandlerFunc)
		return
	}
	if h.Handler != nil {
		metricsServer.Handle(h.Path, h.Handler)
	}
}

func registerDefaults(cfg *config.Configuration) {
	// Register health check endpoint
	Register(&Handler{
		Path:    HealthRouterPath,
		Handler: health.Handler(),
	})

	// Register metrics endpoint
	Register(&Handler{
		Path:    MetricsRouterPath,
		Handler: promhttp.Handler(),
	})

	// Register log level endpoint
	Register(&Handler{
		Path:        LogLevelRouterPath,
		HandlerFunc: management.NewLogLevelHandler(),
	})
}

// newMemberlistHandler serves /admin/memberlist with content negotiation.
// Returns JSON when Accept: application/json, plain text otherwise.
func newMemberlistHandler(callbacks AdminCallbacks) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		accept := r.Header.Get("Accept")
		if accept == "application/json" && callbacks.GetMemberlistJSON != nil {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(callbacks.GetMemberlistJSON())
			return
		}
		if callbacks.GetMemberlistStatus != nil {
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
			_, _ = w.Write([]byte(callbacks.GetMemberlistStatus()))
		}
	}
}

// Start initializes and starts the HTTP server
func Start(cfg *config.Configuration, callbacks AdminCallbacks) error {
	// Register default handlers
	registerDefaults(cfg)

	// Register admin handler for memberlist status (with content negotiation)
	Register(&Handler{
		Path:        AdminMemberlistPath,
		HandlerFunc: newMemberlistHandler(callbacks),
	})

	// Register node lifecycle admin handlers (if callbacks provided)
	if callbacks.GetNodeStatus != nil {
		Register(&Handler{
			Path:        AdminNodeStatusPath,
			HandlerFunc: management.NewNodeStatusHandler(callbacks.GetNodeStatus),
		})
	}
	if callbacks.Decommission != nil {
		Register(&Handler{
			Path:        AdminNodeDecommissionPath,
			HandlerFunc: management.NewNodeDecommissionHandler(callbacks.Decommission),
		})
	}
	if callbacks.GetDecommissionProgress != nil {
		Register(&Handler{
			Path:        AdminNodeDecommissionProgressPath,
			HandlerFunc: management.NewNodeDecommissionProgressHandler(callbacks.GetDecommissionProgress),
		})
	}
	if callbacks.CancelDecommission != nil {
		Register(&Handler{
			Path:        AdminNodeDecommissionCancelPath,
			HandlerFunc: management.NewNodeCancelDecommissionHandler(callbacks.CancelDecommission),
		})
	}
	if callbacks.GetConfig != nil {
		Register(&Handler{
			Path:        AdminConfigPath,
			HandlerFunc: management.NewConfigHandler(callbacks.GetConfig),
		})
	}
	if callbacks.GetLogHealth != nil {
		Register(&Handler{
			Path:        AdminLogHealthPath,
			HandlerFunc: management.NewLogHealthHandler(callbacks.GetLogHealth),
		})
	}

	// /admin/env is self-contained — no callback needed.
	Register(&Handler{
		Path:        AdminEnvPath,
		HandlerFunc: management.NewEnvHandler(),
	})

	// Register logstore admin handlers (Phase 2)
	if callbacks.Logstore.ListSegments != nil {
		Register(&Handler{
			Path:        AdminLogstoreSegmentsPath,
			HandlerFunc: management.NewLogstoreSegmentsHandler(callbacks.Logstore.ListSegments),
		})
	}
	if callbacks.Logstore.GetSegment != nil {
		Register(&Handler{
			Path:        AdminLogstoreSegmentsPath + "/detail",
			HandlerFunc: management.NewLogstoreSegmentShowHandler(callbacks.Logstore.GetSegment),
		})
	}
	if callbacks.Logstore.ForceFlush != nil {
		Register(&Handler{
			Path:        AdminLogstoreFlushPath,
			HandlerFunc: management.NewLogstoreFlushHandler(callbacks.Logstore.ForceFlush),
		})
	}
	if callbacks.Logstore.ForceFence != nil {
		Register(&Handler{
			Path:        AdminLogstoreFencePath,
			HandlerFunc: management.NewLogstoreFenceHandler(callbacks.Logstore.ForceFence),
		})
	}
	if callbacks.Logstore.ForceCompact != nil {
		Register(&Handler{
			Path:        AdminLogstoreCompactPath,
			HandlerFunc: management.NewLogstoreCompactHandler(callbacks.Logstore.ForceCompact),
		})
	}

	// Register log/instance delete admin handlers (Phase 2)
	if callbacks.MarkLogDeleted != nil {
		Register(&Handler{
			Path:        AdminLogDeletePath,
			HandlerFunc: management.NewLogDeleteHandler(callbacks.MarkLogDeleted),
		})
	}
	if callbacks.MarkInstanceDeleted != nil {
		Register(&Handler{
			Path:        AdminInstanceDeletePath,
			HandlerFunc: management.NewInstanceDeleteHandler(callbacks.MarkInstanceDeleted),
		})
	}

	// Register ops admin handlers (Phase 2)
	if callbacks.Ops.List != nil {
		Register(&Handler{
			Path:        AdminRuntimeOpsPath,
			HandlerFunc: management.NewOpsListHandler(callbacks.Ops.List),
		})
	}
	if callbacks.Ops.Get != nil {
		// ops get uses a separate path with query param
		Register(&Handler{
			Path:        AdminRuntimeOpsPath + "/get",
			HandlerFunc: management.NewOpsGetHandler(callbacks.Ops.Get),
		})
	}
	if callbacks.Ops.Stats != nil {
		Register(&Handler{
			Path:        AdminRuntimeOpsStatsPath,
			HandlerFunc: management.NewOpsStatsHandler(callbacks.Ops.Stats),
		})
	}

	// Get listen port from environment or use default
	port := os.Getenv(ListenPortEnvKey)
	if port == "" {
		port = DefaultListenPort
	}

	addr := fmt.Sprintf(":%s", port)
	server = &http.Server{
		Addr:         addr,
		Handler:      metricsServer,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	logger.Ctx(context.Background()).Info("Starting HTTP server",
		zap.String("addr", addr),
		zap.Bool("pprof_enabled", metricsServer == http.DefaultServeMux))

	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Ctx(context.Background()).Error("HTTP server failed", zap.Error(err))
		}
	}()

	return nil
}

// Stop gracefully shuts down the HTTP server
func Stop() error {
	if server == nil {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	logger.Ctx(ctx).Info("Stopping HTTP server")
	if err := server.Shutdown(ctx); err != nil {
		logger.Ctx(ctx).Error("HTTP server shutdown failed", zap.Error(err))
		return err
	}

	logger.Ctx(ctx).Info("HTTP server stopped successfully")
	return nil
}
