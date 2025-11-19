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
	"github.com/zilliztech/woodpecker/common/logger"
)

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
		Path: LogLevelRouterPath,
		HandlerFunc: func(w http.ResponseWriter, req *http.Request) {
			// TODO: implement log level change at runtime
			fmt.Fprintf(w, "Log level change endpoint - TODO\n")
		},
	})
}

// Start initializes and starts the HTTP server
func Start(cfg *config.Configuration, GetServerNodeMemberlistStatus func() string) error {
	// Register default handlers
	registerDefaults(cfg)

	// Register admin handler for memberlist status
	Register(&Handler{
		Path: AdminMemberlistPath,
		HandlerFunc: func(writer http.ResponseWriter, request *http.Request) {
			fmt.Fprintf(writer, GetServerNodeMemberlistStatus())
		},
	})

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
