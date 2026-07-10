// Copyright (C) 2025 Zilliz. All rights reserved.
//
// This file is part of the Woodpecker project.
//
// Woodpecker is dual-licensed under the GNU Affero General Public License v3.0
// (AGPLv3) and the Server Side Public License v1 (SSPLv1). You may use this
// file under either license, at your option.
//
// AGPLv3 License: https://www.gnu.org/licenses/agpl-3.0.html
// SSPLv1 License: https://www.mongodb.com/licensing/server-side-public-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under these licenses is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the license texts for specific language governing permissions and
// limitations under the licenses.

package management

import (
	"net/http"
	"time"

	"go.uber.org/zap"

	"github.com/zilliztech/woodpecker/common/logger"
)

// auditResponseWriter captures the status code written by the wrapped handler.
type auditResponseWriter struct {
	http.ResponseWriter
	status int
}

func (w *auditResponseWriter) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

// WithAuditLog wraps a mutating admin handler and emits one INFO audit line per
// non-GET request: who called (remote addr), what (method + path), and the
// outcome (HTTP status, duration). GET requests pass through unlogged so that
// polling endpoints (progress, status) do not flood the log.
func WithAuditLog(path string, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			next(w, r)
			return
		}
		aw := &auditResponseWriter{ResponseWriter: w, status: http.StatusOK}
		start := time.Now()
		next(aw, r)
		logger.Ctx(r.Context()).Info("admin request handled",
			zap.String("method", r.Method),
			zap.String("path", path),
			zap.String("remoteAddr", r.RemoteAddr),
			zap.Int("status", aw.status),
			zap.Duration("duration", time.Since(start)))
	}
}
