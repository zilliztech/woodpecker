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

package server

import (
	"context"
	"net"
	"sync/atomic"

	"go.uber.org/zap"

	"github.com/zilliztech/woodpecker/common/logger"
)

// LimitedListener wraps a net.Listener and limits the number of concurrent connections.
type LimitedListener struct {
	net.Listener
	sem           chan struct{}
	maxConns      int
	activeConns   atomic.Int64
	rejectedConns atomic.Int64
}

// NewLimitedListener creates a new LimitedListener that allows at most maxConns
// concurrent connections on the wrapped listener.
func NewLimitedListener(inner net.Listener, maxConns int) *LimitedListener {
	return &LimitedListener{
		Listener: inner,
		sem:      make(chan struct{}, maxConns),
		maxConns: maxConns,
	}
}

// Accept waits for and returns the next connection to the listener.
// It blocks if the maximum number of concurrent connections has been reached.
func (l *LimitedListener) Accept() (net.Conn, error) {
	// Try non-blocking acquire first; if full, log and block
	select {
	case l.sem <- struct{}{}:
	default:
		// At limit - log warning, then block until slot available
		logger.Ctx(context.Background()).Warn("connection limit reached, waiting for slot",
			zap.Int("maxConns", l.maxConns),
			zap.Int64("active", l.activeConns.Load()),
			zap.Int64("totalRejectedWaits", l.rejectedConns.Add(1)))
		l.sem <- struct{}{} // blocking
	}

	conn, err := l.Listener.Accept()
	if err != nil {
		<-l.sem // release on error
		return nil, err
	}

	l.activeConns.Add(1)
	return &limitedConn{Conn: conn, onClose: l.release}, nil
}

func (l *LimitedListener) release() {
	l.activeConns.Add(-1)
	<-l.sem
}

// GetActiveConns returns the current number of active connections.
func (l *LimitedListener) GetActiveConns() int64 {
	return l.activeConns.Load()
}

// limitedConn wraps a net.Conn to release the semaphore slot on close.
type limitedConn struct {
	net.Conn
	onClose func()
	closed  atomic.Bool
}

// Close closes the connection and releases the semaphore slot.
func (c *limitedConn) Close() error {
	if c.closed.CompareAndSwap(false, true) {
		c.onClose()
	}
	return c.Conn.Close()
}
