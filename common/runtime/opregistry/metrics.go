package opregistry

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	// OpRegistryEvictedAgeSeconds tracks the age distribution of evicted ops.
	OpRegistryEvictedAgeSeconds = prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace: "woodpecker",
		Subsystem: "server",
		Name:      "op_registry_evicted_age_seconds",
		Help:      "Age in seconds of evicted ops — distribution reveals stall vs throughput",
		Buckets:   []float64{0.1, 0.5, 1, 5, 10, 30, 60, 120, 300},
	})

	// OpRegistryEvictedTotal counts evictions by signal type.
	// signal="young" means normal throughput (age < warnAge).
	// signal="old" means stall indicator (age >= warnAge).
	OpRegistryEvictedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "woodpecker",
		Subsystem: "server",
		Name:      "op_registry_evicted_total",
		Help:      "Total evictions by signal type: young (normal) or old (stall indicator)",
	}, []string{"signal"})

	// OpRegistryPoolUsage tracks the current number of in-flight ops.
	OpRegistryPoolUsage = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "woodpecker",
		Subsystem: "server",
		Name:      "op_registry_pool_usage",
		Help:      "Current number of in-flight ops in the registry",
	})
)

// RegisterMetrics registers op registry Prometheus metrics.
func RegisterMetrics(registerer prometheus.Registerer) {
	registerer.MustRegister(OpRegistryEvictedAgeSeconds)
	registerer.MustRegister(OpRegistryEvictedTotal)
	registerer.MustRegister(OpRegistryPoolUsage)
}

// WirePrometheusCallbacks sets up the default eviction callback that updates
// Prometheus metrics, and a pool-size update hook. Call once after New().
func WirePrometheusCallbacks(r *Registry) {
	r.SetOnEvict(func(age time.Duration, isOld bool) {
		OpRegistryEvictedAgeSeconds.Observe(age.Seconds())
		if isOld {
			OpRegistryEvictedTotal.WithLabelValues("old").Inc()
		} else {
			OpRegistryEvictedTotal.WithLabelValues("young").Inc()
		}
	})
	r.mu.Lock()
	r.onPoolChange = func(size int) {
		OpRegistryPoolUsage.Set(float64(size))
	}
	r.mu.Unlock()
}
