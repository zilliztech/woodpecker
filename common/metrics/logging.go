package metrics

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/zilliztech/woodpecker/common/logger"
)

var loggingCollectors = []prometheus.Collector{
	prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: woodpeckerNamespace, Name: "logging_queue_depth",
		Help: "Number of items waiting for diagnostic log output.",
	}, func() float64 { return float64(logger.GetOutputStats().QueueDepth) }),
	prometheus.NewCounterFunc(prometheus.CounterOpts{
		Namespace: woodpeckerNamespace, Name: "logging_dropped_records_total",
		Help: "Diagnostic log records dropped on queue saturation or shutdown.",
	}, func() float64 { return float64(logger.GetOutputStats().Dropped) }),
	prometheus.NewCounterFunc(prometheus.CounterOpts{
		Namespace: woodpeckerNamespace, Name: "logging_write_errors_total",
		Help: "Diagnostic log writes that failed or completed only partially.",
	}, func() float64 { return float64(logger.GetOutputStats().WriteErrors) }),
}

func registerLoggingMetrics(r prometheus.Registerer) {
	for _, c := range loggingCollectors {
		if err := r.Register(c); err != nil {
			// Embedded deployments may register both roles with one registry.
			if existing, ok := err.(prometheus.AlreadyRegisteredError); !ok || existing.ExistingCollector != c {
				panic(err)
			}
		}
	}
}
