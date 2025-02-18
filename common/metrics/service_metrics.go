package metrics

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	wp_namespace     = "woodpecker"
	server_namespace = "wp_server"
	client_namespace = "wp_client"
)

var (
	WpRegisterOnce sync.Once

	WpAppendOpRequestsGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: wp_namespace,
			Subsystem: client_namespace,
			Name:      "append_requests",
			Help:      "The number of append requests",
		},
		[]string{"log_name"},
	)
	WpAppendOpEntriesGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: wp_namespace,
			Subsystem: client_namespace,
			Name:      "append_entries",
			Help:      "The number of append entries",
		},
		[]string{"log_name"},
	)
	WpAppendBytes = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: wp_namespace,
			Subsystem: server_namespace,
			Name:      "append_bytes",
			Help:      "bytes of append data",
		},
		[]string{"log_name"},
	)
	WpAppendReqLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: wp_namespace,
			Subsystem: server_namespace,
			Name:      "append_req_latency",
			Help:      "The latency of append requests",
		},
		[]string{"log_name"},
	)
	WpReadRequestsGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: wp_namespace,
			Subsystem: server_namespace,
			Name:      "read_requests",
			Help:      "The number of read requests",
		},
		[]string{"log_name"},
	)
	WpReadEntriesGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: wp_namespace,
			Subsystem: server_namespace,
			Name:      "read_entries",
			Help:      "The number of read entries",
		},
		[]string{"log_name"},
	)
	WpReadBytes = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: wp_namespace,
			Subsystem: server_namespace,
			Name:      "read_bytes",
			Help:      "bytes of read data",
		},
		[]string{"log_name"},
	)
	WpReadReqLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: wp_namespace,
			Subsystem: server_namespace,
			Name:      "read_req_latency",
			Help:      "The latency of read requests",
		},
		[]string{"log_name"},
	)
	WpFragmentBufferBytes = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: wp_namespace,
			Subsystem: server_namespace,
			Name:      "fragment_buffer_bytes",
			Help:      "The Size of fragment buffer bytes",
		},
		[]string{"log_name"},
	)
	WpFragmentLoadedGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: wp_namespace,
			Subsystem: server_namespace,
			Name:      "fragment_loaded",
			Help:      "The number of loaded fragments",
		},
		[]string{"log_name"},
	)
	WpFragmentFlushBytes = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: wp_namespace,
			Subsystem: server_namespace,
			Name:      "fragment_flush_bytes",
			Help:      "bytes of fragment flush data",
		},
		[]string{"log_name"},
	)
	WpFragmentFlushLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: wp_namespace,
			Subsystem: server_namespace,
			Name:      "fragment_flush_latency",
			Help:      "The latency of fragment flush",
		},
		[]string{"log_name"},
	)
	WpWriteBufferSlots = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: wp_namespace,
			Subsystem: server_namespace,
			Name:      "write_buffer_slots",
			Help:      "The Size of write buffer slots",
		},
		[]string{"log_name"},
	)
	WpSegmentRollingLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: wp_namespace,
			Subsystem: server_namespace,
			Name:      "segment_rolling_latency",
			Help:      "The latency of segment rolling",
		},
		[]string{"log_name"},
	)
)

func RegisterWoodpeckerWithRegisterer(registerer prometheus.Registerer) {
	WpRegisterOnce.Do(func() {
		// for append
		registerer.MustRegister(WpAppendOpRequestsGauge)
		registerer.MustRegister(WpAppendOpEntriesGauge)
		registerer.MustRegister(WpAppendBytes)
		registerer.MustRegister(WpAppendReqLatency)

		// for read
		registerer.MustRegister(WpReadRequestsGauge)
		registerer.MustRegister(WpReadEntriesGauge)
		registerer.MustRegister(WpReadBytes)
		registerer.MustRegister(WpReadReqLatency)

		// for fragment
		registerer.MustRegister(WpFragmentBufferBytes)
		registerer.MustRegister(WpFragmentLoadedGauge)
		registerer.MustRegister(WpFragmentFlushBytes)
		registerer.MustRegister(WpFragmentFlushLatency)

		// for write buffer
		registerer.MustRegister(WpWriteBufferSlots)

		// for segment
		registerer.MustRegister(WpSegmentRollingLatency)
	})
}

// RegisterWoodpecker register wp metrics
func RegisterWoodpecker(registry *prometheus.Registry) {
	RegisterWoodpeckerWithRegisterer(registry)
}
