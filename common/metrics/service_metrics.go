package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

const (
	wp_namespace     = "woodpecker"
	server_namespace = "wp_server"
	client_namespace = "wp_client"
)

var (
	WpAppendRequestsCounter = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: wp_namespace,
			Subsystem: server_namespace,
			Name:      "append_requests",
			Help:      "The number of append requests",
		},
		[]string{"log_name"},
	)
	WpAppendEntriesCounter = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: wp_namespace,
			Subsystem: server_namespace,
			Name:      "append_entries",
			Help:      "The number of append entries",
		},
		[]string{"log_name"},
	)
	WpAppendBytesCounter = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: wp_namespace,
			Subsystem: server_namespace,
			Name:      "append_bytes",
			Help:      "The size of append data",
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
	WpReadRequestsCounter = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: wp_namespace,
			Subsystem: server_namespace,
			Name:      "read_requests",
			Help:      "The number of read requests",
		},
		[]string{"log_name"},
	)
	WpReadEntriesCounter = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: wp_namespace,
			Subsystem: server_namespace,
			Name:      "read_entries",
			Help:      "The number of read entries",
		},
		[]string{"log_name"},
	)
	WpReadBytesCounter = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: wp_namespace,
			Subsystem: server_namespace,
			Name:      "read_bytes",
			Help:      "The size of read data",
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
)

func RegisterWoodpeckerWithRegisterer(registerer prometheus.Registerer) {
	// for append
	registerer.MustRegister(WpAppendRequestsCounter)
	registerer.MustRegister(WpAppendEntriesCounter)
	registerer.MustRegister(WpAppendBytesCounter)
	registerer.MustRegister(WpAppendReqLatency)

	// for read
	registerer.MustRegister(WpReadRequestsCounter)
	registerer.MustRegister(WpReadEntriesCounter)
	registerer.MustRegister(WpReadBytesCounter)
	registerer.MustRegister(WpReadReqLatency)
}

// RegisterWoodpecker register wp metrics
func RegisterWoodpecker(registry *prometheus.Registry) {
	RegisterWoodpeckerWithRegisterer(registry)
}
