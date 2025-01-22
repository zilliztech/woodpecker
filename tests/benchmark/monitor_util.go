package benchmark

import (
	"crypto/rand"
	"fmt"
	"net/http"
	"net/http/pprof"
	"sync/atomic"
	"time"

	"github.com/google/gops/agent"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/zilliztech/woodpecker/common/metrics"
)

func startGopsAgent() {
	// start gops agent
	if err := agent.Listen(agent.Options{}); err != nil {
		panic(err)
	}
	http.HandleFunc("/pprof/cmdline", pprof.Cmdline)
	http.HandleFunc("/pprof/profile", pprof.Profile)
	http.HandleFunc("/pprof/symbol", pprof.Symbol)
	http.HandleFunc("/pprof/trace", pprof.Trace)
	go func() {
		fmt.Println("Starting gops agent on :6060")
		http.ListenAndServe(":6060", nil)
	}()
}

var testMetricsRegistry prometheus.Registerer

var (
	MinioPutBytes = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "minio",
			Subsystem: "test",
			Name:      "put_bytes",
			Help:      "bytes of put data",
		},
		[]string{"thread_id"},
	)
	MinioPutLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "minio",
			Subsystem: "test",
			Name:      "put_latency",
			Help:      "The latency of put operation",
		},
		[]string{"thread_id"},
	)
)

func startMetrics() {
	testMetricsRegistry = prometheus.DefaultRegisterer
	metrics.RegisterWoodpeckerWithRegisterer(testMetricsRegistry)
	testMetricsRegistry.MustRegister(MinioPutBytes)
	testMetricsRegistry.MustRegister(MinioPutLatency)

	// start metrics
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		http.ListenAndServe(":29092", nil)
	}()
	//go recordMetrics()
}

//func recordMetrics() {
//	go func() {
//		for {
//			metrics.WpAppendRequestsCounter.WithLabelValues("testmylog").Inc()
//			time.Sleep(2 * time.Second)
//		}
//	}()
//}
//func TestAsyncWritePerformance2(t *testing.T) {
//	startMetrics()
//	for {
//		time.Sleep(1 * time.Second)
//	}
//}

var totalBytes atomic.Int64

func startReporting() {
	go func(total *atomic.Int64) {
		ticker := time.NewTicker(time.Duration(1000 * int(time.Millisecond)))
		defer ticker.Stop()
		lastTime := time.Now().UnixMilli()
		lastTotal := total.Load()
		for {
			select {
			case <-ticker.C:
				currentTime := time.Now().UnixMilli()
				currentTotal := total.Load()
				rate := float64(currentTotal-lastTotal) / float64(currentTime-lastTime) * 1000 / 1_000_000
				fmt.Printf("put bytes Rate: %f MB/s \n", rate)
			}
		}

	}(&totalBytes)
}

func generateRandomBytes(length int) ([]byte, error) {
	randomBytes := make([]byte, length)
	_, err := rand.Read(randomBytes)
	if err != nil {
		return nil, err
	}
	return randomBytes, nil
}
