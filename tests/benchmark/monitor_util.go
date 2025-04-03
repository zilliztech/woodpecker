package benchmark

import (
	"crypto/rand"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/pprof"
	"strconv"
	"strings"
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
	MinioIOBytes = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "minio",
			Subsystem: "test",
			Name:      "io_bytes",
			Help:      "bytes of put data",
		},
		[]string{"thread_id"},
	)
	MinioIOLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "minio",
			Subsystem: "test",
			Name:      "io_latency",
			Help:      "The latency of put operation",
		},
		[]string{"thread_id"},
	)
)

func startMetrics() {
	testMetricsRegistry = prometheus.DefaultRegisterer
	metrics.RegisterWoodpeckerWithRegisterer(testMetricsRegistry)
	testMetricsRegistry.MustRegister(MinioIOBytes)
	testMetricsRegistry.MustRegister(MinioIOLatency)

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

func startReporting() {
	go func() {
		ticker := time.NewTicker(time.Duration(1000 * int(time.Millisecond)))
		defer ticker.Stop()
		// put bytes
		firstTimeMs := time.Now().UnixMilli()

		lastTimeMs := time.Now().UnixMilli()
		// put metrics
		var (
			lastBytesSum, lastBytesCount     float64
			lastLatencySum, lastLatencyCount float64
		)
		for {
			select {
			case <-ticker.C:
				currentTimeMs := time.Now().UnixMilli()
				currentBytesSum, currentBytesCount, currentLatencySum, currentLatencyCount, err := getCurrentMetrics()
				if err != nil {
					fmt.Printf("Failed to get metrics: %v\n", err)
					continue
				}
				elapsedTimeMs := float64(currentTimeMs - lastTimeMs)
				deltaBytesSum := currentBytesSum - lastBytesSum
				deltaBytesCount := currentBytesCount - lastBytesCount
				deltaLatencySum := currentLatencySum - lastLatencySum
				deltaLatencyCount := currentLatencyCount - lastLatencyCount
				// Print calculation results
				if deltaBytesCount > 0 {
					avgBytes := deltaBytesSum / deltaBytesCount
					rateMB := deltaBytesSum / 1_000_000 / elapsedTimeMs * 1000                           // Convert to MB/s
					avgRateMB := currentBytesSum / 1_000_000 / float64(currentTimeMs-firstTimeMs) * 1000 // Convert to MB/s
					fmt.Printf("Traffic Monitor - Average size per operation: %.2f bytes, Current throughput: %.2f MB/s, Average throughput: %.2f MB/s\n", avgBytes, rateMB, avgRateMB)
				}

				if deltaLatencyCount > 0 {
					avgLatency := deltaLatencySum / deltaLatencyCount
					totalAvgLatency := currentLatencySum / currentLatencyCount
					fmt.Printf("Operation Latency - Current: %.3f ms, Average: %.3f ms\n", avgLatency, totalAvgLatency)
				}

				// Update metrics state
				lastBytesSum = currentBytesSum
				lastBytesCount = currentBytesCount
				lastLatencySum = currentLatencySum
				lastLatencyCount = currentLatencyCount
				lastTimeMs = currentTimeMs
			}
		}

	}()
}

func printMetrics() {
	resp, err := http.Get("http://localhost:29092/metrics")
	if err != nil {
		fmt.Printf("Failed to get metrics: %v\n", err)
		return
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		fmt.Printf("Failed to read response body: %v\n", err)
		return
	}

	// Direct print
	//fmt.Println(string(body))

	// Filter to print only certain metrics
	metricsFilter := []string{
		"minio_test_put_bytes_sum", "minio_test_put_bytes_count",
		"minio_test_put_latency_sum", "minio_test_put_latency_count",
	}
	lines := strings.Split(string(body), "\n")
	var filtered []string
	for _, line := range lines {
		// Skip comments and empty lines
		if strings.HasPrefix(line, "#") || line == "" {
			continue
		}
		// Check if in filter list
		for _, prefix := range metricsFilter {
			if strings.HasPrefix(line, prefix) {
				filtered = append(filtered, line)
				break
			}
		}
	}
	fmt.Println(strings.Join(filtered, "\n"))
}

func getCurrentMetrics() (bytesSum, bytesCount, latencySum, latencyCount float64, err error) {
	resp, err := http.Get("http://localhost:29092/metrics")
	if err != nil {
		return 0, 0, 0, 0, err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return 0, 0, 0, 0, err
	}

	lines := strings.Split(string(body), "\n")

	// Aggregate metrics from all threads
	for _, line := range lines {
		switch {
		case strings.HasPrefix(line, "minio_test_io_bytes_sum"):
			if value, e := parseMetricValue(line); e == nil {
				bytesSum += value
			}
		case strings.HasPrefix(line, "minio_test_io_bytes_count"):
			if value, e := parseMetricValue(line); e == nil {
				bytesCount += value
			}
		case strings.HasPrefix(line, "minio_test_io_latency_sum"):
			if value, e := parseMetricValue(line); e == nil {
				latencySum += value
			}
		case strings.HasPrefix(line, "minio_test_io_latency_count"):
			if value, e := parseMetricValue(line); e == nil {
				latencyCount += value
			}
		}
	}

	return
}

func parseMetricValue(line string) (float64, error) {
	parts := strings.Fields(line)
	if len(parts) < 2 {
		return 0, fmt.Errorf("invalid metric line: %s", line)
	}
	return strconv.ParseFloat(parts[1], 64)
}

func generateRandomBytes(length int) ([]byte, error) {
	randomBytes := make([]byte, length)
	_, err := rand.Read(randomBytes)
	if err != nil {
		return nil, err
	}
	return randomBytes, nil
}
