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

package grafana

// BuildDashboard assembles the complete Woodpecker monitoring dashboard.
// Panels are organized into 6 rows by architecture layer:
//   1. Client Layer
//   2. Transport Layer (gRPC)
//   3. Engine Layer (LogStore)
//   4. Storage Layer (File I/O & Buffer)
//   5. Object Storage Layer
//   6. Infrastructure (System Resources)
//
// Panel IDs and Y coordinates are computed automatically by layoutPanels.

const (
	DashboardUID   = "woodpecker-overview"
	DashboardTitle = "Woodpecker"
)

// BuildDashboard constructs the full dashboard model.
// The datasource UID is left empty and must be filled by setup.go
// before importing into Grafana.
func BuildDashboard() *Dashboard {
	rows := buildRows()
	panels := layoutPanels(rows)
	return &Dashboard{
		UID:      DashboardUID,
		Title:    DashboardTitle,
		Tags:     []string{"woodpecker", "monitoring"},
		Timezone: "browser",
		Editable: true,
		Refresh:  "5s",
		Time: DashboardTime{
			From: "now-15m",
			To:   "now",
		},
		Panels:        panels,
		SchemaVersion: 39,
	}
}

// buildRows returns all 6 rows with their child panels.
func buildRows() []rowDef {
	return []rowDef{
		clientRow(),
		grpcRow(),
		logStoreRow(),
		storageRow(),
		objectStorageRow(),
		infrastructureRow(),
	}
}

// rowDef groups a row header with its content panels.
type rowDef struct {
	title     string
	collapsed bool
	panels    []Panel
}

// --- Row 1: Client Layer (expanded) ---

func clientRow() rowDef {
	return rowDef{
		title:     "Client Layer",
		collapsed: false,
		panels: []Panel{
			newTimeseries("Append Request Rate",
				"ops",
				rateQuery("woodpecker_client_append_requests_total", "log_id", "{{log_id}}", "A"),
			),
			newTimeseries("Append Latency P50/P99",
				"ms",
				histogramP50P99("woodpecker_client_append_latency_bucket", "log_id", "Append")...,
			),
			newTimeseries("Append Bytes P50/P99",
				"bytes",
				histogramP50P99("woodpecker_client_append_bytes_bucket", "log_id", "Append")...,
			),
			newTimeseries("Read Request Rate",
				"ops",
				rateQuery("woodpecker_client_read_requests_total", "log_id", "{{log_id}}", "A"),
			),
			newTimeseries("Reader/Writer Bytes Rate",
				"Bps",
				rateByteQuery("woodpecker_client_reader_bytes_read", "log_id", "read {{log_id}}", "A"),
				rateByteQuery("woodpecker_client_writer_bytes_written", "log_id", "write {{log_id}}", "B"),
			),
			newTimeseries("Etcd Meta Ops Rate",
				"ops",
				rateQuery("woodpecker_client_etcd_meta_operations_total", "operation, status", "{{operation}} {{status}}", "A"),
			),
		},
	}
}

// --- Row 2: Transport Layer — gRPC (collapsed) ---

func grpcRow() rowDef {
	return rowDef{
		title:     "Transport Layer — gRPC",
		collapsed: true,
		panels: []Panel{
			newTimeseries("gRPC Request Rate",
				"ops",
				rateQuery("grpc_server_started_total", "grpc_method", "{{grpc_method}}", "A"),
			),
			newTimeseries("gRPC Error Rate",
				"ops",
				Target{
					Expr:         `sum(rate(grpc_server_handled_total{grpc_code!="OK"}[1m])) by (grpc_method, grpc_code)`,
					LegendFormat: "{{grpc_method}} {{grpc_code}}",
					RefID:        "A",
				},
			),
			newTimeseries("gRPC Latency P50/P99",
				"s",
				histogramP50P99("grpc_server_handling_seconds_bucket", "grpc_method", "gRPC")...,
			),
		},
	}
}

// --- Row 3: Engine Layer — LogStore (collapsed) ---

func logStoreRow() rowDef {
	return rowDef{
		title:     "Engine Layer — LogStore",
		collapsed: true,
		panels: []Panel{
			newTimeseries("Active Logs & Segments",
				"short",
				gaugeQuery("woodpecker_server_logstore_active_logs", "instance", "logs {{instance}}", "A"),
				gaugeQuery("woodpecker_server_logstore_active_segments", "instance", "segments {{instance}}", "B"),
			),
			newTimeseries("LogStore Instances by Node",
				"short",
				gaugeQuery("woodpecker_server_logstore_instances_total", "instance", "{{instance}}", "A"),
			),
			newTimeseries("LogStore Operations Rate",
				"ops",
				rateQuery("woodpecker_server_logstore_operations_total", "operation, status", "{{operation}} {{status}}", "A"),
			),
			newTimeseries("LogStore Op Latency P50/P99",
				"ms",
				histogramP50P99("woodpecker_server_logstore_operation_latency_bucket", "operation", "LogStore")...,
			),
			newTimeseries("Active Segment Processors",
				"short",
				gaugeQuery("woodpecker_server_logstore_active_segment_processors", "instance", "{{instance}}", "A"),
			),
			newTimeseries("Pending Append Ops",
				"short",
				gaugeQuery("woodpecker_server_segment_handle_pending_append_ops", "instance", "{{instance}}", "A"),
			),
		},
	}
}

// --- Row 4: Storage Layer — File I/O & Buffer (collapsed) ---

func storageRow() rowDef {
	return rowDef{
		title:     "Storage Layer — File I/O & Buffer",
		collapsed: true,
		panels: []Panel{
			newTimeseries("Buffer Wait Latency P50/P99",
				"ms",
				histogramP50P99("woodpecker_server_buffer_wait_latency_bucket", "", "Buffer")...,
			),
			newTimeseries("File Operations Rate",
				"ops",
				rateQuery("woodpecker_server_file_operations_total", "operation, status", "{{operation}} {{status}}", "A"),
			),
			newTimeseries("File Op Latency P50/P99",
				"ms",
				histogramP50P99("woodpecker_server_file_operation_latency_bucket", "operation", "File")...,
			),
			newTimeseries("Active File Writers & Readers",
				"short",
				gaugeQuery("woodpecker_server_file_writer", "instance", "writer {{instance}}", "A"),
				gaugeQuery("woodpecker_server_file_reader", "instance", "reader {{instance}}", "B"),
			),
			newTimeseries("Flush Latency P50/P99",
				"ms",
				histogramP50P99("woodpecker_server_file_flush_latency_bucket", "", "Flush")...,
			),
			newTimeseries("Read Batch Latency P50/P99",
				"ms",
				histogramP50P99("woodpecker_server_read_batch_latency_bucket", "", "ReadBatch")...,
			),
			newTimeseries("Compaction Latency P50/P99",
				"ms",
				histogramP50P99("woodpecker_server_file_compaction_latency_bucket", "", "Compact")...,
			),
			newTimeseries("Flush/Compact Bytes Rate",
				"Bps",
				rateByteQuery("woodpecker_server_flush_bytes_written", "", "flush", "A"),
				rateByteQuery("woodpecker_server_compact_bytes_written", "", "compact", "B"),
			),
		},
	}
}

// --- Row 5: Object Storage Layer (collapsed) ---

func objectStorageRow() rowDef {
	return rowDef{
		title:     "Object Storage Layer",
		collapsed: true,
		panels: []Panel{
			newTimeseries("Object Storage Ops Rate",
				"ops",
				rateQuery("woodpecker_server_object_storage_operations_total", "operation, status", "{{operation}} {{status}}", "A"),
			),
			newTimeseries("Object Storage Latency P50/P99",
				"ms",
				histogramP50P99("woodpecker_server_object_storage_operation_latency_bucket", "operation", "ObjStore")...,
			),
			newTimeseries("Object Storage Request Size P50/P99",
				"bytes",
				histogramP50P99("woodpecker_server_object_storage_request_bytes_bucket", "operation", "ObjStore")...,
			),
			newTimeseries("Object Storage Bytes Transferred",
				"Bps",
				rateByteQuery("woodpecker_server_object_storage_bytes_transferred", "operation", "{{operation}}", "A"),
			),
		},
	}
}

// --- Row 6: Infrastructure — System Resources (collapsed) ---

func infrastructureRow() rowDef {
	return rowDef{
		title:     "Infrastructure — System Resources",
		collapsed: true,
		panels: []Panel{
			newTimeseries("CPU Usage",
				"percent",
				gaugeQuery("woodpecker_server_system_cpu_usage", "instance", "{{instance}}", "A"),
			),
			newTimeseries("Memory Usage",
				"bytes",
				gaugeQuery("woodpecker_server_system_memory_used_bytes", "instance", "used {{instance}}", "A"),
				Target{
					Expr:         "woodpecker_server_system_memory_usage_ratio by (instance)",
					LegendFormat: "ratio {{instance}}",
					RefID:        "B",
				},
			),
			newTimeseries("Disk Usage",
				"bytes",
				gaugeQuery("woodpecker_server_system_disk_used_bytes", "instance", "used {{instance}}", "A"),
				gaugeQuery("woodpecker_server_system_disk_total_bytes", "instance", "total {{instance}}", "B"),
			),
			newTimeseries("IO Wait",
				"percent",
				gaugeQuery("woodpecker_server_system_io_wait", "instance", "{{instance}}", "A"),
			),
		},
	}
}

// --- Layout Engine ---

const (
	panelHeight = 8
	gridWidth   = 24
)

// layoutPanels assigns IDs and grid positions to all panels.
// Expanded rows place child panels directly after the row header.
// Collapsed rows nest child panels inside the row panel.
func layoutPanels(rows []rowDef) []Panel {
	var result []Panel
	id := 1
	y := 0

	for _, row := range rows {
		// Create row panel
		rowPanel := newRow(row.title, row.collapsed)
		rowPanel.ID = id
		rowPanel.GridPos = GridPos{H: 1, W: gridWidth, X: 0, Y: y}
		id++
		y++

		if row.collapsed {
			// Collapsed: assign positions to children and nest them
			childY := y
			for i := range row.panels {
				w := panelWidth(len(row.panels), i)
				x := panelX(len(row.panels), i)
				if i > 0 && x == 0 {
					childY += panelHeight
				}
				row.panels[i].ID = id
				row.panels[i].GridPos = GridPos{H: panelHeight, W: w, X: x, Y: childY}
				id++
			}
			rowPanel.Panels = row.panels
			result = append(result, rowPanel)
			// Collapsed rows don't advance Y for the parent layout
		} else {
			// Expanded: place row header, then children follow
			result = append(result, rowPanel)
			for i := range row.panels {
				w := panelWidth(len(row.panels), i)
				x := panelX(len(row.panels), i)
				if i > 0 && x == 0 {
					y += panelHeight
				}
				row.panels[i].ID = id
				row.panels[i].GridPos = GridPos{H: panelHeight, W: w, X: x, Y: y}
				id++
				result = append(result, row.panels[i])
			}
			y += panelHeight
		}
	}
	return result
}

// panelWidth returns the width for panel at index i out of total panels.
// Uses 2-column layout (W=12) for even counts, 3-column (W=8) for multiples of 3.
func panelWidth(total, i int) int {
	if total%3 == 0 {
		return 8
	}
	return 12
}

// panelX returns the X position for panel at index i.
func panelX(total, i int) int {
	if total%3 == 0 {
		return (i % 3) * 8
	}
	return (i % 2) * 12
}
