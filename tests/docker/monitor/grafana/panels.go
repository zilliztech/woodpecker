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

// Grafana JSON model types and builder helpers.
// These structs map to the Grafana dashboard JSON schema.
// Builder functions eliminate boilerplate when constructing panels.

// Dashboard is the top-level Grafana dashboard model.
type Dashboard struct {
	UID           string        `json:"uid"`
	Title         string        `json:"title"`
	Tags          []string      `json:"tags"`
	Timezone      string        `json:"timezone"`
	Editable      bool          `json:"editable"`
	Refresh       string        `json:"refresh"`
	Time          DashboardTime `json:"time"`
	Panels        []Panel       `json:"panels"`
	SchemaVersion int           `json:"schemaVersion"`
}

// DashboardTime defines the default time range.
type DashboardTime struct {
	From string `json:"from"`
	To   string `json:"to"`
}

// Panel represents a Grafana panel (timeseries, row, etc).
type Panel struct {
	ID         int              `json:"id"`
	Title      string           `json:"title"`
	Type       string           `json:"type"`
	GridPos    GridPos          `json:"gridPos"`
	Datasource *DatasourceRef   `json:"datasource,omitempty"`
	Targets    []Target         `json:"targets,omitempty"`
	FieldConfig *FieldConfig    `json:"fieldConfig,omitempty"`
	Options    *PanelOptions    `json:"options,omitempty"`
	Collapsed  bool             `json:"collapsed,omitempty"`
	Panels     []Panel          `json:"panels,omitempty"`
}

// GridPos defines the position and size of a panel on the grid.
type GridPos struct {
	H int `json:"h"`
	W int `json:"w"`
	X int `json:"x"`
	Y int `json:"y"`
}

// DatasourceRef references a Grafana datasource.
type DatasourceRef struct {
	Type string `json:"type"`
	UID  string `json:"uid"`
}

// Target represents a PromQL query target.
type Target struct {
	Expr         string `json:"expr"`
	LegendFormat string `json:"legendFormat"`
	RefID        string `json:"refId"`
	Interval     string `json:"interval,omitempty"`
}

// FieldConfig holds panel field configuration including defaults and overrides.
type FieldConfig struct {
	Defaults  FieldDefaults   `json:"defaults"`
	Overrides []interface{}   `json:"overrides"`
}

// FieldDefaults holds default field settings.
type FieldDefaults struct {
	Unit   string          `json:"unit,omitempty"`
	Custom *CustomDefaults `json:"custom,omitempty"`
}

// CustomDefaults holds custom visualization settings.
type CustomDefaults struct {
	LineWidth    int     `json:"lineWidth"`
	FillOpacity  int     `json:"fillOpacity"`
	PointSize    int     `json:"pointSize"`
	DrawStyle    string  `json:"drawStyle"`
	ShowPoints   string  `json:"showPoints"`
	SpanNulls    bool    `json:"spanNulls"`
}

// PanelOptions holds visualization options.
type PanelOptions struct {
	Legend  *LegendOptions  `json:"legend,omitempty"`
	Tooltip *TooltipOptions `json:"tooltip,omitempty"`
}

// LegendOptions configures the panel legend.
type LegendOptions struct {
	DisplayMode string   `json:"displayMode"`
	Placement   string   `json:"placement"`
	Calcs       []string `json:"calcs"`
}

// TooltipOptions configures the panel tooltip.
type TooltipOptions struct {
	Mode string `json:"mode"`
	Sort string `json:"sort"`
}

// --- Builder Helpers ---

// promDS returns a placeholder Prometheus datasource reference.
// The UID is set to empty and will be filled in by SetupDashboard.
func promDS() *DatasourceRef {
	return &DatasourceRef{Type: "prometheus", UID: ""}
}

// defaultFieldConfig returns the standard timeseries visual configuration.
func defaultFieldConfig(unit string) *FieldConfig {
	return &FieldConfig{
		Defaults: FieldDefaults{
			Unit: unit,
			Custom: &CustomDefaults{
				LineWidth:   2,
				FillOpacity: 20,
				PointSize:   5,
				DrawStyle:   "line",
				ShowPoints:  "auto",
				SpanNulls:   false,
			},
		},
		Overrides: []interface{}{},
	}
}

// defaultOptions returns the standard panel options (table legend with calcs).
func defaultOptions() *PanelOptions {
	return &PanelOptions{
		Legend: &LegendOptions{
			DisplayMode: "table",
			Placement:   "bottom",
			Calcs:       []string{"lastNotNull", "max", "mean"},
		},
		Tooltip: &TooltipOptions{
			Mode: "multi",
			Sort: "desc",
		},
	}
}

// newTimeseries creates a timeseries panel with standard defaults.
func newTimeseries(title, unit string, targets ...Target) Panel {
	return Panel{
		Title:       title,
		Type:        "timeseries",
		Datasource:  promDS(),
		Targets:     targets,
		FieldConfig: defaultFieldConfig(unit),
		Options:     defaultOptions(),
	}
}

// newRow creates a row panel for organizing dashboard sections.
// If collapsed is true, the children panels are nested inside the row.
func newRow(title string, collapsed bool, children ...Panel) Panel {
	p := Panel{
		Title:     title,
		Type:      "row",
		Collapsed: collapsed,
	}
	if collapsed {
		p.Panels = children
	}
	return p
}

// rateQuery creates a Target for a rate() query with by-label grouping.
func rateQuery(metric, byLabels, legend, refID string) Target {
	var expr string
	if byLabels != "" {
		expr = "sum(rate(" + metric + "[1m])) by (" + byLabels + ")"
	} else {
		expr = "sum(rate(" + metric + "[1m]))"
	}
	return Target{
		Expr:         expr,
		LegendFormat: legend,
		RefID:        refID,
	}
}

// histogramP50P99 creates two Targets for P50 and P99 histogram quantiles.
func histogramP50P99(metric, byLabels, prefix string) []Target {
	byClause := "le"
	if byLabels != "" {
		byClause = byLabels + ", le"
	}
	base := "sum(rate(" + metric + "[1m])) by (" + byClause + ")"
	return []Target{
		{
			Expr:         "histogram_quantile(0.5, " + base + ")",
			LegendFormat: prefix + " P50",
			RefID:        "A",
		},
		{
			Expr:         "histogram_quantile(0.99, " + base + ")",
			LegendFormat: prefix + " P99",
			RefID:        "B",
		},
	}
}

// gaugeQuery creates a Target for an instant gauge metric.
func gaugeQuery(metric, byLabels, legend, refID string) Target {
	var expr string
	if byLabels != "" {
		expr = metric + " by (" + byLabels + ")"
	} else {
		expr = metric
	}
	// For gauge metrics, wrap with sum() if there are labels to aggregate
	// but display per-label if byLabels is set.
	return Target{
		Expr:         expr,
		LegendFormat: legend,
		RefID:        refID,
	}
}

// rateByteQuery creates a Target for a rate() query on a byte counter.
func rateByteQuery(metric, byLabels, legend, refID string) Target {
	var expr string
	if byLabels != "" {
		expr = "sum(rate(" + metric + "[1m])) by (" + byLabels + ")"
	} else {
		expr = "sum(rate(" + metric + "[1m]))"
	}
	return Target{
		Expr:         expr,
		LegendFormat: legend,
		RefID:        refID,
	}
}
