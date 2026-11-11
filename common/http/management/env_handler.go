package management

import (
	"encoding/json"
	"net/http"
	"os"
	"runtime"
	"strings"

	"github.com/zilliztech/woodpecker/common/version"
)

type envResponse struct {
	Env     map[string]string `json:"env"`
	Runtime envRuntime        `json:"runtime"`
	Host    envHost           `json:"host"`
	Build   version.BuildInfo `json:"build"`
}

type envRuntime struct {
	GoVersion    string `json:"go_version"`
	GoMaxProcs   int    `json:"gomaxprocs"`
	GOGC         string `json:"gogc"`
	NumCPU       int    `json:"num_cpu"`
	NumGoroutine int    `json:"num_goroutine"`
	AllocBytes   uint64 `json:"memstats_alloc_bytes"`
	SysBytes     uint64 `json:"memstats_sys_bytes"`
}

type envHost struct {
	Hostname string `json:"hostname"`
	OS       string `json:"os"`
	Arch     string `json:"arch"`
}

// NewEnvHandler returns a handler for GET /admin/env.
// No arguments: the handler collects env vars, Go runtime info, host info,
// and build info at request time.
func NewEnvHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		envMap := make(map[string]string, 32)
		for _, kv := range os.Environ() {
			i := strings.IndexByte(kv, '=')
			if i < 0 {
				continue
			}
			envMap[kv[:i]] = kv[i+1:]
		}

		var ms runtime.MemStats
		runtime.ReadMemStats(&ms)

		hostname, _ := os.Hostname()

		resp := envResponse{
			Env: envMap,
			Runtime: envRuntime{
				GoVersion:    runtime.Version(),
				GoMaxProcs:   runtime.GOMAXPROCS(0),
				GOGC:         os.Getenv("GOGC"),
				NumCPU:       runtime.NumCPU(),
				NumGoroutine: runtime.NumGoroutine(),
				AllocBytes:   ms.Alloc,
				SysBytes:     ms.Sys,
			},
			Host: envHost{
				Hostname: hostname,
				OS:       runtime.GOOS,
				Arch:     runtime.GOARCH,
			},
			Build: version.Info(),
		}

		w.Header().Set("Content-Type", "application/json")
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		_ = enc.Encode(resp)
	}
}
