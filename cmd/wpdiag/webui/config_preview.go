package webui

import (
	"fmt"
	"net/http"
	"os"
	"path/filepath"

	d "github.com/zilliztech/woodpecker/diagnostics"
	"github.com/zilliztech/woodpecker/diagnostics/checks"
)

// configPreview reads the configured local file verbatim. It accepts no path
// from a browser request and does not normalize or rewrite the document.
func configPreview(path string) (string, error) {
	//nolint:gosec // G703: only the allowlisted source selects a startup-configured path; the browser supplies no path.
	body, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("cannot read the configured cli.yaml file")
	}
	return string(body), nil
}

func (s *Server) workspaceConfigPreview(w http.ResponseWriter, r *http.Request) {
	content, err := configPreview(s.workspace.path)
	if err != nil {
		fail(w, 422, "config_preview_unavailable", err.Error())
		return
	}
	send(w, 200, map[string]any{"schema_version": "wp.config-preview/v1", "path": s.workspace.path, "yaml": content, "read_only": true, "redacted": false, "snapshot": "Current file on disk"})
}

// workspaceRulesPreview accepts only a source name, never a filesystem path.
func (s *Server) workspaceRulesPreview(w http.ResponseWriter, r *http.Request) {
	source := r.PathValue("source")
	if source != "metrics" && source != "trace" {
		fail(w, 404, "unknown_rule_source", "Choose metrics or trace")
		return
	}
	path := s.workspace.rulePaths[source]
	explicit := path != ""
	if !explicit {
		path = filepath.Join(filepath.Dir(s.workspace.path), "diag-"+source+".yaml")
	}
	//nolint:gosec // G703: only the allowlisted source selects a startup-configured path; the browser supplies no path.
	body, err := os.ReadFile(path)
	if os.IsNotExist(err) && !explicit {
		settings, _ := checks.LoadSettings(d.Config{})
		template, _ := checks.RuleTemplate(source, settings.Policy)
		send(w, 200, map[string]any{"path": path, "yaml": template, "exists": false, "read_only": true})
		return
	}
	if err != nil || len(body) > 256<<10 {
		fail(w, 422, "rule_file_unavailable", "Cannot read rule file, or file exceeds 256 KiB: "+path)
		return
	}
	send(w, 200, map[string]any{"path": path, "yaml": string(body), "exists": true, "read_only": true})
}
