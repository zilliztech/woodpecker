package checks

import (
	"context"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"

	d "github.com/zilliztech/woodpecker/diagnostics"
)

func writeRules(t *testing.T, dir, source, body string) string {
	t.Helper()
	path := filepath.Join(dir, "diag-"+source+".yaml")
	require.NoError(t, os.WriteFile(path, []byte("schema_version: "+RuleSchema+"\nsource: "+source+"\n"+body), 0o600))
	return path
}

func TestRuleFilesDiscoveryAndCompleteTemplates(t *testing.T) {
	dir := t.TempDir()
	c, err := WithRuleFiles(d.Config{}, dir, nil)
	require.NoError(t, err)
	s, err := LoadSettings(c)
	require.NoError(t, err)
	require.Len(t, s.Policy.RuleFiles, 2)
	for _, source := range []string{"metrics", "trace"} {
		body, err := RuleTemplate(source, s.Policy)
		require.NoError(t, err)
		var doc RuleDocument
		require.NoError(t, yaml.Unmarshal([]byte(body), &doc))
		expected := 39
		if source == "trace" {
			expected = 16
		}
		require.Len(t, doc.Evidence, expected)
		require.NoError(t, os.WriteFile(filepath.Join(dir, "diag-"+source+".yaml"), []byte(body), 0o600))
	}
	c, err = WithRuleFiles(d.Config{}, dir, nil)
	require.NoError(t, err)
	s, err = LoadSettings(c)
	require.NoError(t, err)
	for _, f := range s.Policy.RuleFiles {
		require.True(t, f.Loaded)
		require.Len(t, f.SHA256, 64)
	}
	for _, def := range Definitions(s.Policy) {
		require.NotEmpty(t, def.Assessment.Mode)
		require.NotEqual(t, "built-in", def.Assessment.ConfiguredBy)
	}
}

func TestRuleFilesDefaultsNativeUnitsOverridesAndReload(t *testing.T) {
	dir := t.TempDir()
	path := writeRules(t, dir, "metrics", "defaults:\n  latency_ms: 300\nevidence:\n  W01: {threshold: 200, unit: ms}\n  W02: {threshold: 0, unit: count}\n")
	_, err := writeAndLoadTrace(t, dir)
	require.NoError(t, err)
	c, err := WithRuleFiles(d.Config{}, dir, nil)
	require.NoError(t, err)
	s, err := LoadSettings(c)
	require.NoError(t, err)
	expected := map[string]float64{"W01": 200, "W06": 300, "MV01": .3, "TA01": 2000, "W02": 0}
	for id, n := range expected {
		for _, def := range Definitions(s.Policy) {
			if def.ID == id {
				require.Equal(t, n, *def.Assessment.Threshold, id)
			}
		}
	}
	writeRules(t, dir, "metrics", "evidence:\n  W01: {threshold: 900}\n")
	fresh, err := WithRuleFiles(d.Config{}, dir, nil)
	require.NoError(t, err)
	now, _ := LoadSettings(fresh)
	require.Equal(t, 200.0, s.Policy.Thresholds["W01"])
	require.Equal(t, 900.0, now.Policy.Thresholds["W01"])
	require.Equal(t, path, s.Policy.RuleSources["W01"])
	_, err = WithRuleFiles(d.Config{}, dir, map[string]string{"metrics": "missing.yaml"})
	require.Error(t, err)
	require.NoError(t, os.Rename(path, filepath.Join(dir, "custom.yaml")))
	c, err = WithRuleFiles(d.Config{}, dir, map[string]string{"metrics": "custom.yaml"})
	require.NoError(t, err)
	require.Equal(t, filepath.Join(dir, "custom.yaml"), c.RulePaths["metrics"])
}

func writeAndLoadTrace(t *testing.T, dir string) (d.Config, error) {
	writeRules(t, dir, "trace", "defaults:\n  latency_ms: 2000\nevidence: {}\n")
	return WithRuleFiles(d.Config{}, dir, nil)
}

func TestRuleFilesRejectInvalidConfiguration(t *testing.T) {
	for _, body := range []string{
		"evidence:\n  typo: {threshold: 10}\n", "evidence:\n  TA01: {threshold: 10}\n",
		"evidence:\n  W01: {unknown_threshold: 10}\n", "evidence:\n  W01: {threshold: 1, unit: seconds}\n",
		"evidence:\n  W01: {threshold: .nan}\n", "evidence:\n  W01: {threshold: 0}\n",
		"evidence:\n  W02: {threshold: -1}\n", "evidence:\n  W02: {threshold: 1.5}\n",
		"evidence:\n  W03: {threshold: 10}\n", "evidence:\n  S06: {threshold: 0.9}\n",
		"evidence:\n  H01: {threshold: 101}\n", "evidence:\n  H03: {threshold: 90}\n",
		"evidence:\n  W01: {mode: observe}\n", "defaults:\n  min_observations: 0\n",
		"evidence: {}\n---\nevidence: {}\n",
	} {
		t.Run(strings.ReplaceAll(body, "\n", " "), func(t *testing.T) {
			dir := t.TempDir()
			writeRules(t, dir, "metrics", body)
			_, err := WithRuleFiles(d.Config{}, dir, nil)
			require.Error(t, err)
		})
	}
	dir := t.TempDir()
	writeRules(t, dir, "trace", "defaults:\n  sustain_ms: 1000\n")
	_, err := WithRuleFiles(d.Config{}, dir, nil)
	require.Error(t, err)
}

func TestRuleFilesChangeActualVerdictsAndCLIOverride(t *testing.T) {
	_, server := fixture(t)
	c := configuration(t, server.URL, true, true)
	dir := t.TempDir()
	writeRules(t, dir, "metrics", "evidence:\n  writer-errors: {threshold: 100000}\n")
	writeRules(t, dir, "trace", "evidence:\n  server-append-duration: {threshold: 3000}\n  server-append-errors: {threshold: 2}\n")
	c, err := WithRuleFiles(c, dir, nil)
	require.NoError(t, err)
	for _, item := range []struct {
		source, id string
		override   float64
	}{{"metrics", "W02", 0}, {"trace", "TS01", 1000}, {"trace", "TS02", 0}} {
		q := request()
		q.Scenario = ""
		q.Source = item.source
		q.Check = item.id
		r := Run(context.Background(), c, q, nil)
		require.Len(t, r.Checks, 1)
		expected := "clear"
		if item.source == "trace" {
			expected = "context"
		}
		require.Equal(t, expected, r.Checks[0].Status, r.Checks[0])
		q.Threshold = &item.override
		r = Run(context.Background(), c, q, nil)
		require.Equal(t, "abnormal", r.Checks[0].Status, r.Checks[0])
		require.Equal(t, "command-line --threshold", r.Checks[0].Assessment.ConfiguredBy)
		require.Equal(t, item.override, *r.Checks[0].Assessment.Threshold)
	}
}

func TestCanonicalRuleKeysMatchCatalogAndExamples(t *testing.T) {
	settings, err := LoadSettings(d.Config{})
	require.NoError(t, err)
	pattern := regexp.MustCompile(`^[a-z][a-z0-9]*(?:-[a-z0-9]+)*$`)
	paths := map[string]string{}
	for _, source := range []string{"metrics", "trace"} {
		expected := map[string]bool{}
		for _, def := range Definitions(settings.Policy) {
			if def.Source != source {
				continue
			}
			require.Regexp(t, pattern, def.Name)
			require.False(t, expected[def.Name], "duplicate %s key %s", source, def.Name)
			expected[def.Name] = true
		}
		template, err := RuleTemplate(source, settings.Policy)
		require.NoError(t, err)
		path, err := filepath.Abs(filepath.Join("..", "..", "cmd", "wpcli", "diag-"+source+".example.yaml"))
		require.NoError(t, err)
		paths[source] = path
		example, err := os.ReadFile(path)
		require.NoError(t, err)
		for _, body := range []string{template, string(example)} {
			var doc RuleDocument
			require.NoError(t, yaml.Unmarshal([]byte(body), &doc))
			require.Len(t, doc.Evidence, len(expected))
			for key := range doc.Evidence {
				require.True(t, expected[key], "%s rule %s must be an exact catalog check name", source, key)
				require.Regexp(t, pattern, key)
			}
		}
	}
	_, err = WithRuleFiles(d.Config{}, t.TempDir(), paths)
	require.NoError(t, err)
}

func TestLegacyAndCanonicalRuleAliasesCannotDuplicateOneCheck(t *testing.T) {
	dir := t.TempDir()
	writeRules(t, dir, "metrics", "evidence:\n  W01: {threshold: 200}\n  writer-latency: {threshold: 300}\n")
	_, err := WithRuleFiles(d.Config{}, dir, nil)
	require.ErrorContains(t, err, "duplicate evidence key writer-latency")
	writeRules(t, dir, "metrics", "evidence:\n  Writer Latency: {threshold: 200}\n")
	_, err = WithRuleFiles(d.Config{}, dir, nil)
	require.ErrorContains(t, err, "unknown metrics evidence")
}
