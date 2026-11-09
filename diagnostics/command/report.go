package command

import (
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/spf13/cobra"

	d "github.com/zilliztech/woodpecker/diagnostics"
	"github.com/zilliztech/woodpecker/diagnostics/reports"
	"github.com/zilliztech/woodpecker/diagnostics/wal"
)

// NewReportCommand consumes saved reports. It never reruns a diagnostic check.
func NewReportCommand(options Options) *cobra.Command {
	root := &cobra.Command{Use: "report", Short: "Inspect, export or analyze saved diagnostic JSON"}
	var configPath string
	root.PersistentFlags().StringVar(&configPath, "config", "", "Shared cli.yaml for optional Agent configuration")
	read := func(cmd *cobra.Command, path string) ([]byte, error) {
		var in io.Reader = cmd.InOrStdin()
		if path != "-" {
			f, err := os.Open(path)
			if err != nil {
				return nil, fmt.Errorf("cannot open report")
			}
			defer f.Close()
			in = f
		}
		body, err := io.ReadAll(io.LimitReader(in, (16<<20)+1))
		if err != nil || len(body) > 16<<20 || !json.Valid(body) {
			return nil, fmt.Errorf("expected JSON report, at most 16 MiB")
		}
		return body, nil
	}
	render := func(cmd *cobra.Command, value any) error {
		e := json.NewEncoder(cmd.OutOrStdout())
		e.SetIndent("", "  ")
		return e.Encode(value)
	}
	root.AddCommand(&cobra.Command{Use: "inspect <report.json|-> <check>", Short: "Show recorded evidence detail without querying sources", Args: cobra.ExactArgs(2), RunE: func(cmd *cobra.Command, args []string) error {
		body, err := read(cmd, args[0])
		if err != nil {
			return err
		}
		detail, err := reports.Inspection(body, args[1])
		if err != nil {
			return err
		}
		return render(cmd, detail)
	}})
	var packet bool
	export := &cobra.Command{Use: "export <report.json|->", Short: "Copy original JSON or derive a compact findings packet", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		body, err := read(cmd, args[0])
		if err != nil {
			return err
		}
		if packet {
			value, err := reports.Packet(body)
			if err != nil {
				return err
			}
			return render(cmd, value)
		}
		_, err = cmd.OutOrStdout().Write(body)
		return err
	}}
	export.Flags().BoolVar(&packet, "packet", false, "Export findings, supporting context, gaps and provenance; no model call")
	root.AddCommand(export)
	root.AddCommand(&cobra.Command{Use: "analyze <report.json|->", Short: "Explicitly ask the configured Agent to explain supplied findings", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		body, err := read(cmd, args[0])
		if err != nil {
			return err
		}
		packet, err := reports.Packet(body)
		if err != nil {
			return err
		}
		if len(packet.Findings) == 0 {
			return fmt.Errorf("no abnormal findings to analyze; inspect or export the report instead")
		}
		contextName := ""
		if options.Context != nil {
			contextName = options.Context()
		}
		agent, err := reportAgentConfig(configPath, contextName)
		if err != nil {
			return err
		}
		result := wal.Analyze(cmd.Context(), agent, packet, nil)
		output := "text"
		if options.Output != nil {
			output = options.Output()
		}
		if output == "json" {
			err = render(cmd, result)
		} else if result.Text != "" {
			_, err = fmt.Fprintln(cmd.OutOrStdout(), result.Text)
		} else {
			_, err = fmt.Fprintf(cmd.OutOrStdout(), "Agent: %s (%s)\n", result.Status, result.Code)
		}
		if err != nil {
			return err
		}
		if result.Status != "completed" {
			return &Error{Code: 1, Message: "Agent did not complete; the original report is unchanged"}
		}
		return nil
	}})
	return root
}

// Report analysis consumes frozen evidence. Invalid assessment rules or offline
// observability/Admin services must not prevent reading the Agent configuration.
func reportAgentConfig(path, contextName string) (wal.AgentConfig, error) {
	file, _, err := LoadDocument(path)
	if err != nil {
		return wal.AgentConfig{}, err
	}
	node := file.Agent
	if contextName == "" {
		contextName = file.CurrentContext
	}
	if contextName != "" {
		ctx, ok := file.Contexts[contextName]
		if !ok {
			return wal.AgentConfig{}, fmt.Errorf("choose a configured Agent context")
		}
		if ctx.Agent.Kind != 0 {
			node = ctx.Agent
		}
	}
	return reports.AgentSettings(d.Config{Agent: node})
}
