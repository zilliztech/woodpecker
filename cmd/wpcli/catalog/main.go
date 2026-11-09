// Command catalog emits required-flag annotations for the documentation generator.
// Constructing the Cobra tree performs no command execution or source access.
package main

import (
	"encoding/json"
	"os"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/zilliztech/woodpecker/cmd/wpcli/cmd"
)

func main() {
	out := map[string][]string{}
	var visit func(*cobra.Command)
	visit = func(c *cobra.Command) {
		if c.Hidden {
			return
		}
		names := []string{}
		c.Flags().VisitAll(func(f *pflag.Flag) {
			if len(f.Annotations[cobra.BashCompOneRequiredFlag]) > 0 {
				names = append(names, f.Name)
			}
		})
		out[strings.TrimPrefix(c.CommandPath(), "wp ")] = names
		for _, child := range c.Commands() {
			visit(child)
		}
	}
	visit(cmd.NewRootCommand())
	if json.NewEncoder(os.Stdout).Encode(out) != nil {
		os.Exit(1)
	}
}
