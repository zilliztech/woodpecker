package cmd

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/spf13/cobra"

	wperrors "github.com/zilliztech/woodpecker/cmd/wpcli/internal/errors"
)

func newProfileCommand() *cobra.Command {
	var (
		profileType string
		seconds     int
		outputFile  string
	)
	cmd := &cobra.Command{
		Use:   "profile <node>",
		Short: "Download a Go pprof profile from a node",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			r, err := resolveAndDiscover()
			if err != nil {
				return err
			}
			target, ok := r.Members.Resolve(args[0])
			if !ok {
				return wperrors.NewTargetNotFoundError(args[0])
			}
			peerURL := r.Client.PeerAdminURL(target)

			url := fmt.Sprintf("%s/debug/pprof/%s", peerURL, profileType)
			if profileType == "cpu" || profileType == "trace" {
				url = fmt.Sprintf("%s?seconds=%d", url, seconds)
			}

			if outputFile == "" {
				outputFile = fmt.Sprintf("./%s-%s-%s.pb.gz",
					target.ID, profileType, time.Now().UTC().Format("20060102-150405"))
			}

			fmt.Fprintf(cmd.ErrOrStderr(), "Collecting %s profile from %s ...\n", profileType, target.ID)
			client := &http.Client{Timeout: time.Duration(seconds+30) * time.Second}
			resp, err := client.Get(url)
			if err != nil {
				return wperrors.NewNetworkError(fmt.Sprintf("GET pprof: %v", err))
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				return wperrors.NewNetworkError(fmt.Sprintf("pprof returned status %d", resp.StatusCode))
			}

			f, err := os.Create(outputFile)
			if err != nil {
				return wperrors.NewConfigError(fmt.Sprintf("create output file: %v", err))
			}
			defer f.Close()
			n, err := io.Copy(f, resp.Body)
			if err != nil {
				return wperrors.NewNetworkError(fmt.Sprintf("write pprof: %v", err))
			}
			fmt.Fprintf(cmd.OutOrStdout(), "Saved: %s (%d bytes)\n", outputFile, n)
			fmt.Fprintf(cmd.OutOrStdout(), "To analyze: go tool pprof -http=:0 %s\n", outputFile)
			return nil
		},
	}
	cmd.Flags().StringVar(&profileType, "type", "cpu", "profile type: cpu|heap|goroutine|allocs|mutex|block|threadcreate|trace")
	cmd.Flags().IntVar(&seconds, "seconds", 30, "duration for cpu/trace profiles")
	cmd.Flags().StringVar(&outputFile, "output-file", "", "output file path (default: ./<node>-<type>-<timestamp>.pb.gz)")
	return cmd
}
