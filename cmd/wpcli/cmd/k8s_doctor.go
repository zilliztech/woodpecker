package cmd

import (
	"github.com/spf13/cobra"

	wperrors "github.com/zilliztech/woodpecker/cmd/wpcli/internal/errors"
)

func newK8sDoctorCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "doctor",
		Short: "Diagnose K8s cluster issues (not yet implemented)",
		RunE: func(cmd *cobra.Command, args []string) error {
			return wperrors.NewNotImplementedError("k8s doctor")
		},
	}
}
