//go:build windows

package webui

import "os/exec"

func configureCommandProcess(cmd *exec.Cmd) {
	cmd.Cancel = func() error { return cmd.Process.Kill() }
}
