package errors

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestExitCodeFor_AllTypes(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want int
	}{
		{"nil", nil, 0},
		{"usage", NewUsageError("bad flag"), 2},
		{"network", NewNetworkError("seed unreachable"), 1},
		{"target_not_found", NewTargetNotFoundError("node-99"), 3},
		{"state_conflict", NewStateConflictError("already decommissioned"), 4},
		{"wait_timeout", NewWaitTimeoutError("decommission", 30), 5},
		{"strict_partial", NewStrictPartialFailureError(1, 5), 6},
		{"user_abort", NewUserAbortError(), 7},
		{"yellow", NewYellowFindingError("health warning"), 8},
		{"red", NewRedFindingError("health critical"), 9},
		{"not_impl", NewNotImplementedError("wp node restart"), 10},
		{"resource_not_found", NewResourceNotFoundError("op-id foo"), 11},
		{"config", NewConfigError("bad yaml"), 12},
		{"prereq", NewPrerequisiteError("kubectl not in PATH"), 13},
		{"kubectl_passthrough_2", NewKubectlPassthroughError(2), 102},
		{"kubectl_passthrough_127", NewKubectlPassthroughError(127), 227},
		{"unknown", errors.New("random error"), 1},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			require.Equal(t, c.want, ExitCodeFor(c.err))
		})
	}
}
