package errors

import "errors"

// ExitCodeFor maps an error to the wp CLI exit code per spec §3.5.
// nil → 0. Unknown errors → 1 (treated as generic network/runtime failure).
func ExitCodeFor(err error) int {
	if err == nil {
		return 0
	}
	var cli *CLIError
	if !errors.As(err, &cli) {
		return 1
	}
	switch cli.Type {
	case TypeNetwork:
		return 1
	case TypeUsage:
		return 2
	case TypeTargetNotFound:
		return 3
	case TypeStateConflict:
		return 4
	case TypeWaitTimeout:
		return 5
	case TypeStrictPartial:
		return 6
	case TypeUserAbort:
		return 7
	case TypeYellowFinding:
		return 8
	case TypeRedFinding:
		return 9
	case TypeNotImplemented:
		return 10
	case TypeResourceNotFound:
		return 11
	case TypeConfig:
		return 12
	case TypePrerequisite:
		return 13
	case TypeKubectlPassthrough:
		return 100 + cli.KubectlExitCode
	default:
		return 1
	}
}
