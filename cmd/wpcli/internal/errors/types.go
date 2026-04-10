// Package errors centralizes wp CLI's error types and exit code mapping.
package errors

import "fmt"

// Type identifies the semantic class of a CLI error.
type Type string

const (
	TypeUsage              Type = "Usage"
	TypeNetwork            Type = "Network"
	TypeTargetNotFound     Type = "TargetNotFound"
	TypeStateConflict      Type = "StateConflict"
	TypeWaitTimeout        Type = "WaitTimeout"
	TypeStrictPartial      Type = "StrictPartial"
	TypeUserAbort          Type = "UserAbort"
	TypeYellowFinding      Type = "YellowFinding"
	TypeRedFinding         Type = "RedFinding"
	TypeNotImplemented     Type = "NotImplemented"
	TypeResourceNotFound   Type = "ResourceNotFound"
	TypeConfig             Type = "Config"
	TypePrerequisite       Type = "Prerequisite"
	TypeKubectlPassthrough Type = "KubectlPassthrough"
)

// CLIError is the structured error type wp commands return.
// The root command converts this to an exit code via ExitCodeFor.
type CLIError struct {
	Type    Type
	Message string
	Detail  string
	Hint    string
	// KubectlExitCode is only meaningful when Type == TypeKubectlPassthrough.
	KubectlExitCode int
	Cause           error
}

func (e *CLIError) Error() string {
	if e.Detail != "" {
		return fmt.Sprintf("%s: %s (%s)", e.Type, e.Message, e.Detail)
	}
	return fmt.Sprintf("%s: %s", e.Type, e.Message)
}

func (e *CLIError) Unwrap() error { return e.Cause }

// Constructors — one per type. Keep signatures minimal.

func NewUsageError(msg string) *CLIError {
	return &CLIError{Type: TypeUsage, Message: msg}
}

func NewNetworkError(msg string) *CLIError {
	return &CLIError{Type: TypeNetwork, Message: msg}
}

func NewTargetNotFoundError(target string) *CLIError {
	return &CLIError{Type: TypeTargetNotFound, Message: fmt.Sprintf("target not found: %s", target)}
}

func NewStateConflictError(msg string) *CLIError {
	return &CLIError{Type: TypeStateConflict, Message: msg}
}

func NewWaitTimeoutError(what string, seconds int) *CLIError {
	return &CLIError{Type: TypeWaitTimeout, Message: fmt.Sprintf("%s wait timed out after %ds", what, seconds)}
}

func NewStrictPartialFailureError(unreachable, total int) *CLIError {
	return &CLIError{
		Type:    TypeStrictPartial,
		Message: fmt.Sprintf("%d/%d nodes unreachable in strict mode", unreachable, total),
	}
}

func NewUserAbortError() *CLIError {
	return &CLIError{Type: TypeUserAbort, Message: "user aborted"}
}

func NewYellowFindingError(msg string) *CLIError {
	return &CLIError{Type: TypeYellowFinding, Message: msg}
}

func NewRedFindingError(msg string) *CLIError {
	return &CLIError{Type: TypeRedFinding, Message: msg}
}

func NewNotImplementedError(what string) *CLIError {
	return &CLIError{
		Type:    TypeNotImplemented,
		Message: fmt.Sprintf("%s is intentionally not implemented by this CLI", what),
	}
}

func NewResourceNotFoundError(resource string) *CLIError {
	return &CLIError{Type: TypeResourceNotFound, Message: fmt.Sprintf("resource not found: %s", resource)}
}

func NewConfigError(msg string) *CLIError {
	return &CLIError{Type: TypeConfig, Message: msg}
}

func NewPrerequisiteError(missing string) *CLIError {
	return &CLIError{Type: TypePrerequisite, Message: fmt.Sprintf("prerequisite missing: %s", missing)}
}

func NewKubectlPassthroughError(code int) *CLIError {
	return &CLIError{
		Type:            TypeKubectlPassthrough,
		Message:         fmt.Sprintf("kubectl exited with code %d", code),
		KubectlExitCode: code,
	}
}
