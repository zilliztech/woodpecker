package metrics

import "time"

// OpObserver receives notifications when ops start and end.
// Implementations must be goroutine-safe and fast (called in hot path).
type OpObserver interface {
	// OnOpStart is called when a new op begins. Returns a handle used in OnOpEnd.
	OnOpStart(op *Op) uint64
	// OnOpEnd is called when the op completes.
	OnOpEnd(op *Op, handle uint64, elapsed time.Duration, status string)
}

// observers is the global chain of registered observers.
var observers []OpObserver

// RegisterOpObserver adds an observer to the global chain.
// Must be called during startup, before serving traffic.
func RegisterOpObserver(o OpObserver) {
	observers = append(observers, o)
}

// ResetObservers clears all registered observers (for testing only).
func ResetObservers() {
	observers = nil
}
