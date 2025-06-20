package segment

import "context"

var _ Operation = (*RollingSegmentOp)(nil)

type RollingSegmentOp struct {
	identifier string
	handle     SegmentHandle
}

func NewRollingSegmentOp(ctx context.Context, identifier string, handle SegmentHandle) *RollingSegmentOp {
	return &RollingSegmentOp{
		identifier: identifier,
		handle:     handle,
	}
}

func (r *RollingSegmentOp) Identifier() string {
	return r.identifier
}

func (r *RollingSegmentOp) Execute() {
	r.handle.SetRollingReady(context.TODO())
}
