package wal

import "github.com/zilliztech/woodpecker/diagnostics/evidence"

func DataState(e Evidence) string    { return evidence.DataState(e) }
func DisplayState(e Evidence) string { return evidence.DisplayState(e) }
