// Package output holds wp CLI output renderers.
package output

import "io"

// Format is the enum of supported output formats.
type Format string

const (
	FormatTable Format = "table"
	FormatWide  Format = "wide"
	FormatJSON  Format = "json"
	FormatYAML  Format = "yaml"
	FormatTree  Format = "tree"
	FormatRaw   Format = "raw"
)

// Renderer is anything that can render an object to a writer.
type Renderer interface {
	Render(w io.Writer, data any) error
}
