package output

import (
	"encoding/json"
	"io"
)

// RenderJSON writes data as indented JSON.
func RenderJSON(w io.Writer, data any) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(data)
}
