package output

import (
	"io"

	"gopkg.in/yaml.v3"
)

// RenderYAML writes data as YAML.
func RenderYAML(w io.Writer, data any) error {
	enc := yaml.NewEncoder(w)
	enc.SetIndent(2)
	defer enc.Close()
	return enc.Encode(data)
}
