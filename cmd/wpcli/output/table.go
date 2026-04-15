package output

import (
	"fmt"
	"io"
	"strings"
	"text/tabwriter"
)

// RenderRowTable renders a row-per-entity table for list commands.
// headers is the column headers; rows is [][]string where len(row) == len(headers).
func RenderRowTable(w io.Writer, headers []string, rows [][]string) error {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(tw, strings.Join(headers, "\t")); err != nil {
		return err
	}
	for _, r := range rows {
		if _, err := fmt.Fprintln(tw, strings.Join(r, "\t")); err != nil {
			return err
		}
	}
	return tw.Flush()
}

// Section is one titled block of key-value pairs for sectioned table output.
type Section struct {
	Title string
	Pairs [][2]string // [key, value]
}

// RenderSectionedTable writes sections with a title line followed by indented "key: value" pairs.
// Used for show/stats commands where a single entity has multiple logical groups.
func RenderSectionedTable(w io.Writer, sections []Section) error {
	for i, s := range sections {
		if i > 0 {
			if _, err := fmt.Fprintln(w); err != nil {
				return err
			}
		}
		if _, err := fmt.Fprintln(w, s.Title); err != nil {
			return err
		}
		// Use tabwriter within each section so key column aligns.
		tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
		for _, kv := range s.Pairs {
			if _, err := fmt.Fprintf(tw, "  %s:\t%s\n", kv[0], kv[1]); err != nil {
				return err
			}
		}
		if err := tw.Flush(); err != nil {
			return err
		}
	}
	return nil
}
