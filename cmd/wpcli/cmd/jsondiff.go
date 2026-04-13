package cmd

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// diffEntry represents one key-path where two JSON documents differ.
type diffEntry struct {
	Path     string // dot-separated key path, e.g. "Woodpecker.Logstore.GRPCConfig.ServerMaxSendSize"
	RefValue string // value in reference node
	CmpValue string // value in compared node
}

// jsonDiff compares two JSON byte slices and returns the list of differing paths.
// Only leaf values are compared; structural differences (key present in one but
// not the other) are also reported.
func jsonDiff(refBytes, cmpBytes []byte) []diffEntry {
	var refObj, cmpObj any
	if err := json.Unmarshal(refBytes, &refObj); err != nil {
		return []diffEntry{{Path: "(root)", RefValue: "(parse error)", CmpValue: string(cmpBytes)}}
	}
	if err := json.Unmarshal(cmpBytes, &cmpObj); err != nil {
		return []diffEntry{{Path: "(root)", RefValue: string(refBytes), CmpValue: "(parse error)"}}
	}
	var diffs []diffEntry
	compareValues("", refObj, cmpObj, &diffs)
	return diffs
}

func compareValues(prefix string, a, b any, diffs *[]diffEntry) {
	aMap, aIsMap := a.(map[string]any)
	bMap, bIsMap := b.(map[string]any)

	if aIsMap && bIsMap {
		// Collect all keys from both sides.
		keys := make(map[string]bool)
		for k := range aMap {
			keys[k] = true
		}
		for k := range bMap {
			keys[k] = true
		}
		sorted := sortedBoolMapKeys(keys)
		for _, k := range sorted {
			path := k
			if prefix != "" {
				path = prefix + "." + k
			}
			aVal, aOK := aMap[k]
			bVal, bOK := bMap[k]
			if aOK && bOK {
				compareValues(path, aVal, bVal, diffs)
			} else if aOK {
				*diffs = append(*diffs, diffEntry{Path: path, RefValue: formatValue(aVal), CmpValue: "(missing)"})
			} else {
				*diffs = append(*diffs, diffEntry{Path: path, RefValue: "(missing)", CmpValue: formatValue(bVal)})
			}
		}
		return
	}

	// Leaf comparison.
	aStr := formatValue(a)
	bStr := formatValue(b)
	if aStr != bStr {
		*diffs = append(*diffs, diffEntry{Path: prefix, RefValue: aStr, CmpValue: bStr})
	}
}

func formatValue(v any) string {
	if v == nil {
		return "null"
	}
	switch val := v.(type) {
	case string:
		return val
	case bool:
		if val {
			return "true"
		}
		return "false"
	case float64:
		// Avoid scientific notation for large numbers.
		if val == float64(int64(val)) {
			return fmt.Sprintf("%d", int64(val))
		}
		return fmt.Sprintf("%g", val)
	case json.Number:
		return string(val)
	default:
		b, _ := json.Marshal(v)
		return string(b)
	}
}

func sortedBoolMapKeys(m map[string]bool) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// renderDiffEntries formats diff entries as a readable table.
func renderDiffEntries(diffs []diffEntry, refID, cmpID string) string {
	if len(diffs) == 0 {
		return ""
	}
	var sb strings.Builder
	// Find max path width for alignment.
	maxPath := 4 // "PATH"
	for _, d := range diffs {
		if len(d.Path) > maxPath {
			maxPath = len(d.Path)
		}
	}
	if maxPath > 60 {
		maxPath = 60
	}
	fmtStr := fmt.Sprintf("    %%-%ds  %%-%ds  %%s\n", maxPath, 30)
	sb.WriteString(fmt.Sprintf(fmtStr, "PATH", refID, cmpID))
	sb.WriteString(fmt.Sprintf(fmtStr, strings.Repeat("-", maxPath), strings.Repeat("-", min(len(refID), 30)), strings.Repeat("-", min(len(cmpID), 30))))
	for _, d := range diffs {
		path := d.Path
		if len(path) > 60 {
			path = "..." + path[len(path)-57:]
		}
		ref := d.RefValue
		if len(ref) > 30 {
			ref = ref[:27] + "..."
		}
		cmp := d.CmpValue
		if len(cmp) > 30 {
			cmp = cmp[:27] + "..."
		}
		sb.WriteString(fmt.Sprintf(fmtStr, path, ref, cmp))
	}
	return sb.String()
}
