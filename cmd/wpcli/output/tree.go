package output

import (
	"fmt"
	"io"
)

// TreeNode is one node in the tree structure RenderTree renders.
type TreeNode struct {
	Label    string
	Children []*TreeNode
}

// RenderTree writes an ASCII tree to w starting from root.
func RenderTree(w io.Writer, root *TreeNode) error {
	if root == nil {
		return nil
	}
	if _, err := fmt.Fprintln(w, root.Label); err != nil {
		return err
	}
	return renderChildren(w, root.Children, "")
}

func renderChildren(w io.Writer, children []*TreeNode, prefix string) error {
	for i, c := range children {
		last := i == len(children)-1
		branch := "├── "
		nextPrefix := prefix + "│   "
		if last {
			branch = "└── "
			nextPrefix = prefix + "    "
		}
		if _, err := fmt.Fprintf(w, "%s%s%s\n", prefix, branch, c.Label); err != nil {
			return err
		}
		if err := renderChildren(w, c.Children, nextPrefix); err != nil {
			return err
		}
	}
	return nil
}
