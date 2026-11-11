package k8s

import (
	"fmt"
	"strings"
)

// StatusCommands returns the kubectl arg slices for `wp k8s status`.
func StatusCommands(cluster string) [][]string {
	return [][]string{
		{"get", "woodpeckercluster", cluster, "-o", "wide"},
		{"describe", "woodpeckercluster", cluster},
		{"get", "pods", "-l", "app.kubernetes.io/instance=" + cluster, "-o", "wide"},
	}
}

// ScaleCommand returns the kubectl args for `wp k8s scale --replicas N`.
func ScaleCommand(cluster string, replicas int) []string {
	patch := fmt.Sprintf(`{"spec":{"replicas":%d}}`, replicas)
	return []string{"patch", "woodpeckercluster", cluster, "--type", "merge", "-p", patch}
}

// LogsCommand returns the kubectl args for `wp k8s logs <target>`.
func LogsCommand(cluster, target string, follow bool, tail int, since string) []string {
	podName := ResolvePodName(cluster, target)
	args := []string{"logs", podName}
	if follow {
		args = append(args, "-f")
	}
	if tail > 0 {
		args = append(args, "--tail", fmt.Sprintf("%d", tail))
	}
	if since != "" {
		args = append(args, "--since", since)
	}
	return args
}

// ResolvePodName converts a node identifier to a pod name.
// If the target already contains the cluster prefix, returns it unchanged.
// Otherwise applies the <cluster>-server-<ordinal> convention.
func ResolvePodName(cluster, nodeIdentifier string) string {
	// If it already looks like a pod name (contains the cluster prefix)
	if strings.HasPrefix(nodeIdentifier, cluster+"-") {
		return nodeIdentifier
	}
	// If it's a bare number, treat as ordinal
	if isNumeric(nodeIdentifier) {
		return fmt.Sprintf("%s-server-%s", cluster, nodeIdentifier)
	}
	// Otherwise return as-is (user knows the pod name)
	return nodeIdentifier
}

func isNumeric(s string) bool {
	for _, c := range s {
		if c < '0' || c > '9' {
			return false
		}
	}
	return len(s) > 0
}
