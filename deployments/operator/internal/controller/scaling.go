/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"fmt"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	woodpeckerv1alpha1 "github.com/zilliztech/woodpecker/deployments/operator/api/v1alpha1"
)

// scaleDownResult tells the reconciler what to do after scale-down check.
type scaleDownResult int

const (
	scaleDownNotNeeded scaleDownResult = iota // no scale-down happening
	scaleDownReady                            // all target pods decommissioned, safe to reduce replicas
	scaleDownWaiting                          // still waiting for pods to decommission
)

// checkScaleDown detects a scale-down scenario and decommissions pods that will be removed.
// Returns the result and the current StatefulSet replicas (0 if STS not found).
func (r *WoodpeckerClusterReconciler) checkScaleDown(ctx context.Context, cluster *woodpeckerv1alpha1.WoodpeckerCluster) (scaleDownResult, int32, error) {
	logger := log.FromContext(ctx)

	desired := int32(0)
	if cluster.Spec.Replicas != nil {
		desired = *cluster.Spec.Replicas
	}

	// Get current StatefulSet
	sts := &appsv1.StatefulSet{}
	err := r.Get(ctx, types.NamespacedName{Name: serverName(cluster), Namespace: cluster.Namespace}, sts)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return scaleDownNotNeeded, 0, nil
		}
		return scaleDownNotNeeded, 0, err
	}

	current := *sts.Spec.Replicas
	if desired >= current {
		return scaleDownNotNeeded, current, nil
	}

	// Scale-down detected: desired < current
	logger.Info("Scale-down detected", "current", current, "desired", desired)

	// Decommission pods from highest ordinal down to desired
	allSafe := true
	for ordinal := current - 1; ordinal >= desired; ordinal-- {
		podName := fmt.Sprintf("%s-server-%d", cluster.Name, ordinal)
		pod := &corev1.Pod{}
		err := r.Get(ctx, types.NamespacedName{Name: podName, Namespace: cluster.Namespace}, pod)
		if err != nil {
			if apierrors.IsNotFound(err) {
				continue // pod already gone
			}
			return scaleDownWaiting, current, err
		}

		if pod.Status.Phase != corev1.PodRunning {
			continue
		}

		safe, err := r.decommissionPod(ctx, pod, cluster.Spec.MetricsPort)
		if err != nil {
			logger.Error(err, "Failed to decommission pod during scale-down", "pod", podName)
			allSafe = false
			continue
		}
		if !safe {
			logger.Info("Pod not yet safe to terminate", "pod", podName)
			allSafe = false
		}
	}

	if !allSafe {
		return scaleDownWaiting, current, nil
	}

	logger.Info("All target pods decommissioned, safe to scale down", "from", current, "to", desired)
	return scaleDownReady, current, nil
}

// requeueDelay returns the requeue delay for scale-down waiting.
func requeueDelay() time.Duration {
	return 10 * time.Second
}

// cleanupOrphanedPVCs deletes PVCs left behind after scale-down.
func (r *WoodpeckerClusterReconciler) cleanupOrphanedPVCs(ctx context.Context, cluster *woodpeckerv1alpha1.WoodpeckerCluster, oldReplicas, newReplicas int32) error {
	logger := log.FromContext(ctx)

	for ordinal := oldReplicas - 1; ordinal >= newReplicas; ordinal-- {
		pvcName := fmt.Sprintf("data-%s-server-%d", cluster.Name, ordinal)
		pvc := &corev1.PersistentVolumeClaim{}
		err := r.Get(ctx, types.NamespacedName{Name: pvcName, Namespace: cluster.Namespace}, pvc)
		if err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return err
		}

		logger.Info("Cleaning up orphaned PVC", "pvc", pvcName)
		if err := r.Delete(ctx, pvc, client.PropagationPolicy("Background")); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
	}
	return nil
}
