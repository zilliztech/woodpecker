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
	"io"
	"net/http"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	woodpeckerv1alpha1 "github.com/zilliztech/woodpecker/deployments/operator/api/v1alpha1"
)

// httpClient is overridable in tests so we can replace calls with an
// httptest.Server. Default is http.DefaultClient with a 30s timeout.
var httpClient = &http.Client{Timeout: 30 * time.Second}

const finalizerName = "woodpecker.zilliz.io/finalizer"

// reconcileDelete handles CR deletion: decommission all pods, then remove finalizer.
func (r *WoodpeckerClusterReconciler) reconcileDelete(ctx context.Context, cluster *woodpeckerv1alpha1.WoodpeckerCluster) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	if !controllerutil.ContainsFinalizer(cluster, finalizerName) {
		return ctrl.Result{}, nil
	}

	logger.Info("Handling deletion, decommissioning pods")

	// List all pods owned by this cluster
	podList := &corev1.PodList{}
	err := r.List(ctx, podList,
		client.InNamespace(cluster.Namespace),
		client.MatchingLabelsSelector{Selector: labels.SelectorFromSet(commonLabels(cluster))},
	)
	if err != nil {
		return ctrl.Result{}, err
	}

	// Decommission each running pod
	allSafe := true
	for i := range podList.Items {
		pod := &podList.Items[i]
		if pod.Status.Phase != corev1.PodRunning {
			continue
		}

		safe, err := r.decommissionPod(ctx, pod, cluster.Spec.MetricsPort)
		if err != nil {
			logger.Error(err, "Failed to decommission pod", "pod", pod.Name)
			allSafe = false
			continue
		}
		if !safe {
			allSafe = false
		}
	}

	if !allSafe {
		logger.Info("Not all pods are safe to terminate yet, requeuing")
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}

	// Clean up cluster-scoped RBAC before removing the finalizer. These objects
	// do not have namespace-scoped owner refs, so K8s GC will not cascade.
	if err := r.deleteClusterScopedRBAC(ctx, cluster); err != nil {
		return ctrl.Result{}, err
	}

	// All pods safe — remove finalizer
	logger.Info("All pods decommissioned, removing finalizer")
	controllerutil.RemoveFinalizer(cluster, finalizerName)
	if err := r.Update(ctx, cluster); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

// decommissionPod triggers decommission on a pod and checks if it's safe to terminate.
// Returns (safeToTerminate, error).
func (r *WoodpeckerClusterReconciler) decommissionPod(ctx context.Context, pod *corev1.Pod, metricsPort int32) (bool, error) {
	logger := log.FromContext(ctx)
	podIP := pod.Status.PodIP
	if podIP == "" {
		return false, nil
	}

	baseURL := fmt.Sprintf("http://%s:%d", podIP, metricsPort)

	// Trigger decommission (idempotent)
	resp, err := httpClient.Post(baseURL+"/admin/node/decommission", "", nil)
	if err != nil {
		return false, fmt.Errorf("calling decommission on %s: %w", pod.Name, err)
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, resp.Body)

	// Check progress
	progressResp, err := httpClient.Get(baseURL + "/admin/node/decommission/progress")
	if err != nil {
		return false, fmt.Errorf("checking decommission progress on %s: %w", pod.Name, err)
	}
	defer func() { _ = progressResp.Body.Close() }()
	body, _ := io.ReadAll(progressResp.Body)

	// The response contains "safe_to_terminate":true when ready
	safe := progressResp.StatusCode == http.StatusOK &&
		strings.Contains(string(body), "\"safe_to_terminate\":true")

	logger.Info("Decommission progress", "pod", pod.Name, "safeToTerminate", safe)
	return safe, nil
}

func (r *WoodpeckerClusterReconciler) deleteClusterScopedRBAC(ctx context.Context, cluster *woodpeckerv1alpha1.WoodpeckerCluster) error {
	logger := log.FromContext(ctx)

	crb := &rbacv1.ClusterRoleBinding{ObjectMeta: metav1.ObjectMeta{Name: clusterRoleBindingName(cluster)}}
	if err := r.Delete(ctx, crb); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("deleting ClusterRoleBinding: %w", err)
	}
	logger.Info("ClusterRoleBinding deleted", "name", crb.Name)

	cr := &rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: clusterRoleName(cluster)}}
	if err := r.Delete(ctx, cr); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("deleting ClusterRole: %w", err)
	}
	logger.Info("ClusterRole deleted", "name", cr.Name)

	return nil
}
