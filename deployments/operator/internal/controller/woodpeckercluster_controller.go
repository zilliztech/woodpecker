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

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	woodpeckerv1alpha1 "github.com/zilliztech/woodpecker/deployments/operator/api/v1alpha1"
)

// WoodpeckerClusterReconciler reconciles a WoodpeckerCluster object
type WoodpeckerClusterReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=woodpecker.zilliz.io,resources=woodpeckerclusters,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=woodpecker.zilliz.io,resources=woodpeckerclusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=woodpecker.zilliz.io,resources=woodpeckerclusters/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=pods;services;configmaps;serviceaccounts;persistentvolumeclaims;events,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=apps,resources=statefulsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=policy,resources=poddisruptionbudgets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=coordination.k8s.io,resources=leases,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=rbac.authorization.k8s.io,resources=clusterroles;clusterrolebindings,verbs=get;list;watch;create;update;patch;delete

// Reconcile moves the current state toward the desired state defined by the WoodpeckerCluster CR.
func (r *WoodpeckerClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// 1. Fetch the CR
	cluster := &woodpeckerv1alpha1.WoodpeckerCluster{}
	if err := r.Get(ctx, req.NamespacedName, cluster); err != nil {
		if apierrors.IsNotFound(err) {
			logger.Info("WoodpeckerCluster resource not found, skipping reconcile")
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// 2. Handle deletion (finalizer)
	if !cluster.DeletionTimestamp.IsZero() {
		return r.reconcileDelete(ctx, cluster)
	}

	// 3. Ensure finalizer
	if !controllerutil.ContainsFinalizer(cluster, finalizerName) {
		controllerutil.AddFinalizer(cluster, finalizerName)
		if err := r.Update(ctx, cluster); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	// 4. Skip if paused
	if cluster.Spec.Paused {
		logger.Info("WoodpeckerCluster is paused, skipping reconcile")
		return ctrl.Result{}, nil
	}

	// 5. Check scale-down before reconciling StatefulSet
	scaleResult, oldReplicas, err := r.checkScaleDown(ctx, cluster)
	if err != nil {
		return ctrl.Result{}, err
	}
	if scaleResult == scaleDownWaiting {
		// Pods are still decommissioning — don't update StatefulSet replicas yet
		logger.Info("Waiting for pods to decommission before scaling down")
		_ = r.updateStatus(ctx, cluster)
		return ctrl.Result{RequeueAfter: requeueDelay()}, nil
	}

	// 6. Run sub-reconcilers in order
	reconcilers := []func(context.Context, *woodpeckerv1alpha1.WoodpeckerCluster) error{
		r.reconcileServiceAccount,
		r.reconcileRBAC,
		r.reconcileConfigMap,
		r.reconcileHeadlessService,
		r.reconcileClientService,
		r.reconcileMetricsService,
		r.reconcilePDB,
		r.reconcileStatefulSet,
	}

	for _, rec := range reconcilers {
		if err := rec(ctx, cluster); err != nil {
			_ = r.updateStatus(ctx, cluster)
			return ctrl.Result{}, err
		}
	}

	// 7. Clean up orphaned PVCs after scale-down completes
	if scaleResult == scaleDownReady {
		desired := int32(0)
		if cluster.Spec.Replicas != nil {
			desired = *cluster.Spec.Replicas
		}
		if err := r.cleanupOrphanedPVCs(ctx, cluster, oldReplicas, desired); err != nil {
			logger.Error(err, "Failed to cleanup orphaned PVCs")
			// Non-fatal: don't block reconcile for PVC cleanup failure
		}
	}

	// 8. Update status
	if err := r.updateStatus(ctx, cluster); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *WoodpeckerClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&woodpeckerv1alpha1.WoodpeckerCluster{}).
		Owns(&appsv1.StatefulSet{}).
		Owns(&corev1.Service{}).
		Owns(&corev1.ConfigMap{}).
		Owns(&corev1.ServiceAccount{}).
		Owns(&policyv1.PodDisruptionBudget{}).
		// Watch user-provided ConfigMaps (not owned by CR) for config change detection
		Watches(&corev1.ConfigMap{}, handler.EnqueueRequestsFromMapFunc(r.configMapToCluster)).
		Named("woodpeckercluster").
		Complete(r)
}

// configMapToCluster maps a ConfigMap change to the WoodpeckerCluster(s) that reference it via configRef.
func (r *WoodpeckerClusterReconciler) configMapToCluster(ctx context.Context, obj client.Object) []reconcile.Request {
	cm, ok := obj.(*corev1.ConfigMap)
	if !ok {
		return nil
	}

	clusterList := &woodpeckerv1alpha1.WoodpeckerClusterList{}
	if err := r.List(ctx, clusterList, client.InNamespace(cm.Namespace)); err != nil {
		return nil
	}

	var requests []reconcile.Request
	for i := range clusterList.Items {
		cluster := &clusterList.Items[i]
		if cluster.Spec.ConfigRef != nil && cluster.Spec.ConfigRef.Name == cm.Name {
			requests = append(requests, reconcile.Request{
				NamespacedName: client.ObjectKeyFromObject(cluster),
			})
		}
	}
	return requests
}
