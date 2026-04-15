/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
*/

package controller

import (
	"context"
	"fmt"

	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	woodpeckerv1alpha1 "github.com/zilliztech/woodpecker/deployments/operator/api/v1alpha1"
)

const ownedByLabel = "woodpecker.zilliz.io/owned-by"

func clusterRoleName(cluster *woodpeckerv1alpha1.WoodpeckerCluster) string {
	return fmt.Sprintf("woodpecker-node-reader-%s-%s", cluster.Namespace, cluster.Name)
}

func clusterRoleBindingName(cluster *woodpeckerv1alpha1.WoodpeckerCluster) string {
	return clusterRoleName(cluster)
}

func rbacLabels(cluster *woodpeckerv1alpha1.WoodpeckerCluster) map[string]string {
	l := commonLabels(cluster)
	l[ownedByLabel] = fmt.Sprintf("%s/%s", cluster.Namespace, cluster.Name)
	return l
}

// reconcileRBAC ensures a per-cluster ClusterRole and ClusterRoleBinding exist
// so the pod's ServiceAccount can GET node objects to read topology labels.
// These cluster-scoped objects cannot have namespaced owner references; they
// are cleaned up by the finalizer on CR deletion.
func (r *WoodpeckerClusterReconciler) reconcileRBAC(ctx context.Context, cluster *woodpeckerv1alpha1.WoodpeckerCluster) error {
	logger := log.FromContext(ctx)

	cr := &rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: clusterRoleName(cluster)}}
	crOp, err := controllerutil.CreateOrUpdate(ctx, r.Client, cr, func() error {
		cr.Labels = rbacLabels(cluster)
		cr.Rules = []rbacv1.PolicyRule{{
			APIGroups: []string{""},
			Resources: []string{"nodes"},
			Verbs:     []string{"get"},
		}}
		return nil
	})
	if err != nil {
		return fmt.Errorf("reconciling ClusterRole: %w", err)
	}
	logger.Info("ClusterRole reconciled", "name", cr.Name, "operation", crOp)

	crb := &rbacv1.ClusterRoleBinding{ObjectMeta: metav1.ObjectMeta{Name: clusterRoleBindingName(cluster)}}
	crbOp, err := controllerutil.CreateOrUpdate(ctx, r.Client, crb, func() error {
		crb.Labels = rbacLabels(cluster)
		crb.RoleRef = rbacv1.RoleRef{
			APIGroup: rbacv1.GroupName,
			Kind:     "ClusterRole",
			Name:     clusterRoleName(cluster),
		}
		crb.Subjects = []rbacv1.Subject{{
			Kind:      "ServiceAccount",
			Name:      serverName(cluster),
			Namespace: cluster.Namespace,
		}}
		return nil
	})
	if err != nil {
		return fmt.Errorf("reconciling ClusterRoleBinding: %w", err)
	}
	logger.Info("ClusterRoleBinding reconciled", "name", crb.Name, "operation", crbOp)

	return nil
}
