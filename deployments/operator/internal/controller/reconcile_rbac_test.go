/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
*/

package controller

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	woodpeckerv1alpha1 "github.com/zilliztech/woodpecker/deployments/operator/api/v1alpha1"
)

func newRBACTestScheme(t *testing.T) *runtime.Scheme {
	s := runtime.NewScheme()
	require.NoError(t, woodpeckerv1alpha1.AddToScheme(s))
	require.NoError(t, rbacv1.AddToScheme(s))
	return s
}

func newRBACTestCluster() *woodpeckerv1alpha1.WoodpeckerCluster {
	return &woodpeckerv1alpha1.WoodpeckerCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "wp", Namespace: "ns1"},
		Spec: woodpeckerv1alpha1.WoodpeckerClusterSpec{
			Replicas: ptr.To(int32(1)),
		},
	}
}

func TestClusterRoleNames(t *testing.T) {
	c := newRBACTestCluster()
	assert.Equal(t, "woodpecker-node-reader-ns1-wp", clusterRoleName(c))
	assert.Equal(t, "woodpecker-node-reader-ns1-wp", clusterRoleBindingName(c))
}

func TestReconcileRBAC_CreatesRoleAndBinding(t *testing.T) {
	s := newRBACTestScheme(t)
	cl := fake.NewClientBuilder().WithScheme(s).Build()
	r := &WoodpeckerClusterReconciler{Client: cl, Scheme: s}
	cluster := newRBACTestCluster()

	err := r.reconcileRBAC(context.Background(), cluster)
	require.NoError(t, err)

	cr := &rbacv1.ClusterRole{}
	require.NoError(t, cl.Get(context.Background(), types.NamespacedName{Name: clusterRoleName(cluster)}, cr))
	require.Len(t, cr.Rules, 1)
	assert.Equal(t, []string{""}, cr.Rules[0].APIGroups)
	assert.Equal(t, []string{"nodes"}, cr.Rules[0].Resources)
	assert.Equal(t, []string{"get"}, cr.Rules[0].Verbs)

	crb := &rbacv1.ClusterRoleBinding{}
	require.NoError(t, cl.Get(context.Background(), types.NamespacedName{Name: clusterRoleBindingName(cluster)}, crb))
	assert.Equal(t, "ClusterRole", crb.RoleRef.Kind)
	assert.Equal(t, clusterRoleName(cluster), crb.RoleRef.Name)
	require.Len(t, crb.Subjects, 1)
	assert.Equal(t, "ServiceAccount", crb.Subjects[0].Kind)
	assert.Equal(t, serverName(cluster), crb.Subjects[0].Name)
	assert.Equal(t, cluster.Namespace, crb.Subjects[0].Namespace)
	assert.Equal(t, "ns1/wp", crb.Labels["woodpecker.zilliz.io/owned-by"])
}

func TestReconcileRBAC_Idempotent(t *testing.T) {
	s := newRBACTestScheme(t)
	cl := fake.NewClientBuilder().WithScheme(s).Build()
	r := &WoodpeckerClusterReconciler{Client: cl, Scheme: s}
	cluster := newRBACTestCluster()

	require.NoError(t, r.reconcileRBAC(context.Background(), cluster))
	require.NoError(t, r.reconcileRBAC(context.Background(), cluster))

	list := &rbacv1.ClusterRoleList{}
	require.NoError(t, cl.List(context.Background(), list, client.MatchingLabels{"woodpecker.zilliz.io/owned-by": "ns1/wp"}))
	assert.Len(t, list.Items, 1)
}

func TestReconcileRBAC_DoesNotErrorWhenAlreadyExists(t *testing.T) {
	s := newRBACTestScheme(t)
	cluster := newRBACTestCluster()
	existing := &rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{Name: clusterRoleName(cluster)},
	}
	cl := fake.NewClientBuilder().WithScheme(s).WithObjects(existing).Build()
	r := &WoodpeckerClusterReconciler{Client: cl, Scheme: s}

	err := r.reconcileRBAC(context.Background(), cluster)
	require.NoError(t, err)

	cr := &rbacv1.ClusterRole{}
	require.NoError(t, cl.Get(context.Background(), types.NamespacedName{Name: clusterRoleName(cluster)}, cr))
	assert.Len(t, cr.Rules, 1, "rules should be updated on existing ClusterRole")
}
