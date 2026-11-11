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
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	woodpeckerv1alpha1 "github.com/zilliztech/woodpecker/deployments/operator/api/v1alpha1"
)

// --- resolveServiceAccountName ---

func TestResolveServiceAccountName_Default(t *testing.T) {
	cluster := &woodpeckerv1alpha1.WoodpeckerCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "wp", Namespace: "default"},
	}
	assert.Equal(t, "wp-server", resolveServiceAccountName(cluster))
}

func TestResolveServiceAccountName_External(t *testing.T) {
	cluster := &woodpeckerv1alpha1.WoodpeckerCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "wp", Namespace: "default"},
		Spec: woodpeckerv1alpha1.WoodpeckerClusterSpec{
			ServiceAccountName: "my-external-sa",
		},
	}
	assert.Equal(t, "my-external-sa", resolveServiceAccountName(cluster))
}

// --- reconcileServiceAccount ---

func TestReconcileServiceAccount_SkipsWhenExternalSA(t *testing.T) {
	s := newRBACTestScheme(t)
	require.NoError(t, corev1.AddToScheme(s))
	cl := fake.NewClientBuilder().WithScheme(s).Build()
	r := &WoodpeckerClusterReconciler{Client: cl, Scheme: s}

	cluster := &woodpeckerv1alpha1.WoodpeckerCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "wp", Namespace: "default"},
		Spec: woodpeckerv1alpha1.WoodpeckerClusterSpec{
			ServiceAccountName: "my-external-sa",
			Replicas:           ptr.To(int32(1)),
		},
	}

	err := r.reconcileServiceAccount(context.Background(), cluster)
	require.NoError(t, err)

	// Verify no SA was created
	sa := &corev1.ServiceAccount{}
	err = cl.Get(context.Background(), types.NamespacedName{Name: "wp-server", Namespace: "default"}, sa)
	assert.Error(t, err, "SA should not be created when external SA is specified")
}

func TestReconcileServiceAccount_CreatesWhenNoExternalSA(t *testing.T) {
	s := newRBACTestScheme(t)
	require.NoError(t, corev1.AddToScheme(s))
	cl := fake.NewClientBuilder().WithScheme(s).Build()
	r := &WoodpeckerClusterReconciler{Client: cl, Scheme: s}

	cluster := &woodpeckerv1alpha1.WoodpeckerCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "wp", Namespace: "default"},
		Spec: woodpeckerv1alpha1.WoodpeckerClusterSpec{
			Replicas: ptr.To(int32(1)),
		},
	}

	err := r.reconcileServiceAccount(context.Background(), cluster)
	require.NoError(t, err)

	// Verify SA was created
	sa := &corev1.ServiceAccount{}
	err = cl.Get(context.Background(), types.NamespacedName{Name: "wp-server", Namespace: "default"}, sa)
	require.NoError(t, err)
	assert.Equal(t, "wp-server", sa.Name)
}

// --- buildPodSpec SA reference ---

func TestBuildPodSpec_UsesDefaultSA(t *testing.T) {
	r := &WoodpeckerClusterReconciler{}
	cluster := &woodpeckerv1alpha1.WoodpeckerCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "wp", Namespace: "default"},
		Spec: woodpeckerv1alpha1.WoodpeckerClusterSpec{
			ServicePort: 18080,
			GossipPort:  17946,
		},
	}

	spec := r.buildPodSpec(cluster)
	assert.Equal(t, "wp-server", spec.ServiceAccountName)
}

func TestBuildPodSpec_UsesExternalSA(t *testing.T) {
	r := &WoodpeckerClusterReconciler{}
	cluster := &woodpeckerv1alpha1.WoodpeckerCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "wp", Namespace: "default"},
		Spec: woodpeckerv1alpha1.WoodpeckerClusterSpec{
			ServiceAccountName: "my-external-sa",
			ServicePort:        18080,
			GossipPort:         17946,
		},
	}

	spec := r.buildPodSpec(cluster)
	assert.Equal(t, "my-external-sa", spec.ServiceAccountName)
}

// --- reconcileRBAC binds to correct SA ---

func TestReconcileRBAC_BindsToExternalSA(t *testing.T) {
	s := newRBACTestScheme(t)
	cl := fake.NewClientBuilder().WithScheme(s).Build()
	r := &WoodpeckerClusterReconciler{Client: cl, Scheme: s}

	cluster := &woodpeckerv1alpha1.WoodpeckerCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "wp", Namespace: "ns1"},
		Spec: woodpeckerv1alpha1.WoodpeckerClusterSpec{
			ServiceAccountName: "my-external-sa",
			Replicas:           ptr.To(int32(1)),
		},
	}

	err := r.reconcileRBAC(context.Background(), cluster)
	require.NoError(t, err)

	crb := &rbacv1.ClusterRoleBinding{}
	require.NoError(t, cl.Get(context.Background(), types.NamespacedName{Name: clusterRoleBindingName(cluster)}, crb))
	require.Len(t, crb.Subjects, 1)
	assert.Equal(t, "my-external-sa", crb.Subjects[0].Name)
	assert.Equal(t, "ns1", crb.Subjects[0].Namespace)
}

func TestReconcileRBAC_BindsToDefaultSA(t *testing.T) {
	s := newRBACTestScheme(t)
	cl := fake.NewClientBuilder().WithScheme(s).Build()
	r := &WoodpeckerClusterReconciler{Client: cl, Scheme: s}

	cluster := &woodpeckerv1alpha1.WoodpeckerCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "wp", Namespace: "ns1"},
		Spec: woodpeckerv1alpha1.WoodpeckerClusterSpec{
			Replicas: ptr.To(int32(1)),
		},
	}

	err := r.reconcileRBAC(context.Background(), cluster)
	require.NoError(t, err)

	crb := &rbacv1.ClusterRoleBinding{}
	require.NoError(t, cl.Get(context.Background(), types.NamespacedName{Name: clusterRoleBindingName(cluster)}, crb))
	require.Len(t, crb.Subjects, 1)
	assert.Equal(t, "wp-server", crb.Subjects[0].Name)
}
