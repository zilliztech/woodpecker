/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
*/

package controller

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	woodpeckerv1alpha1 "github.com/zilliztech/woodpecker/deployments/operator/api/v1alpha1"
)

func newTestCluster(name string) *woodpeckerv1alpha1.WoodpeckerCluster {
	return &woodpeckerv1alpha1.WoodpeckerCluster{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
	}
}

func TestDefaultZoneTopologySpreadConstraint(t *testing.T) {
	c := defaultZoneTopologySpreadConstraint(newTestCluster("wp"))

	assert.Equal(t, int32(1), c.MaxSkew)
	assert.Equal(t, "topology.kubernetes.io/zone", c.TopologyKey)
	assert.Equal(t, corev1.DoNotSchedule, c.WhenUnsatisfiable)
	require.NotNil(t, c.LabelSelector)
	assert.Equal(t, "wp", c.LabelSelector.MatchLabels["app.kubernetes.io/instance"])
	assert.Equal(t, "woodpecker", c.LabelSelector.MatchLabels["app.kubernetes.io/name"])
}

func TestMergeTopologySpreadConstraints_EmptyUser_AddsDefault(t *testing.T) {
	cluster := newTestCluster("wp")

	result := mergeTopologySpreadConstraints(cluster, nil)

	require.Len(t, result, 1)
	assert.Equal(t, "topology.kubernetes.io/zone", result[0].TopologyKey)
}

func TestMergeTopologySpreadConstraints_UserOnlyHostname_AddsDefault(t *testing.T) {
	cluster := newTestCluster("wp")
	userC := []corev1.TopologySpreadConstraint{{
		MaxSkew:           1,
		TopologyKey:       "kubernetes.io/hostname",
		WhenUnsatisfiable: corev1.ScheduleAnyway,
	}}

	result := mergeTopologySpreadConstraints(cluster, userC)

	require.Len(t, result, 2)
	assert.Equal(t, "kubernetes.io/hostname", result[0].TopologyKey)
	assert.Equal(t, "topology.kubernetes.io/zone", result[1].TopologyKey)
}

func TestMergeTopologySpreadConstraints_UserHasZone_DefaultNotAdded(t *testing.T) {
	cluster := newTestCluster("wp")
	userC := []corev1.TopologySpreadConstraint{{
		MaxSkew:           3,
		TopologyKey:       "topology.kubernetes.io/zone",
		WhenUnsatisfiable: corev1.ScheduleAnyway,
	}}

	result := mergeTopologySpreadConstraints(cluster, userC)

	require.Len(t, result, 1)
	assert.Equal(t, int32(3), result[0].MaxSkew)
	assert.Equal(t, corev1.ScheduleAnyway, result[0].WhenUnsatisfiable)
}

func TestBuildPodSpec_InjectsDefaultZoneConstraint(t *testing.T) {
	r := &WoodpeckerClusterReconciler{}
	cluster := &woodpeckerv1alpha1.WoodpeckerCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "wp", Namespace: "default"},
		Spec: woodpeckerv1alpha1.WoodpeckerClusterSpec{
			ServicePort: 18080,
			GossipPort:  17946,
		},
	}

	spec := r.buildPodSpec(cluster)

	require.Len(t, spec.TopologySpreadConstraints, 1)
	assert.Equal(t, "topology.kubernetes.io/zone", spec.TopologySpreadConstraints[0].TopologyKey)
}
