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
	"k8s.io/utils/ptr"

	woodpeckerv1alpha1 "github.com/zilliztech/woodpecker/deployments/operator/api/v1alpha1"
)

func newTestCluster() *woodpeckerv1alpha1.WoodpeckerCluster {
	return &woodpeckerv1alpha1.WoodpeckerCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "wp", Namespace: "default"},
	}
}

func TestDefaultZoneTopologySpreadConstraint(t *testing.T) {
	c := defaultZoneTopologySpreadConstraint(newTestCluster())

	assert.Equal(t, int32(1), c.MaxSkew)
	assert.Equal(t, "topology.kubernetes.io/zone", c.TopologyKey)
	assert.Equal(t, corev1.DoNotSchedule, c.WhenUnsatisfiable)
	require.NotNil(t, c.LabelSelector)
	assert.Equal(t, "wp", c.LabelSelector.MatchLabels["app.kubernetes.io/instance"])
	assert.Equal(t, "woodpecker", c.LabelSelector.MatchLabels["app.kubernetes.io/name"])
}

func TestMergeTopologySpreadConstraints_EmptyUser_AddsDefault(t *testing.T) {
	cluster := newTestCluster()

	result := mergeTopologySpreadConstraints(cluster, nil)

	require.Len(t, result, 1)
	assert.Equal(t, "topology.kubernetes.io/zone", result[0].TopologyKey)
}

func TestMergeTopologySpreadConstraints_UserOnlyHostname_AddsDefault(t *testing.T) {
	cluster := newTestCluster()
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
	cluster := newTestCluster()
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

func TestBuildInitContainers_UsesCurlImage(t *testing.T) {
	r := &WoodpeckerClusterReconciler{}
	cluster := &woodpeckerv1alpha1.WoodpeckerCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "wp", Namespace: "default"},
		Spec: woodpeckerv1alpha1.WoodpeckerClusterSpec{
			GossipPort: 17946,
			Replicas:   ptr.To(int32(3)),
		},
	}

	initCs := r.buildInitContainers(cluster)
	require.Len(t, initCs, 1)
	assert.Equal(t, "init-topology", initCs[0].Name)
	assert.Contains(t, initCs[0].Image, "curlimages/curl")
}

func TestBuildInitContainers_HasHostNodeNameFromSpecNodeName(t *testing.T) {
	r := &WoodpeckerClusterReconciler{}
	cluster := &woodpeckerv1alpha1.WoodpeckerCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "wp", Namespace: "default"},
		Spec: woodpeckerv1alpha1.WoodpeckerClusterSpec{
			GossipPort: 17946,
			Replicas:   ptr.To(int32(1)),
		},
	}
	initCs := r.buildInitContainers(cluster)
	require.Len(t, initCs, 1)

	var hostNodeName *corev1.EnvVar
	for i := range initCs[0].Env {
		if initCs[0].Env[i].Name == "HOST_NODE_NAME" {
			hostNodeName = &initCs[0].Env[i]
			break
		}
	}
	require.NotNil(t, hostNodeName, "expected HOST_NODE_NAME env var")
	require.NotNil(t, hostNodeName.ValueFrom)
	require.NotNil(t, hostNodeName.ValueFrom.FieldRef)
	assert.Equal(t, "spec.nodeName", hostNodeName.ValueFrom.FieldRef.FieldPath)
}

func TestBuildInitContainers_ScriptCallsK8sAPI(t *testing.T) {
	r := &WoodpeckerClusterReconciler{}
	cluster := &woodpeckerv1alpha1.WoodpeckerCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "wp", Namespace: "default"},
		Spec: woodpeckerv1alpha1.WoodpeckerClusterSpec{
			GossipPort: 17946,
			Replicas:   ptr.To(int32(1)),
		},
	}
	initCs := r.buildInitContainers(cluster)
	require.Len(t, initCs, 1)
	require.Len(t, initCs[0].Command, 3)
	script := initCs[0].Command[2]
	assert.Contains(t, script, "kubernetes.default.svc")
	assert.Contains(t, script, "topology.kubernetes.io/zone")
	assert.Contains(t, script, "topology.kubernetes.io/region")
	assert.Contains(t, script, `: "${AZ:=}"`)
	assert.Contains(t, script, `: "${REGION:=}"`)
	assert.Contains(t, script, "CLUSTER_NAME=wp")
	assert.Contains(t, script, "REGION=${REGION}")
	assert.Contains(t, script, "AVAILABILITY_ZONE=${AZ}")
	assert.NotContains(t, script, "default-az")
	assert.NotContains(t, script, "default-cluster")
	assert.NotContains(t, script, "CLUSTER_NAME=${REGION}")
	// Regex must allow optional whitespace after the colon — K8s API returns
	// pretty-printed JSON with `"key": "value"` (note the space).
	assert.Contains(t, script, `"topology.kubernetes.io/zone": *"[^"]*"`)
	assert.Contains(t, script, `"topology.kubernetes.io/region": *"[^"]*"`)
}

func TestBuildContainers_ExportsClusterName(t *testing.T) {
	r := &WoodpeckerClusterReconciler{}
	cluster := &woodpeckerv1alpha1.WoodpeckerCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "wp", Namespace: "default"},
		Spec: woodpeckerv1alpha1.WoodpeckerClusterSpec{
			Image:       "zilliztech/woodpecker:v0.1.26",
			ServicePort: 18080,
			GossipPort:  17946,
			MetricsPort: 9091,
		},
	}
	cs := r.buildContainers(cluster)
	require.Len(t, cs, 1)
	require.Len(t, cs[0].Args, 1)
	assert.Contains(t, cs[0].Args[0], "export SEEDS CLUSTER_NAME REGION AVAILABILITY_ZONE RESOURCE_GROUP")
}
