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

	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/log"

	woodpeckerv1alpha1 "github.com/zilliztech/woodpecker/deployments/operator/api/v1alpha1"
)

func (r *WoodpeckerClusterReconciler) updateStatus(ctx context.Context, cluster *woodpeckerv1alpha1.WoodpeckerCluster) error {
	logger := log.FromContext(ctx)

	stsName := serverName(cluster)
	sts := &appsv1.StatefulSet{}
	err := r.Get(ctx, types.NamespacedName{Name: stsName, Namespace: cluster.Namespace}, sts)
	if err != nil {
		if apierrors.IsNotFound(err) {
			cluster.Status.Phase = woodpeckerv1alpha1.PhasePending
			cluster.Status.ReadyReplicas = 0
		} else {
			return err
		}
	} else {
		// Populate server component status
		cluster.Status.Server = woodpeckerv1alpha1.ComponentStatus{
			ReadyReplicas:   sts.Status.ReadyReplicas,
			CurrentReplicas: sts.Status.CurrentReplicas,
			UpdatedReplicas: sts.Status.UpdatedReplicas,
			StatefulSetName: stsName,
		}
		cluster.Status.ReadyReplicas = sts.Status.ReadyReplicas
		cluster.Status.CurrentImage = cluster.Spec.Image

		// Determine phase
		cluster.Status.Phase = r.determinePhase(cluster, sts)

		// Set endpoint
		cluster.Status.Endpoint = fmt.Sprintf("%s.%s.svc:%d",
			clientServiceName(cluster), cluster.Namespace, cluster.Spec.ServicePort)
	}

	// Set conditions
	r.setConditions(cluster)

	// Update observed generation
	cluster.Status.ObservedGeneration = cluster.Generation

	if err := r.Status().Update(ctx, cluster); err != nil {
		return err
	}
	logger.Info("Status updated", "phase", cluster.Status.Phase, "ready", cluster.Status.ReadyReplicas)
	return nil
}

func (r *WoodpeckerClusterReconciler) determinePhase(
	cluster *woodpeckerv1alpha1.WoodpeckerCluster,
	sts *appsv1.StatefulSet,
) woodpeckerv1alpha1.ClusterPhase {
	desired := int32(0)
	if cluster.Spec.Replicas != nil {
		desired = *cluster.Spec.Replicas
	}
	ready := sts.Status.ReadyReplicas
	current := sts.Status.CurrentReplicas
	updated := sts.Status.UpdatedReplicas

	switch {
	case ready == 0 && current == 0:
		return woodpeckerv1alpha1.PhaseCreating
	case updated < desired:
		return woodpeckerv1alpha1.PhaseUpgrading
	case current > desired:
		return woodpeckerv1alpha1.PhaseScalingDown
	case current < desired:
		return woodpeckerv1alpha1.PhaseScalingUp
	case ready == desired:
		return woodpeckerv1alpha1.PhaseRunning
	default:
		return woodpeckerv1alpha1.PhaseCreating
	}
}

func (r *WoodpeckerClusterReconciler) setConditions(cluster *woodpeckerv1alpha1.WoodpeckerCluster) {
	desired := int32(0)
	if cluster.Spec.Replicas != nil {
		desired = *cluster.Spec.Replicas
	}
	ready := cluster.Status.ReadyReplicas

	// Ready condition: all replicas ready
	readyCondition := metav1.Condition{
		Type:               woodpeckerv1alpha1.ConditionReady,
		ObservedGeneration: cluster.Generation,
	}
	if ready == desired && desired > 0 {
		readyCondition.Status = metav1.ConditionTrue
		readyCondition.Reason = "AllReplicasReady"
		readyCondition.Message = fmt.Sprintf("%d/%d replicas ready", ready, desired)
	} else {
		readyCondition.Status = metav1.ConditionFalse
		readyCondition.Reason = "ReplicasNotReady"
		readyCondition.Message = fmt.Sprintf("%d/%d replicas ready", ready, desired)
	}
	meta.SetStatusCondition(&cluster.Status.Conditions, readyCondition)

	// Available condition: at least one replica serving
	availableCondition := metav1.Condition{
		Type:               woodpeckerv1alpha1.ConditionAvailable,
		ObservedGeneration: cluster.Generation,
	}
	if ready > 0 {
		availableCondition.Status = metav1.ConditionTrue
		availableCondition.Reason = "AtLeastOneReplicaReady"
		availableCondition.Message = fmt.Sprintf("%d replicas available", ready)
	} else {
		availableCondition.Status = metav1.ConditionFalse
		availableCondition.Reason = "NoReplicasAvailable"
		availableCondition.Message = "No replicas are ready"
	}
	meta.SetStatusCondition(&cluster.Status.Conditions, availableCondition)
}
