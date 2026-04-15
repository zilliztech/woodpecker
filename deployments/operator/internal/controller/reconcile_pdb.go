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

	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	woodpeckerv1alpha1 "github.com/zilliztech/woodpecker/deployments/operator/api/v1alpha1"
)

func pdbName(cluster *woodpeckerv1alpha1.WoodpeckerCluster) string {
	return cluster.Name + "-server-pdb"
}

func (r *WoodpeckerClusterReconciler) reconcilePDB(ctx context.Context, cluster *woodpeckerv1alpha1.WoodpeckerCluster) error {
	logger := log.FromContext(ctx)

	pdb := &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pdbName(cluster),
			Namespace: cluster.Namespace,
		},
	}

	// minAvailable = replicas/2 + 1 (quorum majority)
	replicas := int32(3)
	if cluster.Spec.Replicas != nil {
		replicas = *cluster.Spec.Replicas
	}
	minAvailable := intstr.FromInt32(replicas/2 + 1)

	op, err := controllerutil.CreateOrUpdate(ctx, r.Client, pdb, func() error {
		pdb.Labels = commonLabels(cluster)
		pdb.Spec = policyv1.PodDisruptionBudgetSpec{
			MinAvailable: &minAvailable,
			Selector: &metav1.LabelSelector{
				MatchLabels: commonLabels(cluster),
			},
		}
		return ctrl.SetControllerReference(cluster, pdb, r.Scheme)
	})
	if err != nil {
		return err
	}

	logger.Info("PDB reconciled", "name", pdb.Name, "minAvailable", minAvailable.IntValue(), "operation", op)
	return nil
}
