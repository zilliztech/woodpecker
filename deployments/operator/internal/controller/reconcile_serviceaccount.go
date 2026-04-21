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

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	woodpeckerv1alpha1 "github.com/zilliztech/woodpecker/deployments/operator/api/v1alpha1"
)

func (r *WoodpeckerClusterReconciler) reconcileServiceAccount(ctx context.Context, cluster *woodpeckerv1alpha1.WoodpeckerCluster) error {
	logger := log.FromContext(ctx)

	if cluster.Spec.ServiceAccountName != "" {
		logger.Info("Using external ServiceAccount, skipping creation", "name", cluster.Spec.ServiceAccountName)
		return nil
	}

	sa := &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:      serverName(cluster),
			Namespace: cluster.Namespace,
		},
	}

	op, err := controllerutil.CreateOrUpdate(ctx, r.Client, sa, func() error {
		sa.Labels = commonLabels(cluster)
		return ctrl.SetControllerReference(cluster, sa, r.Scheme)
	})
	if err != nil {
		return err
	}

	logger.Info("ServiceAccount reconciled", "name", sa.Name, "operation", op)
	return nil
}
