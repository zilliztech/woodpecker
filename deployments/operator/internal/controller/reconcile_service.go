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
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	woodpeckerv1alpha1 "github.com/zilliztech/woodpecker/deployments/operator/api/v1alpha1"
)

// reconcileHeadlessService creates the headless service for pod DNS (gossip discovery).
func (r *WoodpeckerClusterReconciler) reconcileHeadlessService(ctx context.Context, cluster *woodpeckerv1alpha1.WoodpeckerCluster) error {
	logger := log.FromContext(ctx)

	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      headlessServiceName(cluster),
			Namespace: cluster.Namespace,
		},
	}

	op, err := controllerutil.CreateOrUpdate(ctx, r.Client, svc, func() error {
		svc.Labels = commonLabels(cluster)
		svc.Spec = corev1.ServiceSpec{
			ClusterIP: corev1.ClusterIPNone,
			Selector:  commonLabels(cluster),
			Ports: []corev1.ServicePort{
				{
					Name:       "grpc",
					Port:       cluster.Spec.ServicePort,
					TargetPort: intstr.FromString("grpc"),
					Protocol:   corev1.ProtocolTCP,
				},
				{
					Name:       "gossip-tcp",
					Port:       cluster.Spec.GossipPort,
					TargetPort: intstr.FromString("gossip-tcp"),
					Protocol:   corev1.ProtocolTCP,
				},
				{
					Name:       "gossip-udp",
					Port:       cluster.Spec.GossipPort,
					TargetPort: intstr.FromString("gossip-udp"),
					Protocol:   corev1.ProtocolUDP,
				},
				{
					Name:       "metrics",
					Port:       cluster.Spec.MetricsPort,
					TargetPort: intstr.FromString("metrics"),
					Protocol:   corev1.ProtocolTCP,
				},
			},
			PublishNotReadyAddresses: true,
		}
		return ctrl.SetControllerReference(cluster, svc, r.Scheme)
	})
	if err != nil {
		return err
	}
	logger.Info("Headless Service reconciled", "name", svc.Name, "operation", op)
	return nil
}

// reconcileClientService creates the ClusterIP service for gRPC client access.
func (r *WoodpeckerClusterReconciler) reconcileClientService(ctx context.Context, cluster *woodpeckerv1alpha1.WoodpeckerCluster) error {
	logger := log.FromContext(ctx)

	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      clientServiceName(cluster),
			Namespace: cluster.Namespace,
		},
	}

	op, err := controllerutil.CreateOrUpdate(ctx, r.Client, svc, func() error {
		svc.Labels = commonLabels(cluster)
		svc.Spec = corev1.ServiceSpec{
			Type:     corev1.ServiceTypeClusterIP,
			Selector: commonLabels(cluster),
			Ports: []corev1.ServicePort{
				{
					Name:       "grpc",
					Port:       cluster.Spec.ServicePort,
					TargetPort: intstr.FromString("grpc"),
					Protocol:   corev1.ProtocolTCP,
				},
			},
		}
		return ctrl.SetControllerReference(cluster, svc, r.Scheme)
	})
	if err != nil {
		return err
	}
	logger.Info("Client Service reconciled", "name", svc.Name, "operation", op)
	return nil
}

// reconcileMetricsService creates the ClusterIP service for metrics/health endpoint.
func (r *WoodpeckerClusterReconciler) reconcileMetricsService(ctx context.Context, cluster *woodpeckerv1alpha1.WoodpeckerCluster) error {
	logger := log.FromContext(ctx)

	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      metricsServiceName(cluster),
			Namespace: cluster.Namespace,
		},
	}

	op, err := controllerutil.CreateOrUpdate(ctx, r.Client, svc, func() error {
		svc.Labels = commonLabels(cluster)
		svc.Spec = corev1.ServiceSpec{
			Type:     corev1.ServiceTypeClusterIP,
			Selector: commonLabels(cluster),
			Ports: []corev1.ServicePort{
				{
					Name:       "metrics",
					Port:       cluster.Spec.MetricsPort,
					TargetPort: intstr.FromString("metrics"),
					Protocol:   corev1.ProtocolTCP,
				},
			},
		}
		return ctrl.SetControllerReference(cluster, svc, r.Scheme)
	})
	if err != nil {
		return err
	}
	logger.Info("Metrics Service reconciled", "name", svc.Name, "operation", op)
	return nil
}
