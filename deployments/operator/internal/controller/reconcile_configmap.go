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
	"crypto/sha256"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/log"

	woodpeckerv1alpha1 "github.com/zilliztech/woodpecker/deployments/operator/api/v1alpha1"
)

const defaultConfig = `woodpecker:
  storage:
    type: service
    rootPath: /woodpecker/data
log:
  level: info
  format: json
  stdout: true
`

// reconcileConfigMap validates the user-provided ConfigMap or creates a default one.
// It computes and stores a config hash for rolling update detection.
func (r *WoodpeckerClusterReconciler) reconcileConfigMap(ctx context.Context, cluster *woodpeckerv1alpha1.WoodpeckerCluster) error {
	logger := log.FromContext(ctx)

	if cluster.Spec.ConfigRef != nil {
		// User provided a ConfigMap reference — validate it exists and has the right key
		cm := &corev1.ConfigMap{}
		err := r.Get(ctx, types.NamespacedName{
			Name:      cluster.Spec.ConfigRef.Name,
			Namespace: cluster.Namespace,
		}, cm)
		if err != nil {
			if apierrors.IsNotFound(err) {
				return fmt.Errorf("configRef ConfigMap %q not found in namespace %q", cluster.Spec.ConfigRef.Name, cluster.Namespace)
			}
			return err
		}
		if _, ok := cm.Data["woodpecker.yaml"]; !ok {
			return fmt.Errorf("configRef ConfigMap %q missing required key \"woodpecker.yaml\"", cluster.Spec.ConfigRef.Name)
		}
		logger.Info("ConfigMap validated", "name", cm.Name)
		return nil
	}

	// No configRef — operator manages a default ConfigMap
	return r.ensureDefaultConfigMap(ctx, cluster)
}

func (r *WoodpeckerClusterReconciler) ensureDefaultConfigMap(ctx context.Context, cluster *woodpeckerv1alpha1.WoodpeckerCluster) error {
	logger := log.FromContext(ctx)

	cm := &corev1.ConfigMap{}
	cmName := configMapName(cluster)
	err := r.Get(ctx, types.NamespacedName{Name: cmName, Namespace: cluster.Namespace}, cm)
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return err
		}
		// Create default ConfigMap
		cm = r.buildDefaultConfigMap(cluster)
		if err := r.Create(ctx, cm); err != nil {
			return err
		}
		logger.Info("Default ConfigMap created", "name", cmName)
	}
	return nil
}

func (r *WoodpeckerClusterReconciler) buildDefaultConfigMap(cluster *woodpeckerv1alpha1.WoodpeckerCluster) *corev1.ConfigMap {
	cm := &corev1.ConfigMap{}
	cm.Name = configMapName(cluster)
	cm.Namespace = cluster.Namespace
	cm.Labels = commonLabels(cluster)
	cm.Data = map[string]string{
		"woodpecker.yaml": defaultConfig,
	}
	// Note: we don't set owner reference on default ConfigMap so user can take ownership
	return cm
}

// ConfigHash returns the SHA-256 hash of the active woodpecker.yaml content.
func (r *WoodpeckerClusterReconciler) configHash(ctx context.Context, cluster *woodpeckerv1alpha1.WoodpeckerCluster) (string, error) {
	cmName := configMapName(cluster)
	if cluster.Spec.ConfigRef != nil {
		cmName = cluster.Spec.ConfigRef.Name
	}

	cm := &corev1.ConfigMap{}
	if err := r.Get(ctx, types.NamespacedName{Name: cmName, Namespace: cluster.Namespace}, cm); err != nil {
		return "", err
	}

	content := cm.Data["woodpecker.yaml"]
	hash := sha256.Sum256([]byte(content))
	return fmt.Sprintf("%x", hash[:8]), nil
}

// ActiveConfigMapName returns the name of the ConfigMap to mount in pods.
func ActiveConfigMapName(cluster *woodpeckerv1alpha1.WoodpeckerCluster) string {
	if cluster.Spec.ConfigRef != nil {
		return cluster.Spec.ConfigRef.Name
	}
	return configMapName(cluster)
}
