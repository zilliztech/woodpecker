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
	woodpeckerv1alpha1 "github.com/zilliztech/woodpecker/deployments/operator/api/v1alpha1"
)

func commonLabels(cluster *woodpeckerv1alpha1.WoodpeckerCluster) map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":       "woodpecker",
		"app.kubernetes.io/instance":   cluster.Name,
		"app.kubernetes.io/component":  "server",
		"app.kubernetes.io/part-of":    "woodpecker",
		"app.kubernetes.io/managed-by": "woodpecker-operator",
	}
}

func serverName(cluster *woodpeckerv1alpha1.WoodpeckerCluster) string {
	return cluster.Name + "-server"
}

func headlessServiceName(cluster *woodpeckerv1alpha1.WoodpeckerCluster) string {
	return cluster.Name + "-server-headless"
}

func clientServiceName(cluster *woodpeckerv1alpha1.WoodpeckerCluster) string {
	return cluster.Name + "-server-client"
}

func metricsServiceName(cluster *woodpeckerv1alpha1.WoodpeckerCluster) string {
	return cluster.Name + "-server-metrics"
}

func configMapName(cluster *woodpeckerv1alpha1.WoodpeckerCluster) string {
	return cluster.Name + "-config"
}
