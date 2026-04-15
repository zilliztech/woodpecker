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
	"maps"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	woodpeckerv1alpha1 "github.com/zilliztech/woodpecker/deployments/operator/api/v1alpha1"
)

func (r *WoodpeckerClusterReconciler) reconcileStatefulSet(ctx context.Context, cluster *woodpeckerv1alpha1.WoodpeckerCluster) error {
	logger := log.FromContext(ctx)

	configHash, err := r.configHash(ctx, cluster)
	if err != nil {
		return fmt.Errorf("computing config hash: %w", err)
	}

	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      serverName(cluster),
			Namespace: cluster.Namespace,
		},
	}

	op, err := controllerutil.CreateOrUpdate(ctx, r.Client, sts, func() error {
		r.buildStatefulSet(cluster, sts, configHash)
		return ctrl.SetControllerReference(cluster, sts, r.Scheme)
	})
	if err != nil {
		return err
	}

	logger.Info("StatefulSet reconciled", "name", sts.Name, "operation", op)
	return nil
}

func (r *WoodpeckerClusterReconciler) buildStatefulSet(cluster *woodpeckerv1alpha1.WoodpeckerCluster, sts *appsv1.StatefulSet, configHash string) {
	labels := commonLabels(cluster)

	// Pod annotations: merge user-provided + config hash for rolling update detection
	podAnnotations := make(map[string]string)
	maps.Copy(podAnnotations, cluster.Spec.PodAnnotations)
	podAnnotations["woodpecker.zilliz.io/config-hash"] = configHash

	// Pod labels: merge common + user-provided
	podLabels := make(map[string]string)
	maps.Copy(podLabels, labels)
	maps.Copy(podLabels, cluster.Spec.PodLabels)

	sts.Labels = labels
	sts.Spec = appsv1.StatefulSetSpec{
		Replicas:    cluster.Spec.Replicas,
		ServiceName: headlessServiceName(cluster),
		Selector: &metav1.LabelSelector{
			MatchLabels: labels,
		},
		UpdateStrategy: appsv1.StatefulSetUpdateStrategy{
			Type: appsv1.RollingUpdateStatefulSetStrategyType,
			RollingUpdate: &appsv1.RollingUpdateStatefulSetStrategy{
				MaxUnavailable: ptr.To(intstr.FromInt32(1)),
			},
		},
		PodManagementPolicy: appsv1.ParallelPodManagement,
		Template: corev1.PodTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{
				Labels:      podLabels,
				Annotations: podAnnotations,
			},
			Spec: r.buildPodSpec(cluster),
		},
		VolumeClaimTemplates: r.buildVolumeClaimTemplates(cluster),
	}
}

func (r *WoodpeckerClusterReconciler) buildPodSpec(cluster *woodpeckerv1alpha1.WoodpeckerCluster) corev1.PodSpec {
	return corev1.PodSpec{
		ServiceAccountName:            serverName(cluster),
		TerminationGracePeriodSeconds: ptr.To(int64(300)),
		InitContainers:                r.buildInitContainers(cluster),
		Containers:                    r.buildContainers(cluster),
		Volumes:                       r.buildVolumes(cluster),
		Affinity:                      cluster.Spec.Affinity,
		Tolerations:                   cluster.Spec.Tolerations,
		NodeSelector:                  cluster.Spec.NodeSelector,
		TopologySpreadConstraints:     mergeTopologySpreadConstraints(cluster, cluster.Spec.TopologySpreadConstraints),
	}
}

func (r *WoodpeckerClusterReconciler) buildContainers(cluster *woodpeckerv1alpha1.WoodpeckerCluster) []corev1.Container {
	return []corev1.Container{
		{
			Name:            "woodpecker",
			Image:           cluster.Spec.Image,
			ImagePullPolicy: cluster.Spec.ImagePullPolicy,
			// Use wrapper to source topology.env (SEEDS from init container) then exec original entrypoint via tini
			Command: []string{"/bin/sh", "-c"},
			Args:    []string{". /etc/woodpecker/topology.env && export SEEDS AVAILABILITY_ZONE CLUSTER_NAME RESOURCE_GROUP && exec /tini -- /woodpecker/bin/start-woodpecker.sh"},
			Ports: []corev1.ContainerPort{
				{Name: "grpc", ContainerPort: cluster.Spec.ServicePort, Protocol: corev1.ProtocolTCP},
				{Name: "gossip-tcp", ContainerPort: cluster.Spec.GossipPort, Protocol: corev1.ProtocolTCP},
				{Name: "gossip-udp", ContainerPort: cluster.Spec.GossipPort, Protocol: corev1.ProtocolUDP},
				{Name: "metrics", ContainerPort: cluster.Spec.MetricsPort, Protocol: corev1.ProtocolTCP},
			},
			Env:       r.buildEnvVars(cluster),
			Resources: cluster.Spec.Resources,
			VolumeMounts: []corev1.VolumeMount{
				{Name: "config", MountPath: "/woodpecker/configs", ReadOnly: true},
				{Name: "data", MountPath: "/woodpecker/data"},
				{Name: "topology", MountPath: "/etc/woodpecker"},
			},
			StartupProbe: &corev1.Probe{
				ProbeHandler: corev1.ProbeHandler{
					HTTPGet: &corev1.HTTPGetAction{Path: "/healthz", Port: intstr.FromString("metrics")},
				},
				InitialDelaySeconds: 10,
				PeriodSeconds:       5,
				FailureThreshold:    30,
			},
			LivenessProbe: &corev1.Probe{
				ProbeHandler: corev1.ProbeHandler{
					HTTPGet: &corev1.HTTPGetAction{Path: "/healthz", Port: intstr.FromString("metrics")},
				},
				InitialDelaySeconds: 30,
				PeriodSeconds:       10,
				FailureThreshold:    3,
			},
			ReadinessProbe: &corev1.Probe{
				ProbeHandler: corev1.ProbeHandler{
					HTTPGet: &corev1.HTTPGetAction{Path: "/healthz", Port: intstr.FromString("metrics")},
				},
				InitialDelaySeconds: 5,
				PeriodSeconds:       5,
				FailureThreshold:    3,
			},
		},
	}
}

func (r *WoodpeckerClusterReconciler) buildEnvVars(cluster *woodpeckerv1alpha1.WoodpeckerCluster) []corev1.EnvVar {
	headless := headlessServiceName(cluster)
	return []corev1.EnvVar{
		// Pod identity — ORDER MATTERS: POD_NAMESPACE must come before HEADLESS_SVC
		{Name: "POD_NAME", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"}}},
		{Name: "POD_NAMESPACE", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.namespace"}}},
		// Use POD_NAME as node name (unique per pod, unlike spec.nodeName which is the K8s node)
		{Name: "NODE_NAME", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"}}},
		// Woodpecker server config
		{Name: "SERVICE_PORT", Value: fmt.Sprintf("%d", cluster.Spec.ServicePort)},
		{Name: "GOSSIP_PORT", Value: fmt.Sprintf("%d", cluster.Spec.GossipPort)},
		{Name: "DATA_DIR", Value: "/woodpecker/data"},
		{Name: "CONFIG_FILE", Value: "/woodpecker/configs/woodpecker.yaml"},
		{Name: "WAIT_FOR_DEPS", Value: "false"},
		// Headless service FQDN — $(POD_NAMESPACE) is expanded by kubelet because it's defined above
		{Name: "HEADLESS_SVC", Value: fmt.Sprintf("%s.$(POD_NAMESPACE).svc.cluster.local", headless)},
		// Advertise addresses: pod FQDN via headless service so other nodes can reach this pod
		{Name: "ADVERTISE_GOSSIP_ADDR", Value: fmt.Sprintf("$(POD_NAME).%s.$(POD_NAMESPACE).svc.cluster.local:%d", headless, cluster.Spec.GossipPort)},
		{Name: "ADVERTISE_SERVICE_ADDR", Value: fmt.Sprintf("$(POD_NAME).%s.$(POD_NAMESPACE).svc.cluster.local:%d", headless, cluster.Spec.ServicePort)},
	}
}

// buildInitContainers creates an init container that:
//  1. Computes gossip seeds from pod ordinal and headless service DNS
//  2. Queries the K8s API using the pod's ServiceAccount token to read the
//     node's topology labels (zone, region) and writes them to a shared env file.
//
// On any lookup failure the container still writes fallback values and exits 0,
// so a transient API-server hiccup will not block pod startup.
func (r *WoodpeckerClusterReconciler) buildInitContainers(cluster *woodpeckerv1alpha1.WoodpeckerCluster) []corev1.Container {
	maxSeeds := int32(3)
	if cluster.Spec.Replicas != nil && *cluster.Spec.Replicas < maxSeeds {
		maxSeeds = *cluster.Spec.Replicas
	}
	var seedPatterns []string
	for i := int32(0); i < maxSeeds; i++ {
		seedPatterns = append(seedPatterns, fmt.Sprintf(
			"%s-server-%d.${HEADLESS_SVC}:${GOSSIP_PORT}", cluster.Name, i,
		))
	}
	seedsExpr := strings.Join(seedPatterns, ",")

	script := fmt.Sprintf(`#!/bin/sh
set -e

# All nodes get the same seeds — gossip ignores the seed pointing to self
SEEDS="%s"

# Query the node's topology labels via K8s API. Fail-soft: any error → fallback.
APISERVER=https://kubernetes.default.svc
TOKEN_FILE=/var/run/secrets/kubernetes.io/serviceaccount/token
CA_FILE=/var/run/secrets/kubernetes.io/serviceaccount/ca.crt
NODE_JSON=""
if [ -r "$TOKEN_FILE" ] && [ -r "$CA_FILE" ] && [ -n "$HOST_NODE_NAME" ]; then
    NODE_JSON=$(curl -sf --max-time 10 --cacert "$CA_FILE" \
        -H "Authorization: Bearer $(cat $TOKEN_FILE)" \
        "$APISERVER/api/v1/nodes/$HOST_NODE_NAME" || true)
fi

AZ=$(printf '%%s' "$NODE_JSON" \
    | grep -o '"topology.kubernetes.io/zone":"[^"]*"' \
    | head -n1 | cut -d'"' -f4)
REGION=$(printf '%%s' "$NODE_JSON" \
    | grep -o '"topology.kubernetes.io/region":"[^"]*"' \
    | head -n1 | cut -d'"' -f4)

: "${AZ:=default-az}"
: "${REGION:=default-cluster}"

cat > /etc/woodpecker/topology.env << EOF
SEEDS=${SEEDS}
AVAILABILITY_ZONE=${AZ}
CLUSTER_NAME=${REGION}
RESOURCE_GROUP=default
EOF

echo "Init complete: pod=$POD_NAME node=$HOST_NODE_NAME az=$AZ cluster=$REGION"
`, seedsExpr)

	return []corev1.Container{
		{
			Name:    "init-topology",
			Image:   "curlimages/curl:8.7.1",
			Command: []string{"/bin/sh", "-c", script},
			Env: []corev1.EnvVar{
				{Name: "POD_NAME", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"}}},
				{Name: "POD_NAMESPACE", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.namespace"}}},
				// HOST_NODE_NAME is the K8s node this pod landed on (spec.nodeName),
				// distinct from POD_NAME which is the pod's own name.
				{Name: "HOST_NODE_NAME", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "spec.nodeName"}}},
				{Name: "HEADLESS_SVC", Value: fmt.Sprintf("%s.$(POD_NAMESPACE).svc.cluster.local", headlessServiceName(cluster))},
				{Name: "GOSSIP_PORT", Value: fmt.Sprintf("%d", cluster.Spec.GossipPort)},
			},
			VolumeMounts: []corev1.VolumeMount{
				{Name: "topology", MountPath: "/etc/woodpecker"},
			},
		},
	}
}

func (r *WoodpeckerClusterReconciler) buildVolumes(cluster *woodpeckerv1alpha1.WoodpeckerCluster) []corev1.Volume {
	cmName := ActiveConfigMapName(cluster)
	return []corev1.Volume{
		{
			Name: "config",
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{Name: cmName},
				},
			},
		},
		{
			Name: "topology",
			VolumeSource: corev1.VolumeSource{
				EmptyDir: &corev1.EmptyDirVolumeSource{},
			},
		},
	}
}

func (r *WoodpeckerClusterReconciler) buildVolumeClaimTemplates(cluster *woodpeckerv1alpha1.WoodpeckerCluster) []corev1.PersistentVolumeClaim {
	return []corev1.PersistentVolumeClaim{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name: "data",
			},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				StorageClassName: cluster.Spec.StorageClassName,
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceStorage: cluster.Spec.StorageSize,
					},
				},
			},
		},
	}
}

// defaultZoneTopologySpreadConstraint returns the operator's default
// TopologySpreadConstraint on the well-known zone label. It enforces
// at-most-1 skew across zones and blocks scheduling if the constraint
// cannot be satisfied.
func defaultZoneTopologySpreadConstraint(cluster *woodpeckerv1alpha1.WoodpeckerCluster) corev1.TopologySpreadConstraint {
	return corev1.TopologySpreadConstraint{
		MaxSkew:           1,
		TopologyKey:       "topology.kubernetes.io/zone",
		WhenUnsatisfiable: corev1.DoNotSchedule,
		LabelSelector: &metav1.LabelSelector{
			MatchLabels: commonLabels(cluster),
		},
	}
}

// mergeTopologySpreadConstraints returns the user-supplied constraints plus
// the operator's default zone constraint, unless the user already specified
// a constraint on the zone topologyKey — in which case the user's wins.
func mergeTopologySpreadConstraints(
	cluster *woodpeckerv1alpha1.WoodpeckerCluster,
	user []corev1.TopologySpreadConstraint,
) []corev1.TopologySpreadConstraint {
	result := append([]corev1.TopologySpreadConstraint{}, user...)
	for _, c := range user {
		if c.TopologyKey == "topology.kubernetes.io/zone" {
			return result
		}
	}
	return append(result, defaultZoneTopologySpreadConstraint(cluster))
}
