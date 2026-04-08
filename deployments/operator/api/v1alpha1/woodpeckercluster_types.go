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

package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// WoodpeckerClusterSpec defines the desired state of a Woodpecker cluster.
// Only contains K8s deployment-level configuration.
// Woodpecker process configuration (etcd, object storage, etc.) is provided via ConfigMap.
type WoodpeckerClusterSpec struct {
	// image is the Woodpecker server container image.
	// +kubebuilder:default="zilliztech/woodpecker:v0.1.26"
	// +optional
	Image string `json:"image,omitempty"`

	// imagePullPolicy defaults to IfNotPresent.
	// +kubebuilder:default="IfNotPresent"
	// +optional
	ImagePullPolicy corev1.PullPolicy `json:"imagePullPolicy,omitempty"`

	// imagePullSecrets for private registries.
	// +optional
	ImagePullSecrets []corev1.LocalObjectReference `json:"imagePullSecrets,omitempty"`

	// replicas is the number of Woodpecker server nodes.
	// +kubebuilder:default=3
	// +kubebuilder:validation:Minimum=1
	Replicas *int32 `json:"replicas"`

	// resources defines CPU/memory requests and limits.
	// +optional
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`

	// storageClassName for the data PVC (local disk staging area).
	// +optional
	StorageClassName *string `json:"storageClassName,omitempty"`

	// storageSize for the data PVC.
	// +kubebuilder:default="10Gi"
	// +optional
	StorageSize resource.Quantity `json:"storageSize,omitempty"`

	// servicePort is the gRPC service port.
	// +kubebuilder:default=18080
	// +optional
	ServicePort int32 `json:"servicePort,omitempty"`

	// gossipPort is the gossip protocol port.
	// +kubebuilder:default=17946
	// +optional
	GossipPort int32 `json:"gossipPort,omitempty"`

	// metricsPort is the HTTP metrics/health port.
	// +kubebuilder:default=9091
	// +optional
	MetricsPort int32 `json:"metricsPort,omitempty"`

	// configRef references a ConfigMap containing woodpecker.yaml.
	// The ConfigMap must have a key "woodpecker.yaml" with the full Woodpecker configuration.
	// If not set, operator creates a minimal default ConfigMap.
	// +optional
	ConfigRef *ConfigMapReference `json:"configRef,omitempty"`

	// affinity for pod scheduling.
	// +optional
	Affinity *corev1.Affinity `json:"affinity,omitempty"`

	// tolerations for pod scheduling.
	// +optional
	Tolerations []corev1.Toleration `json:"tolerations,omitempty"`

	// nodeSelector for pod scheduling.
	// +optional
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`

	// topologySpreadConstraints for pod distribution.
	// +optional
	TopologySpreadConstraints []corev1.TopologySpreadConstraint `json:"topologySpreadConstraints,omitempty"`

	// podAnnotations are additional annotations added to server pods.
	// +optional
	PodAnnotations map[string]string `json:"podAnnotations,omitempty"`

	// podLabels are additional labels added to server pods.
	// +optional
	PodLabels map[string]string `json:"podLabels,omitempty"`

	// paused stops reconciliation when true (for manual maintenance).
	// +kubebuilder:default=false
	// +optional
	Paused bool `json:"paused,omitempty"`
}

// ConfigMapReference references a ConfigMap by name in the same namespace.
type ConfigMapReference struct {
	// name is the ConfigMap name.
	Name string `json:"name"`
}

// ClusterPhase represents the high-level state of a WoodpeckerCluster.
// +kubebuilder:validation:Enum=Pending;Creating;Running;Upgrading;ScalingUp;ScalingDown;Failed
type ClusterPhase string

const (
	PhasePending     ClusterPhase = "Pending"
	PhaseCreating    ClusterPhase = "Creating"
	PhaseRunning     ClusterPhase = "Running"
	PhaseUpgrading   ClusterPhase = "Upgrading"
	PhaseScalingUp   ClusterPhase = "ScalingUp"
	PhaseScalingDown ClusterPhase = "ScalingDown"
	PhaseFailed      ClusterPhase = "Failed"
)

// Standard condition types for WoodpeckerCluster.
const (
	ConditionReady       = "Ready"
	ConditionAvailable   = "Available"
	ConditionProgressing = "Progressing"
	ConditionDegraded    = "Degraded"
)

// WoodpeckerClusterStatus defines the observed state of WoodpeckerCluster.
type WoodpeckerClusterStatus struct {
	// phase is the high-level cluster state.
	// +optional
	Phase ClusterPhase `json:"phase,omitempty"`

	// conditions represent the current state of the WoodpeckerCluster resource.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// observedGeneration is the last spec generation the controller acted on.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// server contains the server component status.
	// +optional
	Server ComponentStatus `json:"server,omitempty"`

	// currentImage is the image currently running.
	// +optional
	CurrentImage string `json:"currentImage,omitempty"`

	// readyReplicas is the total number of ready server pods.
	// +optional
	ReadyReplicas int32 `json:"readyReplicas,omitempty"`

	// endpoint is the client-facing service endpoint.
	// +optional
	Endpoint string `json:"endpoint,omitempty"`
}

// ComponentStatus tracks the status of a managed StatefulSet.
type ComponentStatus struct {
	// readyReplicas is the number of pods with Ready condition.
	ReadyReplicas int32 `json:"readyReplicas,omitempty"`
	// currentReplicas is the number of pods created by the current revision.
	CurrentReplicas int32 `json:"currentReplicas,omitempty"`
	// updatedReplicas is the number of pods updated to the desired revision.
	UpdatedReplicas int32 `json:"updatedReplicas,omitempty"`
	// statefulSetName is the name of the managed StatefulSet.
	StatefulSetName string `json:"statefulSetName,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Ready",type=integer,JSONPath=`.status.readyReplicas`
// +kubebuilder:printcolumn:name="Replicas",type=integer,JSONPath=`.spec.replicas`
// +kubebuilder:printcolumn:name="Image",type=string,JSONPath=`.spec.image`,priority=1
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// WoodpeckerCluster is the Schema for the woodpeckerclusters API
type WoodpeckerCluster struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of WoodpeckerCluster
	// +required
	Spec WoodpeckerClusterSpec `json:"spec"`

	// status defines the observed state of WoodpeckerCluster
	// +optional
	Status WoodpeckerClusterStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// WoodpeckerClusterList contains a list of WoodpeckerCluster
type WoodpeckerClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []WoodpeckerCluster `json:"items"`
}

func init() {
	SchemeBuilder.Register(&WoodpeckerCluster{}, &WoodpeckerClusterList{})
}
