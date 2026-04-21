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
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/validation/field"
	ctrl "sigs.k8s.io/controller-runtime"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	woodpeckerv1alpha1 "github.com/zilliztech/woodpecker/deployments/operator/api/v1alpha1"
)

// nolint:unused
// log is for logging in this package.
var woodpeckerclusterlog = logf.Log.WithName("woodpeckercluster-resource")

// SetupWoodpeckerClusterWebhookWithManager registers the webhook for WoodpeckerCluster in the manager.
func SetupWoodpeckerClusterWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr, &woodpeckerv1alpha1.WoodpeckerCluster{}).
		WithValidator(&WoodpeckerClusterCustomValidator{}).
		WithDefaulter(&WoodpeckerClusterCustomDefaulter{}).
		Complete()
}

// TODO(user): EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!

// +kubebuilder:webhook:path=/mutate-woodpecker-zilliz-io-v1alpha1-woodpeckercluster,mutating=true,failurePolicy=fail,sideEffects=None,groups=woodpecker.zilliz.io,resources=woodpeckerclusters,verbs=create;update,versions=v1alpha1,name=mwoodpeckercluster-v1alpha1.kb.io,admissionReviewVersions=v1

// WoodpeckerClusterCustomDefaulter sets default values on the WoodpeckerCluster resource.
type WoodpeckerClusterCustomDefaulter struct{}

// Default implements webhook.CustomDefaulter.
func (d *WoodpeckerClusterCustomDefaulter) Default(_ context.Context, obj *woodpeckerv1alpha1.WoodpeckerCluster) error {
	woodpeckerclusterlog.Info("Defaulting for WoodpeckerCluster", "name", obj.GetName())
	spec := &obj.Spec

	if spec.Image == "" {
		spec.Image = "zilliztech/woodpecker:v0.1.26"
	}
	if spec.ImagePullPolicy == "" {
		spec.ImagePullPolicy = "IfNotPresent"
	}
	if spec.Replicas == nil {
		replicas := int32(3)
		spec.Replicas = &replicas
	}
	if spec.StorageSize.IsZero() {
		spec.StorageSize = resource.MustParse("10Gi")
	}
	if spec.ServicePort == 0 {
		spec.ServicePort = 18080
	}
	if spec.GossipPort == 0 {
		spec.GossipPort = 17946
	}
	if spec.MetricsPort == 0 {
		spec.MetricsPort = 9091
	}

	return nil
}

// TODO(user): change verbs to "verbs=create;update;delete" if you want to enable deletion validation.
// NOTE: If you want to customise the 'path', use the flags '--defaulting-path' or '--validation-path'.
// +kubebuilder:webhook:path=/validate-woodpecker-zilliz-io-v1alpha1-woodpeckercluster,mutating=false,failurePolicy=fail,sideEffects=None,groups=woodpecker.zilliz.io,resources=woodpeckerclusters,verbs=create;update,versions=v1alpha1,name=vwoodpeckercluster-v1alpha1.kb.io,admissionReviewVersions=v1

// WoodpeckerClusterCustomValidator validates WoodpeckerCluster resources.
type WoodpeckerClusterCustomValidator struct{}

func validateWoodpeckerCluster(obj *woodpeckerv1alpha1.WoodpeckerCluster) field.ErrorList {
	var allErrs field.ErrorList
	spec := &obj.Spec
	specPath := field.NewPath("spec")

	if spec.Replicas != nil && *spec.Replicas < 1 {
		allErrs = append(allErrs, field.Invalid(specPath.Child("replicas"), *spec.Replicas, "must be >= 1"))
	}
	if spec.ServicePort < 1 || spec.ServicePort > 65535 {
		allErrs = append(allErrs, field.Invalid(specPath.Child("servicePort"), spec.ServicePort, "must be between 1 and 65535"))
	}
	if spec.GossipPort < 1 || spec.GossipPort > 65535 {
		allErrs = append(allErrs, field.Invalid(specPath.Child("gossipPort"), spec.GossipPort, "must be between 1 and 65535"))
	}
	if spec.MetricsPort < 1 || spec.MetricsPort > 65535 {
		allErrs = append(allErrs, field.Invalid(specPath.Child("metricsPort"), spec.MetricsPort, "must be between 1 and 65535"))
	}
	if spec.ServicePort == spec.GossipPort {
		allErrs = append(allErrs, field.Invalid(specPath.Child("gossipPort"), spec.GossipPort, "must differ from servicePort"))
	}

	return allErrs
}

// ValidateCreate implements webhook.CustomValidator.
func (v *WoodpeckerClusterCustomValidator) ValidateCreate(_ context.Context, obj *woodpeckerv1alpha1.WoodpeckerCluster) (admission.Warnings, error) {
	woodpeckerclusterlog.Info("Validation for WoodpeckerCluster upon creation", "name", obj.GetName())
	if errs := validateWoodpeckerCluster(obj); len(errs) > 0 {
		return nil, fmt.Errorf("validation failed: %s", errs.ToAggregate().Error())
	}
	return nil, nil
}

// ValidateUpdate implements webhook.CustomValidator.
func (v *WoodpeckerClusterCustomValidator) ValidateUpdate(_ context.Context, _, newObj *woodpeckerv1alpha1.WoodpeckerCluster) (admission.Warnings, error) {
	woodpeckerclusterlog.Info("Validation for WoodpeckerCluster upon update", "name", newObj.GetName())
	if errs := validateWoodpeckerCluster(newObj); len(errs) > 0 {
		return nil, fmt.Errorf("validation failed: %s", errs.ToAggregate().Error())
	}
	return nil, nil
}

// ValidateDelete implements webhook.CustomValidator.
func (v *WoodpeckerClusterCustomValidator) ValidateDelete(_ context.Context, _ *woodpeckerv1alpha1.WoodpeckerCluster) (admission.Warnings, error) {
	return nil, nil
}
