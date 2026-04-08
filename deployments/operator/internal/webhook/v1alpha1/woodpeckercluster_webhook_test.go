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
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	woodpeckerv1alpha1 "github.com/zilliztech/woodpecker/deployments/operator/api/v1alpha1"
	// TODO (user): Add any additional imports if needed
)

var _ = Describe("WoodpeckerCluster Webhook", func() {
	var (
		obj       *woodpeckerv1alpha1.WoodpeckerCluster
		oldObj    *woodpeckerv1alpha1.WoodpeckerCluster
		validator WoodpeckerClusterCustomValidator
		defaulter WoodpeckerClusterCustomDefaulter
	)

	BeforeEach(func() {
		obj = &woodpeckerv1alpha1.WoodpeckerCluster{}
		oldObj = &woodpeckerv1alpha1.WoodpeckerCluster{}
		validator = WoodpeckerClusterCustomValidator{}
		Expect(validator).NotTo(BeNil(), "Expected validator to be initialized")
		defaulter = WoodpeckerClusterCustomDefaulter{}
		Expect(defaulter).NotTo(BeNil(), "Expected defaulter to be initialized")
		Expect(oldObj).NotTo(BeNil(), "Expected oldObj to be initialized")
		Expect(obj).NotTo(BeNil(), "Expected obj to be initialized")
	})

	AfterEach(func() {
		// TODO (user): Add any teardown logic common to all tests
	})

	Context("When creating WoodpeckerCluster under Defaulting Webhook", func() {
		It("Should apply all defaults when spec is empty", func() {
			By("calling Default on an empty spec")
			Expect(defaulter.Default(ctx, obj)).To(Succeed())

			By("checking defaults are set")
			Expect(obj.Spec.Image).To(Equal("zilliztech/woodpecker:v0.1.26"))
			Expect(string(obj.Spec.ImagePullPolicy)).To(Equal("IfNotPresent"))
			Expect(*obj.Spec.Replicas).To(Equal(int32(3)))
			Expect(obj.Spec.ServicePort).To(Equal(int32(18080)))
			Expect(obj.Spec.GossipPort).To(Equal(int32(17946)))
			Expect(obj.Spec.MetricsPort).To(Equal(int32(9091)))
			Expect(obj.Spec.StorageSize.String()).To(Equal("10Gi"))
		})

		It("Should not overwrite user-provided values", func() {
			replicas := int32(5)
			obj.Spec.Replicas = &replicas
			obj.Spec.Image = "custom:latest"
			obj.Spec.ServicePort = 9999

			Expect(defaulter.Default(ctx, obj)).To(Succeed())

			Expect(*obj.Spec.Replicas).To(Equal(int32(5)))
			Expect(obj.Spec.Image).To(Equal("custom:latest"))
			Expect(obj.Spec.ServicePort).To(Equal(int32(9999)))
			// other fields still get defaults
			Expect(obj.Spec.GossipPort).To(Equal(int32(17946)))
		})
	})

	Context("When creating or updating WoodpeckerCluster under Validating Webhook", func() {
		BeforeEach(func() {
			// apply defaults first so we have a valid baseline
			Expect(defaulter.Default(ctx, obj)).To(Succeed())
		})

		It("Should admit a valid CR", func() {
			_, err := validator.ValidateCreate(ctx, obj)
			Expect(err).NotTo(HaveOccurred())
		})

		It("Should reject replicas < 1", func() {
			replicas := int32(0)
			obj.Spec.Replicas = &replicas
			_, err := validator.ValidateCreate(ctx, obj)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("replicas"))
		})

		It("Should reject invalid port ranges", func() {
			obj.Spec.ServicePort = 0
			_, err := validator.ValidateCreate(ctx, obj)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("servicePort"))
		})

		It("Should reject servicePort == gossipPort", func() {
			obj.Spec.ServicePort = 18080
			obj.Spec.GossipPort = 18080
			_, err := validator.ValidateCreate(ctx, obj)
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("gossipPort"))
		})

		It("Should validate updates the same way", func() {
			replicas := int32(0)
			newObj := obj.DeepCopy()
			newObj.Spec.Replicas = &replicas
			_, err := validator.ValidateUpdate(ctx, obj, newObj)
			Expect(err).To(HaveOccurred())
		})

		It("Should always allow delete", func() {
			_, err := validator.ValidateDelete(ctx, obj)
			Expect(err).NotTo(HaveOccurred())
		})
	})

})
