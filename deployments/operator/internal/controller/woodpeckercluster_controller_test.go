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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	woodpeckerv1alpha1 "github.com/zilliztech/woodpecker/deployments/operator/api/v1alpha1"
)

var _ = Describe("WoodpeckerCluster Controller", func() {

	// Helper: create a test CR and run reconcile twice (finalizer + resources)
	newReconciler := func() *WoodpeckerClusterReconciler {
		return &WoodpeckerClusterReconciler{
			Client: k8sClient,
			Scheme: k8sClient.Scheme(),
		}
	}
	reconcileFull := func(r *WoodpeckerClusterReconciler, nn types.NamespacedName) {
		// First reconcile adds finalizer
		_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
		Expect(err).NotTo(HaveOccurred())
		// Second reconcile creates resources
		_, err = r.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
		Expect(err).NotTo(HaveOccurred())
	}

	Context("When reconciling a resource", func() {
		const resourceName = "test-basic"
		ctx := context.Background()
		nn := types.NamespacedName{Name: resourceName, Namespace: "default"}

		BeforeEach(func() {
			cm := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{Name: "test-wp-config", Namespace: "default"},
				Data:       map[string]string{"woodpecker.yaml": "log:\n  level: info\n"},
			}
			if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(cm), &corev1.ConfigMap{}); errors.IsNotFound(err) {
				Expect(k8sClient.Create(ctx, cm)).To(Succeed())
			}

			cr := &woodpeckerv1alpha1.WoodpeckerCluster{
				ObjectMeta: metav1.ObjectMeta{Name: resourceName, Namespace: "default"},
				Spec: woodpeckerv1alpha1.WoodpeckerClusterSpec{
					Image:       "zilliztech/woodpecker:v0.1.26",
					Replicas:    ptr.To(int32(1)),
					ServicePort: 18080,
					GossipPort:  17946,
					MetricsPort: 9091,
					StorageSize: resource.MustParse("1Gi"),
					ConfigRef:   &woodpeckerv1alpha1.ConfigMapReference{Name: "test-wp-config"},
				},
			}
			if err := k8sClient.Get(ctx, nn, &woodpeckerv1alpha1.WoodpeckerCluster{}); errors.IsNotFound(err) {
				Expect(k8sClient.Create(ctx, cr)).To(Succeed())
			}
		})

		AfterEach(func() {
			cr := &woodpeckerv1alpha1.WoodpeckerCluster{}
			if err := k8sClient.Get(ctx, nn, cr); err == nil {
				// Remove finalizer so delete can proceed in envtest
				cr.Finalizers = nil
				_ = k8sClient.Update(ctx, cr)
				_ = k8sClient.Delete(ctx, cr)
			}
		})

		It("should create all sub-resources", func() {
			r := newReconciler()
			reconcileFull(r, nn)

			By("Checking ServiceAccount")
			sa := &corev1.ServiceAccount{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: resourceName + "-server", Namespace: "default"}, sa)).To(Succeed())
			Expect(sa.Labels["app.kubernetes.io/name"]).To(Equal("woodpecker"))

			By("Checking Headless Service")
			svc := &corev1.Service{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: resourceName + "-server-headless", Namespace: "default"}, svc)).To(Succeed())
			Expect(svc.Spec.ClusterIP).To(Equal(corev1.ClusterIPNone))
			Expect(svc.Spec.PublishNotReadyAddresses).To(BeTrue())

			By("Checking Client Service")
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: resourceName + "-server-client", Namespace: "default"}, &corev1.Service{})).To(Succeed())

			By("Checking Metrics Service")
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: resourceName + "-server-metrics", Namespace: "default"}, &corev1.Service{})).To(Succeed())

			By("Checking StatefulSet")
			sts := &appsv1.StatefulSet{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: resourceName + "-server", Namespace: "default"}, sts)).To(Succeed())
			Expect(*sts.Spec.Replicas).To(Equal(int32(1)))
			Expect(sts.Spec.ServiceName).To(Equal(resourceName + "-server-headless"))
			Expect(sts.Spec.Template.Spec.Containers).To(HaveLen(1))
			Expect(sts.Spec.Template.Spec.Containers[0].Image).To(Equal("zilliztech/woodpecker:v0.1.26"))
			Expect(sts.Spec.Template.Spec.InitContainers).To(HaveLen(1))
			Expect(sts.Spec.VolumeClaimTemplates).To(HaveLen(1))

			By("Checking PDB")
			pdb := &policyv1.PodDisruptionBudget{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: resourceName + "-server-pdb", Namespace: "default"}, pdb)).To(Succeed())

			By("Checking config-hash annotation on pod template")
			Expect(sts.Spec.Template.Annotations).To(HaveKey("woodpecker.zilliz.io/config-hash"))
		})

		It("should add finalizer to the CR", func() {
			r := newReconciler()
			// Only one reconcile — should add finalizer
			_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
			Expect(err).NotTo(HaveOccurred())

			cr := &woodpeckerv1alpha1.WoodpeckerCluster{}
			Expect(k8sClient.Get(ctx, nn, cr)).To(Succeed())
			Expect(cr.Finalizers).To(ContainElement("woodpecker.zilliz.io/finalizer"))
		})

		It("should skip reconcile when paused", func() {
			r := newReconciler()
			reconcileFull(r, nn)

			By("Setting paused=true")
			cr := &woodpeckerv1alpha1.WoodpeckerCluster{}
			Expect(k8sClient.Get(ctx, nn, cr)).To(Succeed())
			cr.Spec.Paused = true
			Expect(k8sClient.Update(ctx, cr)).To(Succeed())

			By("Reconciling — should skip")
			_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
			Expect(err).NotTo(HaveOccurred())
		})

		It("should update StatefulSet when image changes", func() {
			r := newReconciler()
			reconcileFull(r, nn)

			By("Changing image")
			cr := &woodpeckerv1alpha1.WoodpeckerCluster{}
			Expect(k8sClient.Get(ctx, nn, cr)).To(Succeed())
			cr.Spec.Image = "zilliztech/woodpecker:v0.2.0"
			Expect(k8sClient.Update(ctx, cr)).To(Succeed())

			By("Reconciling again")
			_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: nn})
			Expect(err).NotTo(HaveOccurred())

			By("Checking StatefulSet has new image")
			sts := &appsv1.StatefulSet{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: resourceName + "-server", Namespace: "default"}, sts)).To(Succeed())
			Expect(sts.Spec.Template.Spec.Containers[0].Image).To(Equal("zilliztech/woodpecker:v0.2.0"))
		})

		It("should handle not-found CR gracefully", func() {
			r := newReconciler()
			_, err := r.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{Name: "nonexistent", Namespace: "default"},
			})
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Context("When using default ConfigMap", func() {
		const resourceName = "test-default-config"
		ctx := context.Background()
		nn := types.NamespacedName{Name: resourceName, Namespace: "default"}

		AfterEach(func() {
			cr := &woodpeckerv1alpha1.WoodpeckerCluster{}
			if err := k8sClient.Get(ctx, nn, cr); err == nil {
				cr.Finalizers = nil
				_ = k8sClient.Update(ctx, cr)
				_ = k8sClient.Delete(ctx, cr)
			}
			// Cleanup default configmap
			cm := &corev1.ConfigMap{}
			if err := k8sClient.Get(ctx, types.NamespacedName{Name: resourceName + "-config", Namespace: "default"}, cm); err == nil {
				_ = k8sClient.Delete(ctx, cm)
			}
		})

		It("should create a default ConfigMap when configRef is not set", func() {
			cr := &woodpeckerv1alpha1.WoodpeckerCluster{
				ObjectMeta: metav1.ObjectMeta{Name: resourceName, Namespace: "default"},
				Spec: woodpeckerv1alpha1.WoodpeckerClusterSpec{
					Image:       "zilliztech/woodpecker:v0.1.26",
					Replicas:    ptr.To(int32(1)),
					ServicePort: 18080,
					GossipPort:  17946,
					MetricsPort: 9091,
					StorageSize: resource.MustParse("1Gi"),
					// No ConfigRef — operator should create default
				},
			}
			Expect(k8sClient.Create(ctx, cr)).To(Succeed())

			r := newReconciler()
			reconcileFull(r, nn)

			By("Checking default ConfigMap was created")
			cm := &corev1.ConfigMap{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Name: resourceName + "-config", Namespace: "default"}, cm)).To(Succeed())
			Expect(cm.Data).To(HaveKey("woodpecker.yaml"))
		})
	})
})
