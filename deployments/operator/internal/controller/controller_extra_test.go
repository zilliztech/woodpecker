/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package controller

import (
	"context"
	"net/http"
	"net/http/httptest"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	woodpeckerv1alpha1 "github.com/zilliztech/woodpecker/deployments/operator/api/v1alpha1"
)

var _ = Describe("configMapToCluster", func() {
	It("maps a referenced ConfigMap to its owning clusters", func() {
		ctx := context.Background()
		cmName := "cm-mapped"
		clusterName := "cluster-using-cm"

		cm := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: cmName, Namespace: "default"},
			Data:       map[string]string{"woodpecker.yaml": "log:\n  level: info\n"},
		}
		Expect(k8sClient.Create(ctx, cm)).To(Succeed())
		defer func() { _ = k8sClient.Delete(ctx, cm) }()

		cr := &woodpeckerv1alpha1.WoodpeckerCluster{
			ObjectMeta: metav1.ObjectMeta{Name: clusterName, Namespace: "default"},
			Spec: woodpeckerv1alpha1.WoodpeckerClusterSpec{
				Image: "zilliztech/woodpecker:v0.1.26", Replicas: ptr.To(int32(1)),
				ServicePort: 18080, GossipPort: 17946, MetricsPort: 9091,
				StorageSize: resource.MustParse("1Gi"),
				ConfigRef:   &woodpeckerv1alpha1.ConfigMapReference{Name: cmName},
			},
		}
		Expect(k8sClient.Create(ctx, cr)).To(Succeed())
		defer func() {
			_ = k8sClient.Get(ctx, client.ObjectKeyFromObject(cr), cr)
			cr.Finalizers = nil
			_ = k8sClient.Update(ctx, cr)
			_ = k8sClient.Delete(ctx, cr)
		}()

		r := &WoodpeckerClusterReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
		reqs := r.configMapToCluster(ctx, cm)
		Expect(reqs).To(HaveLen(1))
		Expect(reqs[0].NamespacedName).To(Equal(types.NamespacedName{Name: clusterName, Namespace: "default"}))
	})

	It("returns nil for non-ConfigMap objects", func() {
		r := &WoodpeckerClusterReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
		reqs := r.configMapToCluster(context.Background(), &corev1.Pod{})
		Expect(reqs).To(BeNil())
	})

	It("returns empty when no cluster references the ConfigMap", func() {
		ctx := context.Background()
		cm := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: "cm-unreferenced", Namespace: "default"},
			Data:       map[string]string{"woodpecker.yaml": ""},
		}
		r := &WoodpeckerClusterReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
		reqs := r.configMapToCluster(ctx, cm)
		Expect(reqs).To(BeEmpty())
	})
})

var _ = Describe("reconcileDelete", func() {
	It("returns early when no finalizer is present", func() {
		ctx := context.Background()
		cr := &woodpeckerv1alpha1.WoodpeckerCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "del-no-fin", Namespace: "default"},
		}
		r := &WoodpeckerClusterReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
		result, err := r.reconcileDelete(ctx, cr)
		Expect(err).NotTo(HaveOccurred())
		Expect(result.RequeueAfter).To(BeZero())
	})

	It("requeues when pods are still decommissioning", func() {
		ctx := context.Background()
		clusterName := "del-waiting"

		// Create CR with finalizer
		cr := &woodpeckerv1alpha1.WoodpeckerCluster{
			ObjectMeta: metav1.ObjectMeta{Name: clusterName, Namespace: "default"},
			Spec: woodpeckerv1alpha1.WoodpeckerClusterSpec{
				Image: "img", Replicas: ptr.To(int32(1)),
				ServicePort: 18080, GossipPort: 17946, MetricsPort: 9091,
				StorageSize: resource.MustParse("1Gi"),
			},
		}
		Expect(k8sClient.Create(ctx, cr)).To(Succeed())
		controllerutil.AddFinalizer(cr, finalizerName)
		Expect(k8sClient.Update(ctx, cr)).To(Succeed())
		defer func() {
			_ = k8sClient.Get(ctx, client.ObjectKeyFromObject(cr), cr)
			cr.Finalizers = nil
			_ = k8sClient.Update(ctx, cr)
			_ = k8sClient.Delete(ctx, cr)
		}()

		// Create a running pod labeled as part of the cluster
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: clusterName + "-server-0", Namespace: "default",
				Labels: commonLabels(cr),
			},
			Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "c", Image: "busybox"}}},
		}
		Expect(k8sClient.Create(ctx, pod)).To(Succeed())
		defer func() { _ = k8sClient.Delete(ctx, pod) }()

		// Patch pod status to Running + PodIP
		pod.Status.Phase = corev1.PodRunning
		pod.Status.PodIP = "1.2.3.4"
		Expect(k8sClient.Status().Update(ctx, pod)).To(Succeed())

		// Mock the admin endpoint to report "not safe yet"
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"safe_to_terminate":false}`))
		}))
		defer srv.Close()
		orig := httpClient
		httpClient = srv.Client()
		httpClient.Transport = &rewriteTransport{target: srv.URL}
		defer func() { httpClient = orig }()

		r := &WoodpeckerClusterReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
		result, err := r.reconcileDelete(ctx, cr)
		Expect(err).NotTo(HaveOccurred())
		Expect(result.RequeueAfter).NotTo(BeZero())
	})
})
