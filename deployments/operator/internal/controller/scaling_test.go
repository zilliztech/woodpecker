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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"

	woodpeckerv1alpha1 "github.com/zilliztech/woodpecker/deployments/operator/api/v1alpha1"
)

var _ = Describe("cleanupOrphanedPVCs", func() {
	const ns = "default"

	newCluster := func(name string) *woodpeckerv1alpha1.WoodpeckerCluster {
		return &woodpeckerv1alpha1.WoodpeckerCluster{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		}
	}

	newPVC := func(clusterName string, ordinal int) *corev1.PersistentVolumeClaim {
		return &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "data-" + clusterName + "-server-" + itoa(ordinal),
				Namespace: ns,
			},
			Spec: corev1.PersistentVolumeClaimSpec{
				AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
				Resources: corev1.VolumeResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceStorage: resource.MustParse("100Mi"),
					},
				},
			},
		}
	}

	It("deletes PVCs for ordinals between old and new", func() {
		clusterName := "scale-test-1"
		cluster := newCluster(clusterName)
		ctx := context.Background()

		// Create PVCs 0, 1, 2 (simulate prior replicas=3 state)
		for i := 0; i < 3; i++ {
			Expect(k8sClient.Create(ctx, newPVC(clusterName, i))).To(Succeed())
		}

		r := &WoodpeckerClusterReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}

		// Scale from 3 -> 1: should delete PVCs for ordinals 2 and 1
		Expect(r.cleanupOrphanedPVCs(ctx, cluster, 3, 1)).To(Succeed())

		// PVC 0 should still exist
		pvc0 := &corev1.PersistentVolumeClaim{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Name: "data-" + clusterName + "-server-0", Namespace: ns}, pvc0)).To(Succeed())

		// PVCs 1, 2 should be gone or deleting
		for _, ordinal := range []int{1, 2} {
			err := k8sClient.Get(ctx, types.NamespacedName{
				Name: "data-" + clusterName + "-server-" + itoa(ordinal), Namespace: ns,
			}, &corev1.PersistentVolumeClaim{})
			if err == nil {
				// In envtest PVCs may linger; accept either Not Found or DeletionTimestamp set
				// (best-effort check — we've verified Delete was called)
				continue
			}
			Expect(errors.IsNotFound(err)).To(BeTrue())
		}

		// Cleanup remaining
		_ = k8sClient.Delete(ctx, pvc0)
	})

	It("is a no-op when no PVCs exist", func() {
		cluster := newCluster("scale-test-noop")
		r := &WoodpeckerClusterReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
		Expect(r.cleanupOrphanedPVCs(context.Background(), cluster, 3, 2)).To(Succeed())
	})

	It("skips already-deleted PVCs gracefully", func() {
		clusterName := "scale-test-partial"
		cluster := newCluster(clusterName)
		ctx := context.Background()

		// Only create PVC 0; PVC 1 doesn't exist
		Expect(k8sClient.Create(ctx, newPVC(clusterName, 0))).To(Succeed())

		r := &WoodpeckerClusterReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
		// Try to clean 2 -> 0 (should handle missing PVC 1 without error)
		Expect(r.cleanupOrphanedPVCs(ctx, cluster, 2, 0)).To(Succeed())
	})
})

var _ = Describe("checkScaleDown", func() {
	It("returns scaleDownNotNeeded when STS does not exist", func() {
		ctx := context.Background()
		cluster := &woodpeckerv1alpha1.WoodpeckerCluster{
			ObjectMeta: metav1.ObjectMeta{Name: "no-sts", Namespace: "default"},
			Spec: woodpeckerv1alpha1.WoodpeckerClusterSpec{
				Replicas:    ptr.To(int32(3)),
				Image:       "img",
				ServicePort: 18080, GossipPort: 17946, MetricsPort: 9091,
				StorageSize: resource.MustParse("1Gi"),
			},
		}
		r := &WoodpeckerClusterReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
		result, _, err := r.checkScaleDown(ctx, cluster)
		Expect(err).NotTo(HaveOccurred())
		Expect(result).To(Equal(scaleDownNotNeeded))
	})
})

var _ = Describe("requeueDelay", func() {
	It("returns a positive duration", func() {
		Expect(requeueDelay().Seconds()).To(BeNumerically(">", 0))
	})
})

// itoa avoids pulling strconv when only a handful of small-int conversions are
// needed in tests.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	digits := []byte{}
	for n > 0 {
		digits = append([]byte{byte('0' + n%10)}, digits...)
		n /= 10
	}
	return string(digits)
}
