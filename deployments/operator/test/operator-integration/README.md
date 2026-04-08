# Operator Integration Tests (kind)

This directory contains kubebuilder-scaffolded integration tests that verify the operator's CRD deployment and resource creation using a kind (Kubernetes IN Docker) cluster.

These tests focus on the **operator itself** (CRD installation, controller deployment, RBAC) rather than Woodpecker server functionality. They use the `e2e` build tag and are excluded from normal `go test` / `make test` runs.

## How to Run

```bash
cd deployments/operator
make test-e2e
```

This requires kind and Docker to be installed. The test will:

1. Create a kind cluster
2. Build and load the operator image
3. Deploy the operator (CRD + RBAC + controller)
4. Verify the controller pod starts and becomes ready
5. Clean up

## Related

For full smoke testing with a real Woodpecker cluster (including write/read verification and scale-up/down), see:

- Smoke test script: [`../smoke-test.sh`](../smoke-test.sh)
- Client test code: [`../../../../tests/e2e_operator/`](../../../../tests/e2e_operator/)
- Guide: [`../../../../docs/admin-guides/operator-e2e-build-from-source.md`](../../../../docs/admin-guides/operator-e2e-build-from-source.md)
