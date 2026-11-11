# Woodpecker Operator E2E Tests

This package contains end-to-end tests for Woodpecker clusters deployed on Kubernetes via the Woodpecker Operator.

These tests are designed to run **inside a K8s pod** that can reach the Woodpecker server services. They are not meant to be run locally or in the unit test CI workflow.

## Tests

| Test | Description |
|---|---|
| `TestOperatorE2E_HealthCheck` | Verifies all server nodes are healthy via HTTP `/healthz` |
| `TestOperatorE2E_WriteAndRead` | Creates a log, writes 100 entries, reads them back, verifies data integrity |
| `TestOperatorE2E_MultipleLogsParallel` | Creates 3 logs, writes 50 entries each, reads back and verifies |

## How to Run

These tests are used by the automated E2E script:

```bash
cd deployments/operator
./test/smoke-test.sh
```

For manual execution, see [Operator E2E Build from Source Guide](../../docs/admin-guides/operator-e2e-build-from-source.md).

## Related

- Operator source code: [`deployments/operator/`](../../deployments/operator/)
- Operator documentation: [`docs/woodpecker_operator.md`](../../docs/woodpecker_operator.md)
- E2E test script: [`deployments/operator/test/smoke-test.sh`](../../deployments/operator/test/smoke-test.sh)
