# Quick Start sources and runnable examples

`docs.html#quickstart` contains three native tabs: Kubernetes Service, Compose
Service, and Embedded. Each tab includes deployment, exact SDK configuration,
client initialization and read/write code, verification and cleanup.

Run `python3 docs/quickstart/generate.py` from the repository root after changing
this guide or any sample. The generator embeds the actual YAML/Go sources into
the marked Quick Start region, leaving the rest of Docs unchanged. Commit source
and generated `docs.html` together. `generate.py` owns prose and command examples;
`assets/quickstart.css` owns the small amount of walkthrough-specific layout.

- `client.go`: complete application for service/minio storage; verifies both
  acknowledged message ID and payload, creates a fresh log per invocation.
- `compose.yaml`: private three-node Docker Service deployment plus dependencies;
  only etcd/MinIO publish workstation loopback ports for the Embedded path.
- `compose-client.yaml`: shared SDK/server service-mode configuration.
- `embedded-host.yaml`: workstation SDK configuration for the Embedded tab.
- `kubernetes.yaml`: isolated evaluation namespace, dependencies, PVCs and plain
  StatefulSet (no operator/CRD prerequisite).
- `kubernetes-client.yaml`, `kubernetes-job.yaml`: in-cluster application config/Job.
- `Dockerfile`: builds the same SDK program, reusable by Compose and Kubernetes.

From the repository root, `go build ./docs/quickstart/client.go` checks the app.
The HTML walkthroughs include the actual deployment/run commands. Kubernetes
requires an accessible application image matching the nodes' CPU architecture;
minikube loading and remote-registry alternatives are explained in the guide.

Verify all three mode links and their six step links, keyboard tab navigation,
code-copy buttons, light/dark themes, mobile overflow, and legacy quickstart.html
redirects. Existing #quickstart/service and #quickstart/embedded links remain valid.
