#!/usr/bin/env python3
"""Embed complete deployment-to-client paths in the native Docs Quick Start."""
from pathlib import Path
import html
import re

HERE = Path(__file__).resolve().parent
DOCS = HERE.parent


def code(text, label='bash'):
    return ('<div class="code-container"><div class="code-header"><span>' + html.escape(label)
            + '</span><button class="code-copy" onclick="copyCode(this)">Copy</button></div>'
            + '<div class="code-body"><pre><code>' + html.escape(text.strip()) + '</code></pre></div></div>')


def file_code(name):
    return code((HERE / name).read_text(), name)


def stage(mode, key, title):
    return f'<h3 id="qs-{mode}-{key}">{html.escape(title)}</h3>'


def steps(mode):
    return '<nav class="qs-step-nav" aria-label="Steps in this walkthrough">' + ''.join(
        f'<a href="#quickstart/{mode}/{key}">{i}. {label}</a>'
        for i, (key, label) in enumerate([
            ('prepare', 'Prepare'), ('deploy', 'Deploy'), ('configure', 'Configure'),
            ('application', 'Run the app'), ('verify', 'Verify'), ('cleanup', 'Clean up')], 1)) + '</nav>'


CLONE = '''git clone --depth 1 https://github.com/zilliztech/woodpecker.git
cd woodpecker'''
DC = 'docker compose -f docs/quickstart/compose.yaml'
SDK = (HERE / 'client.go').read_text()
SERVICE_INIT = '''// cfg is loaded from the path-specific client YAML below.
etcdClient, err := clientv3.New(clientv3.Config{
    Endpoints: cfg.Etcd.GetEndpoints(),
    DialTimeout: 5 * time.Second,
})
if err != nil {
    return err
}
defer etcdClient.Close()
client, err := woodpecker.NewClient(ctx, cfg, etcdClient, false)
if err != nil {
    return err
}
defer client.Close(context.Background())'''
EMBED_INIT = '''// storage.type is minio; the LogStore runs inside this application.
client, err := woodpecker.NewEmbedClientFromConfig(ctx, cfg)
if err != nil {
    return err
}
defer client.Close(context.Background())'''
READ_WRITE = '''// After CreateLog / OpenLog / OpenLogWriter (see the complete program):
result := writer.Write(ctx, &wplog.WriteMessage{Payload: []byte("hello, Woodpecker")})
if result.Err != nil {
    return result.Err
}
reader, err := handle.OpenLogReader(ctx, result.LogMessageId, "quickstart")
if err != nil {
    return err
}
defer reader.Close(context.Background())
message, err := reader.ReadNext(ctx)
if err != nil {
    return err
}
fmt.Println(string(message.Payload))'''


def application(mode):
    return ('<p>The runnable <a href="quickstart/client.go">client.go</a> loads the selected YAML, '
            + ('creates an external etcd connection and a service client:' if mode != 'embedded'
               else 'starts an embedded client connected to etcd and object storage:') + '</p>'
            + code(SERVICE_INIT if mode != 'embedded' else EMBED_INIT, 'Client initialization · excerpt')
            + '<p>It creates a fresh log, appends a message, and reads from the acknowledged position:</p>'
            + code(READ_WRITE, 'Write and read · excerpt')
            + '<details class="qs-source"><summary>Complete application, including imports, configuration loading and cleanup</summary>'
            + code(SDK, 'docs/quickstart/client.go') + '</details>')


def verify(mode):
    return (stage(mode, 'verify', '5. Verify the result')
            + '<p>Expected application output (log name varies; runtime messages may also appear):</p>'
            + code('''WROTE log=quickstart-... position=0:0
READ position=0:0 payload="hello, Woodpecker"
VERIFIED mode=''' + ('minio' if mode == 'embedded' else 'service') + ' messages=1', 'Example output')
            + '<p><code>WROTE</code> reports the acknowledged position. <code>READ</code> reports the entry read back. '
              'The program prints <code>VERIFIED</code> only after both the message ID and payload match. '
              'A nonzero exit or missing VERIFIED line requires investigation.</p>')


intro = '''<h1>Quick Start</h1>
<p class="page-desc">Choose one complete path: deploy the dependencies and WAL, configure the matching client, then write and read a message.</p>
<table class="doc-table qs-choice"><thead><tr><th>Path</th><th>Where the application runs</th><th>Client</th></tr></thead><tbody>
<tr><td><a href="#quickstart/kubernetes">Kubernetes · Service</a></td><td>A Job in the same namespace as the LogStore cluster</td><td>NewClient</td></tr>
<tr><td><a href="#quickstart/service">Docker Compose · Service</a></td><td>A container on the same Compose network as the LogStore nodes</td><td>NewClient</td></tr>
<tr><td><a href="#quickstart/embedded">Embedded · Go library</a></td><td>Your workstation; etcd and MinIO run in Docker</td><td>NewEmbedClientFromConfig</td></tr>
</tbody></table>
<p>The two Service paths deploy the same architecture in different environments. Embedded runs its LogStore inside the application. Follow one tab from beginning to end; each includes its own configuration, commands and output interpretation.</p>
<div class="callout info"><span>These development examples use a released <code>milvusdb/woodpecker:v0.1.46</code> server image and the v0.1.47 SDK API. Clone the current source checkout for the tutorial files added after that release. Development credentials, single-instance dependencies and small PVCs are for evaluation; use your own dependency, storage and lifecycle configuration for an operational deployment.</span></div>
<div class="doc-tabs qs-mode-tabs" role="tablist" aria-label="Deployment walkthrough">
<button class="doc-tab" id="qs-tab-kubernetes" role="tab" aria-selected="false" aria-controls="qs-kubernetes" data-qs-mode="kubernetes" tabindex="-1" onclick="openDocRoute('quickstart/kubernetes')">Kubernetes · Service</button>
<button class="doc-tab active" id="qs-tab-service" role="tab" aria-selected="true" aria-controls="qs-service" data-qs-mode="service" tabindex="0" onclick="openDocRoute('quickstart/service')">Docker Compose · Service</button>
<button class="doc-tab" id="qs-tab-embedded" role="tab" aria-selected="false" aria-controls="qs-embedded" data-qs-mode="embedded" tabindex="-1" onclick="openDocRoute('quickstart/embedded')">Embedded · Library</button>
</div>'''

k8s = ('<div class="doc-tab-panel" id="qs-kubernetes" role="tabpanel" aria-labelledby="qs-tab-kubernetes">'
       '<h2>Kubernetes: deploy a WAL service and run an application</h2>' + steps('kubernetes')
       + stage('kubernetes', 'prepare', '1. Prepare your cluster and checkout')
       + '<p>Use Kubernetes 1.28+, kubectl, Docker, and a default StorageClass with at least 6Gi available for the example PVCs. Allocate resources for three LogStore nodes plus etcd, MinIO and an application Job. Commands assume the namespace <code>wp-quickstart</code> and the usual cluster DNS suffix <code>cluster.local</code>. Start at the repository root:</p>'
       + code(CLONE) + code('kubectl config current-context\nkubectl get storageclass')
       + stage('kubernetes', 'deploy', '2. Deploy etcd, MinIO and three LogStore nodes')
       + '<p>The <a href="quickstart/kubernetes.yaml">complete manifest</a> creates a dedicated namespace, dependency Services/Deployments/PVCs, a server ConfigMap, a headless Service and a three-replica StatefulSet. The headless Service publishes each node for direct quorum access.</p>'
       + code('''kubectl apply -f docs/quickstart/kubernetes.yaml
kubectl -n wp-quickstart rollout status deployment/qs-etcd --timeout=180s
kubectl -n wp-quickstart rollout status deployment/qs-minio --timeout=180s
kubectl -n wp-quickstart rollout status statefulset/wp --timeout=300s
kubectl -n wp-quickstart get pods''')
       + '<p>Continue when <code>qs-etcd</code>, <code>qs-minio</code> and <code>wp-0/1/2</code> are Ready. Pending PVCs mean the StorageClass/capacity needs attention; ImagePullBackOff means the registry/image is unavailable. Inspect with <code>kubectl -n wp-quickstart describe pod &lt;pod&gt;</code>.</p>'
       + stage('kubernetes', 'configure', '3. Use the Kubernetes client configuration')
       + '<p><a href="quickstart/kubernetes-client.yaml">kubernetes-client.yaml</a> uses names resolvable from the application Job in this namespace. Its metadata namespace, bucket and root path match the server ConfigMap:</p>'
       + file_code('kubernetes-client.yaml')
       + '<div class="callout warning"><span><strong>A seed is discovery, not a proxy.</strong> The SDK must reach every selected node, etcd, and object storage. A single port-forward or LoadBalancer does not expose all quorum endpoints. This walkthrough runs the app inside Kubernetes; for an external SDK, provide DNS/routing to every advertised gRPC address and reachable dependencies. The wp CLI’s node-admin mappings apply to HTTP administration, not SDK gRPC traffic.</span></div>'
       + stage('kubernetes', 'application', '4. Build and run the application Job')
       + application('kubernetes')
       + '<p>Build the container on the same CPU architecture as the cluster nodes:</p>'
       + code('docker build -f docs/quickstart/Dockerfile -t woodpecker-quickstart-client:local .')
       + '<p>For minikube, load the image into the intended profile (replace <code>minikube</code> if yours has another name):</p>'
       + code('minikube -p minikube image load woodpecker-quickstart-client:local')
       + '<p>For a remote cluster, replace the example registry below with one your cluster can pull from, push the image, and set that exact image in <a href="quickstart/kubernetes-job.yaml">kubernetes-job.yaml</a>:</p>'
       + code('docker tag woodpecker-quickstart-client:local registry.example.com/team/wp-quickstart-client:demo\ndocker push registry.example.com/team/wp-quickstart-client:demo')
       + '<p>Then create the client ConfigMap and Job:</p>'
       + code('''kubectl -n wp-quickstart create configmap wp-client-config \\
  --from-file=client.yaml=docs/quickstart/kubernetes-client.yaml \\
  --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -f docs/quickstart/kubernetes-job.yaml
kubectl -n wp-quickstart wait --for=condition=complete job/quickstart-client --timeout=180s
kubectl -n wp-quickstart logs job/quickstart-client''')
       + verify('kubernetes')
       + '<p>If the Job fails, inspect its logs and <code>kubectl -n wp-quickstart describe job quickstart-client</code>. Check Service DNS and node seeds for connection errors; compare metadata/bucket/root-path settings if the intended log or data is absent. To repeat the application, delete only its Job and reapply its manifest:</p>'
       + code('''kubectl -n wp-quickstart delete job quickstart-client
kubectl apply -f docs/quickstart/kubernetes-job.yaml''')
       + stage('kubernetes', 'cleanup', '6. Clean up the evaluation environment')
       + '<p>This deletes the dedicated namespace, its example workloads, PVCs and their data:</p>'
       + code('kubectl delete namespace wp-quickstart')
       + '<p>The manifest is a plain StatefulSet evaluation deployment. For managed Woodpecker lifecycle operations, see the <a href="https://github.com/zilliztech/woodpecker/tree/master/deployments/operator">Operator guide</a>.</p></div>')

compose = ('<div class="doc-tab-panel active" id="qs-service" role="tabpanel" aria-labelledby="qs-tab-service">'
           '<h2>Docker Compose: deploy a WAL service and run an application</h2>' + steps('service')
           + stage('service', 'prepare', '1. Prepare Docker and the checkout')
           + '<p>Install Git and Docker with Compose v2.20+. Docker builds the Go application, so no host Go installation is needed for this path. Run all commands from this checkout’s repository root:</p>' + code(CLONE)
           + stage('service', 'deploy', '2. Start dependencies and three LogStore nodes')
           + '<p>The <a href="quickstart/compose.yaml">tutorial-specific Compose file</a> creates its own network and volumes, etcd, MinIO and three LogStore nodes. It advertises container DNS names so the application and nodes can reach one another; it is separate from the general <code>deployments/</code> template.</p>'
           + code(DC + ' --profile service up -d --wait --wait-timeout 180 etcd minio wp-0 wp-1 wp-2\n' + DC + ' --profile service ps')
           + '<p>All five services must be healthy. Etcd and MinIO also bind workstation-only ports <code>12379</code> and <code>19000</code>, used by the Embedded tab. Change those bindings if occupied; the Service app uses internal names.</p>'
           + stage('service', 'configure', '3. Use the Compose client configuration')
           + '<p><a href="quickstart/compose-client.yaml">compose-client.yaml</a> is mounted into the servers and the application. It sets service mode, seed addresses, metadata namespace and object-storage location:</p>'
           + file_code('compose-client.yaml')
           + '<p>Names such as <code>wp-0:18080</code>, <code>etcd:2379</code> and <code>minio:9000</code> are Docker-network addresses. Run the application container below on that network. Setting only a localhost seed on the workstation does not rewrite the addresses returned in quorum metadata.</p>'
           + stage('service', 'application', '4. Create the service client, write and read')
           + application('service')
           + code(DC + ' build client\n' + DC + ' run --rm client --config /examples/compose-client.yaml')
           + verify('service')
           + '<p>For startup or connection failures, inspect the dependency and LogStore logs, verify that all nodes joined, and rerun the client once they are healthy:</p>'
           + code(DC + ' --profile service logs --tail=80 etcd minio wp-0 wp-1 wp-2')
           + stage('service', 'cleanup', '6. Stop the example')
           + code(DC + ' --profile service down')
           + '<p>Named volumes retain the example data. To discard that data as well, add <code>--volumes</code> to the down command. Each application run creates a new log.</p></div>')

embedded = ('<div class="doc-tab-panel" id="qs-embedded" role="tabpanel" aria-labelledby="qs-tab-embedded">'
            '<h2>Embedded: run the WAL engine inside a Go application</h2>' + steps('embedded')
            + stage('embedded', 'prepare', '1. Prepare Go, Docker and the checkout')
            + '<p>Install Git, Go 1.25+ (see <a href="https://github.com/zilliztech/woodpecker/blob/master/go.mod">go.mod</a>), and Docker with Compose v2.20+. This application runs on your workstation; only etcd and MinIO run in containers.</p>' + code(CLONE)
            + stage('embedded', 'deploy', '2. Start just etcd and MinIO')
            + code(DC + ' up -d --wait --wait-timeout 180 etcd minio\n' + DC + ' ps')
            + '<p>No standalone LogStore nodes are required. The two dependencies must be healthy and reachable at <code>127.0.0.1:12379</code> and <code>127.0.0.1:19000</code>. This reuses the tutorial Compose dependencies if the Service example is already running.</p>'
            + stage('embedded', 'configure', '3. Use the workstation client configuration')
            + '<p><a href="quickstart/embedded-host.yaml">embedded-host.yaml</a> selects <code>minio</code> storage, host-reachable dependency ports, and a separate metadata/bucket namespace. This is the SDK YAML, not wp’s cli.yaml:</p>'
            + file_code('embedded-host.yaml')
            + stage('embedded', 'application', '4. Start the embedded client, write and read')
            + application('embedded')
            + code('go run ./docs/quickstart/client.go --config docs/quickstart/embedded-host.yaml')
            + verify('embedded')
            + '<p>Connection refused usually means a dependency is not healthy or the workstation port differs from the YAML. An authentication error requires matching MinIO credentials. Use the <code>NewEmbedClientFromConfig</code> path with <code>storage.type: minio</code>; service storage requires the other client constructor.</p>'
            + stage('embedded', 'cleanup', '6. Stop dependencies')
            + code(DC + ' down')
            + '<p>Stop any Service example using these dependencies first. Named volumes retain metadata and object data; add <code>--volumes</code> to discard the tutorial data. For an application in its own module, install <code>github.com/zilliztech/woodpecker@v0.1.47</code> and copy the complete Go source above.</p></div>')

content = '<!-- BEGIN GENERATED QUICK START -->\n' + intro + k8s + compose + embedded + '''
<div class="doc-pagination"><a href="#overview" onclick="showPage('overview',document.querySelector('.sidebar-link[onclick*=overview]'))"><div class="pg-label">← Previous</div><div class="pg-title">Overview</div></a><a href="#configuration" onclick="showPage('configuration',document.querySelector('.sidebar-link[onclick*=configuration]'))"><div class="pg-label">Next →</div><div class="pg-title">Configuration</div></a></div>
<!-- END GENERATED QUICK START -->'''
p = DOCS / 'docs.html'
s = p.read_text()
if '<!-- BEGIN GENERATED QUICK START -->' in s:
    s = re.sub(r'<!-- BEGIN GENERATED QUICK START -->.*?<!-- END GENERATED QUICK START -->', lambda _: content, s, count=1, flags=re.S)
else:
    s = re.sub(r'(<div class="doc-page" id="page-quickstart">).*?(\n        </div>\n\n        <!-- ═══════ CONFIGURATION)', lambda m: m[1] + '\n' + content + m[2], s, count=1, flags=re.S)
p.write_text(s)
print('Embedded three complete Quick Start paths and exact client/configuration sources.')
