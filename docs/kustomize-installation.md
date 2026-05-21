# Kustomize Installation Guide

This guide covers deploying and customizing the Spark Operator using its
[Kustomize](https://kustomize.io/) manifests.  The Kustomize manifests live
under `config/` and provide a Helm-free, declarative installation path.

## Architecture Breakdown

`config/default/kustomization.yaml` is the main entry point.  It aggregates
four sub-directories into a single deployable unit:

```
config/default
  ├── ../crd        3 CustomResourceDefinitions (SparkApplication,
  │                 ScheduledSparkApplication, SparkConnect) with webhook
  │                 conversion patches.
  │
  ├── ../rbac       ClusterRoles, ClusterRoleBindings, Roles, and
  │                 RoleBindings for the controller (auto-generated from
  │                 Go markers by controller-gen), plus viewer/editor
  │                 ClusterRoles for end users.
  │
  ├── ../manager    The controller Deployment and its ServiceAccount.
  │                 Runs the `controller start` sub-command.
  │
  └── ../webhook    The webhook Deployment, Service, ServiceAccount,
                    RBAC, and MutatingWebhookConfiguration /
                    ValidatingWebhookConfiguration.  Runs the
                    `webhook start` sub-command.
```

Both the controller and webhook Deployments use the same container image
(`ghcr.io/kubeflow/spark-operator/controller`), differing only in the
sub-command they run.

All resources produced by `config/default` are placed in the `spark-operator`
namespace and labelled with `app.kubernetes.io/name: spark-operator`.

 A successful `kubectl apply -k config/default --server-side --force-conflicts` creates:
 - The `spark-operator` Namespace
 - 3 CRDs (SparkApplication, ScheduledSparkApplication, SparkConnect)
 - Controller Deployment (1 replica) with ServiceAccount, ClusterRole, and leader-election Role
 - Webhook Deployment (1 replica) with ServiceAccount, ClusterRole, Role, Service, and self-signed TLS
 - MutatingWebhookConfiguration and ValidatingWebhookConfiguration

The webhook generates its own TLS certificates at startup (no cert-manager required by default).
See the cert-manager section below if you want managed certificate rotation.


### Spark Application RBAC (config/spark-rbac)

`config/spark-rbac/` is intentionally **not** included in `config/default`.
It creates the ServiceAccount, Role, and RoleBinding that Spark driver pods
need to manage executor pods.  These resources belong in each application
namespace, not in the operator namespace:

```bash
kubectl apply -k config/spark-rbac/ -n <app-namespace>
```

## Quick Start

Clone and install the operator:

```bash
git clone https://github.com/kubeflow/spark-operator.git && cd spark-operator
kubectl apply -k config/default --server-side --force-conflicts
```

Verify the operator is running:

```bash
kubectl -n spark-operator get pods
```

Create a namespace for your spark applications (example is using the default namespace) and install Spark application RBAC resources in this namespace:

```bash
kubectl -n default apply -k config/spark-rbac
```

Create an example application in the default namespace:

```bash
kubectl apply -f https://raw.githubusercontent.com/kubeflow/spark-operator/refs/heads/master/examples/spark-pi.yaml
```

## Component Guide

Three optional capabilities are shipped as standalone Kustomize directories
under `config/`.  Enable them by adding them to the `resources` list in
your overlay.

### Pod Disruption Budget (config/pdb)

Creates a `PodDisruptionBudget` with `minAvailable: 1` targeting the
controller Deployment.  Scale the controller to 2+ replicas before enabling
this so that at least one pod remains available during voluntary disruptions:

```yaml
# my-overlay/kustomization.yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization

resources:
  - ../config/default
  - ../config/pdb
```

### Prometheus Monitoring (config/prometheus)

Creates a `PodMonitor` that tells the Prometheus Operator to scrape the
`/metrics` endpoint (port `metrics`, 8080) on all pods labelled
`app.kubernetes.io/name: spark-operator`:

```yaml
# my-overlay/kustomization.yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization

resources:
  - ../config/default
  - ../config/prometheus
```

The controller and webhook Deployments already carry the Prometheus
annotations (`prometheus.io/scrape`, `prometheus.io/port`,
`prometheus.io/path`), so basic annotation-based scraping works without this
resource.  The `PodMonitor` is for clusters using the Prometheus Operator's
CRD-based service discovery.

### cert-manager Integration (config/certmanager)

Uses cert-manager to provision a self-signed TLS certificate for the webhook
server.  Enabling this requires two changes:

1. Add `config/certmanager` to your overlay's resources.
2. Uncomment the CA-injection patches in `config/crd/kustomization.yaml`
   so that cert-manager injects the CA bundle into the CRD conversion
   webhook configuration.

```yaml
# my-overlay/kustomization.yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization

resources:
  - ../config/default
  - ../config/certmanager
```

Then in `config/crd/kustomization.yaml`, uncomment the three
`cainjection_in_*` patch lines:

```yaml
patches:
  - path: patches/cainjection_in_scheduledsparkapplications.yaml
  - path: patches/cainjection_in_sparkapplications.yaml
  - path: patches/cainjection_in_sparkconnects.yaml
```

cert-manager must already be installed in the cluster before applying.

### Uninstalling

To uninstall the operator, the spark-operator and RBAC resources must be deleted:

```sh
kubectl delete -k config/spark-rbac -n default
kubectl delete -k config/default
```

Note: CRDs are not removed automatically. To remove them manually:
```sh
kubectl delete crd sparkapplications.sparkoperator.k8s.io scheduledsparkapplications.sparkoperator.k8s.io sparkconnects.sparkoperator.k8s.io
```
