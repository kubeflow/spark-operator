# Building Custom Operator Images

The official controller images published to `ghcr.io/kubeflow/spark-operator/controller`
are the recommended way to run Spark Operator. Some organizations, however, need to
build their own image: to use an approved base image, apply internal hardening, embed a
corporate CA bundle, or satisfy private registry conventions.

Every release attaches prebuilt `spark-operator` binaries so you can do this without
installing a Go toolchain or reproducing the project build.

## Release artifacts

Each GitHub release includes:

| Artifact | Contents |
| --- | --- |
| `spark-operator_<version>_linux_amd64.tar.gz` | `spark-operator` binary and `LICENSE` |
| `spark-operator_<version>_linux_arm64.tar.gz` | `spark-operator` binary and `LICENSE` |
| `SHA256SUMS` | SHA-256 checksums for the archives above |

The binaries are statically linked and carry the same version metadata as the official
images, so `spark-operator version` reports the released version, commit and build date.

## Downloading and verifying

```bash
VERSION=v2.5.1
ARCH=amd64

curl -fsSLO "https://github.com/kubeflow/spark-operator/releases/download/${VERSION}/spark-operator_${VERSION}_linux_${ARCH}.tar.gz"
curl -fsSLO "https://github.com/kubeflow/spark-operator/releases/download/${VERSION}/SHA256SUMS"

# Verify before use.
sha256sum --ignore-missing --check SHA256SUMS

tar -xzf "spark-operator_${VERSION}_linux_${ARCH}.tar.gz"
./spark-operator version
```

## Building a custom image

The operator invokes `${SPARK_HOME}/bin/spark-submit` as a subprocess, so the runtime
image must contain a Spark distribution and a JVM. The example below layers the released
binary onto the official Spark image; substitute your own approved base as needed.

```dockerfile
ARG SPARK_VERSION=4.0.4
ARG SPARK_IMAGE=docker.io/apache/spark:${SPARK_VERSION}
FROM ${SPARK_IMAGE}

ARG SPARK_UID=185
ARG SPARK_GID=185

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends catatonit \
    && rm -rf /var/lib/apt/lists/*

# The webhook Deployment runs this same image and mounts serving certs here.
RUN mkdir -p /etc/k8s-webhook-server/serving-certs /home/spark \
    && chmod -R g+rw /etc/k8s-webhook-server/serving-certs \
    && chown -R "${SPARK_UID}:${SPARK_GID}" /etc/k8s-webhook-server/serving-certs /home/spark

COPY spark-operator /usr/bin/spark-operator
COPY entrypoint.sh /usr/bin/entrypoint.sh

USER ${SPARK_UID}:${SPARK_GID}

ENTRYPOINT ["/usr/bin/entrypoint.sh"]
```
Override `SPARK_VERSION` to target a different Spark line, or `SPARK_IMAGE` to point at an
approved internal base image. Pinning `SPARK_IMAGE` by digest, as the project's own
`Dockerfile` does, is recommended for reproducible builds.

Build it alongside the extracted binary and `entrypoint.sh` from the release tag:

```bash
docker build -t my-registry.example.com/spark-operator:${VERSION} .
```

### Requirements for a custom base image

If you replace the Spark base image, the resulting image must still provide:

- `SPARK_HOME` set, with a working `bin/spark-submit` and a JVM. The operator fails at
  startup if `SPARK_HOME` is unset.
- `bash`, since `entrypoint.sh` and `spark-submit` are Bash scripts.
- `catatonit` on `PATH`. `spark-submit` forks child JVMs, and without a PID 1 reaper the
  controller pod accumulates zombie processes.
- `libnss_wrapper.so`, if you run on OpenShift. `entrypoint.sh` uses it to synthesize a
  passwd entry under an arbitrary UID.
- A writable, group-readable `/etc/k8s-webhook-server/serving-certs`, used by the webhook
  Deployment.

Changing the default UID/GID away from `185:185` may break existing Pod Security
Admission policies and `securityContext` overrides, so keep it unless you have a reason
not to.

## Using the image

Point the Helm chart at your registry:

```yaml
image:
  registry: my-registry.example.com
  repository: spark-operator
  tag: v2.5.1
```

```bash
helm upgrade --install spark-operator spark-operator/spark-operator \
    --namespace spark-operator \
    --create-namespace \
    -f values.yaml
```