# Validating Remote Dependency URLs

Spark can fetch application and dependency URLs during submission. These requests can use the network access and identity of the Spark Operator or the REST submitter. A submitted application could therefore try to access a destination that the submitter can reach.

The validating webhook can reject declared fetch URLs that do not match an allowlist of URL schemes and hosts. This reduces server-side request forgery (SSRF) risk from these fields.

:::{warning}
URL validation is opt-in. It is disabled by default in the operator binary, the Helm chart, and `config/default`. Installing or upgrading the operator does not enable it.

URL validation is not a complete SSRF defense. Use it with restricted egress, least-privilege workload identities, and trusted artifact sources.
:::

## Validation Scope

When enabled, the webhook validates these fields on `SparkApplication` create and spec update requests:

- `spec.mainApplicationFile`
- `spec.deps.jars`
- `spec.deps.files`
- `spec.deps.pyFiles`
- `spec.deps.repositories`
- `spec.deps.archives`
- `spec.sparkConf["spark.jars"]`
- `spec.sparkConf["spark.files"]`
- `spec.sparkConf["spark.submit.pyFiles"]`
- `spec.sparkConf["spark.archives"]`
- `spec.sparkConf["spark.jars.repositories"]`
- `spec.sparkConf["spark.kubernetes.driver.podTemplateFile"]`
- `spec.sparkConf["spark.kubernetes.executor.podTemplateFile"]`

The same checks apply to `ScheduledSparkApplication.spec.template`. The webhook checks each item in comma-separated URL lists and reports all detected errors.

Schemeless local paths, `file://` URLs, and `local://` URLs are always allowed when they do not contain a host. Network-path references such as `//host/path` and local schemes with a host, such as `file://host/path`, are rejected.

The validator does not inspect:

- runtime-only `sparkConf` settings, such as `spark.eventLog.dir`;
- `hadoopConf` settings;
- Maven coordinates in `spec.deps.packages` or `spec.deps.excludePackages`;
- URLs created or used by application code, container images, init containers, or sidecars; or
- upload destinations such as `spark.kubernetes.file.upload.path`.

Host rules compare the URL scheme and hostname. They do not restrict the port. Host validation also cannot prevent DNS rebinding, unsafe redirects, or the compromise of an allowed host.

## Configure with Helm

Enable validation and allow only the remote locations that applications need:

```yaml
webhook:
  urlSchemeValidation:
    enable: true
    allowedSchemes:
      - https
      - s3a
    allowedHosts:
      - https://repo.example.com
      - s3a://trusted-bucket
    allowedWildcardHosts:
      - https://*.artifacts.example.com
    allowedSchemesAnyHost: []
```

A remote URL must match both `allowedSchemes` and a scheme-qualified entry in `allowedHosts` or `allowedWildcardHosts`, unless its scheme is listed in `allowedSchemesAnyHost`. Without that exception, empty host lists deny all remote URLs. A leftmost wildcard matches subdomains, but it does not match the base domain or an IP address.

`allowedSchemesAnyHost` bypasses host validation for the listed schemes. Each scheme must also be in `allowedSchemes`. Use this option only when separate network and service authorization controls limit the endpoints and resources that the scheme can access.

## Configure with Kustomize

The standard `config/default` deployment keeps validation disabled. Apply the opt-in overlay to enable validation:

```bash
kubectl apply -k config/overlays/url-scheme-validation --server-side
```

The built-in overlay does not allow any remote schemes or hosts. Add the required flags to a deployment-specific overlay if applications must use remote URLs:

```yaml
- op: add
  path: /spec/template/spec/containers/0/args/-
  value: --allowed-url-schemes=https
- op: add
  path: /spec/template/spec/containers/0/args/-
  value: --allowed-url-hosts=https://repo.example.com
```

## Add Other Security Controls

Use URL validation as one layer in a broader security policy:

- Apply default-deny egress rules to the controller or REST submitter and to driver and executor pods. Allow only required artifact services.
- Use separate, least-privilege identities. Do not expose cloud metadata or ambient credentials to the submitter or Spark workloads.
- Use trusted artifact repositories. Validate redirects and resolved destinations at an egress proxy or artifact mirror.
- Where client deployment mode is supported, restrict it for untrusted users. Client-mode application code can run in the submitter process.
- Enforce image, pod, service account, and admission policies for submitted workloads.

These controls must cover the controller submitter or REST submitter that your installation uses, and all driver and executor workloads.
