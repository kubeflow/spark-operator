# spark-history-server

![Version: 0.1.0](https://img.shields.io/badge/Version-0.1.0-informational?style=flat-square) ![Type: application](https://img.shields.io/badge/Type-application-informational?style=flat-square) ![AppVersion: 3.5.0](https://img.shields.io/badge/AppVersion-3.5.0-informational?style=flat-square)

A Helm chart for Spark history server

**Homepage:** <https://github.com/kubeflow/spark-operator>

## Maintainers

| Name | Email | Url |
| ---- | ------ | --- |
| ChenYi015 | <github@chenyicn.net> |  |

## Values

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| affinity | object | `{}` | Pod affinity |
| env | list | `[]` | List of environment variables to set in the container. |
| envFrom | list | `[]` | List of sources to populate environment variables in the container. |
| fullnameOverride | string | `""` | String to fully override `spark-history-server.fullname` template |
| image.pullPolicy | string | `"IfNotPresent"` | Image pull policy |
| image.pullSecrets | list | `[]` | Image pull secrets |
| image.registry | string | `"docker.io"` | Image registry |
| image.repository | string | `"kubeflow/spark-history-server"` | Image repository |
| image.tag | string | `""` | Image tag, if set, overrides the image tag whose default is the chart appVersion. |
| ingress.annotations | object | `{}` | Annotations to add to the ingress |
| ingress.className | string | `""` | Ingress class name |
| ingress.enable | bool | `false` | Specifies whether an ingress resource should be created |
| ingress.hosts[0].host | string | `"chart-example.local"` |  |
| ingress.hosts[0].paths[0].path | string | `"/"` |  |
| ingress.hosts[0].paths[0].pathType | string | `"ImplementationSpecific"` |  |
| ingress.tls | list | `[]` |  |
| nameOverride | string | `""` | String to partially override `spark-history-server.fullname` template (will maintain the release name) |
| nodeSelector | object | `{}` | Node selector |
| podAnnotations | object | `{}` | Annotations to add to the pod |
| podSecurityContext | object | `{}` | Pod security context |
| replicaCount | int | `1` | Desired number of pods |
| resources | object | `{}` | Pod resources |
| securityContext | object | `{}` | Container security context |
| service.port | int | `18080` | Service port |
| service.type | string | `"ClusterIP"` | Service type |
| serviceAccount.annotations | object | `{}` | Annotations to add to the service account. |
| serviceAccount.create | bool | `true` | Specifies whether a service account should be created. |
| serviceAccount.name | string | `""` | The name of the service account to use. If not set and create is true, a name is generated using the fullname template. |
| sparkConf."spark.history.fs.logDirectory" | string | `"file:///tmp/spark-events"` |  |
| storage.abs.createSecret | object | `{"azureStorageAccessKey":"","azureStorageConnectionString":""}` | If ABS is enabled as artifact store backend and no existing secret is specified, create the secret used to connect to Azure. |
| storage.abs.enable | bool | `false` | Specifies whether to enable Azure Blob Storage (ABS) as event log storage. |
| storage.abs.existingSecret | string | `""` | Name of an existing secret containing the key `AZURE_STORAGE_CONNECTION_STRING` or `AZURE_STORAGE_ACCESS_KEY` to store credentials to access ABS. |
| storage.gcs.createSecret | object | `{"keyFile":""}` | If GCS is enabled and no existing secret is specified, create the secret used to connect to GCS. |
| storage.gcs.createSecret.keyFile | string | `""` | Content of key file |
| storage.gcs.enable | bool | `false` | Specifies whether to enable Google Cloud Storage (GCS) as event log storage. |
| storage.gcs.existingSecret | string | `""` | Name of an existing secret containing the key `keyfile.json` used to store credentials to access GCS. |
| storage.oss.createSecret | object | `{"accessKeyId":"","accessKeySecret":""}` | If OSS is enabled and no existing secret is specified, create the secret used to store OSS access credentials. |
| storage.oss.createSecret.accessKeyId | string | `""` | Alibaba Cloud access key ID |
| storage.oss.createSecret.accessKeySecret | string | `""` | Alibaba Cloud access key secret |
| storage.oss.enable | bool | `false` | Specifies whether to enable Alibaba Cloud Object Storage Service (OSS) as event log storage. |
| storage.oss.endpoint | string | `""` | Endpoint of OSS e.g. `oss-cn-beijing-internal.aliyuncs.com`. |
| storage.oss.existingSecret | string | `""` | Name of an existing secret containing the key `OSS_ACCESS_KEY_ID` and `OSS_ACCESS_KEY_SECRET` to store credentials to access OSS. |
| storage.s3.createCaSecret | object | `{"caBundle":""}` | If S3 is enabled and no existing CA secret is specified, create the secret used to secure connection to S3 / Minio. |
| storage.s3.createCaSecret.caBundle | string | `""` | Content of CA bundle |
| storage.s3.createSecret | object | `{"accessKeyId":"","secretAccessKey":""}` | If S3 is enabled and no existing secret is specified, create the secret used to connect to S3 / Minio. |
| storage.s3.createSecret.accessKeyId | string | `""` | AWS access key ID |
| storage.s3.createSecret.secretAccessKey | string | `""` | AWS secret access key |
| storage.s3.enable | bool | `false` | Specifies whether to enable AWS S3 / Minio as event log storage. |
| storage.s3.existingCaSecret | string | `""` | Name of an existing secret containing the key `ca-bundle.crt` used to store the CA certificate for TLS connections. |
| storage.s3.existingSecret | string | `""` | Name of an existing secret containing the keys `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` to access artifact storage on AWS S3 / MINIO. |
| tolerations | list | `[]` | Pod tolerations |
| volumeMounts | list | `[]` | List of volume mounts that can be referenced by the containers. |
| volumes | list | `[]` | List of volumes that can be mounted by the containers. |

