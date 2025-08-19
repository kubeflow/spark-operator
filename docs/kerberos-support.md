# Kerberos Authentication Support

The Spark Operator now supports Kerberos authentication for secure access to Hadoop clusters, optimized for Apache Spark. This enables Spark applications to authenticate with Kerberos-enabled services such as HDFS, Hive, HBase, and other components in a secure Hadoop ecosystem using the latest Spark security features.

## Overview

Kerberos is a network authentication protocol designed to provide strong authentication for client/server applications. In Hadoop clusters, Kerberos is commonly used to secure access to:

- HDFS (Hadoop Distributed File System)
- Hive Metastore
- HBase
- YARN Resource Manager
- Other Hadoop ecosystem components

The Spark Operator's Kerberos support automates the configuration of Kerberos authentication for Spark applications, making it easier to run secure Spark jobs.

## Prerequisites

Before using Kerberos authentication with Spark Operator, ensure:

1. **Kerberos Infrastructure**: A working Kerberos KDC (Key Distribution Center) is available
2. **Keytab Files**: Service keytab files are available for the Spark application principal
3. **Kerberos Configuration**: A properly configured `krb5.conf` file
4. **Hadoop Cluster**: The target Hadoop cluster is configured for Kerberos authentication
5. **Network Connectivity**: Spark pods can reach the Kerberos KDC and Hadoop services

## Configuration

### 1. Create Kerberos Secrets

First, create Kubernetes secrets containing your keytab and Kerberos configuration files:

```bash
# Create secret with keytab file
kubectl create secret generic spark-kerberos-keytab \
  --from-file=krb5.keytab=/path/to/your/spark.keytab

# Create secret with Kerberos configuration
kubectl create secret generic spark-kerberos-config \
  --from-file=krb5.conf=/path/to/your/krb5.conf
```

### 2. Configure SparkApplication

Add the Kerberos configuration to your SparkApplication specification:

```yaml
apiVersion: "sparkoperator.k8s.io/v1beta2"
kind: SparkApplication
metadata:
  name: spark-pi-kerberos
spec:
  # ... other configuration ...
  
  # Kerberos authentication configuration
  kerberos:
    principal: "spark@EXAMPLE.COM"          # Kerberos principal
    realm: "EXAMPLE.COM"                    # Kerberos realm (optional)
    kdc: "kdc.example.com:88"              # KDC address (optional)
    keytabSecret: "spark-kerberos-keytab"  # Secret containing keytab
    configSecret: "spark-kerberos-config"  # Secret containing krb5.conf
    
  # Hadoop configuration for Kerberos
  hadoopConf:
    "hadoop.security.authentication": "kerberos"
    "hadoop.security.authorization": "true"
    # Add other Hadoop-specific configuration as needed
    
  driver:
    # Mount Kerberos secrets
    secrets:
      - name: "spark-kerberos-keytab"
        path: "/etc/kerberos/keytab"
        secretType: "KerberosKeytab"
      - name: "spark-kerberos-config"
        path: "/etc/kerberos/conf"
        secretType: "Generic"
        
  executor:
    # Mount Kerberos secrets
    secrets:
      - name: "spark-kerberos-keytab"
        path: "/etc/kerberos/keytab"
        secretType: "KerberosKeytab"
      - name: "spark-kerberos-config"
        path: "/etc/kerberos/conf"
        secretType: "Generic"
```

## Configuration Options

### KerberosSpec Fields

| Field | Type | Description | Required |
|-------|------|-------------|----------|
| `principal` | `string` | Kerberos principal name (e.g., `spark@EXAMPLE.COM`) | Yes |
| `realm` | `string` | Kerberos realm (can be inferred from principal) | No |
| `kdc` | `string` | Key Distribution Center address | No |
| `keytabSecret` | `string` | Name of secret containing keytab file | Yes |
| `keytabFile` | `string` | Filename within keytab secret (default: `krb5.keytab`) | No |
| `configSecret` | `string` | Name of secret containing krb5.conf | Yes |
| `configFile` | `string` | Filename within config secret (default: `krb5.conf`) | No |
| `renewalCredentials` | `string` | Credential renewal strategy: `keytab` (default) or `ccache` | No |
| `enabledServices` | `[]string` | Services with Kerberos enabled (default: `["hadoopfs", "hbase", "hive"]`) | No |

### SecretType

The operator supports a new secret type `KerberosKeytab` which automatically:
- Sets the `KRB5_KEYTAB_FILE` environment variable
- Points to the correct keytab file location

## Environment Variables

The Kerberos implementation uses standard Kerberos environment variables:

- `KRB5_KEYTAB_FILE`: Path to the keytab file
- `KRB5_CONFIG`: Path to the Kerberos configuration file

These are automatically configured when using the `KerberosKeytab` secret type.

## Spark Configuration

### Automatic Configuration

The Kerberos configuration automatically adds the following Spark/Hadoop configurations:

```
# Hadoop-level configuration
spark.hadoop.hadoop.security.authentication=kerberos
spark.hadoop.hadoop.security.authorization=true
spark.hadoop.hadoop.kerberos.principal=<principal>
spark.hadoop.hadoop.kerberos.keytab=<keytab-path>
spark.hadoop.java.security.krb5.conf=<config-path>

# Spark-level Kerberos configuration  
spark.kerberos.principal=<principal>
spark.kerberos.keytab=<keytab-path>
spark.kerberos.renewal.credentials=keytab

# Service credentials (automatically enabled)
spark.security.credentials.hadoopfs.enabled=true
spark.security.credentials.hbase.enabled=true
spark.security.credentials.hive.enabled=true

# Kerberos access configuration
spark.kerberos.access.hadoopFileSystems=hdfs

# JVM configuration for krb5.conf
spark.driver.extraJavaOptions=-Djava.security.krb5.conf=<config-path>
spark.executor.extraJavaOptions=-Djava.security.krb5.conf=<config-path>
```

### Credential Renewal Strategies

**Keytab-based Renewal (Recommended)**:
```yaml
kerberos:
  renewalCredentials: "keytab"  # Default for long-running applications
```

**Ticket Cache Renewal**:
```yaml
kerberos:
  renewalCredentials: "ccache"  # Requires external ticket management
```

### Service-Specific Configuration

Control which services have Kerberos credentials enabled:

```yaml
kerberos:
  enabledServices: ["hadoopfs", "hbase", "hive", "yarn"]
```

## Troubleshooting

### Common Issues

1. **Authentication Failures**
   - Verify keytab file is valid: `klist -k /path/to/keytab`
   - Check principal name matches exactly
   - Ensure clocks are synchronized between Spark pods and KDC

2. **Network Connectivity**
   - Test KDC connectivity: `telnet kdc.example.com 88`
   - Check DNS resolution for KDC and Hadoop services
   - Verify firewall rules allow Kerberos traffic

3. **Configuration Issues**
   - Validate `krb5.conf` syntax and realm configuration
   - Check Hadoop service configurations match SparkApplication
   - Verify secret names and paths are correct

### Debug Commands

```bash
# Check if secrets are properly mounted
kubectl exec -it <spark-driver-pod> -- ls -la /etc/kerberos/keytab/
kubectl exec -it <spark-driver-pod> -- ls -la /etc/kerberos/conf/

# Test Kerberos authentication
kubectl exec -it <spark-driver-pod> -- kinit -kt /etc/kerberos/keytab/krb5.keytab spark@EXAMPLE.COM

# View current Kerberos tickets
kubectl exec -it <spark-driver-pod> -- klist
```

## Examples

See `examples/spark-pi-kerberos.yaml` for a complete example of a SparkApplication with Kerberos authentication configured for secure Hadoop access.

## Security Considerations

1. **Secret Management**: Store keytab files securely using Kubernetes secrets
2. **RBAC**: Ensure proper RBAC is configured to limit access to Kerberos secrets
3. **Network Security**: Use network policies to control traffic to/from Kerberos services
4. **Key Rotation**: Implement processes for regular keytab renewal and rotation
5. **Audit Logging**: Enable audit logging for Kerberos authentication events

## Migration from Manual Configuration

If you were previously configuring Kerberos manually via spark configuration, you can migrate to the new native support:

### Before (Manual Configuration)
```yaml
sparkConf:
  "spark.hadoop.hadoop.security.authentication": "kerberos"
  "spark.hadoop.hadoop.kerberos.principal": "spark@EXAMPLE.COM"
  "spark.hadoop.hadoop.kerberos.keytab": "/etc/secrets/keytab"
# ... manual secret mounting ...
```

### After (Native Support)
```yaml
kerberos:
  principal: "spark@EXAMPLE.COM"
  keytabSecret: "spark-kerberos-keytab"
  configSecret: "spark-kerberos-config"
```

The native support automatically handles the underlying Spark and Hadoop configurations, secret mounting, and environment variable setup.