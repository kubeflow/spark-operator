# Enabling Resource Quota Enforcement

The Spark Operator provides limited support for resource quota enforcement using a validating webhook. It will count the resources of non-terminal-phase SparkApplications and Pods, and determine whether a requested SparkApplication will fit given the remaining resources. ResourceQuota scope selectors are not supported, any ResourceQuota object that does not match the entire namespace will be ignored. Like the native Pod quota enforcement, current usage is updated asynchronously, so some overscheduling is possible.

If you are running Spark applications in namespaces that are subject to resource quota constraints, consider enabling this feature to avoid driver resource starvation. Quota enforcement can be enabled with the command line arguments `-enable-resource-quota-enforcement=true`. It is recommended to also set `-webhook-fail-on-error=true`.
