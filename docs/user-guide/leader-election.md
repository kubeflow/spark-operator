# Enabling Leader Election for High Availability

The operator supports a high-availability (HA) mode, in which there can be more than one replicas of the operator, with only one of the replicas (the leader replica) actively operating. If the leader replica fails, the leader election process is engaged again to determine a new leader from the replicas available. The HA mode can be enabled through an optional leader election process. Leader election is disabled by default but can be enabled via a command-line flag. The following table summarizes the command-line flags relevant to leader election:

| Flag | Default Value | Description |
| ------------- | ------------- | ------------- |
| `leader-election` | `false` | Whether to enable leader election (or the HA mode) or not. |
| `leader-election-lock-namespace` | `spark-operator` | Kubernetes namespace of the lock resource used for leader election. |
| `leader-election-lock-name` | `spark-operator-lock` | Name of the lock resource used for leader election. |
| `leader-election-lease-duration` | 15 seconds | Leader election lease duration. |
| `leader-election-renew-deadline` | 14 seconds | Leader election renew deadline. |
| `leader-election-retry-period` | 4 seconds | Leader election retry period. |
