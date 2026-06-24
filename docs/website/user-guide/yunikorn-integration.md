# Integration with YuniKorn

:::{admonition} Warning
:class: warning

This feature is only supported on version 2.0.0 of the operator and above.
:::

[YuniKorn](https://yunikorn.apache.org/) is an alternative scheduler for Kubernetes that provides batch scheduling capabilities over the default scheduler and can notably improve the experience of running Spark on Kubernetes. These capabilities include gang scheduling, application-aware job queueing, hierarchical resource quotas and improved binpacking.

## How to use

### 1. Install YuniKorn

Install YuniKorn on your Kubernetes cluster by following the [Get Started](https://yunikorn.apache.org/docs/) guide on the YuniKorn docs.

**Note:** By default, YuniKorn will install an admission controller that will set the `schedulerName` field on all pods to YuniKorn. Please consult the YuniKorn docs for instructions on disabling this if this is not desired.

### 2. Enable batch scheduling in the controller

Set the following values on your operator installation to enable batch scheduling in the controller. You can also optionally set the default batch scheduler for all `SparkApplication` definitions if not specified by the user in `.spec.batchScheduler`.

```yaml
controller:
  batchScheduler:
    enable: true
    # Setting the default batch scheduler is optional. The default only
    # applies if the batchScheduler field on the SparkApplication spec is not set
    default: yunikorn
```

### 3. Submit an application

Specify the batch scheduler on your `SparkApplication` as shown below. You can find a full example in the repo under [`examples/spark-pi-yunikorn.yaml`](https://github.com/kubeflow/spark-operator/blob/master/examples/spark-pi-yunikorn.yaml).

```yaml
spec:
  ...
  batchScheduler: yunikorn
  batchSchedulerOptions:
    queue: root.default
```

Using the above example, the Spark operator will do the following:

1. Annotate the driver pod with task group annotations
2. Set the `schedulerName` field on the driver and executor pods to `yunikorn`
3. Add a queue label to the driver and executor pods if specified under `batchSchedulerOptions`

For more information on gang scheduling, task groups and queue routing, please consult the following YuniKorn doc pages:

* [Gang scheduling](https://yunikorn.apache.org/docs/next/user_guide/gang_scheduling/)
* [Placement rules](https://yunikorn.apache.org/docs/next/user_guide/placement_rules)

You should see the following pod events that show the pod was gang scheduled with YuniKorn:

```
Type    Reason             Age   From      Message
----    ------             ----  ----      -------
Normal  Scheduling         20s   yunikorn  default/spark-pi-yunikorn-driver is queued and waiting for allocation
Normal  GangScheduling     20s   yunikorn  Pod belongs to the taskGroup spark-driver, it will be scheduled as a gang member
Normal  Scheduled          19s   yunikorn  Successfully assigned default/spark-pi-yunikorn-driver to node spark-operator-worker
Normal  PodBindSuccessful  19s   yunikorn  Pod default/spark-pi-yunikorn-driver is successfully bound to node spark-operator-worker
Normal  TaskCompleted      4s    yunikorn  Task default/spark-pi-yunikorn-driver is completed
Normal  Pulling            20s   kubelet   Pulling image "spark:3.5.2"
Normal  Pulled             13s   kubelet   Successfully pulled image "spark:3.5.2" in 6.162s (6.162s including waiting)
Normal  Created            13s   kubelet   Created container spark-kubernetes-driver
Normal  Started            13s   kubelet   Started container spark-kubernetes-driver
```

The following annotations and labels should also be present on the driver pod:

```yaml
apiVersion: v1
kind: Pod
metadata:
  annotations:
    yunikorn.apache.org/allow-preemption: "true"
    yunikorn.apache.org/task-group-name: spark-driver
    yunikorn.apache.org/task-groups: '[{"name":"spark-driver","minMember":1,"minResource":{"cpu":"1","memory":"896Mi"},"labels":{"queue":"root.default","version":"3.5.2"}},{"name":"spark-executor","minMember":2,"minResource":{"cpu":"1","memory":"896Mi"},"labels":{"queue":"root.default","version":"3.5.2"}}]'
    yunikorn.apache.org/user.info: '{"user":"system:serviceaccount:spark-operator:spark-operator-controller","groups":["system:serviceaccounts","system:serviceaccounts:spark-operator","system:authenticated"]}'
  creationTimestamp: "2024-09-10T04:40:37Z"
  labels:
    queue: root.default
    spark-app-name: spark-pi-yunikorn
    spark-app-selector: spark-1bfe85bb77df4d5594337249b38c9648
    spark-role: driver
    spark-version: 3.5.2
    sparkoperator.k8s.io/app-name: spark-pi-yunikorn
    sparkoperator.k8s.io/launched-by-spark-operator: "true"
    sparkoperator.k8s.io/submission-id: 1a71de55-cdc7-4e62-b997-197883dc4cbe
    version: 3.5.2
  name: spark-pi-yunikorn-driver
  ...
```
