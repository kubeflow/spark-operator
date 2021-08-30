# Integration with Volcano for Batch Scheduling

[Volcano](https://github.com/volcano-sh/volcano) is a batch system built on Kubernetes. It provides a suite of mechanisms
currently missing from Kubernetes that are commonly required by many classes
of batch & elastic workloads.
With the integration with Volcano, Spark application pods can be scheduled for better scheduling efficiency.
# Requirements

## Volcano components

Before using Kubernetes Operator for Apache Spark, with Volcano enabled, user need to ensure Volcano has been successfully installed in the
same environment, please refer [Quick Start Guide](https://github.com/volcano-sh/volcano#quick-start-guide) for Volcano installation.

## Install Kubernetes Operator for Apache Spark with Volcano enabled

Within the help of Helm chart, Kubernetes Operator for Apache Spark with Volcano can be easily installed with the command below:
```bash
$ helm repo add spark-operator https://googlecloudplatform.github.io/spark-on-k8s-operator
$ helm install my-release spark-operator/spark-operator --namespace spark-operator --set enableBatchScheduler=true --set enableWebhook=true
```

# Run Spark Application with Volcano scheduler

Now, we can run a updated version of spark application (with `batchScheduler` configured), for instance:
```yaml
apiVersion: "sparkoperator.k8s.io/v1beta2"
kind: SparkApplication
metadata:
  name: spark-pi
  namespace: default
spec:
  type: Scala
  mode: cluster
  image: "gcr.io/spark-operator/spark:v3.1.1"
  imagePullPolicy: Always
  mainClass: org.apache.spark.examples.SparkPi
  mainApplicationFile: "local:///opt/spark/examples/jars/spark-examples_2.12-v3.1.1.jar"
  sparkVersion: "3.1.1"
  batchScheduler: "volcano"   #Note: the batch scheduler name must be specified with `volcano`
  restartPolicy:
    type: Never
  volumes:
    - name: "test-volume"
      hostPath:
        path: "/tmp"
        type: Directory
  driver:
    cores: 1
    coreLimit: "1200m"
    memory: "512m"        
    labels:
      version: 3.1.1
    serviceAccount: spark
    volumeMounts:
      - name: "test-volume"
        mountPath: "/tmp"
  executor:
    cores: 1
    instances: 1
    memory: "512m"    
    labels:
      version: 3.1.1
    volumeMounts:
      - name: "test-volume"
        mountPath: "/tmp"
```
When running, the Pods Events can be used to verify that whether the pods have been scheduled via Volcano.
```
Type    Reason     Age   From                          Message
----    ------     ----  ----                          -------
Normal  Scheduled  23s   volcano                       Successfully assigned default/spark-pi-driver to integration-worker2
```

# Technological detail

If SparkApplication is configured to run with Volcano, there are some details underground that make the two systems integrated:

1. Kubernetes Operator for Apache Spark's webhook will patch pods' `schedulerName` according to the `batchScheduler` in SparkApplication Spec.
2. Before submitting spark application, Kubernetes Operator for Apache Spark will create a Volcano native resource 
   `PodGroup`[here](https://github.com/volcano-sh/volcano/blob/a8fb05ce6c6902e366cb419d6630d66fc759121e/pkg/apis/scheduling/v1alpha2/types.go#L93) for the whole application.
   and as a brief introduction, most of the Volcano's advanced scheduling features, such as pod delay creation, resource fairness and gang scheduling are all depend on this resource. 
   Also a new pod annotation named `scheduling.k8s.io/group-name` will be added.
3. Volcano scheduler will take over all of the pods that both have schedulerName and annotation correctly configured for scheduling.


Kubernetes Operator for Apache Spark enables end user to have fine-grained controlled on batch scheduling via attribute `BatchSchedulerOptions`. `BatchSchedulerOptions` is a string dictionary
that different batch scheduler can utilize it to expose different attributes.
For now, volcano support these attributes below:

| Name  | Description                                                                | example                                                        |
|-------|----------------------------------------------------------------------------|----------------------------------------------------------------|
| queue | Used to specify which volcano queue will this spark application belongs to |  batchSchedulerOptions:<br/>  &nbsp; &nbsp; queue: "queue1" |
| priorityClassName | Used to specify which priorityClass this spark application will use        |  batchSchedulerOptions:<br/>  &nbsp; &nbsp; priorityClassName: "pri1" |

