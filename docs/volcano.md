# Integrate with volcano scheduler for Spark Application

[Volcano](https://github.com/volcano-sh/volcano) is a batch system built on Kubernetes. It provides a suite of mechanisms
currently missing from Kubernetes that are commonly required by many classes
of batch & elastic workload.
Now the spark application pods can be scheduled via volcano for better scheduling efficiency.

# Requirements

## Volcano components
Before using spark-operator with volcano enabled, user need to ensure volcano has been successfully installed in the
same environment, please refer [quick start guide](https://github.com/volcano-sh/volcano#quick-start-guide) for installation.

## Install spark operator with volcano enabled.
Within the help of helm chart, spark operator with volcano can be easily installed with the command below:
```bash
$ helm repo add incubator http://storage.googleapis.com/kubernetes-charts-incubator
$ helm install incubator/sparkoperator --namespace spark-operator --set batchSchedulerName=volcano
```

## Run Spark Application with volcano scheduler
Now, we can run a updated version of spark application, for instance:
```yaml
apiVersion: "sparkoperator.k8s.io/v1beta1"
kind: SparkApplication
metadata:
  name: spark-pi
  namespace: default
spec:
  type: Scala
  mode: cluster
  image: "gcr.io/spark-operator/spark:v2.4.0"
  imagePullPolicy: Always
  mainClass: org.apache.spark.examples.SparkPi
  mainApplicationFile: "local:///opt/spark/examples/jars/spark-examples_2.11-2.4.0.jar"
  sparkVersion: "2.4.0"
  restartPolicy:
    type: Never
  volumes:
    - name: "test-volume"
      hostPath:
        path: "/tmp"
        type: Directory
  driver:
    cores: 0.1
    coreLimit: "200m"
    memory: "512m"
    schedulerName: "volcano"     #Note: the scheduler name must be specified with `volcano`
    labels:
      version: 2.4.0
    serviceAccount: spark
    volumeMounts:
      - name: "test-volume"
        mountPath: "/tmp"
  executor:
    cores: 1
    instances: 1
    memory: "512m"
    schedulerName: "volcano"     #Note: the scheduler name must be specified with `volcano`
    labels:
      version: 2.4.0
    volumeMounts:
      - name: "test-volume"
        mountPath: "/tmp"
```
When running, the Pod Events can be used to verify that whether it have been scheduled via volcano.
```
Type    Reason     Age   From                          Message
----    ------     ----  ----                          -------
Normal  Scheduled  23s   volcano                       Successfully assigned default/spark-pi-driver to integration-worker2
```

# Technological detail


