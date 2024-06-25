# Running Spark Applications on a Schedule using a ScheduledSparkApplication

The operator supports running a Spark application on a standard [cron](https://en.wikipedia.org/wiki/Cron) schedule using objects of the `ScheduledSparkApplication` custom resource type. A `ScheduledSparkApplication` object specifies a cron schedule on which the application should run and a `SparkApplication` template from which a `SparkApplication` object for each run of the application is created. The following is an example `ScheduledSparkApplication`:

```yaml
apiVersion: "sparkoperator.k8s.io/v1beta2"
kind: ScheduledSparkApplication
metadata:
  name: spark-pi-scheduled
  namespace: default
spec:
  schedule: "@every 5m"
  concurrencyPolicy: Allow
  successfulRunHistoryLimit: 1
  failedRunHistoryLimit: 3
  template:
    type: Scala
    mode: cluster
    image: gcr.io/spark/spark:v3.1.1
    mainClass: org.apache.spark.examples.SparkPi
    mainApplicationFile: local:///opt/spark/examples/jars/spark-examples_2.12-3.1.1.jar
    driver:
      cores: 1
      memory: 512m
    executor:
      cores: 1
      instances: 1
      memory: 512m
    restartPolicy:
      type: Never
```

The concurrency of runs of an application is controlled by `.spec.concurrencyPolicy`, whose valid values are `Allow`, `Forbid`, and `Replace`, with `Allow` being the default. The meanings of each value is described below:

* `Allow`: more than one run of an application are allowed if for example the next run of the application is due even though the previous run has not completed yet.
* `Forbid`: no more than one run of an application is allowed. The next run of the application can only start if the previous run has completed.
* `Replace`: no more than one run of an application is allowed. When the next run of the application is due, the previous run is killed and the next run starts as a replacement.

A scheduled `ScheduledSparkApplication` can be temporarily suspended (no future scheduled runs of the application will be triggered) by setting `.spec.suspend` to `true`. The schedule can be resumed by removing `.spec.suspend` or setting it to `false`. A `ScheduledSparkApplication` can have names of `SparkApplication` objects for the past runs of the application tracked in the `Status` section as discussed below. The numbers of past successful runs and past failed runs to keep track of are controlled by field `.spec.successfulRunHistoryLimit` and field `.spec.failedRunHistoryLimit`, respectively. The example above allows 1 past successful run and 3 past failed runs to be tracked.

The `Status` section of a `ScheduledSparkApplication` object shows the time of the last run and the proposed time of the next run of the application, through `.status.lastRun` and `.status.nextRun`, respectively. The names of the `SparkApplication` object for the most recent run (which may  or may not be running) of the application are stored in `.status.lastRunName`. The names of `SparkApplication` objects of the past successful runs of the application are stored in `.status.pastSuccessfulRunNames`. Similarly, the names of `SparkApplication` objects of the past failed runs of the application are stored in `.status.pastFailedRunNames`.

Note that certain restart policies (specified in `.spec.template.restartPolicy`) may not work well with the specified schedule and concurrency policy of a `ScheduledSparkApplication`. For example, a restart policy of `Always` should never be used with a `ScheduledSparkApplication`. In most cases, a restart policy of `OnFailure` may not be a good choice as the next run usually picks up where the previous run left anyway. For these reasons, it's often the right choice to use a restart policy of `Never` as the example above shows.
