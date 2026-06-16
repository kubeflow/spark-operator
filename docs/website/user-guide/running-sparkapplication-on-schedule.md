# Running Spark Applications on a Schedule

The Spark Operator supports running Spark applications on a standard [cron](https://en.wikipedia.org/wiki/Cron) schedule using the `ScheduledSparkApplication` custom resource. A `ScheduledSparkApplication` object defines a cron schedule and a `SparkApplication` template from which the operator creates a new `SparkApplication` object for each scheduled run.

## Example ScheduledSparkApplication

The following example shows a `ScheduledSparkApplication` that runs the Spark Pi example every 5 minutes:

```yaml
apiVersion: sparkoperator.k8s.io/v1beta2
kind: ScheduledSparkApplication
metadata:
  name: spark-pi-scheduled
  namespace: default
spec:
  schedule: "@every 5m"
  concurrencyPolicy: Allow
  successfulRunHistoryLimit: 3
  failedRunHistoryLimit: 3
  template:
    type: Scala
    mode: cluster
    sparkVersion: 4.0.0
    image: docker.io/library/spark:4.0.0
    imagePullPolicy: Always
    mainClass: org.apache.spark.examples.SparkPi
    mainApplicationFile: local:///opt/spark/examples/jars/spark-examples.jar
    arguments:
    - "5000"
    restartPolicy:
      type: Never
    driver:
      cores: 1
      memory: 512m
      template:
        metadata:
          labels:
            spark.apache.org/version: 4.0.0
          annotations:
            spark.apache.org/version: 4.0.0
        spec:
          containers:
          - name: spark-kubernetes-driver
            securityContext:
              allowPrivilegeEscalation: false
              capabilities:
                drop:
                - ALL
              runAsGroup: 185
              runAsNonRoot: true
              runAsUser: 185
              seccompProfile:
                type: RuntimeDefault
          serviceAccount: spark-operator-spark
    executor:
      instances: 2
      cores: 1
      memory: 512m
      template:
        metadata:
          labels:
            spark.apache.org/version: 4.0.0
          annotations:
            spark.apache.org/version: 4.0.0
        spec:
          containers:
          - name: spark-kubernetes-executor
            securityContext:
              allowPrivilegeEscalation: false
              capabilities:
                drop:
                - ALL
              runAsGroup: 185
              runAsNonRoot: true
              runAsUser: 185
              seccompProfile:
                type: RuntimeDefault
```

## Cron Schedule

The cron schedule can be configured with `.spec.schedule`. The Spark Operator parses the schedule with [github.com/robfig/cron/v3](https://pkg.go.dev/github.com/robfig/cron/v3) `ParseStandard`, which accepts:

- Standard 5-field crontab specs (`minute hour day-of-month month day-of-week`), e.g. `"*/5 * * * *"`. Note that the Quartz-style `?` placeholder is not supported.
- Descriptors, e.g. `"@midnight"`, `"@every 1h30m"`.

## Concurrency Policy

The concurrency of runs is controlled by `.spec.concurrencyPolicy`, which has the following valid values (with `Allow` being the default):

- `Allow`: More than one run of an application is allowed. For example, the next run can start even if the previous run has not completed yet.
- `Forbid`: No more than one run of an application is allowed. The next run can only start after the previous run has completed.
- `Replace`: No more than one run of an application is allowed. When the next run is due, the previous run is killed and the next run starts as a replacement.

## Suspending Schedules

A scheduled `ScheduledSparkApplication` can be temporarily suspended (no future runs will be triggered) by setting `.spec.suspend` to `true`. To resume the schedule, remove `.spec.suspend` or set it to `false`.

## Specifying Time Zone

The timezone for the schedule is specified by `.spec.timezone`. If not specified, the default timezone is `Local`. If you want to use a different timezone, you can specify a timezone using the IANA timezone name (e.g. `America/New_York`).

## Status and Run History

The `Status` section of a `ScheduledSparkApplication` object shows:

- The time of the last run through `.status.lastRun`.
- The proposed time of the next run through `.status.nextRun`.

Additional status information includes:

- `.status.lastRunName`: The name of the `SparkApplication` object for the most recent run.
- `.status.pastSuccessfulRunNames`: The names of `SparkApplication` objects of past successful runs.
- `.status.pastFailedRunNames`: The names of `SparkApplication` objects of past failed runs.

A `ScheduledSparkApplication` can track the names of `SparkApplication` objects for past runs in the `Status` section. The numbers of past successful and failed runs to keep track of are controlled by `.spec.successfulRunHistoryLimit` and `.spec.failedRunHistoryLimit`, respectively. In the example above, 3 past successful runs and 3 past failed runs are tracked.

## Restart Policy

Certain restart policies specified in `.spec.template.restartPolicy` may not work well with the schedule and concurrency policy. For example:

- A restart policy of `Always` should never be used with a `ScheduledSparkApplication`.
- A restart policy of `OnFailure` may not be a good choice since the next scheduled run usually continues where the previous run left off.

For these reasons, it's often best to use a restart policy of `Never`, as shown in the example above.

## Updating a ScheduledSparkApplication

When the spec of a `ScheduledSparkApplication` is updated, existing SparkApplications that have already been created will not be updated or deleted. The changes only affect subsequently scheduled SparkApplications.

This behavior ensures that running applications are not disrupted by spec changes. If you need to update a currently running SparkApplication, you must manually delete and recreate it or update it directly.

## Deleting a ScheduledSparkApplication

Deleting a `ScheduledSparkApplication` will cascade delete all the `SparkApplication` objects that were created by that schedule. This means all historical and currently running Spark applications associated with the schedule will be deleted. If you want to preserve the Spark applications that have already been scheduled, make sure to update the `ScheduledSparkApplication` with `.spec.suspend=true` instead of deleting it.
