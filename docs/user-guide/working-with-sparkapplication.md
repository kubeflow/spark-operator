# Working with SparkApplications

A `SparkApplication` can be created from a YAML file storing the `SparkApplication` specification using either the `kubectl apply -f <YAML file path>` command or the `sparkctl create <YAML file path>` command. Please refer to the `sparkctl` [README](../../sparkctl/README.md#create) for usage of the `sparkctl create` command. Once a `SparkApplication` is successfully created, the operator will receive it and submits the application as configured in the specification to run on the Kubernetes cluster. Please note, that `SparkOperator` submits `SparkApplication` in `Cluster` mode only. 

## Deleting a SparkApplication

A `SparkApplication` can be deleted using either the `kubectl delete <name>` command or the `sparkctl delete <name>` command. Please refer to the `sparkctl` [README](../../sparkctl/README.md#delete) for usage of the `sparkctl delete`
command. Deleting a `SparkApplication` deletes the Spark application associated with it. If the application is running when the deletion happens, the application is killed and all Kubernetes resources associated with the application are deleted or garbage collected.

## Updating a SparkApplication

A `SparkApplication` can be updated using the `kubectl apply -f <updated YAML file>` command. When a `SparkApplication`  is successfully updated, the operator will receive both the updated and old `SparkApplication` objects. If the specification of the `SparkApplication` has changed, the operator submits the application to run, using the updated specification. If the application is currently running, the operator kills the running application before submitting a new run with the updated specification. There is planned work to enhance the way `SparkApplication` updates are handled. For example, if the change was to increase the number of executor instances, instead of killing the currently running application and starting a new run, it is a much better user experience to incrementally launch the additional executor pods.

## Checking a SparkApplication

A `SparkApplication` can be checked using the `kubectl describe sparkapplications <name>` command. The output of the command shows the specification and status of the `SparkApplication` as well as events associated with it. The events communicate the overall process and errors of the `SparkApplication`.

## Configuring Automatic Application Restart and Failure Handling

The operator supports automatic application restart with a configurable `RestartPolicy` using the optional field
`.spec.restartPolicy`. The following is an example of a sample `RestartPolicy`:

```yaml
  restartPolicy:
     type: OnFailure
     onFailureRetries: 3
     onFailureRetryInterval: 10
     onSubmissionFailureRetries: 5
     onSubmissionFailureRetryInterval: 20
```

The valid types of restartPolicy include `Never`, `OnFailure`, and `Always`. Upon termination of an application,
the operator determines if the application is subject to restart based on its termination state and the
`RestartPolicy` in the specification. If the application is subject to restart, the operator restarts it by
submitting a new run of it. For `OnFailure`, the Operator further supports setting limits on number of retries
via the `onFailureRetries` and `onSubmissionFailureRetries` fields. Additionally, if the  submission retries has not been reached,
the operator retries submitting the application using a linear backoff with the interval specified by
`onFailureRetryInterval` and `onSubmissionFailureRetryInterval` which are required for both `OnFailure` and `Always` `RestartPolicy`.
The old resources like driver pod, ui service/ingress etc. are deleted if it still exists before submitting the new run, and a new  driver pod is created by the submission
client so effectively the driver gets restarted.

## Setting TTL for a SparkApplication

The `v1beta2` version of the `SparkApplication` API starts having TTL support for `SparkApplication`s through a new optional field named `.spec.timeToLiveSeconds`, which if set, defines the Time-To-Live (TTL) duration in seconds for a SparkApplication after its termination. The `SparkApplication` object will be garbage collected if the current time is more than the `.spec.timeToLiveSeconds` since its termination. The example below illustrates how to use the field:

```yaml
spec:
  timeToLiveSeconds: 3600
```

Note that this feature requires that informer cache resync to be enabled, which is true by default with a resync internal of 30 seconds. You can change the resync interval by setting the flag `-resync-interval=<interval>`.
