# E2E Testing

End-to-end (e2e) testing is automated testing for real user scenarios.

## Build and Run Tests

Prerequisites:
- A running k8s cluster and kube config. We will need to pass kube config as arguments.
- Have kubeconfig file ready.
- Have a Kubernetes Operator for Spark image ready.

e2e tests are written as Go test. All go test techniques apply (e.g. picking what to run, timeout length). Let's say I want to run all tests in "test/e2e/":

```bash
$ docker build -t gcr.io/spark-operator/spark-operator:local .
$ go test -v ./test/e2e/ --kubeconfig "$HOME/.kube/config" --operator-image=gcr.io/spark-operator/spark-operator:local
```

### Available Tests

Note that all tests are run on a live Kubernetes cluster. After the tests are done, the Spark Operator deployment and associated resources (e.g. ClusterRole and ClusterRoleBinding) are deleted from the cluster.

* `basic_test.go`

  This test submits `spark-pi.yaml` contained in `\examples`. It then checks that the Spark job successfully completes with the correct result of Pi.
  
* `volume_mount_test.go`

  This test submits `spark-pi-configmap.yaml` contained in `\examples`. It verifies that a dummy ConfigMap can be mounted in the Spark pods.

* `lifecycle_test.go`

  This test submits `spark-pi.yaml` contained in `\examples`. It verifies that the created SparkApplication CRD object goes through the correct series of states as dictated by the controller. Once the job is finished, an update operation is performed on the CRD object to trigger a re-run. The transition from a completed job to a new running job is verified for correctness.
