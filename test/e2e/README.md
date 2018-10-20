# E2E Testing

End-to-end (e2e) testing is automated testing for real user scenarios.

## Build and run test

Prerequisites:
- A running k8s cluster and kube config. We will need to pass kube config as arguments.
- Have kubeconfig file ready.
- Have Spark operator image ready.

e2e tests are written as Go test. All go test techniques apply (e.g. picking what to run, timeout length). Let's say I want to run all tests in "test/e2e/":

```bash
$ go test -v ./test/e2e/ --kubeconfig "$HOME/.kube/config" --operator-image=gcr.io/spark-operator/spark-operator:v2.3.1-v1alpha1-latest
```
