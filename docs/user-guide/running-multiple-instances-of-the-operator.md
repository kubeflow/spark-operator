# Running Multiple Instances Of The Operator Within The Same K8s Cluster

If you need to run multiple instances of the operator within the same k8s cluster. Therefore, you need to make sure that the running instances should not compete for the same custom resources or pods. You can achieve this:

Either:

- By specifying a different `namespace` flag for each instance of the operator.

Or if you want your operator to watch specific resources that may exist in different namespaces:

- You need to add custom labels on resources by defining for each instance of the operator a different set of labels in `-label-selector-filter (e.g. env=dev,app-type=spark)`.
- Run different `webhook` instances by specifying different `-webhook-config-name` flag for each deployment of the operator.
- Specify different `webhook-svc-name` and/or `webhook-svc-namespace` for each instance of the operator.
- Edit the job that generates the certificates `webhook-init` by specifying the namespace and the service name of each instance of the operator, `e.g. command: ["/usr/bin/gencerts.sh", "-n", "ns-op1", "-s", "spark-op1-webhook", "-p"]`. Where `spark-op1-webhook` should match what you have specified in `webhook-svc-name`. For instance, if you use the following [helm chart](https://github.com/helm/charts/tree/master/incubator/sparkoperator) to deploy the operator you may specify for each instance of the operator a different `--namespace` and `--name-template` arguments to make sure you generate a different certificate for each instance, e.g:

```shell
helm install spark-op1 incubator/sparkoperator --namespace ns-op1
helm install spark-op2 incubator/sparkoperator --namespace ns-op2
```

Will run 2 `webhook-init` jobs. Each job executes respectively the command:

```yaml
command: ["/usr/bin/gencerts.sh", "-n", "ns-op1", "-s", "spark-op1-webhook", "-p"`]
command: ["/usr/bin/gencerts.sh", "-n", "ns-op2", "-s", "spark-op2-webhook", "-p"`]
```

- Although resources are already filtered with respect to the specified labels on resources. You may also specify different labels in `-webhook-namespace-selector` and attach these labels to the namespaces on which you want the webhook to listen to.
