# Sharded Manager Product Example

This example demonstrates how to register a custom manager product for the Spark Operator that
integrates with [kubernetes-controller-sharding](https://github.com/timebertt/kubernetes-controller-sharding).
Once compiled into a sidecar or linked into a custom build, you can activate it with:

```
spark-operator start --manager-product=sharded
```

## Using the example

1. From the root of the repository, set up a Go workspace that points the example module at the
   local operator sources:

   ```bash
   cd examples/manager-products/sharded
   go mod edit -replace github.com/kubeflow/spark-operator/v2=../../..
   ```

2. Build the module (for example as a shared library or as part of a binary that runs alongside the
   operator).

3. Ensure the resulting binary is on the operator's `import` path or executed before the operator
   starts so that the `init()` function registers the product.

4. Start the operator with `--manager-product=sharded` (or, via Helm, set
   `controller.managerProduct=sharded`).

The registration code lives in `sharded_product.go` and can be adapted for other sharding strategies
or manager customizations.
