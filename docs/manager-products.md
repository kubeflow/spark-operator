# Manager Product Extensions

The operator exposes a manager-level extension point that allows integrators to swap out the
`controller-runtime` manager startup sequence without modifying the core binary. The operator keeps
its built-in factory unchanged and delegates to a configurable **manager product** – the concrete
implementation ("worker") responsible for instantiating the `ctrl.Manager`.

By default, the factory uses the bundled product that simply calls `controller-runtime` with the
options returned from `newManagerOptions()`.

## Selecting products through configuration

The operator CLI now accepts the `--manager-product` flag (and corresponding environment/config key
`manager-product`). When left unset or set to `default`, the operator behaves exactly as it always
has. Setting the flag to a different value will switch to a registered product implementation. Use
`--manager-product-fallback` (default `default`) to specify a backup product if the primary choice
fails to load.

```bash
spark-operator start --manager-product=sharded
```

Programmatic overrides via `controller.SetDefaultManagerProduct` are also available; these take
precedence over flag-based configuration.

## Registering a custom product

External modules can register additional products during their `init()` logic. Every product must
implement the `controller.ManagerProduct` interface:

```go
package myintegration

import (
    sparkcontroller "github.com/kubeflow/spark-operator/v2/cmd/operator/controller"
)

func init() {
    sparkcontroller.RegisterManagerProduct("my-product", myProduct{})
}

type myProduct struct{}

func (myProduct) CreateManager(cfg *rest.Config, opts ctrl.Options) (ctrl.Manager, error) {
    // build or modify opts as needed before creating the manager
    return ctrl.NewManager(cfg, opts)
}
```

Once registered, a user can select the product through the `--manager-product` flag without needing
any further changes to the operator binary.

> A ready-to-use reference implementation can be found in
> [`examples/manager-products/sharded`](../examples/manager-products/sharded), which wires the
> operator to `kubernetes-controller-sharding`.

## Example: controller sharding via shard leases

The following example integrates
[`kubernetes-controller-sharding`](https://github.com/timebertt/kubernetes-controller-sharding) to
lease shards using controller-runtime's leader election infrastructure. The product tweaks the
manager options before instantiation and registers itself under the name `sharded`.

> **Note:** This file is provided for illustration only; it should live in a separate module or Go
> plugin that imports the operator package and is built alongside it.

```go
package shardedproduct

import (
    shardlease "github.com/timebertt/kubernetes-controller-sharding/pkg/shard/lease"
    "k8s.io/client-go/rest"
    ctrl "sigs.k8s.io/controller-runtime"
    sparkcontroller "github.com/kubeflow/spark-operator/v2/cmd/operator/controller"
)

func init() {
    sparkcontroller.RegisterManagerProduct("sharded", shardedProduct{})
}

type shardedProduct struct{}

func (shardedProduct) CreateManager(cfg *rest.Config, opts ctrl.Options) (ctrl.Manager, error) {
    shardLease, err := shardlease.NewResourceLock(cfg, shardlease.Options{
        ControllerRingName: "my-controllerring",
    })
    if err != nil {
        return nil, err
    }

    opts.LeaderElection = true
    opts.LeaderElectionResourceLockInterface = shardLease
    opts.LeaderElectionReleaseOnCancel = true

    // additional option customisation …

    return ctrl.NewManager(cfg, opts)
}
```

With the product registered, running the operator with `--manager-product=sharded` will select the
sharded manager implementation.

## Observability

The operator logs which product is active at start-up (and, at debug level, the registered product list):

```
INFO	registered manager products	{"products": ["default", "sharded"]}
INFO	using manager product	{"product": "sharded"}
```

Any errors during product selection cause the operator to exit early, making misconfiguration
obvious during deployment.
