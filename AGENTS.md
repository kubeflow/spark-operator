# AGENTS.md

## Who This Is For

- **AI agents**: Automate repository tasks with minimal context
- **Contributors**: Humans using AI assistants or working directly
- **Maintainers**: Ensure assistants follow project conventions and CI rules

## Agent Behavior Policy

AI agents should:

- Make atomic, minimal, and reversible changes.
- Run the same `make` targets CI runs (see [Commands](#commands)) before proposing commits.
- NEVER modify CI/CD workflows (`.github/`) or release automation unless explicitly requested.
- Use `AGENTS.md` and `Makefile` as the source of truth for development commands.

Agents must NOT:

- Bypass tests or linters
- Hand-edit generated files (see [Generated Files](#generated-files))
- Introduce Go dependencies without running `go mod tidy` in both the root and `test/e2e` modules
- Modify CRD schemas or API versions without explicit instruction

### Context Awareness

Before writing code, agents should:

- Read existing code patterns, comments, and neighboring `*_test.go` files for alignment
- Preserve existing logging and error-handling conventions
- Review the API types in `api/` before changing CRD structures
- Call out any breaking change to the `v1beta2` API

For additional context see [the Spark Operator docs](https://spark.kubeflow.org/en/latest/).

## Repository Map

```
kubeflow/spark-operator/
├── .github/                 # GitHub Actions workflows (CI, release, docs)
├── api/                     # CRD API types (Go) and generated artifacts
│   ├── v1beta2/               # SparkApplication, ScheduledSparkApplication (stable API)
│   ├── v1alpha1/              # SparkConnect
│   ├── openapi-spec/          # Generated OpenAPI spec
│   └── python_api/            # Generated Python API models
├── charts/spark-operator-chart/  # Helm chart (templates, values.yaml, crds/, tests/)
├── cmd/operator/            # Operator binary: `controller`, `webhook`, `version` subcommands
├── config/                  # Kustomize manifests; generated CRDs in config/crd/bases
├── docker/                  # Auxiliary Dockerfiles
├── docs/                    # API reference and documentation website sources
├── examples/                # Example SparkApplication manifests
├── hack/                    # Code generation, API docs, and helper scripts
├── internal/                # Operator logic (not importable by other modules)
│   ├── controller/            # Controllers for SparkApplication, ScheduledSparkApplication, SparkConnect, webhook configurations
│   ├── metrics/               # Prometheus metrics
│   ├── scheduler/             # Batch scheduler integrations (Volcano, YuniKorn, kube-scheduler)
│   └── webhook/               # Admission webhook logic
├── pkg/                     # Shared packages (client, common, util, features, certificate, ...)
├── proposals/               # Design proposals
├── spark-docker/            # Spark image variant with GCS access and Prometheus metrics
└── test/
    ├── e2e/                   # End-to-end tests (separate Go module, run on Kind)
    ├── drift/                 # Helm chart vs Kustomize drift test
    └── kustomize/             # Kustomize build test
```

Unit tests live next to the code they test as `*_test.go` files.

## Environment & Tooling

- **Go**: version from `go.mod`; primary language for the operator
- **Build**: `make`, `go build`, `docker`
- **Lint/format**: `golangci-lint` (pinned in `Makefile`, config in `.golangci.yaml`), `go fmt`, `go vet`
- **Tests**: `go test` with envtest (unit), Ginkgo on Kind (e2e), `helm unittest` (chart)
- **Code generation**: `controller-gen`, `code-generator`, `openapi-generator` (runs in a container)
- **Pre-commit**: `helm-docs`, `shfmt`, `shellcheck` hooks in `.pre-commit-config.yaml`

Tools are downloaded on demand into `bin/` by the `make` targets.

## Commands

### Build

```bash
make build-operator           # Build the operator binary into bin/
make docker-build             # Build the operator image
```

### Testing

```bash
make unit-test                # Go unit tests (envtest: local API server, no real cluster)
make helm-unittest            # Helm chart unit tests
make e2e-test                 # End-to-end tests on a Kind cluster
make e2e-test DEPLOY_METHOD=kustomize   # e2e with the Kustomize install (CI runs helm and kustomize)

# Targeted tests (envtest binaries must be downloaded first)
make setup-envtest
go test ./internal/controller/sparkapplication/...
```

### Lint/format (run before every commit; CI fails on any diff)

```bash
make go-fmt                   # go fmt (root and test/e2e)
make go-vet                   # go vet (root and test/e2e)
make go-lint                  # golangci-lint (root and test/e2e)
go mod tidy && go -C test/e2e mod tidy
```

### Code generation (run after changing anything in `api/`)

```bash
make manifests                # Regenerate CRD, RBAC and webhook manifests into config/
make generate                 # Regenerate deepcopy code and Python API models
make update-crd               # Copy regenerated CRDs into the Helm chart
make build-api-docs           # Regenerate docs/api-docs.md
./hack/update-codegen.sh      # Regenerate pkg/client (clientset, listers, informers)
make verify-codegen           # Check pkg/client is up to date
make detect-crds-drift        # Check Helm chart CRDs match config/crd/bases
```

`make generate` runs `openapi-generator-cli` in a container (`CONTAINER_TOOL`, default `docker`), so it needs a container runtime.

### Helm chart

```bash
make helm-lint                # Chart lint (runs in a container)
make helm-docs                # Regenerate chart README.md from README.md.gotmpl
make drift-check              # Detect drift between Helm chart and Kustomize manifests
```

### Other CI checks

```bash
make shell-fmt                # Format shell scripts (CI fails on diff)
make shell-lint               # shellcheck
make kustomize-lint           # Validate Kustomize build output
make docs-test                # Build the docs website strictly (warnings are errors)
```

## Generated Files

Never edit these by hand; change the source and regenerate:

| Generated file(s) | Source | Command |
| --- | --- | --- |
| `config/crd/bases/*.yaml` | Go types in `api/` | `make manifests` |
| `charts/spark-operator-chart/crds/*.yaml` | `config/crd/bases/` | `make update-crd` |
| `api/**/zz_generated.deepcopy.go` | Go types in `api/` | `make generate` |
| `api/**/zz_generated.openapi.go`, `api/openapi-spec/swagger.json` | Go types in `api/` | `make generate` (via `hack/openapi/gen-openapi.sh`) |
| `api/python_api/` | `api/openapi-spec/swagger.json` | `make generate` |
| `pkg/client/**` | Go types in `api/` | `hack/update-codegen.sh` (checked by `make verify-codegen`) |
| `docs/api-docs.md` | Go types in `api/` | `make build-api-docs` |
| `charts/spark-operator-chart/README.md` | `README.md.gotmpl`, `values.yaml` | `make helm-docs` |

CI regenerates these files (or, for the chart CRDs, diffs them with `make detect-crds-drift`) and fails if the result differs from what is committed.

## Development Workflow for AI Agents

**Before making changes**:

1. Read existing code patterns, comments, and tests for alignment
2. Check which generated files your change affects (see [Generated Files](#generated-files))

**Before proposing changes**, run the same checks as CI (`.github/workflows/`):

1. `make go-fmt go-vet go-lint`
2. `go mod tidy && go -C test/e2e mod tidy`
3. If `api/` changed: `./hack/update-codegen.sh && make generate update-crd build-api-docs verify-codegen detect-crds-drift`
4. `make unit-test`
5. If the Helm chart changed: `make helm-unittest helm-docs drift-check`
6. Depending on what else changed:
   - Shell scripts changed: `make shell-fmt shell-lint`
   - `config/` changed: `make kustomize-lint drift-check`
   - `internal/` changed: `make drift-check`
   - `docs/website/` changed: `make docs-test`

**Commit/PR hygiene**:

- Sign off every commit: `git commit -s` (DCO check)
- Use Conventional Commits in titles, e.g. `fix(webhook): ...`, `feat(controller): ...`, `docs: ...`
- Fill in `.github/PULL_REQUEST_TEMPLATE.md` and include the rationale ("why")
- Do not push secrets or change git config
- Scope discipline: only modify files relevant to the task; keep diffs minimal
