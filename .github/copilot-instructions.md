# GitHub Copilot Code Review Instructions

## Review Philosophy

- Only comment when you have HIGH CONFIDENCE (>80%) that an issue exists
- Be concise: one sentence per comment when possible
- Focus on actionable feedback, not observations
- When reviewing text, only comment on clarity issues if the text is genuinely confusing or could lead to errors.
  "Could be clearer" is not the same as "is confusing" - stay silent unless HIGH confidence it will cause problems

## Priority Areas (Review These)

### Security & Safety

- Command injection risks (shell commands, user input, workflow `${{ }}` expressions in `run:` steps)
- Path traversal vulnerabilities
- Credential exposure or hardcoded secrets
- RBAC rules in the Helm chart or `config/` that grant more than the operator needs
- Missing input validation on external data
- Improper error handling that could leak sensitive info

### Correctness Issues

- Logic errors that could cause panics or incorrect behavior
- Resource leaks (goroutines, watches, connections, Kubernetes objects left behind)
- Off-by-one errors or boundary conditions
- Errors that are dropped, or reconcile paths that return success after a failure
- Status written with `Update` instead of `Status().Update`, or written on every reconcile
- Optional fields that don't need to be optional, or pointers where the zero value is fine
- Overly defensive code that adds unnecessary checks
- Unnecessary comments that just restate what the code already shows (remove them)

### Architecture & Patterns

- Code that violates existing patterns in the codebase
- Missing error handling
- Code that is not following [Effective Go](https://go.dev/doc/effective_go)
- Violating [Kubernetes API guidelines](https://github.com/kubernetes/community/blob/master/contributors/devel/sig-architecture/api-conventions.md),
  or breaking compatibility of the `v1beta2` API
- Helm chart changes without matching Kustomize changes in `config/` (or the reverse)

## Project-Specific Context

- See [AGENTS.md](../AGENTS.md) in the root directory for project guidelines, generated files and the comment policy.

## CI Pipeline Context

**Important**: You review PRs immediately, before CI completes. Do not flag issues that CI will catch.

### What Our CI Checks

- `.github/workflows/integration.yaml` - `go mod tidy`, code generation, `go fmt`, `go vet`, golangci-lint, API docs,
  unit tests, Helm lint, unit tests and docs, and e2e tests on Kind (Helm and Kustomize installs)
- `.github/workflows/kustomize-drift-check.yaml` - drift between the Helm chart and Kustomize manifests
- `.github/workflows/kustomize-lint.yaml` - Kustomize build validation
- `.github/workflows/shell-lint.yaml` - shfmt and shellcheck
- `.github/workflows/code-quality-check.yaml` - license boilerplate headers
- `.github/workflows/docs.yaml` - documentation website build and link check
- `.github/workflows/check-release.yaml` - release version and changelog consistency

## Skip These (IMPORTANT)

Do not comment on:

- **Auto generated code** - CI handles this (`make generate`, `make manifests`, `make helm-docs`)
- **Style/formatting** - CI handles this (gofmt, golangci-lint, shfmt)
- **Test failures** - CI handles this (full test suite)
- **Missing dependencies** - CI handles this (`go mod tidy`)

## Response Format

When you identify an issue:

1. **State the problem** (1 sentence)
2. **Why it matters** (1 sentence, only if not obvious)
3. **Suggested fix** (code snippet or specific action)

## When to Stay Silent

If you're uncertain whether something is an issue, don't comment. False positives create noise and reduce trust in the review process.
