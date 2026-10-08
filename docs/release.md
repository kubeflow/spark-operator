# Releasing the Spark operator

Spark Operator releases are automated: a maintainer opens a single
"release PR", and merging it runs the [release workflow](../.github/workflows/release.yaml),
which publishes every artifact.

## Prerequisites

- [Write](https://docs.github.com/organizations/managing-access-to-your-organizations-repositories/repository-permission-levels-for-an-organization#permission-levels-for-repositories-owned-by-an-organization)
  permission for the Spark operator repository, and membership in the reviewers of the
  `release` GitHub environment (to approve the release run).
- For final (non-RC) releases, a [GitHub token](https://docs.github.com/github/authenticating-to-github/keeping-your-account-and-data-secure/creating-a-personal-access-token)
  and `PyGithub` to generate the [changelog](../CHANGELOG.md):

```bash
  pip install PyGithub==2.3.0
```

- A git remote named `upstream` pointing at `kubeflow/spark-operator` (falls back to `origin`).

No PyPI token is needed: the Python API package is published with
[PyPI trusted publishing](https://docs.pypi.org/trusted-publishers/).

## Versioning policy

Spark Operator version format follows [Semantic Versioning](https://semver.org/). Spark Operator
versions are in the format of `vX.Y.Z`, where `X` is the major version, `Y` is the minor version,
and `Z` is the patch version. The patch version contains only bug fixes.

Additionally, Spark Operator does pre-releases in this format: `vX.Y.Z-rc.N` where `N` is a number
of the `Nth` release candidate (RC) before an upcoming public release named `vX.Y.Z`.

One version is used for every artifact of a release:

| Artifact | Version for `v2.6.0-rc.0` |
| --- | --- |
| Git tag and GitHub release | `v2.6.0-rc.0` |
| Container images `ghcr.io/kubeflow/spark-operator/{controller,kubectl}` | `2.6.0-rc.0` |
| Helm chart `version` and `appVersion` | `2.6.0-rc.0` |
| Kustomize image tag in `config/default/kustomization.yaml` | `2.6.0-rc.0` |
| Python API [`kubeflow-spark-api`](https://pypi.org/project/kubeflow-spark-api/) | `2.6.0rc0` (PEP 440) |

Image tags, the chart version and the Kustomize tag carry no leading `v`.

## Release branches and tags

Spark Operator releases are tagged with tags like `vX.Y.Z`, for example `v2.6.0`.

Release branches are in the format of `release-X.Y`, where `X.Y` stands for the minor release.
All `vX.Y.Z` releases, including patch releases, are released from the `release-X.Y` branch.
For example, `v2.6.1` is released from `release-2.6`. Do not create per-patch branches such as
`release-2.6.1`.

The release workflow creates `release-X.Y` from `master` when the first release of a minor
line (normally `vX.Y.0-rc.0`) is merged.

If you want to push changes to the `release-X.Y` branch, cherry-pick them from `master` and
submit a PR against `release-X.Y`. When the next release of the line is merged on `master`, only
the release commit itself is cherry-picked to `release-X.Y`: fixes merged to `master` after the
branch was cut are **not** included unless they were cherry-picked to the release branch.

## Create a release

### 1. Prepare the release PR

Choose the target branch:

- **Latest minor line** (new minor, RC, or patch of the newest minor): work from `master`.
- **Patch of an older minor line** (for example `v2.5.3` while `master` is on `v2.6.x`): work
  from `release-X.Y`.

Then run:

```bash
git fetch upstream --tags
git checkout -b release-vX.Y.Z upstream/master   # or upstream/release-X.Y

# Release candidate (no changelog):
make release VERSION=vX.Y.Z-rc.N

# Final release (generates the CHANGELOG.md section):
make release VERSION=vX.Y.Z GITHUB_TOKEN=<github-token>
```

This will:

1. Update `VERSION` to `vX.Y.Z[-rc.N]`.
1. Update the Helm chart `version` and `appVersion` to `X.Y.Z[-rc.N]`.
1. Update the Kustomize controller image tag to `X.Y.Z[-rc.N]`.
1. Update the Python API package version.
1. Regenerate the Helm chart README (`make helm-docs`).
1. For final releases, prepend a `## [vX.Y.Z]` section to `CHANGELOG.md`, generated from the
   PRs between the previous release and the release branch (or `master` for a new minor line).
   Set `PREVIOUS_VERSION` or `CHANGELOG_HEAD_REF` to override the range.

Group the generated changelog entries into Features, Bug Fixes, Documentation, etc. The section
is used verbatim as the GitHub release notes. Then validate, commit and open the PR:

```bash
make check-release
git add -A && git commit -s -m "Release vX.Y.Z"
git push origin release-vX.Y.Z
```

The [Check Release](../.github/workflows/check-release.yaml) workflow verifies that all versions
agree, that the tag does not exist yet and that final releases have a changelog section.

### 2. Merge and approve

When the PR is merged, the [release workflow](../.github/workflows/release.yaml) starts and waits
for approval on the `release` environment. Once approved, it runs:

1. **Prepare release branch**: creates `release-X.Y` from `master`, or cherry-picks the release
   commit onto the existing `release-X.Y` (when merged to `master`), then re-validates the branch.
1. **Verify**: builds the operator, checks the generated Python API is up to date, runs Helm unit
   tests, the Helm/Kustomize drift check and the Kustomize build validation.
1. **Build Python package** with `uv` and validates it with `twine check`.
1. **Create and push tag**: an annotated `vX.Y.Z` tag on the release branch commit.
1. **Build and publish images**: multi-arch (`linux/amd64`, `linux/arm64`) controller and
   kubectl images, plus operator binary archives extracted and verified from the image.
1. **Publish Helm chart** to `oci://ghcr.io/kubeflow/helm-charts/spark-operator`.
1. **Publish to PyPI** with trusted publishing (approval on the `release` environment again).
1. **Create GitHub release**: a draft with the changelog section as notes (generated notes for
   RCs), the Helm chart archive, the binary archives, the Python package and `SHA256SUMS`, which
   is then published. RCs are marked as pre-releases.
1. **Update Helm repository index** on the `gh-pages` branch so that
   `helm repo add spark-operator https://kubeflow.github.io/spark-operator` serves the release.

Every step is idempotent (existing tags, PyPI files, releases and index entries are detected),
so a failed run can be re-run from the Actions UI. The Helm chart phases can also be re-run on
their own with the [Release Helm charts](../.github/workflows/release-helm-charts.yaml) workflow.

### 3. Verify the release

```bash
VERSION=X.Y.Z
docker buildx imagetools inspect ghcr.io/kubeflow/spark-operator/controller:${VERSION}
helm show chart oci://ghcr.io/kubeflow/helm-charts/spark-operator --version ${VERSION}
helm repo update spark-operator && helm search repo spark-operator/spark-operator --versions --devel | head
pip download --no-deps kubeflow-spark-api==${VERSION/-rc./rc}
kustomize build "github.com/kubeflow/spark-operator/config/default?ref=v${VERSION}" | grep image:
```

## Announcement

Post the release announcement in:

- `#kubeflow-spark-operator` channel in the [CNCF Slack](https://communityinviter.com/apps/cloud-native/cncf)
- [`kubeflow-discuss`](https://groups.google.com/g/kubeflow-discuss) mailing list

Update the Spark Operator version in
[kubeflow/manifests](https://github.com/kubeflow/manifests) for final releases.

## Bump the milestone applier

When a new minor release branch (`release-X.Y`) is cut, update the
[`milestone_applier`](https://github.com/GoogleCloudPlatform/oss-test-infra/blob/master/prow/oss/plugins.yaml)
configuration for `kubeflow/spark-operator` in `GoogleCloudPlatform/oss-test-infra`, so Prow
keeps applying the correct milestone to PRs on each branch:

1. Bump the `master` milestone to the next minor (for example `v2.6` to `v2.7`).
1. Add an entry pinning the new release branch to its milestone (for example `release-2.6: v2.6`).

## Repository settings required by the release workflow

These are one-time settings for repository administrators:

- **`release` environment** with required reviewers (Settings → Environments). Both the
  `prepare` and `publish-pypi` jobs run in it.
- **PyPI trusted publisher** for `kubeflow-spark-api`: owner `kubeflow`, repository
  `spark-operator`, workflow `release.yaml`, environment `release`.
- **Branch protection** for `release-*` must allow `github-actions[bot]` to push the release
  commit and create the branch, and tag protection must allow it to push `v*` tags.
- **Workflow permissions**: GitHub Actions must be allowed to create releases and push to
  `ghcr.io/kubeflow/spark-operator/*` and `ghcr.io/kubeflow/helm-charts/*`.
- Optionally enable **immutable releases**; the workflow attaches all assets while the release is
  still a draft, so it works with immutable releases enabled.
