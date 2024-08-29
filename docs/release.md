# Releasing the Spark operator

## Prerequisites

- [Write](https://docs.github.com/organizations/managing-access-to-your-organizations-repositories/repository-permission-levels-for-an-organization#permission-levels-for-repositories-owned-by-an-organization) permission for the Spark operator repository.

- Create a [GitHub Token](https://docs.github.com/github/authenticating-to-github/keeping-your-account-and-data-secure/creating-a-personal-access-token).

- Install `PyGithub` to generate the [Changelog](../CHANGELOG.md):

  ```bash
  pip install PyGithub==2.3.0
  ```

## Versioning policy

Spark Operator version format follows [Semantic Versioning](https://semver.org/). Spark Operator versions are in the format of `vX.Y.Z`, where `X` is the major version, `Y` is the minor version, and `Z` is the patch version. The patch version contains only bug fixes.

Additionally, Spark Operator does pre-releases in this format: `vX.Y.Z-rc.N` where `N` is a number of the `Nth` release candidate (RC) before an upcoming public release named `vX.Y.Z`.

## Release branches and tags

Spark Operator releases are tagged with tags like `vX.Y.Z`, for example `v1.7.2`.

Release branches are in the format of `release-X.Y`, where `X.Y` stands for the minor release.

`vX.Y.Z` releases are released from the `release-X.Y` branch. For example, `v1.7.2` release should be on `release-1.7` branch.

If you want to push changes to the `release-X.Y` release branch, you have to cherry pick your changes from the `master` branch and submit a PR.

## Create a new release

### Create release branch

1. Depends on what version you want to release,

   - Major or Minor version - Use the GitHub UI to create a release branch from `master` and name the release branch `release-X.Y`.
   - Patch version - You don't need to create a new release branch.

2. Fetch the upstream changes into your local directory:

   ```bash
   git fetch upstream
   ```

3. Checkout into the release branch:

   ```bash
   git checkout release-X.Y
   git rebase upstream/release-X.Y
   ```

### Create GitHub tag

1. Modify `VERSION` file in the root directory of the project:

    - For the RC tag as follows:

    ```bash
    vX.Y.Z-rc.N
    ```

    - For the official release tag as follows:

    ```bash
    vX.Y.Z
    ```

2. Modify `version` and `appVersion` in `Chart.yaml`:

    ```bash
    # Get version and remove the leading 'v'
    VERSION=$(cat VERSION | sed "s/^v//")

    # Change the version and appVersion in Chart.yaml
    # On Linux
    sed -i "s/^version.*/version: ${VERSION}/" charts/spark-operator-chart/Chart.yaml
    sed -i "s/^appVersion.*/appVersion: ${VERSION}/" charts/spark-operator-chart/Chart.yaml

    # On MacOS
    sed -i '' "s/^version.*/version: ${VERSION}/" charts/spark-operator-chart/Chart.yaml
    sed -i '' "s/^appVersion.*/appVersion: ${VERSION}/" charts/spark-operator-chart/Chart.yaml
    ```

3. Update the Helm chart README:

    ```bash
    make helm-docs
    ```

4. Commit the changes:

    ```bash
    git add VERSION
    git add charts/spark-operator-chart/Chart.yaml
    git add charts/spark-operator-chart/README.md
    git commit -s -m "Spark Operator Official Release v${VERSION}"
    git push origin release-X.Y
    ```

5. Submit a PR to the release branch.

### Release Spark Operator Image

After `VERSION` file is modified and pushed to the release branch, a release workflow will be triggered to build and push Spark operator docker images to Docker Hub.

### Publish release

After `VERSION` file is modified and pushed to the release branch, a release workflow will be triggered to create a new draft release with the Spark operator Helm chart packaged as an artifact. After modifying the release notes, then publish the release.

### Release Spark Operator Helm Chart

After the draft release is published, a release workflow will be triggered to update the Helm chart repo index and publish it to the Helm repository.

## Update Changelog

Update the `CHANGELOG.md` file by running:

```bash
python hack/generate-changelog.py \
    --token=<github-token> \
    --range=<previous-release>..<current-release>
```

If you are creating the **first minor pre-release** or the **minor** release (`X.Y`), your `previous-release` is equal to the latest release on the `release-X.Y` branch.
For example: `--range=v1.7.1..v1.8.0`.

Otherwise, your `previous-release` is equal to the latest release on the `release-X.Y` branch.
For example: `--range=v1.7.0..v1.8.0-rc.0`

Group PRs in the Changelog into Features, Bug fixes, Documentation, etc.

Finally, submit a PR with the updated Changelog.
