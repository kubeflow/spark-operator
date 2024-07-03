# Releasing the Spark Operator

## Prerequisite

1. Permissions
   - You need write permissions on the repository to create a release tag/branch.
2. Prepare your Github Token

### Release Process

1. Make sure the last commit you want to release passed `spark-operator -postsubmit` testing.

2. Check out that commit.

3. Depends on what version you want to release,

   - Major or Minor version - Use the GitHub UI to cut a release branch and name the release branch `v{MAJOR}.${MINOR}-branch`
   - Patch version - You don't need to cut release branch.

4. Create a new PR against the release branch to change container image in manifest to point to that commit hash.

   ```
   images:
   - name: kubeflow/spark-operator 
     newName: kubeflow/spark-operator
     newTag: ${commit_hash}
   ```

   > note: post submit job will always build a new image using the `PULL_BASE_HASH` as image tag.

5. Create a tag and push tag to upstream.

   ```
   git tag v1.2.0
   git push upstream v1.2.0
   ```

6. Update the Changelog by running:

   ```
   python docs/release/changelog.py --token=<github-token> --range=<previous-release>..<current-release>
   ```

   If you are creating the **first minor pre-release** or the **minor** release (`X.Y`), your
   `previous-release` is equal to the latest release on the `vX.Y-branch` branch.
   For example: `--range=v1.7.1..v1.8.0`

   Otherwise, your `previous-release` is equal to the latest release on the `vX.Y-branch` branch.
   For example: `--range=v1.7.0..v1.8.0-rc.0`

   Group PRs in the Changelog into Features, Bug fixes, Documentation, etc.

   Finally, submit a PR with the updated Changelog.
