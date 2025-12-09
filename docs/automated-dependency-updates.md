# Automated Dependency Update Workflow

This document describes the automated workflow for updating the spark-base image version in the spark-on-k8s-operator repository.

## Overview

The workflow consists of three main components:

1. **Update Spark Base Version** (`update_spark_base.yml`) - Receives updates from the spark-base repository and creates a PR
2. **Auto Release on Merge** (`auto_release_on_merge.yml`) - Creates a release after the PR is merged
3. **Trigger to spark-operator** - Notifies the spark-operator repository to update its image references

## Workflow Details

### 1. Update Spark Base Version Workflow

**Trigger:** 
- `repository_dispatch` event from spark-base repository
- Manual trigger via `workflow_dispatch` with version input

**Actions:**
- Receives the new spark-base version (e.g., `v1.9.18`)
- Updates the `SPARK_IMAGE` build argument in:
  - `.github/workflows/relativity_main.yml`
  - `.github/workflows/relativity_release.yaml`
- Creates a Pull Request with the changes to the `relativity-main` branch
- PR title format: "Update spark-base version to vX.Y.Z"

**Usage:**
```bash
# Manual trigger via GitHub UI or API
curl -X POST \
  -H "Accept: application/vnd.github.v3+json" \
  -H "Authorization: token ${GITHUB_TOKEN}" \
  https://api.github.com/repos/relativityone/spark-on-k8s-operator/actions/workflows/update_spark_base.yml/dispatches \
  -d '{"ref":"relativity-main","inputs":{"spark_base_version":"v1.9.18"}}'
```

### 2. Auto Release on Merge Workflow

**Trigger:**
- Pull Request closed event on `relativity-main` branch

**Actions:**
- Retrieves the latest tag version
- Increments the patch version (e.g., `v1.1.67` → `v1.1.68`)
- Creates and pushes a new annotated tag
- Creates a GitHub release with auto-generated release notes
- Triggers a repository dispatch event to the spark-operator repository to update the operator image

**Version Management:**
The workflow automatically increments the patch version. For manual version control or major/minor version changes, create tags manually.

### 3. Integration with spark-operator Repository

After a successful release, the workflow automatically triggers the `update-operator-image` workflow in the spark-operator repository:

```json
{
  "event_type": "update-operator-image",
  "client_payload": {
    "spark_on_k8s_version": "v1.1.68"
  }
}
```

**Setup Required:** (Current PAT expires on 9th Dec 2026)
To enable automatic triggering of the spark-operator repository, add a GitHub Personal Access Token as a secret:

1. Create a PAT with `repo` scope for the spark-operator repository
2. Ensure the PAT owner has write access to the `relativityone/spark-operator` repository
3. Add it as a secret named `SPARK_OPERATOR_DISPATCH_TOKEN` in this repository
4. The workflow will automatically trigger the `update-operator-image` workflow in the spark-operator repository

## Integration with spark-base Repository

For the spark-base repository to trigger this workflow, it should dispatch an event after creating a release:

```yaml
# In spark-base repository's release workflow
- name: Trigger spark-on-k8s-operator update
  run: |
    curl -X POST \
      -H "Accept: application/vnd.github.v3+json" \
      -H "Authorization: token ${{ secrets.SPARK_ON_K8S_OPERATOR_DISPATCH_TOKEN }}" \
      https://api.github.com/repos/relativityone/spark-on-k8s-operator/dispatches \
      -d '{
        "event_type": "spark-base-updated",
        "client_payload": {
          "version": "${{ steps.get-version.outputs.version }}"
        }
      }'
```

## Workflow Diagram

```
spark-base repository
    |
    | (creates release v1.9.18)
    |
    v
[Dispatch Event] ──> update_spark_base.yml
                            |
                            | (creates PR)
                            v
                     Pull Request Created
                            |
                            | (manual review & merge)
                            v
                     auto_release_on_merge.yml
                            |
                            | (creates release v1.1.68)
                            v
                     GitHub Release Created
                            |
                            | (dispatch: update-operator-image)
                            v
                     spark-operator repository
                            |
                            v
                     (updates operator image to v1.1.68)
```

## Testing

### Test the Update Workflow Manually

1. Go to Actions tab in GitHub
2. Select "Update Spark Base Version"
3. Click "Run workflow"
4. Enter a version (e.g., `v1.9.18`)
5. Verify the PR is created correctly

### Test the Release Workflow

1. Merge any PR to the `relativity-main` branch
2. Verify a new tag and release are created with incremented patch version
3. Check the Actions tab for workflow execution logs
4. Verify the spark-operator repository receives the dispatch event

## Troubleshooting

### PR Not Created
- Check the workflow logs in the Actions tab
- Verify the version format is correct (e.g., `v1.9.18`)
- Ensure the branch `relativity-main` exists and is accessible

### Release Not Created
- Check that the PR was merged (not just closed)
- Review the workflow permissions (needs write access to tags and releases)
- Verify Git configuration is set correctly (user.name and user.email)

### spark-operator Not Triggered
- Ensure `SPARK_OPERATOR_DISPATCH_TOKEN` secret is configured
- Verify the PAT has `repo` scope and the token owner has write access to `relativityone/spark-operator`
- Check the workflow logs for API response codes (403/404 indicate permission issues)
- Verify the target repository has a workflow that listens for `update-operator-image` repository_dispatch events

## Security Considerations

- The `GITHUB_TOKEN` used in workflows has limited permissions
- Use separate PATs for cross-repository communication
- Review PRs before merging to prevent malicious version updates
- Consider requiring code owner approval for version update PRs
