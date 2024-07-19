import argparse

from github import Github

REPO_NAME = "kubeflow/spark-operator"
CHANGELOG_FILE = "CHANGELOG.md"

parser = argparse.ArgumentParser()
parser.add_argument("--token", type=str, help="GitHub Access Token")
parser.add_argument(
    "--range", type=str, help="Changelog is generated for this release range"
)
args = parser.parse_args()

if args.token is None:
    raise Exception("GitHub Token must be set")
try:
    previous_release = args.range.split("..")[0]
    current_release = args.range.split("..")[1]
except Exception:
    raise Exception("Release range must be set in this format: v1.7.0..v1.8.0")

# Get list of commits from the range.
github_repo = Github(args.token).get_repo(REPO_NAME)
comparison = github_repo.compare(previous_release, current_release)
commits = comparison.commits

# The latest commit contains the release date.
release_date = str(commits[-1].commit.author.date).split(" ")[0]
release_url = "https://github.com/{}/tree/{}".format(REPO_NAME, current_release)

# Get all PRs in reverse chronological order from the commits.
pr_list = ""
pr_set = set()
for commit in commits.reversed:
    # Only add commits with PRs.
    for pr in commit.get_pulls():
        # Each PR is added only one time to the list.
        if pr.number in pr_set:
            continue
        if not pr.merged:
            continue
        pr_set.add(pr.number)

        new_pr = "- {title} ([#{id}]({pr_link}) by [@{user_id}]({user_url}))\n".format(
            title=pr.title,
            id=pr.number,
            pr_link=pr.html_url,
            user_id=pr.user.login,
            user_url=pr.user.html_url,
        )
        pr_list += new_pr

change_log = [
    "\n",
    "## [{}]({}) ({})\n".format(current_release, release_url, release_date),
    "\n",
    pr_list,
    "\n",
    "[Full Changelog]({})\n".format(comparison.html_url),
]

# Update Changelog with the new changes.
with open(CHANGELOG_FILE, "r+") as f:
    lines = f.readlines()
    f.seek(0)
    lines = lines[:1] + change_log + lines[1:]
    f.writelines(lines)

print("Changelog has been updated\n")
print("Group PRs in the Changelog into Features, Bug fixes, Misc, etc.\n")
print("After that, submit a PR with the updated Changelog")
