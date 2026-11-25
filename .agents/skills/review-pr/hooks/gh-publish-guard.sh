#!/usr/bin/env bash
# Copyright The Kubeflow Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# PreToolUse guard for review-pr skill.
#
# Forces a confirmation prompt before any Bash command that PUBLISHES to GitHub
# (posting PR/issue reviews or comments, merging, creating/editing PRs or
# releases, or writing via `gh api`). Read-only commands such as `gh pr view`,
# `gh pr diff`, `gh pr list`, and GET `gh api` calls pass straight through.
#
# This exists because the review-pr skill instructs the model to show a draft
# for human review before posting -- but a skill instruction can be skipped.
# This hook is enforced by the harness, so it cannot be.
set -euo pipefail

payload="$(cat)"

# Pull the Bash command string out of the hook payload.
if command -v jq > /dev/null 2>&1; then
  cmd="$(printf '%s' "$payload" | jq -r '.tool_input.command // ""')"
else
  cmd="$(printf '%s' "$payload" | python3 -c 'import sys, json; print(json.load(sys.stdin).get("tool_input", {}).get("command", ""))')"
fi

# `gh` followed by any global options (e.g. `--repo owner/repo`, `-R owner/repo`).
gh_re='gh([[:space:]]+-[^[:space:]]+([[:space:]]+[^-[:space:]][^[:space:]]*)?)*[[:space:]]+'

# Commands that write to GitHub. `gh api` defaults to GET, but switches to
# POST when request fields or an input body are passed.
write_re="${gh_re}(pr|issue)[[:space:]]+(review|comment|merge|create|edit|close|reopen|ready)"
write_re+="|${gh_re}release[[:space:]]+(create|edit|delete)"
write_re+="|${gh_re}api[[:space:]].*(--method([[:space:]]+|=)|-X[[:space:]]*)(POST|PATCH|PUT|DELETE)"
write_re+="|${gh_re}api[[:space:]](.*[[:space:]])?(-f|-F|--field|--raw-field|--input)([[:space:]]|=)"

# Join shell line continuations so options split across lines are matched.
if printf '%s' "${cmd//$'\\\n'/ }" | grep -Eiq "$write_re"; then
  cat << 'JSON'
{
  "hookSpecificOutput": {
    "hookEventName": "PreToolUse",
    "permissionDecision": "ask",
    "permissionDecisionReason": "This command publishes to GitHub. Confirm the draft review/comment was shown and approved before posting."
  }
}
JSON
fi

# Non-matching commands: emit nothing and exit 0 -> normal permission flow.
exit 0
