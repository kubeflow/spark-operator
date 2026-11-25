# Agent Resources

Shared resources for AI coding agents working on Kubeflow Spark Operator. Project conventions and
`make` targets live in [AGENTS.md](../AGENTS.md); this directory holds reusable skills.

```
.agents/
└── skills/     # Agent skills
    └── review-pr/
```

## review-pr

Runs a comprehensive pull request review using specialized subagents and posts the inline comments
as a pending GitHub review. Pending reviews are visible only to you: inspect the draft in the PR's
**Files changed** tab, edit or remove comments, and submit it yourself.

```
skills/review-pr/
├── SKILL.md                     # Review workflow
├── agents/                      # Subagent prompts
│   ├── code-reviewer.md
│   ├── code-simplifier.md
│   ├── comment-analyzer.md
│   ├── pr-test-analyzer.md
│   ├── silent-failure-hunter.md
│   └── type-design-analyzer.md
└── hooks/
    └── gh-publish-guard.sh      # Confirmation guard for commands that publish to GitHub
```

### Usage

```
/review-pr <PR_LINK>           # Run all applicable reviews, e.g. /review-pr https://github.com/kubeflow/spark-operator/pull/1234
/review-pr tests errors        # Review only test coverage and error handling
/review-pr all parallel        # Launch all agents in parallel
```

Available aspects: `comments`, `tests`, `errors`, `types`, `code`, `simplify`, `all`.

### Configure for Claude Code

Claude Code discovers skills from `.claude/skills/` and subagents from `.claude/agents/`. Symlink
them from the repository root (`.claude/` is git-ignored, so these stay local):

```bash
mkdir -p .claude/skills .claude/agents
ln -s ../../.agents/skills/review-pr .claude/skills/review-pr
for f in .agents/skills/review-pr/agents/*.md; do
  ln -s "../../$f" ".claude/agents/$(basename "$f")"
done
```

If the subagents are not registered, the skill falls back to launching a general-purpose subagent
with the prompt from `agents/<name>.md`.

#### GitHub publish guard hook

The skill asks the agent to show draft comments before posting them, but an agent can skip an
instruction. `hooks/gh-publish-guard.sh` is a Claude Code `PreToolUse` hook that the harness
enforces: it forces a confirmation prompt before any Bash command that writes to GitHub.

The Agent Skills spec has no field for hooks, and Claude Code does not auto-load hook files from
`.claude/hooks/`, so register the hook in `.claude/settings.json`. Run from the repository root:

```bash
[ -f .claude/settings.json ] || echo '{}' > .claude/settings.json
jq --arg cmd '"$CLAUDE_PROJECT_DIR"/.agents/skills/review-pr/hooks/gh-publish-guard.sh' '
  if any(.hooks.PreToolUse[]?.hooks[]?; .command == $cmd) then .
  else .hooks.PreToolUse += [{matcher: "Bash", hooks: [{type: "command", command: $cmd}]}]
  end' .claude/settings.json > .claude/settings.json.tmp && mv .claude/settings.json.tmp .claude/settings.json
```

Run `/hooks` in Claude Code to verify that it is registered.
