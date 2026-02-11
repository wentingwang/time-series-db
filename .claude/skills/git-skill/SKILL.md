# Git Workflow Skill

## Working with Git Worktrees (PREFERRED for Agents)

**For feature implementation and refactoring tasks, ALWAYS use worktrees to avoid conflicts with user's current work:**

### Creating a Worktree for New Task

```bash
# 1. Generate branch name from task description
# Format: feature/<brief-description> or refactor/<brief-description>
# Examples: feature/moving-average-stage, refactor/convert-to-records

# 2. Ensure main is up to date (in original repo)
cd /Users/wenting.wang/Uber/opensearch-tsdb-internal
git fetch origin

# 3. Create worktree with new branch
git worktree add ../opensearch-tsdb-<task-name> -b <branch-name> origin/main

# Example:
# git worktree add ../opensearch-tsdb-moving-avg -b feature/moving-average-stage origin/main

# 4. Navigate to worktree
cd ../opensearch-tsdb-<task-name>

# 5. Do all work in this worktree
# ... implement, test, commit ...

# 6. Push when ready
git push -u origin <branch-name>
```

### Worktree Naming Convention

```bash
# Pattern: ../opensearch-tsdb-<short-task-description>
# Branch: feature/<task> or refactor/<task>

# Examples:
../opensearch-tsdb-moving-avg     → feature/moving-average-stage
../opensearch-tsdb-record-convert → refactor/convert-to-records
../opensearch-tsdb-fix-nulls      → fix/null-pointer-handling
```

### Why Use Worktrees for Agents

- ✅ **No conflicts**: Agents work in separate directory from user's current work
- ✅ **Isolation**: User can review/test in main repo while agent works
- ✅ **Parallel work**: Multiple agents can work on different features simultaneously
- ✅ **Clean state**: Fresh checkout, no stale build artifacts
- ✅ **Easy cleanup**: User removes worktree after PR merge

### Working in a Worktree

All git commands work normally:
```bash
# In the worktree directory
git status
git add .
git commit -m "message"
git push origin <branch-name>
```

### IMPORTANT: Never Remove Worktrees

**Agents should NEVER run:**
```bash
git worktree remove ...  # ❌ DON'T DO THIS
rm -rf ../opensearch-tsdb-*  # ❌ DON'T DO THIS
```

**User will remove worktrees after PR is merged.**

### Checking Worktrees

```bash
# List all worktrees
git worktree list

# See where you are
pwd
git branch
```

## Starting New Tasks (Legacy - Use Worktrees Instead)

**If NOT using worktrees, create a new branch from latest main:**
```bash
git checkout main
git pull origin main
git checkout -b <descriptive-branch-name>
```

## Handling Conflicts

**STOP and ask user what to do if:**
- Merge conflicts occur
- Rebase conflicts occur
- Any git operation fails unexpectedly

Do NOT attempt to resolve conflicts automatically.

## Pre-PR Workflow

**Before raising a PR, sync fork with upstream:**
```bash
git checkout main
git pull upstream main
git push origin main
git checkout <your-branch>
git rebase main
```

If rebase has conflicts, STOP and ask user.

## Committing Changes

**When user agrees to commit:**
1. Stage relevant files
2. Create commit with summary:
   - 3 bullet points maximum
   - Each bullet d10 words
   - Focus on "what changed" not "how"
3. commit with signature like git commit -sm "comment"

Example commit message:
```
<Brief title>

- Added TimeSeriesUnfoldAggregator benchmark infrastructure
- Refactored base class for reusability
- Fixed parameter naming conventions

> Generated with [Claude Code](https://claude.ai/code)

Co-Authored-By: Claude <noreply@anthropic.com>
```

## Key Reminders

- Never push to main directly
- Never auto-resolve conflicts
- Always use descriptive branch names
- Keep commits focused and atomic
