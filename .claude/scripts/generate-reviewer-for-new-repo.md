# How to Generate Repo-Specific Code Reviewer

When starting work on a new repository, use this process to create a customized code-reviewer agent.

## Quick Start

```bash
cd ~/path/to/new-repo

# 1. Authenticate GitHub CLI (if not already done)
gh auth login

# 2. Ask Claude Code to generate the reviewer
claude
```

Then in Claude Code:

```
I want to create a repo-specific code-reviewer agent for this repository.

Repository: <current repo name>
Primary reviewer to emphasize: <name>  # e.g., "senior-engineer-name"

Please:
1. Use a general-purpose agent to analyze the last 50 merged PRs
2. Give 3x weight to reviews from <primary reviewer>
3. Extract team-specific patterns categorized by:
   - Architecture & Design
   - Code Quality
   - Correctness & Bugs
   - Testing Standards
   - Performance
   - Security
   - Project-Specific Conventions

4. Generate two files:
   - .claude/agents/code-reviewer.md (the agent configuration)
   - PR_REVIEW_PATTERN_ANALYSIS.md (detailed analysis report)

5. The agent should include:
   - Team-specific pre-review checklist (top issues)
   - Severity levels based on actual review patterns
   - Common pattern references with PR numbers
   - Review question templates in the style of <primary reviewer>
   - References to existing code examples

Work autonomously and report back when complete.
```

## What You'll Get

After analysis completes:

### File 1: `.claude/agents/code-reviewer.md`
- Project-specific agent (version controlled)
- Overrides any user-level `code-reviewer` agent
- Team members share the same rules via git
- Based on YOUR team's actual review patterns

### File 2: `PR_REVIEW_PATTERN_ANALYSIS.md`
- Detailed analysis report
- 40-50+ concrete rules with examples
- PR references for credibility
- Can be shared with team as documentation

## Commit and Share

```bash
git add .claude/agents/code-reviewer.md PR_REVIEW_PATTERN_ANALYSIS.md
git commit -m "Add code reviewer agent based on team review patterns"
git push
```

Now your entire team uses the same review standards!

## Update Over Time

Re-run the analysis every 3-6 months to capture evolving patterns:

```
"Update the code-reviewer agent by analyzing the last 50 PRs again.
Preserve existing rules but add new patterns and update priorities
based on recent review trends."
```

## Multi-Repo Setup

```
~/.claude/agents/
└── code-reviewer-base.md          # Generic fallback

~/Uber/opensearch-tsdb-internal/
├── .claude/agents/
│   └── code-reviewer.md           # TSDB-specific (philiplhchan-style)

~/Uber/go-code/
├── .claude/agents/
│   └── code-reviewer.md           # Go-code-specific (different team)

~/PersonalProjects/my-app/
├── (no .claude/agents/)
└── (uses code-reviewer-base.md)  # Fallback to generic
```

## Tips

1. **Different teams, different agents**: Each repo's `.claude/agents/code-reviewer.md` reflects that team's culture
2. **Version control**: Commit agents so team shares same standards
3. **Iterate**: Update quarterly as team standards evolve
4. **Customize**: Edit the generated agent to add/remove rules
5. **Document**: The analysis report serves as team documentation

## Advanced: Per-Language Reviewers

You can also create specialized reviewers:

```
.claude/agents/
├── code-reviewer.md           # General (used by default)
├── code-reviewer-go.md        # Go-specific
├── code-reviewer-python.md    # Python-specific
└── code-reviewer-frontend.md  # Frontend-specific
```

Then explicitly call:
```
"Use the code-reviewer-go agent to review this Go service"
```
