# Issue and pull request commands

Post a command as a new comment on an issue or pull request. Use one command per
comment, on a single line starting with `/` (no indentation, quotes or code fences).
Editing an existing comment does not run a command.

| Command | Effect | Who can use it |
| --- | --- | --- |
| `/assign` | Add yourself as an assignee | Any contributor who can comment |
| `/unassign` | Remove yourself as an assignee | Any contributor who can comment |
| `/assign @username` | Add the specified assignee, keeping existing assignees | Yourself, or someone with repository write access |
| `/unassign @username` | Remove only the specified assignee | Yourself, or someone with repository write access |
| `/set-milestone v0.15.0` | Set an existing open milestone by its exact title | Someone with repository write access |
| `/milestone v0.15.0` | Alias for `/set-milestone` | Someone with repository write access |
| `/clear-milestone` | Remove the current milestone | Someone with repository write access |

`v0.15.0` is an example; use an actual open milestone title. Titles containing
spaces are supported: `/set-milestone Next release`. Do not quote the title.
Milestone matching is case sensitive. Commands do not create or reopen milestones.
Specify at most one username per assignment command. GitHub's assignment eligibility
rules and assignee limit still apply; a command reports an error if GitHub does not
apply the assignment. Repeating an assignment/removal already in the desired state
is safe.

Successful commands receive a 👍 reaction on the original comment. Invalid
arguments, insufficient permissions and API failures receive an explanatory reply.
Unrelated comments, quoted examples and bot comments are ignored. A title or command
embedded in a larger comment will not run.

## Workflow maintenance

The handler is `.github/scripts/issue-commands.cjs`, invoked by
`.github/workflows/issue-commands.yaml`. The workflow must be merged into the
repository's **default branch** before comments can trigger it. It uses the built-in
`GITHUB_TOKEN`; no personal access token or external bot service is needed. Repository
or organization policies must permit the workflow's `contents: read`, `issues: write`
and `pull-requests: write` permissions.

The privileged job checks out only the default branch commit provided by the
`issue_comment` event, including for comments on fork pull requests. It must never
check out or execute PR-head code. Comments are parsed as data, and authorization
uses the comment author's current repository permissions, not their organization
membership or contributor association. Write, maintain and admin roles (including
custom roles with base write access) can manage other assignees and milestones.
Read and triage roles can only manage their own assignment.

Run the dependency-free regression suite with:

```sh
node --test .github/scripts/issue-commands.test.cjs
```

The separate `Issue Command Tests` workflow runs these tests on pull requests and
pushes to `master`/`dev` that change the handler, tests or related workflows. Tests
mock GitHub APIs; they do not change live issues. After merging, verify the commands
on a test issue with both a contributor and a maintainer account.
