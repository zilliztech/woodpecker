'use strict';

class CommandError extends Error {}

function parseCommand(body, actor) {
  // Require a slash at column zero. Prose, quotes, fenced/indented code and
  // command prefixes such as /assign-extra must not perform an operation.
  const text = body.trimEnd();
  const match = /^\/(assign|unassign|set-milestone|milestone|clear-milestone)(?=$|\s)/.exec(text);
  if (!match) return null;
  if (/[\r\n]/.test(text)) {
    throw new CommandError('Use one command per comment, on a single line.');
  }

  const name = match[1];
  const argument = text.slice(match[0].length).trim();
  if (name === 'assign' || name === 'unassign') {
    if (!argument) return { name, target: actor };
    const user = /^@([a-z\d](?:[a-z\d-]{0,37}[a-z\d])?)$/i.exec(argument);
    if (!user) throw new CommandError(`Usage: /${name} or /${name} @username.`);
    return { name, target: user[1] };
  }
  if (name === 'clear-milestone') {
    if (argument) throw new CommandError('Usage: /clear-milestone (no arguments).');
    return { name };
  }
  if (!argument) throw new CommandError('Usage: /set-milestone <exact milestone title>.');
  return { name: 'set-milestone', title: argument };
}

module.exports = async function handle({ github, context, core }) {
  const { comment, issue } = context.payload;
  if (!issue || !comment || comment.user.type !== 'User') return;

  const repo = context.repo;
  const actor = comment.user.login;
  const issueParams = { ...repo, issue_number: issue.number };
  const help = `https://github.com/${repo.owner}/${repo.repo}/blob/HEAD/.github/ISSUE_COMMANDS.md`;
  let command;

  try {
    command = parseCommand(comment.body || '', actor);
    if (!command) return;

    const selfService = (command.name === 'assign' || command.name === 'unassign') &&
      command.target.toLowerCase() === actor.toLowerCase();
    if (!selfService) {
      // author_association (e.g. MEMBER or CONTRIBUTOR) does not establish
      // repository write access. Query current permissions before any mutation.
      let allowed = false;
      try {
        const { data } = await github.rest.repos.getCollaboratorPermissionLevel({
          ...repo, username: actor,
        });
        allowed = data.user?.permissions?.push === true ||
          ['write', 'maintain', 'admin'].includes(data.permission);
      } catch (error) {
        if (error.status !== 404) throw error;
      }
      if (!allowed) {
        throw new CommandError('Repository write access is required to manage other assignees or milestones. You can use /assign and /unassign for yourself.');
      }
    }

    if (command.name === 'assign' || command.name === 'unassign') {
      // These endpoints add/remove only the requested user, preserving others.
      const params = { ...issueParams, assignees: [command.target] };
      const { data } = command.name === 'assign'
        ? await github.rest.issues.addAssignees(params)
        : await github.rest.issues.removeAssignees(params);
      const assigned = data.assignees.some(user =>
        user.login.toLowerCase() === command.target.toLowerCase());
      // GitHub may silently ignore an ineligible assignee. HTTP success alone
      // is insufficient, so never acknowledge a change that did not take effect.
      if (assigned !== (command.name === 'assign')) {
        throw new CommandError('GitHub did not apply the requested assignment. The account may not be eligible for assignment, or the issue may have reached its assignee limit. Ask a maintainer to check.');
      }
    } else if (command.name === 'clear-milestone') {
      await github.rest.issues.update({ ...issueParams, milestone: null });
    } else {
      const milestones = await github.paginate(github.rest.issues.listMilestones, {
        ...repo, state: 'open', per_page: 100,
      });
      const milestone = milestones.find(item => item.title === command.title);
      if (!milestone) {
        throw new CommandError('No open milestone has that exact title. Use an existing open milestone title without quotes; this command does not create milestones.');
      }
      await github.rest.issues.update({ ...issueParams, milestone: milestone.number });
    }
  } catch (error) {
    const expected = error instanceof CommandError;
    if (!expected) core.setFailed(`Issue command failed: ${error.message}`);
    const message = expected ? error.message :
      `GitHub API request failed${error.status ? ` (HTTP ${error.status})` : ''}. Check the workflow run for details before retrying.`;
    try {
      await github.rest.issues.createComment({
        ...issueParams,
        body: `@${actor}: ${message}\n\n[Issue command help](${help})`,
      });
    } catch (feedbackError) {
      core.setFailed(`Could not post command feedback: ${feedbackError.message}`);
    }
    return;
  }

  // Keep feedback failures separate: the operation has already succeeded.
  try {
    await github.rest.reactions.createForIssueComment({
      ...repo, comment_id: comment.id, content: '+1',
    });
  } catch (error) {
    core.warning(`Command succeeded, but could not add a reaction: ${error.message}`);
  }
};
