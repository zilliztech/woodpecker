'use strict';

const assert = require('node:assert/strict');
const { test } = require('node:test');
const handle = require('./issue-commands.cjs');

function fixture(body, options = {}) {
  const calls = [];
  const failures = [];
  const warnings = [];
  const api = (name, result) => async params => {
    calls.push({ name, params });
    if (options.errors?.[name]) throw options.errors[name];
    return typeof result === 'function' ? result(params) : result;
  };
  const github = {
    rest: {
      repos: {
        getCollaboratorPermissionLevel: api('permission', { data: {
          permission: options.permission || 'read',
          user: { permissions: { push: options.push === true } },
        } }),
      },
      issues: {
        addAssignees: api('assign', params => ({ data: {
          assignees: options.assignees || [{ login: 'existing' }, { login: params.assignees[0] }],
        } })),
        removeAssignees: api('unassign', { data: {
          assignees: options.assignees || [{ login: 'existing' }],
        } }),
        update: api('update', { data: {} }),
        listMilestones: () => { throw new Error('Milestones must be paginated'); },
        createComment: api('comment', { data: {} }),
      },
      reactions: { createForIssueComment: api('reaction', { data: {} }) },
    },
  };
  github.paginate = async (method, params) => {
    assert.equal(method, github.rest.issues.listMilestones);
    return api('milestones', options.milestones || [])(params);
  };
  const context = {
    repo: { owner: 'zilliztech', repo: 'woodpecker' },
    // actor can identify someone rerunning the workflow. Authorization must
    // always use payload.comment.user, the original comment author.
    actor: options.runner || 'alice',
    payload: {
      issue: { number: 123, ...(options.pr ? { pull_request: {} } : {}) },
      comment: {
        id: 456,
        body,
        user: { login: 'alice', type: options.userType || 'User' },
        author_association: options.association || 'CONTRIBUTOR',
      },
    },
  };
  return {
    calls, failures, warnings, github, context,
    run: () => handle({ github, context, core: {
      setFailed: message => failures.push(message),
      warning: message => warnings.push(message),
    } }),
    params: name => calls.filter(call => call.name === name).map(call => call.params),
  };
}

const issueParams = { owner: 'zilliztech', repo: 'woodpecker', issue_number: 123 };
const apiError = status => Object.assign(new Error('API unavailable'), { status });

for (const pr of [false, true]) {
  for (const command of ['assign', 'unassign']) {
    for (const argument of ['', ' @ALICE']) {
      test(`${pr ? 'PR' : 'issue'}: /${command}${argument} is self service`, async () => {
        const f = fixture(`/${command}${argument}`, { pr });
        await f.run();
        assert.deepEqual(f.params(command), [{
          ...issueParams, assignees: [argument ? 'ALICE' : 'alice'],
        }]);
        assert.deepEqual(f.params('permission'), []);
        assert.deepEqual(f.params('update'), []);
        assert.deepEqual(f.params('comment'), []);
        assert.deepEqual(f.params('reaction'), [{
          owner: 'zilliztech', repo: 'woodpecker', comment_id: 456, content: '+1',
        }]);
        assert.deepEqual(f.failures, []);
      });
    }
  }
}

for (const permission of ['write', 'maintain', 'admin']) {
  for (const command of ['assign', 'unassign']) {
    test(`${permission} can /${command} @bob without replacing other assignees`, async () => {
      const f = fixture(`/${command} @bob`, { permission });
      await f.run();
      assert.deepEqual(f.params('permission'), [{
        owner: 'zilliztech', repo: 'woodpecker', username: 'alice',
      }]);
      assert.deepEqual(f.params(command), [{ ...issueParams, assignees: ['bob'] }]);
      assert.deepEqual(f.params('update'), []);
      assert.equal(f.params('reaction').length, 1);
    });
  }
}

test('custom role with base write access is allowed', async () => {
  const f = fixture('/assign @bob', { permission: 'custom', push: true });
  await f.run();
  assert.equal(f.params('assign').length, 1);
  assert.equal(f.params('reaction').length, 1);
});

for (const permission of ['read', 'triage', 'none']) {
  for (const command of ['/assign @bob', '/unassign @bob', '/set-milestone v1', '/milestone v1', '/clear-milestone']) {
    test(`${permission} cannot run ${command}, even with MEMBER association or a maintainer rerun`, async () => {
      const f = fixture(command, { permission, association: 'MEMBER', runner: 'admin' });
      await f.run();
      assert.deepEqual(f.calls.map(call => call.name), ['permission', 'comment']);
      assert.equal(f.params('permission')[0].username, 'alice');
      assert.match(f.params('comment')[0].body, /Repository write access is required/);
      assert.deepEqual(f.failures, []);
    });
  }
}

test('non-collaborator 404 denies privileged commands', async () => {
  const f = fixture('/assign @bob', { errors: { permission: apiError(404) } });
  await f.run();
  assert.deepEqual(f.calls.map(call => call.name), ['permission', 'comment']);
  assert.match(f.params('comment')[0].body, /Repository write access is required/);
});

for (const name of ['set-milestone', 'milestone']) {
  test(`/${name} resolves exact open titles with spaces through pagination`, async () => {
    const f = fixture(`/${name} Next release`, {
      permission: 'write', pr: true,
      milestones: [{ title: 'Next', number: 1 }, { title: 'Next release', number: 27 }],
    });
    await f.run();
    assert.deepEqual(f.params('milestones'), [{
      owner: 'zilliztech', repo: 'woodpecker', state: 'open', per_page: 100,
    }]);
    assert.deepEqual(f.params('update'), [{ ...issueParams, milestone: 27 }]);
    assert.equal(f.params('reaction').length, 1);
  });
}

test('clear-milestone sends null, without changing other fields', async () => {
  const f = fixture('/clear-milestone', { permission: 'write' });
  await f.run();
  assert.deepEqual(f.params('update'), [{ ...issueParams, milestone: null }]);
  assert.equal(f.params('reaction').length, 1);
});

for (const title of ['missing', 'NEXT RELEASE', '"Next release"']) {
  test(`missing, differently cased or quoted milestone title ${title} does not mutate`, async () => {
    const f = fixture(`/set-milestone ${title}`, {
      permission: 'write', milestones: [{ title: 'Next release', number: 27 }],
    });
    await f.run();
    assert.deepEqual(f.params('update'), []);
    assert.deepEqual(f.params('reaction'), []);
    assert.match(f.params('comment')[0].body, /No open milestone has that exact title/);
  });
}

for (const body of [
  'Please /assign', '> /assign', '    /assign', '\t/assign', '```\n/assign\n```',
  '`/assign`', '/assign-extra @bob', '/unassignment', '/milestones v1', '/rerun', '',
]) {
  test(`unrelated or quoted text is ignored: ${JSON.stringify(body)}`, async () => {
    const f = fixture(body);
    await f.run();
    assert.deepEqual(f.calls, []);
    assert.deepEqual(f.failures, []);
  });
}

for (const body of [
  '/assign bob', '/assign @bob @charlie', '/unassign @bob,', '/assign @a/b',
  '/assign $(id)', '/assign `id`', '/assign @bob; echo hi', '/assign @bob\n/unassign',
  '/assign\r\nSome prose', '/set-milestone', '/milestone', '/clear-milestone v1',
]) {
  test(`invalid arguments produce help without mutation: ${JSON.stringify(body)}`, async () => {
    const f = fixture(body, { permission: 'admin' });
    await f.run();
    assert.deepEqual(f.calls.map(call => call.name), ['comment']);
    assert.match(f.params('comment')[0].body, /Issue command help/);
    assert.deepEqual(f.failures, []);
  });
}

test('trailing whitespace and CRLF from the comment editor are accepted', async () => {
  const f = fixture('/assign  \r\n');
  await f.run();
  assert.equal(f.params('assign').length, 1);
  assert.equal(f.params('reaction').length, 1);
});

for (const userType of ['Bot', 'Mannequin']) {
  test(`${userType} comments are ignored`, async () => {
    const f = fixture('/assign', { userType });
    await f.run();
    assert.deepEqual(f.calls, []);
  });
}

test('payloads without an issue or comment are ignored', async () => {
  const f = fixture('/assign');
  f.context.payload = {};
  await f.run();
  assert.deepEqual(f.calls, []);
});

test('silently ignored assignment is reported instead of acknowledged', async () => {
  const f = fixture('/assign', { assignees: [{ login: 'existing' }] });
  await f.run();
  assert.deepEqual(f.params('reaction'), []);
  assert.match(f.params('comment')[0].body, /GitHub did not apply/);
});

test('silently ignored removal is reported instead of acknowledged', async () => {
  const f = fixture('/unassign', { assignees: [{ login: 'ALICE' }] });
  await f.run();
  assert.deepEqual(f.params('reaction'), []);
  assert.match(f.params('comment')[0].body, /GitHub did not apply/);
});

test('an existing assignment is acknowledged idempotently regardless of case', async () => {
  const f = fixture('/assign', { assignees: [{ login: 'ALICE' }] });
  await f.run();
  assert.equal(f.params('reaction').length, 1);
  assert.deepEqual(f.params('comment'), []);
});

for (const [endpoint, body] of [
  ['permission', '/assign @bob'], ['assign', '/assign'], ['unassign', '/unassign'],
  ['milestones', '/milestone v1'], ['update', '/clear-milestone'],
]) {
  test(`${endpoint} API failure fails the run and gives feedback without a success reaction`, async () => {
    const f = fixture(body, { permission: 'write', errors: { [endpoint]: apiError(403) } });
    await f.run();
    assert.equal(f.failures.length, 1);
    assert.match(f.params('comment')[0].body, /HTTP 403/);
    assert.deepEqual(f.params('reaction'), []);
    if (endpoint === 'permission') {
      assert.deepEqual(f.calls.map(call => call.name), ['permission', 'comment']);
    }
  });
}

test('failure to post error feedback fails the run', async () => {
  const f = fixture('/assign invalid', { errors: { comment: apiError(403) } });
  await f.run();
  assert.match(f.failures[0], /Could not post command feedback/);
});

test('reaction failure warns without claiming the completed operation failed', async () => {
  const f = fixture('/assign', { errors: { reaction: apiError(403) } });
  await f.run();
  assert.equal(f.params('assign').length, 1);
  assert.deepEqual(f.params('comment'), []);
  assert.deepEqual(f.failures, []);
  assert.match(f.warnings[0], /Command succeeded/);
});
