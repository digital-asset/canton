Git and GitHub Guidelines
=========================

We use [GitHub Issues](https://github.com/DACH-NY/canton/issues) to track planned work and manage code reviews.

# Issue Hygiene

When creating a new issue:
* Assign a Milestone.
* Add relevant labels.
* Assign the issue to you, if and only if you take responsibility for completing.

Please perform the following cleanup tasks periodically:
* For every issue created by you, close it if it has been completed or is no longer relevant.
* For every issue assigned to you, unassign it if you no longer feel responsible for completing.

# Pull Requests

To add commits to protected branches, such as `main` and release lines, you need to open a pull request.
The title and description should be helpful to reviewers and document why the change has been made.
In particular:
* The description should summarize all changes made and explain why they have been made.
* The description should reference all related issues using [GitHub notation](https://help.github.com/en/articles/closing-issues-using-keywords) (e.g. `closes #42` or `part of #733`).

When a pull request is not ready for review,
either make sure that no reviewer is assigned or put the pull request into draft state.

*Note*: avoid creating nested branch name structures like `user-da/xxx/yyy` and then `user-da/xxx` (the other way around is ok).
It can at least break `canton-testing`-scheduled performance tests when pulling the repo with:
```
error: cannot lock ref 'refs/remotes/origin/user-da/xxx': 'refs/remotes/origin/user-da/xxx/yyy' exists; cannot create 'refs/remotes/origin/user-da/xxx'
```

## Selecting a reviewer

By default, select a single reviewer.
Pick one reviewer from GitHub's recommended reviewers unless you already made arrangements with a particular reviewer.

For possibly breaking changes (e.g. protobuf changes), you need a review from `canton-change-owners`.
For release-process related PRs, you need a review from `canton-release-owners`.
Ask on `#team-canton-code-owners`, to find a suitable reviewer.

If you need a review from a more senior team member, consider asking first another team member,
implement his/her comments and then ask the more senior one for a second review.
The goal is both to share knowledge and reduce review load on some team members.

Note that some team members receive huge numbers of notifications from GitHub.
If you request a review, make them reviewers in GitHub **and** write them a Slack message so that your request is noticed.

## Merging to Protected Branches

We strive to maintain a clean and linear history on protected branches.
By default, when merging pull requests use "squash" to merge as a single commit with a descriptive commit message.
Sometimes, we need to read commit messages years later to understand a design choice,
so please make an effort to write a comprehensive message.

By "descriptive", we mean:
* The first line must give a succinct summary of the change
* The first line must state the number of the main GitHub issue in parentheses at the end.
  For example, `New command for in-flight submissions and transactions (#19592)`
* The body of the message should summarize all changes made and explain why they have been made (just like the pull request description).
* Avoid messy commit messages such as `wip almost done`, `addressing comments`, `trying again...`.

In exceptional cases, you may also merge using "rebase", but then make sure that the sequence of commits merged makes sense, i.e.:
* every commit has a descriptive message,
* CI checks pass on every commit.
