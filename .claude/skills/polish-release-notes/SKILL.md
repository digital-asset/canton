---
name: polish-release-notes
description: Polish and complete the release notes (UNRELEASED.md) of a release line before a minor or patch release. Use when asked to polish, review or complete release notes.
argument-hint: <version to release, e.g. 3.7.0 or 3.7.1>
---

# Polish release notes

Arguments: `$ARGUMENTS`. The version to release (e.g. `3.7.0` or `3.7.1`) is required; ask if it is missing. Its
release line is `release-line-<major>.<minor>` (e.g. `release-line-3.7`).

## 0. Release type

The version decides the baseline (what counts as already released), the model, and the structure:

| | Minor release `3.x.0` | Patch release `3.x.y`, `y >= 1` |
|---|---|---|
| Baseline | the previous line `release-line-3.(x-1)` and its latest tag `v3.(x-1).N` | the previous tag of the same line `v3.x.(y-1)` |
| Changes to cover | `origin/release-line-3.(x-1)..origin/release-line-3.x` | `v3.x.(y-1)..origin/release-line-3.x` |
| Already released | all `release-notes/3.(x-1).*.md` | all `release-notes/3.x.*.md` |
| Model | the previous minor release notes, i.e. the oldest `release-notes/3.(x-1).*.md` on `release-line-3.(x-1)` (e.g. `3.7.0.md`, or `3.6.1.md` for 3.6) | the latest `release-notes/3.x.*.md` on the line, or the previous line's patch notes (e.g. `3.6.3.md`) if this is the first patch |
| Structure | full (step 2, minor) | short (step 2, patch) |
| Steps | all | all, but the commit audit (step 4) and verification (step 5) cover only the few commits since the tag, so do them inline instead of with parallel agents |

If the previous tag does not exist yet (e.g. `v3.7.0` not cut), ask the user which commit is the baseline.

## Hard rules

- Work on a local branch `polish-release-notes-<version>` (e.g. `polish-release-notes-3.7.1`) created from the
  latest release line. Fetch over HTTPS
  right before creating or rebasing it: `git fetch https://github.com/DACH-NY/canton.git release-line-<v>:refs/remotes/origin/release-line-<v>`.
- Only `UNRELEASED.md` changes. Keep the substitutions of `release/propose.sh`: `CANTON_VERSION`,
  `RELEASE_DATE`, `PROTOCOL_VERSIONS`, `JAVA_VERSION`, `POSTGRES_VERSION`, `DAML_VERSION`.
- Never claim something about the code without checking it on the release line. Notes often describe changes that
  are not there (see step 4).
- Never push, open PRs, or post on GitHub. Commit locally; the user reviews and pushes.
- No em or en dashes anywhere in the notes, and avoid semicolons: split into sentences or use parentheses.

## 1. Gather inputs

- Current notes: `UNRELEASED.md` on the release line.
- Model and already-released notes as given by step 0, e.g.
  `git show origin/release-line-3.7:release-notes/3.7.0.md`.
- Baseline tag from step 0. Anything in the baseline or in already-released notes is not news for this release.

## 2. Structure

`UNRELEASED.md` starts from `release/RELEASE-NOTES-TEMPLATE.md`, which already has the sections of a minor release.
For both kinds of release, remove all template placeholders: the `<!-- ... -->` comments (including the
advisory template), "_Write summary of release_", and empty sections. Rename "New protocol version" to "Protocol
version <n>". Older notes may still have "Topic A", "Preview Features" with `- preview feature`, or an uncommented
"(YY-nnn, Risk): Title"; remove those too.

### Advisories

Every bug fix with an advisory id and severity (Critical, High, Medium, Low) is written as an advisory, security
fixes and other bug fixes alike. Call out every Critical or High security fix, but only once the fix is deployed
on MainNet. Not every security fix is marked; collect candidates from all of these:

- commits with `[SEC` in the title (`git log -i -F --grep='[SEC'`, also matches `[SEC, 3.7 backport]`);
- PRs labelled `security` or `team-canton-security`
  (`GH_PAGER=cat gh pr list -R DACH-NY/canton --state merged --label security --base <branch> --json number,title`),
  matched to the commits by PR number, including the PRs that a backport ports;
- audit finding ids in titles (e.g. `DIA-06-015:`);
- commit titles and bodies matching
  `git log -i -E --grep='secur|vulnerab|CVE-|GHSA|harden|exploit|attack|unauthori|bypass|denial'`;
- dependency updates for security vulnerabilities;
- fixes the user names.

Git records neither the advisory id, the severity nor the MainNet status: ask the user for each
candidate, and leave out any security fix that is not yet on MainNet. Report the left-out fixes to the user.

Write each advisory with the heading `### (<id>, <severity>): <title>` and the sections of the template
(Issue Description, Affected Deployments, Affected Versions, Impact, Symptom, Workaround, Likeliness,
Recommendation), as in `release-notes/3.4.9.md` on `release-line-3.6` (`25-012, Low` and `25-013, Critical`).
Place them at the top of "Security and Bug Fixes", before the other fixes.

### Patch release

A patch release is about stability, performance and reliability. Patch notes are short. Follow the model:
- **Summary**: one or two sentences that classify the release and say what it improves or fixes and who should
  upgrade, e.g. "This is a maintenance release that fixes an out of memory issue during synchronizer reconnect."
  - *bugfix release*: only bug fixes;
  - *security release*: mainly security fixes or dependency updates for security vulnerabilities. Be careful:
    the label alone tells readers that older versions are vulnerable. Before using it, ask the user whether every
    security fix in the release is deployed on MainNet. If any is not, flag it heavily (as for breaking changes)
    and do not call it a security release or describe the fix until the user decides;
  - *maintenance release*: stability, performance and reliability improvements, usually with some fixes.

  Mention a required DB migration here too.
- **What's New**: stability, performance and reliability improvements, one subsection per notable change (with
  `#### Background`, `#### Specific Changes` and `#### Impact and Migration` when it needs explanation), small
  ones as a bullet list. Say what gets better for the reader (e.g. faster reconnects, lower memory use, recovery
  after a crash).
- **Security and Bug Fixes**: advisories first, then the other fixes as a bullet list.
- **Compatibility**: unchanged template.

Group What's New entries by audience (application developers, all node operators, participant node operators,
synchronizer operators) only if there are many. Drop the summary of breaking changes and the Breaking changes section.
A patch release should not contain new functionality, breaking changes or deprecations:

- **New functionality** (new endpoints, commands, config keys, metrics, features): flag each item to the user with
  its commit and PR, and ask whether it belongs in the patch and how to describe it.
- **Breaking changes and deprecations**: flag them heavily. Stop and tell the user right away, before polishing
  further, with commit, PR, what breaks, and who is affected. Never list one silently in the notes. Repeat them at
  the top of every report (step 9) until the user decides.

Also check that fixes released in newer patches of the previous line (e.g. `3.6.5` for a `3.7.1`) are on this
line; report missing forward ports.

### Minor release

Follow the model's sections, split by audience:

- **Summary**: a few themed bullets (e.g. new protocol version, multi-synchronizer, party replication, ordering,
  traffic management, hardening and scalability). High-level, factual, no marketing speak. No internal mechanics,
  no small endpoints, no "drops support for old protocol versions" headline. Link to a migration guide when a
  theme needs action.
- **Summary of breaking changes**, inside Summary: a table with the columns Ledger API, Admin API and console,
  Protocol, Configuration, each `yes` or `no`, then one short bullet per breaking change grouped by these
  categories. When the protocol changes only with a new protocol version, mark it `yes*` and add the footnote that
  synchronizers on the current protocol version are not affected and the new one is rolled out as a new protocol
  version. Describe configuration changes as changed default values for safety.
- **What's New**: "Protocol version <n>" first (both audiences: new limits with their metrics, new message or
  topology versions with a migration guide as its own subsection, forbidden downgrades), then "For application
  developers" (Ledger API, JSON API, bindings), "All node operators" for entries that apply to every node type,
  "Participant node operators" (Canton naming, not "validator"), and "Synchronizer operators". Mark experimental
  features "(experimental)" in the heading, and put a warning in bold when a feature must not be used yet.
- **Breaking changes**: "Ledger API (application developers)", "All node operators", "Participant node
  operators", "Synchronizer operators". Every entry says what changed, who is affected, and how to migrate; the summary
  bullets match these entries one to one.
- **Deprecations**: the deprecated item, its replacement, and the removal plan with any flag to disable it early.
  Leave out removals of features deprecated in an earlier release.
- **Minor Improvements**: the same audience split as What's New, for improvements that are not fixes.
- **Security and Bug Fixes**: advisories first, then the other bug fixes, security hardening and dependency updates
  for security vulnerabilities as one bullet list, not split by audience.
- **Compatibility**: the template, plus a note on protocol versions no longer supported and how to upgrade
  synchronizers still on them. End of support for a protocol version is not a breaking change.

Not breaking changes: new error codes, removals of features deprecated in an earlier release, end of support for
protocol versions. Ask the user when unsure.

## 3. Drop what is not news

- Entries that already shipped in a release of the baseline (compare with the notes and tag from step 1). For a
  minor release this includes forward ports of fixes that a patch release of the previous line already announced.
- For a minor release, work merged on `main` before the previous line branched, even if it shows up in this
  line's history.
- Entries without an observable effect for either audience: internal refactorings, internal APIs, protocol
  internals, doc comment fixes, test or CI changes.
- Daml changes (compiler, standard library, Daml-LF, engine), unless the user asks to include them.
- Artifacts that only ship as examples (e.g. Grafana dashboards under `examples/`).

## 4. Find leaked and missing entries

Leaks: backports sometimes bring unrelated `UNRELEASED.md` lines from `main` through conflict resolution. List
the backports that added several lines:

```bash
for c in $(git rev-list <baseline>..origin/release-line-<v> -i --grep="backport\|cherry-pick\|forward port" -- UNRELEASED.md); do
  echo "$(git show $c -- UNRELEASED.md | grep -c '^+[-*] ') $(git log -1 --format='%h %s' $c)"
done | sort -rn | head
```

For each suspicious line, check that the change is in the code of the release line (`git merge-base
--is-ancestor <originating commit> origin/release-line-<v>`, or `git grep` its identifiers).

Missing: commits since the baseline without release notes that touch user-facing code.

1. `git rev-list --no-merges <changes to cover from step 0>`, minus those touching `UNRELEASED.md`.
2. Keep commits whose non-test files match protos, `console/commands/`, `*Config*.scala`, error codes,
   `ledger-json-api`, OpenAPI, metrics, Docker, `src/pack`, `.conf`, or SQL migrations.
3. For a minor release, drop commits whose PR number also appears in the previous line's history (backported
   there and released with it).
4. Split the rest into batches and review them with parallel agents (read-only). Brief: decide per commit whether
   it has a user-facing effect for application developers or node operators, whether the draft or the previous
   line's notes cover it, and report only missing items with sha, PR, concrete identifiers, audience, suggested
   section and confidence.

## 5. Verify every entry against the code

Split the draft by sections and verify with parallel read-only agents. For each entry: the change is on
`origin/release-line-<v>`, it is absent from the baseline tag, and its identifiers are right. Common
mistakes:

- Config keys: Scala fields are camelCase, HOCON is kebab-case; give the full path
  (`canton.participants.<participant>.parameters...`).
- Metric names: check the full prefix (e.g. `daml.participant.sync.` vs `daml.participant.`).
- Error code ids, console command names (singular vs plural), proto field and message names.
- Defaults that changed in the same commit (e.g. a feature now off by default).
- Dependency versions: compare `project/` or the shared dependencies file between the baseline tag and the line.

Fix wrong details; drop entries for changes that are not on the line.

## 6. Writing style

- Write for the reader's decision: what changed, why it matters to them, what to do. Add a short "why" to an
  effect when it helps (e.g. "reconnects faster: the lookup on reconnect now uses an index").
- Assume readers never query database tables: describe schema, index and query changes as performance
  improvements without table or index names. Keep a config key only when it is the operator's control.
- Security-relevant changes other than advisories: call out the change and the required action, but never why
  the old behavior was a problem or how it could be abused. No triggers, no "previously X was possible", no "not
  charged". Drop entries whose only content is a hardening detail.
- Do not call a feature GA or announce timelines unless the user confirms it.
- For a new message or format version, say who needs to migrate and how (e.g. decoders of a Ledger API result
  must support the new version, or request the old one with a field).
- Keep tables aligned; move long cell text into a note below the table.

## 7. Heavy DB migrations

List migration files added since the baseline (`git diff --name-status <baseline> origin/release-line-<v> --
'*.sql'`), assess each statement on Postgres (metadata-only `ALTER`/`DROP` are fast;
`CREATE INDEX`, rewrites, and `UPDATE`s on large tables are not), and ask the user for timings of the expensive
ones before adding a "Heavy DB migrations" section. Migrations can be dropped later on purpose (e.g. merged V7
files); check the final files, not the commit that introduced them.

## 8. Commit and iterate

- Commit as one commit "Polish the <version> release notes" (e.g. "Polish the 3.7.1 release notes"). During review, amend it; keep a single commit.
- Rebase on request: fetch first. On `UNRELEASED.md` conflicts, take the polished version
  (`git checkout --theirs UNRELEASED.md` during a rebase) and fold the new upstream entries into the right
  sections, applying steps 3 to 6 to them.
- To squash, soft-reset to the merge-base, never to a fetched `origin/...` ref, and check `git diff --stat
  HEAD~1 HEAD` touches only `UNRELEASED.md`.

## 9. Report

After each round, show what changed and list open questions for the user. For a patch release, start with any
breaking change or deprecation under a **BREAKING CHANGE IN A PATCH RELEASE** heading and any security fix not yet
on MainNet under a **SECURITY FIX NOT ON MAINNET** heading, then any new functionality.
Other open questions, e.g.: entries that may be internal, announced removals that did not happen, missing
timings, unclear audience, code that looks like a regression (e.g. a fix of the previous line not forward ported).
