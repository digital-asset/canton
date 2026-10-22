---
name: investigate-flaky-test
description: Diagnose a flaky Canton test from a downloaded flake-pkg investigation package, write a triage note for the flaky issue, and optionally propose a verified minimal fix. Use when given a flake-pkg-*.zip or a flake-pkg/ directory.
argument-hint: <package.zip | flake-pkg dir | suite dir> [suite]
---

# Investigate a flaky test

Arguments: `$ARGUMENTS`

The input is an investigation package produced by `scripts/ci/collect_flake_evidence.py` (format described in `scripts/ci/README.md`, "Investigation package"). The workflow follows the team's Canton Flaky Tests Guide, condensed in `fix-patterns.md` next to this file. Output templates are in `triage-template.md`.

## Hard rules

- **No GitHub Issues, API or web access.** No `gh`, no GitHub API or web fetches. Reading the repository is fine: any local git command that reads, and `git fetch` of commits. Never `git push`. Everything about the failure comes from the package. If something is missing, ask the user.
- **No commits, branches or PRs.** Edit files only after the user accepts the fix offer in step 6. The human commits and opens the PR.
- **Never read a whole log into context.** Logs can be hundreds of MB. Use `zgrep`, `grep -n`, `head`, `tail`, `wc -l` and `sed -n '<from>,<to>p'` ranges, and cap output (`| head -50`).

## 1. Open the package

If no package is given, ask the user for its path. Never pick one yourself, not even when only one `flake-pkg-*` file is around.

Accept any of:
- a zip (`flake-pkg-*.zip`, as downloaded from the browser): extract it into a fresh temp dir outside the repo, `d=$(mktemp -d) && unzip -q <zip> -d "$d"`;
- the package root (what `gh run download -n flake-pkg-...` extracts): it holds one directory per suite;
- one suite directory: it has a `manifest.json` itself.

List the suites: the directories holding a `manifest.json` (`find <dir> -maxdepth 3 -name manifest.json`).
- With a `suite` argument: match it case-insensitively against the directory names and the manifests' `suite`, also accepting the `-<hash>` suffixed directory names used when names collide. If nothing matches, list the suites found and ask.
- Without one: if there is exactly one suite, use it. If there are several, ask with AskUserQuestion, one option per suite, showing its failing tests (or "log problems only"). AskUserQuestion takes at most 4 options: with more suites, list them all and ask for the name.

Check the manifest: `format` must be `flake-pkg`. If `version` is not `1`, warn the user that this skill knows version 1 and continue using only the fields described here; stop if a field it needs is missing.

## 2. Orient

- Read the suite's `README.md` and `manifest.json` (both small). Note: `tests` (name, type, log), `problem_count`, `tids`, `ci.sha`, `ci.branch`, `ci.job`, `ci.shard`, `ci.protocol_version`, `ci.runner`, `ci.date`, `ci.run_id`, and `truncated` (logs where only head and tail were kept: the cause may be in the cut-out middle, say so if the evidence points there).
- Find the suite source; the manifest has only the simple name: `git grep -l -E "(class|object|trait) <Suite>\b" -- '*.scala'`. A `Postgres`/`H2`/`Default` suffixed suite is often a small subclass, so also open the trait or abstract class it extends. Chaos tests are in scope like any other suite.
- `git cat-file -e <ci.sha>^{commit}`: if the commit is missing, `git fetch origin <ci.sha>`; if that fails too, ask the user.
- `git log --oneline <ci.sha>..HEAD -- <test file> <code under test>`: if the test or the code it exercises changed since, say so up front, since the flake may already be fixed.
- `git log --oneline -i --grep=<Suite>` (also try the name without the `Postgres`/`H2` suffix): earlier fixes and investigations of the same suite.

## 3. Gather evidence, cheapest first

Stop as soon as the evidence is conclusive, but do at least steps 1, 2 and 4.

1. **JUnit:** the failure message and stack trace in `junit/*.xml` (`grep -n -E -A30 '<(failure|error)' junit/*.xml | head -80`). Note the test file line from the trace.
2. **Test log:** `tests/<test>.log` is already cut from `Starting '<Suite>/<test>'...` to the end of `Test failed: '<Suite>/<test>'` (the guide's lnav `:hide-lines-before/after` recipe). Check its size first (`wc -l`), then read the end around the failure and the `Entering`/`Leaving` scope markers. If the manifest gives the test no `log` (`null`), find the same range in `suite.log.gz` with `zgrep -n -F -e "Starting '<Suite>/<test>'" -e "Test failed: '<Suite>/<test>'"`.
3. **Known signatures:** grep the test log, then `suite.log.gz`, for the signatures listed in `fix-patterns.md`, for example `PassiveInstanceException`, `RejectedExecutionException`, `UnknownInformees`, ACS commitment mismatch warnings, `awaitSnapshot` timeouts, and `SuppressingLogger` re-entry.
4. **Unsuppressed WARN/ERROR:** `grep -n -E '^\S+ \S+ \[[^]]*\] (WARN|ERROR) ' tests/<test>.log | grep -v LogReporter | head -50` (or `zgrep` on `suite.log.gz` for the whole suite). Records the test suppressed are re-logged at INFO with the prefix `Suppressed WARN:` or `Suppressed ERROR:`, so they do not show here; grep for that prefix to see what the test expected. An unprefixed WARN/ERROR was not suppressed. The first unexpected one is usually closer to the cause than the final assertion.
5. **Follow the failing operation:** `zgrep -n 'tid:<id>' suite.log.gz | head -100` for the trace ids in the manifest and the failing records. The Ledger API command service starts a new trace id, so one command can span two or more tids: find the command id or submission id in the records and grep for it to collect the other tids, then search for all of them together (`zgrep -n -E 'tid:(<a>|<b>)'`). Use timestamps to bound `sed -n` ranges.
6. **Log-only failures (`problems.txt`, no failing test):** a warning flake can come from another test of the suite, or from a background process after the test ended (ACS commitments, pruning). Attribute each WARN to its test through the `:<Suite>/` part of the logger name and the `Starting`/`Test succeeded` ranges in `suite.log.gz`. Check whether `project/errors-in-log-to-ignore.txt` already ignores a slightly different form of the message.
7. **Code:** read the test and the production code paths named in the traces, at `ci.sha`: `git show <ci.sha>:<path>` where they differ from HEAD (`git diff --stat <ci.sha> HEAD -- <path>`).

Record every quoted line with its file and line number (`suite.log.gz:<n>`, `tests/<test>.log:<n>`) and timestamp. Never invent log lines: quote only what grep returned.

## 4. Diagnose and classify

Pick one category (see `fix-patterns.md` for the catalog of each):
- **A, timing:** a timeout, `eventually` or empirical range too tight for a loaded CI runner.
- **B, log assertions:** an unexpected or leaked WARN/ERROR, an over-strict `assertLogs`, or a false positive needing an ignore entry.
- **C, test race or ordering:** missing topology or node synchronization, background ACS commitments or pruning, or a wrong assumption about offsets or ticks.
- **D, environment:** ports, database, runner resources, infrastructure.
- **E, production bug or race:** shutdown (`FutureUnlessShutdown`, execution context closed too early), a stuck promise, a topology deadlock.
- **F, not a flake: a real failure.** The test fails on every run, independent of timing or ordering, for example because a recent change to the test or the code under test broke it. Flaky issues are raised for failures on `main`, and some suites run only on `main`, so a real breakage can arrive through the flaky pipeline. Signs: the failure follows directly from the code at `ci.sha`, with nothing racy in the log; the test or code under test changed shortly before `ci.sha` (`git log --oneline -5 <ci.sha> -- <test file> <code under test>`).

State:
- the hypothesis, in one or two sentences, as a causal chain from trigger to failure;
- a confidence, high, med or low, with the reason;
- the evidence: quoted log lines with timestamps and file:line references, and code references (`path:line` at `ci.sha`);
- what would falsify it (a log line that should exist if it is right, or a run that should fail if it is wrong).

For F, flag it to the user up front, since it needs a fix rather than flaky-duty triage, and name the commit that likely broke it. Still analyze it fully and propose a fix in step 6.

If the evidence does not support a single cause, say so and list the candidates with what would tell them apart. Low confidence is a valid result.

For A or D, suggest the human check the runner's resource usage in Datadog for `ci.runner` around `ci.date`; you cannot access it.

## 5. Write the triage note

- Fill in the "Issue comment" part of `triage-template.md` and write it to `<suite dir>/triage.md`, next to the package. If the suite dir is inside the git checkout, write it to a temp dir instead so it cannot be committed by accident.
- Show it to the user as the draft of the issue comment (the guide asks to document the analysis on the issue). Leave "Fix" and "Verification" as "none yet" for now.
- If the cause is a recurring pattern that `fix-patterns.md` does not cover, add the "Guide update" section with a two-line symptom/cause/fix entry for the guide.

## 6. Offer a fix

Ask with AskUserQuestion: apply the proposed fix, show it as a diff only, or stop at the triage note. Describe the change in one line per option. If the cause is category E (production code), ask separately and explicitly before touching production code.

Make the smallest change that addresses the root cause, not the symptom. Guardrails, from the guide:
- Never add `@UnstableTest` or the `UnstableTest` tag, and never delete or `ignore` the test.
- No new `utils.synchronize_topology()`. `parties.enable` and DAR vetting synchronize on all participants and synchronizers by default; otherwise use `eventually` on the expected topology state on each node that needs it.
- Suppress a WARN/ERROR only with a stated reason why the message is expected, and with the narrowest matcher (logger, level, message).
- Use the rewrite appender (`rewrite-appender.xml`) only as a last resort.
- Add an `errors-in-log-to-ignore.txt` or `errors-in-sbt-output-to-ignore.txt` entry only for a proven false positive.
- Increase a timeout or loosen an empirical range only with a stated reason (for example CI load, with the observed duration from the log), and conservatively.
- Keep the change local to the suite unless the cause is in shared test utilities or production code.

## 7. Verify

- Before the fix, if practical, try to reproduce: `sbt "testManyTimes <N> <fully qualified suite name>"`. After the fix, run the same. N defaults to 20; for slow suites use fewer. Check the duration of one run first (the JUnit `time` attribute in the package) and ask before starting if one run takes more than about 2 minutes. `testManyTimes` stops at the first failure or log problem, and `log/canton_test.log` holds only the last run.
- Read the sbt output for the number of runs that actually completed and for failures (`Tests: succeeded`, `*** FAILED ***`, `checkErrors`), rather than trusting the exit code. Report the result as it is: runs completed, runs failed, and any failure that differs from the original.
- If the flake did not reproduce before the fix, say that the local verification is weak: passing runs show the fix does not break the test, not that it removes the flake. If the flake is rare or needs CI load, suggest the "Run a Test Many Times" pipeline in `contributing/5_ci-usage.md`, which the human triggers; mention that it costs 50 runs of CI time, so it is worth it only for a plausible fix.
- Fill in the "Fix" and "Verification" sections of `triage.md`, and the "PR description" part of `triage-template.md` (analysis plus fix, as the guide asks). Show both to the user. The human commits and opens the PR.
