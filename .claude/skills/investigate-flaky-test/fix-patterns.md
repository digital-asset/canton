# Flaky test fix patterns

Symptom, cause and fix catalog for `/investigate-flaky-test`. Sources: the team's Canton Flaky Tests Guide (condensed here, since the agent cannot open web links) and flake fixes in the git history. Examples are commits in this repo (`git show <hash>`); `#nnnnn` are PR numbers for the human. Paths are relative to the repo root; `.../` abbreviates `src/main/scala/com/digitalasset/canton/`.

Grep with `zgrep -n -E '<signature>' suite.log.gz | head` and `grep -n -E '<signature>' tests/*.log | head`.

## A. Timing and empirical ranges

| Symptom (grep signature) | Cause | Fix | Example |
|---|---|---|---|
| `timed out`, `TimeoutException`, ping failures, `eventually` giving up after its default | Timeout too tight for a loaded CI runner (check the elapsed time between the `Starting` record and the failure) | Raise that one timeout conservatively, with the observed duration as the reason; or retry the operation | `7a690906df4` (#36272): `startupCompletionTimeLimit = 1.minute` |
| Ping fails once, the rest of the test is fine | Single ping attempt under load | `ConfigTransforms.setPingRetries(true)` (`community/integration-testing/.../integration/ConfigTransforms.scala`) | `f356ac8a538` (#36132), `d3a71cfd29b` |
| `eventually` fails on an exception, not an assertion | `BaseTest.eventually` (`community/testing/.../BaseTest.scala:453`) defaults to 20s and retries only `TestFailedException` | `eventually(retryOnTestFailuresOnly = false)` where the condition can throw transiently | `c2b43e8a165` (#36133) |
| Out-of-range throughput or latency, e.g. `BackPressureIntegrationTest` | Empirical ranges depend on contention: a faster system or more parallel tests shift them | Tweak the range conservatively or add a retry loop | guide |
| BFT orderer test with sim clock stuck or out of order | Sim clock and sequencing time drift | Use the time tracker with `timeTracker.observationLatency` set to zero, e.g. `_.focus(_.timeTracker.observationLatency).replace(NonNegativeFiniteDuration.Zero)` | #29837, #29885; `TransactionTimeoutsIntegrationTest` |

For any timing cause, the human can check the runner's load in Datadog (`ci.runner`, `ci.date`).

## B. Logs and log assertions

| Symptom (grep signature) | Cause | Fix | Example |
|---|---|---|---|
| Unprefixed WARN/ERROR after `Leaving`/`Test succeeded`, or in a later test | Leaked WARN: background work logged after suppression ended | Wait for the background work to finish inside the suppression block | `6d27a6aa76f` (#36081), `332982c8a5a` |
| `assertLogs` fails on a missing or extra entry that appears in the log | Over-strict matching: order, count, or which node logs it | Match per node (`forAll`), `assertLogsUnordered`, or `assertLogsUnorderedOptional` with `LogEntryOptionality.{Required, Optional, OptionalMany}` | `ac57bb5aaf4` (#36355), `880bf945153` |
| New expected benign WARN/ERROR | The test provokes it legitimately | Suppress it narrowly (logger, level, message), with a comment saying why it is expected | `45b38fe3fe9` (#36240) |
| `support neither nested nor concurrent calls` (`SuppressingLogger.suppress`) | `suppress` is not reentrant: nested, or concurrent through an `AsyncWordSpec` test not sequencing its steps | Sequence the steps; prefer `futureValue` over `AsyncWordSpec` | guide |
| Log-only failure (`problems.txt`) with a known-benign message | False positive in `checkErrors` | Ignore entry in `project/errors-in-log-to-ignore.txt` (PCRE and `grep -E` compatible) or `project/errors-in-sbt-output-to-ignore.txt`. First check whether a slightly different form is already ignored | guide |
| Benign WARN logged by production code in many tests | Known noise | Rewrite rule in `community/util-observability/src/main/resources/rewrite-appender.xml` (downgrades to INFO). Last resort, use sparingly | guide |
| A new unexpected WARN | May hint at a deeper issue: diagnose before suppressing | Usually a category C or E fix, not a suppression | guide |
| Log output missing or odd | Logback configuration | `sbt -Dlogback.statusListenerClass=ch.qos.logback.core.status.OnConsoleStatusListener '<command>'` | guide |

`SuppressingLogger` (`base/testing-utils/.../logging/SuppressingLogger.scala`): `assertLogs` (default rule `LevelAndAbove(WARN)`, ordered, leftover entries fail), `assertLogsSeq` (entries kept on failure, so it can be wrapped in `eventually`), `assertLogsUnordered`, `assertLogsUnorderedOptional`, `suppressWarnings`/`suppressWarningsAndErrors`/`suppress(rule)`. Rules in `SuppressionRule.scala`, matchers in `LogEntry.scala` (`shouldBeCantonErrorCode`, `warningMessage`, ...). Default poll timeout 1s.

## C. Test races and ordering

| Symptom (grep signature) | Cause | Fix | Example |
|---|---|---|---|
| `UNKNOWN_INFORMEES`, `UnknownInformees`, or a party, key or DAR unknown right after it was added | The topology transaction has not reached or become effective on every node | `eventually { <node>.topology.<mapping>.is_known(...) shouldBe true }` (or `.list(...) should not be empty`) on each node that needs it. `parties.enable` and DAR vetting already synchronize on all participants and synchronizers by default | guide |
| Only some nodes see a topology change, e.g. a new `MaxRequestSize` seen by the sequencer but not the sending participant | Since `b668b2c514f` (#23890), commands no longer wait for topology traffic to become idle | As above, on the node that acts on the change | #24264 |
| Exact offset or count off by one | Wrong assumption: an ACS commitment tick or other background event can land in between | Check a range or the relevant events, not an exact offset | `031c366f1e2` (#36333) |
| Assertion fails after a crash or restart | Nodes not caught up before the crash | Sync first (`fetch_synchronizer_times`, pings), then crash; fix the expected counts | `0d25b6640f9` (#36278) |
| `ACS_COMMITMENT_MISMATCH`, `ACS_MISMATCH_NO_SHARED_CONTRACTS`, `ACS_COMMITMENT_DEGRADATION` between or after tests | Commitments are sent with a randomized delay, possibly after the test ended; some tests provoke failing commitments on purpose (e.g. unauthorizing a participant, `SimpleUpgradeTest`) | Start the synchronizer with a very long `reconciliationInterval`, or remove the randomization with `TestingConfigInternal(maxCommitmentSendDelayMillis = Some(NonNegativeInt.zero))` | guide |
| Pruning-related WARN or missing data | Background participant pruning | `participant.pruning.clear_schedule()` | guide |
| Config-driven interference | A connection or test feature not needed here, e.g. blacklisting | Turn it off in the test config | `b6e6aee0896` (#36083) |

Never use `utils.synchronize_topology()`: it does not guarantee that all participants observe the latest topology state (issue #24538).

## D. Environment

| Symptom (grep signature) | Cause | Fix | Example |
|---|---|---|---|
| `Address already in use`, `BindException` | Port clash between parallel tests | Let the test environment allocate ports instead of fixed ones, e.g. register the `UseBftSequencer` plugin in each suite | `4b37569e162` (#35999) |
| DB connection errors, `NoConnectionAvailable` | DB container or pool under load | Check whether other suites on the shard failed at the same time (`suite.log.gz` around `ci.date`); retry or raise pool timeouts only with a reason | |
| Many unrelated failures, OOM, GC pauses | Runner resources | Human checks Datadog for `ci.runner`; usually no code fix | |

## E. Production bugs, races and shutdown

| Symptom (grep signature) | Cause | Fix | Example |
|---|---|---|---|
| `PassiveInstanceException` (`DbStorage instance is not active`) | Early way to abort storage when a node becomes passive, predating `FutureUnlessShutdown`; some storage calls run outside code that handles it | Migrate the store to the `FutureUnlessShutdown`-returning storage methods (`Storage.scala`: `update`, `query`, ...), treating passivation as `AbortedDueToShutdown`, and propagate `FutureUnlessShutdown` upwards (can become a rabbit hole). In tests mix in `FailOnShutdown` or use `inUS`; in gRPC services use `.asGrpcResponse` (`CantonGrpcUtil`) | guide |
| `RejectedExecutionException: null` in a WARN/ERROR cause | A `map`/`flatMap` runs after its execution context was closed: not synchronized with shutdown | `synchronizeWithClosingF` and friends (`PerformUnlessClosing.scala`) on the resulting future; or `DirectExecutionContext` when the continuation is cheap | guide |
| `has not completed after` (`FutureSupervisor`), `still not completed after`, no progress until timeout | A promise that never completes, or `awaitSnapshot(ts)` with a future `ts` blocking the processing that would make `ts` available: deadlock | Use a timestamp in the past (e.g. the request time); if the snapshot is ready only after some promise completes, use `awaitSnapshotUSSupervised` | guide |
| Race between two production steps visible as wrong state in the test | Production ordering bug | Fix the order in production code; ask the user first | `c38097ee808` (#35987) |

## F. Not a flake: real failures

| Symptom (grep signature) | Cause | Fix | Example |
|---|---|---|---|
| Assertion contradicts the normal outcome, nothing racy in the log, would fail on every run | A recent change to the test or the code under test, often in a suite that runs only on `main` | Fix the test or the code; name the breaking commit (`git log <ci.sha> -- <paths>`) | |

## Reading logs

- Record format (`base/testing-utils/src/main/resources/logback-test.xml`): `<date> [<thread>] <LEVEL> <logger>:<Suite>/<node...> tid:<trace id> - <message>, context: ... err-context: ...`; Canton loggers log at DEBUG. Multi-line records (stack traces) continue on lines without a date.
- `canton.lnav.json` in the repo root parses it for lnav. lnav input filters (TAB, then `i`) are OR-ed, which helps to follow several tids at once.
- LogReporter (`community/testing/.../LogReporter.scala`) markers, INFO: `Starting test suite '<S>'`, `Starting '<S>/<t>'...`, `Test succeeded: '<S>/<t>'`, `Entering`/`Leaving '<scope>'`; WARN: `Test failed: '<S>/<t>'`, `Aborted test run.`
- Suppressed records are re-logged at INFO with the prefix `Suppressed WARN: ` or `Suppressed ERROR: `.
- `checkErrors` (`scripts/ci/check-logs.sh`) reports any WARN/ERROR record not matched by `project/errors-in-log-to-ignore.txt`.

## Repeating a test

- `sbt "testManyTimes <N> <test qualifier>"` (`build.sbt`, `testManyTimesCommand`): runs `truncateLogs; testOnly <q>; checkErrors` N times, stopping at the first failure; `log/` holds the last run only.
- `"<test>" taggedAs RepeatTest(n) in {...}` (`community/testing/.../RepeatableTest.scala`): repeats one test case up to n times or until it fails. Local reproduction only, do not commit it.
- `LOG_FILE_NAME=<file> LOG_APPEND=false` sends a run's log to a separate file.
- CI "Run a Test Many Times" pipeline (`contributing/5_ci-usage.md`): 50 runs on 10 runners, triggered by the human. Costly; use it before merging a new integration test or to confirm a fix for a rare flake.
