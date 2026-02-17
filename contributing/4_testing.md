Development Testing Methodology
===============================

We use a variety of strategies to test Canton during development in order to detect regressions or bugs in our code.
In summary:

* We use Scala as a functional and strongly typed language that reduces programming errors.
* We use the standard Scala linter [Wartremover](https://www.wartremover.org/) to find further programming errors at compile time.
* We use [additional custom Wartremover plugins](https://github.com/digital-asset/canton/tree/main/community/lib/wartremover/src/main/scala/com/digitalasset/canton) to extend the check surface.
* We use standard unit tests to check the functionality of individual components.
* We use integration tests to test our operational capabilities and create a diverse set of scenarios.
* We run tests under overload-load scenarios.
* We run crash recovery, fail-over and restart tests as part of our integration test suite. Here, we kill (-9 or graceful restart) nodes under full load and check that the workflows resumes after a restart.
* We run all Daml Ledger conformance tests on all database backends.
* We run protocol interleaving tests where we manually control the series of events that make up a scenario.
* We test time dependent functionality using static time / sim-clock tests.
* We perform negative tests where nodes have to deal with invalid / broken messages
* We perform security tests for a variety of scenarios, e.g. testing our authentication mechanism, correct TLS configurations or JWT authorization.
* We run Toxiproxy based tests, where we introduce artificial network issues between components and infrastructure (database), testing whether the artefacts properly behave under such scenarios.
* We test for data continuity by running tests against databases created with older versions.
* We run a series of tests against the packaged artifacts and the CLI arguments.
* We include our documentation in our testing, and we include tested configuration examples in our configs, rather than write them out as text.
* In our tests, we assert for correct error and warning messages, and we fail a build if an unaccounted error or warning message is emitted.
* We track our test coverage using a systematic test inventory (see the artifacts of the CI job `build_systematic_testing_inventory`).
  We try to systematically test every input message for all variants of possible attacks we can envision.
* We use our performance load generator tooling to create a heavily (over-)loaded system and run these systems for weeks under full load.
* Using above model, we run regression / performance tests after each merge to main (5/10 min) or nightly (30 min+).
* We run all our tests on main multiple times with various number of processors to test for instabilities and deadlocks.
* We run all tests, except a few very cpu intensive, sequentially with a single thread to find deadlocks.
* We run conformance tests against multiple versions of Postgres.
* We did pen and paper proofs / analysis of our architecture, proving privacy, transparency and integrity properties.
* We did partial theoretical verifications of our ledger model and our protocol.

Integration testing with the BFT sequencer
------------------------------------------

The BFT sequencer largely replaces the reference sequencer in integration tests and there are some important things
to keep in mind when using it:

- While the number of sequencers is not important when using the reference sequencer (which is really a centralized
  DB-based sequencer under the hood), it is very important when using the BFT sequencer
  (and decentralized sequencers in general) since it affects liveness and sequencing time behavior.
  - E.g.: sequencers set grows from 1 to 2, sequencer 2 enters the ordering topology but is still onboarding
    and unresponsive for a few seconds; during that time there is no quorum so nothing gets ordered and `getTime`
    is also stuck in the past. Only once sequencer 2 is caught up and responsive does the network resume ordering and `getTime`
    (and sequencing time) aligns again with the wall clocks after a few empty blocks.
  - This is an inherent problem of consensus-based sequencers (like BFT sequencers), i.e., sequencing time
    is also a result of consensus, so when no consensus is possible, sequencing time doesn't advance either.
  - Using 4 sequencers to start with (as to tolerate 1 unresponsive or otherwise broken sequencer node)
    and reducing the `consensusBlockCompletionTimeout` config (view change timeout) is a way to mitigate the issue.
    Another way is being tolerant to sequencing time lagging behind for a while.
- With the BFT sequencer, the base sequencing time of events in a block is determined based on the previous block.
  - This isn't generally a problem even in the absence of traffic because the BFT sequencers order empty blocks at
    regular time intervals to make sequencing time advance, This interval is set to 250ms in tests,
    so pretty short (the default for production is currently 5s).
  - However, when using the `SimClock`, it's not enough to advance it to obtain an up-to-date sequencing time
    for the subsequent events, but a block must also be ordered after advancing the clock but before
    sequencing those events, for example by submitting a time proof.
    - Consider that the BFT orderer ignores the `SimClock` and uses the local wall-clock for
      internally generated timeouts, like the timeout before ordering an empty block to let sequencing time advance,
      or the view change timeout.
      - On the one hand, this allows the protocol to work correctly.
      - On the other hand, since sequencing time is based on the wall-clock of the sequencers, if the
        `SimClock` is not advanced, sequencing time is only advancing minimally or not at all, regardless if
        there is traffic or if empty blocks are being ordered.
    - Many flakes of this kind have however been fixed already.

