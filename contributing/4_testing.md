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
