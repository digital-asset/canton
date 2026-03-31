// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.manual.acs.commitment

import com.digitalasset.canton.admin.api.client.commands.ParticipantAdminCommands.Inspection.{
  SynchronizerTimeRange,
  TimeRange,
}
import com.digitalasset.canton.config
import com.digitalasset.canton.config.RequireTypes.{
  NonNegativeInt,
  NonNegativeProportion,
  PositiveInt,
}
import com.digitalasset.canton.config.{CommitmentSendDelay, RequireTypes}
import com.digitalasset.canton.console.{
  CommandFailure,
  LocalMediatorReference,
  LocalParticipantReference,
  LocalSequencerReference,
  SequencerReference,
}
import com.digitalasset.canton.data.CantonTimestampSecond
import com.digitalasset.canton.examples.java as M
import com.digitalasset.canton.examples.java.iou.Iou
import com.digitalasset.canton.integration.bootstrap.{
  NetworkBootstrapper,
  NetworkTopologyDescription,
}
import com.digitalasset.canton.integration.tests.acs.LargeAcsIntegrationTestBase
import com.digitalasset.canton.integration.tests.acs.commitment.util.ContractsAndCommitment.{
  IouCommitment,
  IouCommitmentWithContracts,
}
import com.digitalasset.canton.integration.tests.acs.commitment.util.{
  CommitmentTestUtil,
  ContractsAndCommitment,
  IntervalDuration,
  JFRTestPhase,
}
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.{
  ConfigTransforms,
  EnvironmentDefinition,
  TestConsoleEnvironment,
}
import com.digitalasset.canton.participant.ledger.api.client.JavaDecodeUtil
import com.digitalasset.canton.participant.pruning.AcsCommitmentProcessor.SentCmtState
import com.digitalasset.canton.participant.pruning.{ContractActive, ContractArchived}
import com.digitalasset.canton.participant.util.JavaCodegenUtil.*
import com.digitalasset.canton.protocol.messages.CommitmentPeriod
import com.digitalasset.canton.time.PositiveSeconds
import com.digitalasset.canton.util.MonadUtil
import monocle.Monocle.toAppliedFocusOps

import java.util.UUID
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import scala.collection.concurrent.TrieMap
import scala.concurrent.duration.*
import scala.concurrent.{Await, ExecutionContext}
import scala.util.{Failure, Success}

/** Given a large active contract set (ACS), we want to test the ACS commitment tools' performance
  * using the abstract test foundation below.
  *
  * Unless setting the *CI* env variables, this test uses local Postgres data store that is deployed
  * via TestContainers so Docker should be run in the background, see
  * [[LargeAcsIntegrationTestBase.dumpRestore]].
  *
  * In order to do ACS commitment tooling effectively on large AC sets, the contract creation time
  * on the nodes should be minimized that is why for a given
  * [[LargeAcsIntegrationTestBase.AcsTestSet.acsSize]],
  * [[LargeAcsIntegrationTestBase.AcsTestSet.temporaryContracts]] the test checks existing data in
  * the corresponding [[LargeAcsIntegrationTestBase.AcsTestSet.directory]] and import if data exists
  * there. If dumped data is not found, the contract creation takes time alongside with its eventual
  * export.
  *
  * ## Reproducible test run script
  *
  * There is shell script that can be executed to reproduce the test runs here:
  * [acs-scalability-test.sh](/resources/scripts/acs-scalability/acs-scalability-test.sh) This test
  * defaults to [[AcsCommitmentScalabilityTestOpenCommitment]] with the detailed JVM parameters and
  * the usage of the [profile.jfc](/resources/scripts/acs-scalability/profile.jfc) file. Ensure you
  * have `chmod +x` permissions before execution.
  *
  * Note: the JVM depends on your settings. Try to set your JDK_HOME or JAVA_HOME to bypass the
  * default JVM
  *
  * ## Test Topology
  *   - 2 participants
  *     - participant1 has *Bank* (and by default admin) parties
  *     - participant2 has *owner-0*...*19* parties
  *   - 1 mediator
  *   - 1 sequencer
  *
  * ## Notable environment variables used in the tests
  *   - `testAcsSize` - contains the initially deployed (or deployable) ACS size
  *   - `directorySuffix` - the suffix of the dump directory (under Canton's `tmp` folder), with the
  *     format `{testAcsSize}-temporary=0_{directorySuffix}`. For the ACS commitment scalability
  *     test it is recommended to use the same suffix for all tests that use the same `testAcsSize`
  *     so they can import data from the same dump files
  *   - `pruningCantonJournalDelay` - the pruning period in Canton journal store. On large sets the
  *     computation of acs might take significant time, so this settings should be approximately
  *     *8-10 times* higher (depending on how many times we advance the `simClock`) than the ACS
  *     commitment reconciliation period `reconciliationInterval`). This multiplication factor is
  *     coming from the fact that in the longest test currently we advance the simClock 6 times and
  *     it is good to leave a safety time gap for the pruning time, otherwise we might have some
  *     unpleasent surprises when we try to open a pruned commitment. In the environment
  *     configuration, the setting that uses this value is `journalGarbageCollectionDelay`
  *   - `internalPort` - all nodes use the same ports so that imported data can be reused without
  *     port issues
  *   - `enableAdditionalConsistencyChecks` - if set to `true`, the *AcsCommitmentProcessor* perform
  *     additional consistency checks. Ideally this should be turned off because it is memory
  *     intensive
  *   - `commitmentMismatchDebugging` - if set to `true`, similarly as for
  *     `enableAdditionalConsistencyChecks`, consistency checks are executed in the
  *     *AcsCommitmentProcessor*. Ideally this should be disabled.
  *   - `commitmentSendDelay` - after commitment is computed, we can delay its sending. Ideally this
  *     should be zero (no delay)
  *   - `reconciliationInterval` - The ACS commitment reconciliation time. For large ACS, the
  *     commitment computation can take time, which is why 10 minutes are used in the tests
  *
  * ## Foundational test data flow
  *
  * We have two **abstract foundational non-runnable test definitions** that serves as base for the
  * concrete runnable tests. [[AcsCommitmentScalabilityTestBase]] is one that has the test flow
  * detailed below. Check [[AcsCommitmentScalabilityTestMismatch]] for the other one.
  *
  * Create or import data based on [[testAcsSize]]
  *   - check if there is data in the dump directory
  *   - if there is no data, create the contracts (can be long process for a large set)
  *     - **P1** hosts party *Bank*, **P2** hosts party *Owner-0, ..., Owner-19*
  *     - Bank creates a specified number [[testAcsSize]] of active IOU contracts with the owners
  *   - if there is data, import it from the files (for large ACS this is much faster)
  *   - Check ACS commitment **match** between **P1** and **P2**
  *     - get ACS commitment computation time
  *     - get when the participants sent the related ACS commitment (because this should be 'not
  *       compared')
  *     - get received times (at that point the reconciliation can happen)
  *     - get the status of the commitments (should be matching at this point)
  *
  * ## JFR phasing
  *
  * The `jdk.jfr` package contains several helpful types, such as `Event`.
  *
  * In our tests, we wanted to label parts of the execution that might cause issues in our ACS
  * commitment tooling tests. For this reason, we mix in the [[JFRTestPhase]] trait, which provides
  * an implementation for surrounding code with JFR event recording and committing events with a
  * label. These phases can be nested. > Note: even without these phases, JFR recording still works.
  * Labeling phases helps us separate the code sections of interest when we open the recording in
  * `JMC`.
  *
  * The following phases were interesting to label in this test base (which is used and can be
  * extended by the concrete tests):
  *   - `acs-commitment-compute-after-deploy` - after the deployment of the [[testAcsSize]]
  *     contracts, the first computation of an ACS commitment might take some time and consume
  *     significant memory. Recording this phase helped us discover that
  *     [[com.digitalasset.canton.participant.pruning.AcsCommitmentProcessor]] was performing
  *     consistency checks, which became the bottleneck in the tests and concealed the memory issues
  *     of ACS commitment tooling (specifically the `open_commitment`).
  *   - `load-create-contracts` - the phase in which contracts are created or imported. Creation is
  *     significantly more time - and memory-sensitive than import. From a memory perspective,
  *     however, many objects created during batch creation and command submission are short-lived,
  *     so the GC can reclaim them relatively easily.
  *   - `restoring-database` - nested phase of restoring the state of our test network from the dump
  *     files (i.e. import).
  *   - `check-loaded-acs-size` - when checking the size of the loaded contracts, we download them
  *     into a Scala `Seq`. This can be memory-intensive.
  *   - `verify-state-match` - verifies that the ACS commitment states of P1 and P2 match. We are
  *     primarily interested in the time aspect of this call because the ACS commitment tooling used
  *     here does not load a large amount of data into memory.
  *
  * ## About the dumped test data
  *
  * Creating a large ACS and use open commitment on it is time-consuming. The creation of contracts
  * and investigating the participants with opening commitments, are memory intensive so please make
  * sure that the memory settings for running the tests is appropriate.
  *
  * As a reference, here are some example test data size produced by the export into dump files
  * (running [[AcsCommitmentScalabilityTestOpenCommitment]]), on an *MacBook M4 Pro, 48GB RAM* with
  * JVM (**GraalVM 21, G1GC**) params
  *   - -Xmx16G
  *   - -Xms8G
  *
  * using the command: `du -dh tmp`
  * {{{
  * | ACS size | dump dir size | data generation time |
  * |:---------|:--------------|:---------------------|
  * | 10k      | 84 MB         |                      |
  * | 100k     | 804 MB        |                      |
  * | 500k     | 3.9 GB        |                      |
  * | 700k     | 5.4 GB        |                      |
  * | 1m       | 7.7 GB        | ~42 mins             |
  * | 1.25m    | 9.6 GB        | ~48 mins             |
  * | 1.5m     | 12 GB         | ~52 mins             |
  * | 1.6m     | 12.8 GB       | ~55 mins             |
  * | 1.75m    | 14 GB         | ~62 mins             |
  * | 2m       | 16 GB         | ~69 mins             |
  * | 2.25m    | 18 GB         | ~79 mins             |
  * }}}
  *
  * ### Hint: {{{For testing with an ACS size of 100'000 active contracts or larger, you
  * definitively want to use a previously created database dump of the test. However, please note
  * that generating all the above data sets takes more than a workday!}}}
  *
  * ## Performance Bottlenecks in Test Execution
  *
  * At the time of writing, the ACS reconciliation between P1 and P2 is the most time-consuming
  * phase. The JFR phase 'check-loaded-acs-size' took almost 7 minutes for an ACS of 2 million, and
  * this duration is directly proportional to the ACS size. Because we perform similar checks after
  * an additional contract deployment and subsequent purge, this verification phase has a compounded
  * impact on the total test execution time.
  */
abstract class AcsCommitmentScalabilityTestBase(
    val testAcsSize: PositiveInt = PositiveInt.tryCreate(1000),
    // Changing this forces the dump files to be saved/checked against a different folder
    val directorySuffix: String = "AcsCommitmentScalabilityTest",
    // For large sets (millions of ACS) the computation time might be really long
    val pruningCantonJournalDelay: config.NonNegativeFiniteDuration =
      config.NonNegativeFiniteDuration.ofHours(2),
    // For large ACS, it is better to give time (10 minutes)
    // because the ACS commitment computation can take time
    val reconciliationInterval: PositiveSeconds = PositiveSeconds.tryOfSeconds(10 * 60),
) extends LargeAcsIntegrationTestBase
    with CommitmentTestUtil
    with JFRTestPhase {

  private lazy val maxCommandDeduplicationDuration = java.time.Duration.ofHours(1)

  // We are deploying Iou contracts and at some point we purge one from P2
  // When we do that that purged contract will be "missing" on P2
  // In order to be able to easily check against this missing contract in later tests we store it mutating this val
  protected val maybeMissingContract = new AtomicReference[Option[Iou.Contract]](None)
  // All the commitment by participant that later can be used for different test scenarios
  protected val iouCommitments =
    TrieMap[LocalParticipantReference, ContractsAndCommitment]()

  // We have many methods where the tick interval uses an implicit parameter
  protected implicit val interval: IntervalDuration = IntervalDuration(
    reconciliationInterval.duration
  )

  override protected def forceLocalPostgres: Boolean = true

  /** Test definition, in particular how many active contracts should be used */
  override protected val testSet: LargeAcsIntegrationTestBase.AcsTestSet =
    LargeAcsIntegrationTestBase.AcsTestSet(
      acsSize = testAcsSize,
      temporaryContracts = NonNegativeInt.zero,
      directorySuffix = directorySuffix,
    )

  override protected val baseEnvironmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2S1M1_Manual
      .addConfigTransforms(ConfigTransforms.allDefaultsButGloballyUniquePorts*)
      .addConfigTransforms(
        ConfigTransforms.useStaticTime,
        ConfigTransforms.updateMaxDeduplicationDurations(maxCommandDeduplicationDuration),
        ConfigTransforms.updateTargetTimestampForwardTolerance(24.hours),
        // Hard-coded ports ensure connectivity across node restarts. To save time,
        // participants are restored from database dumps which contain persisted
        // sequencer configurations. Static ports are required so these restored
        // nodes can successfully reconnect.
        ConfigTransforms.updateSequencerConfig("sequencer1")(cfg =>
          cfg
            .focus(_.publicApi.internalPort)
            .replace(Some(RequireTypes.Port.tryCreate(9018)))
            .focus(_.adminApi.internalPort)
            .replace(Some(RequireTypes.Port.tryCreate(9019)))
        ),
        ConfigTransforms.updateParticipantConfig("participant1")(cfg =>
          cfg
            .focus(_.ledgerApi.internalPort)
            .replace(Some(RequireTypes.Port.tryCreate(9011)))
            .focus(_.adminApi.internalPort)
            .replace(Some(RequireTypes.Port.tryCreate(9012)))
        ),
        ConfigTransforms.updateParticipantConfig("participant2")(cfg =>
          cfg
            .focus(_.ledgerApi.internalPort)
            .replace(Some(RequireTypes.Port.tryCreate(9021)))
            .focus(_.adminApi.internalPort)
            .replace(Some(RequireTypes.Port.tryCreate(9022)))
        ),
        // This pruning parameter is important for the tests where after pruning we would like to use ACS commitment tooling
        // this parameter allows us to determine a pruning time that can be advanced by a simClock
        ConfigTransforms.updateAllParticipantConfigs_(
          _.focus(_.parameters.journalGarbageCollectionDelay)
            .replace(pruningCantonJournalDelay)
        ),
        // Use distinct timeout values so that it is clear which timeout expired
        _.focus(_.parameters.timeouts.processing.unbounded)
          .replace(config.NonNegativeDuration.tryFromDuration(31.minute)),
        _.focus(_.parameters.timeouts.processing.network)
          // Addresses c.d.c.r.DbLockedConnection...=participant2/connId=pool-2 - Task connection check read-only did not complete within 2 minutes.
          .replace(config.NonNegativeDuration.tryFromDuration(32.minute)),
        _.focus(_.parameters.timeouts.console.bounded)
          // Addresses import_acs GrpcClientGaveUp: DEADLINE_EXCEEDED/CallOptions in was 3 min for 100_000
          .replace(config.NonNegativeDuration.tryFromDuration(33.minute)),
        _.focus(_.parameters.timeouts.console.unbounded)
          // Defaults to 3 minutes for tests (not enough for 250_000)
          // Addresses import_acs GrpcClientGaveUp: DEADLINE_EXCEEDED/CallOptions for ParticipantAdministration$synchronizers$.reconnect in was 3 min for 100_000
          .replace(config.NonNegativeDuration.tryFromDuration(34.minute)),
        // ACS computation with computeRunningCommitmentsFromAcs takes some extra time and memory
        // so we disable it -> why we have two with the same name?
        _.focus(_.parameters.enableAdditionalConsistencyChecks)
          .replace(false),
        // Disable the warnings for enabled consistency checks as we're importing a large ACS
        ConfigTransforms.updateAllParticipantConfigs_(
          _.focus(_.parameters.activationFrequencyForWarnAboutConsistencyChecks)
            .replace(Long.MaxValue)
        ),
        ConfigTransforms.updateAllParticipantConfigs_(
          // ACS computation with computeRunningCommitmentsFromAcs takes some extra time and memory
          // so we disable it -> why we have in engine the same name?
          _.focus(_.parameters.engine.enableAdditionalConsistencyChecks)
            .replace(false)
        ),
        ConfigTransforms.updateAllParticipantConfigs_(
          _.focus(_.parameters.commitmentMismatchDebugging)
            .replace(false)
        ),
      )
      // Disabling LAPI verification to reduce test termination time
      .updateTestingConfig(
        _.focus(_.participantsWithoutLapiVerification)
          .replace(
            Set(
              "participant1",
              "participant2",
            )
          )
          .focus(_.commitmentSendDelay)
          .replace(
            Some(
              CommitmentSendDelay(
                minCommitmentSendDelay = Some(NonNegativeProportion.zero),
                maxCommitmentSendDelay = Some(NonNegativeProportion.zero),
              )
            )
          )
      )

  private def loadContractsAndGetAcsCommitment(implicit
      env: TestConsoleEnvironment
  ): IouCommitment = {
    import env.*
    // here we are at T0, eg. 1970-01-01T00:00:00Z
    // tick1 = T1 (T0 + reconciliationInterval) eg. 1970-01-01T00:05:00Z if reconciliationTime is 5 minutes
    // T1 is the beginning of load/create contracts time
    // Advance to T1 (eg. 1970-01-01T00:05:00Z) + 1 microsecond and get T1
    val tick1 = advanceAndGetNextTick()

    // Recording JFR phase for collecting more sophisticated JVM stats what GC log can provide
    jfrPhaseTry("load-create-contracts") {
      loadOrCreateContracts(
        sequencers = List(sequencer1),
        mediators = List(mediator1),
        participants = List(participant1, participant2),
        List(EnvironmentDefinition.S1M1),
      )
    } match {
      case Success(_) =>
        // We advance to T2 + 1 microsecond and get T2
        val tick2 = advanceAndGetNextTick()

        // Poke the participant1 and participant2's synchronizer to advance there the time
        // If we don't fetch the time, participants wouldn't see the time advance.
        // This pokes and gives back the synchronizer time of T2
        participant1.testing.fetch_synchronizer_times()
        participant2.testing.fetch_synchronizer_times()

        // Even if we change the reconciliation time under 1 minute, it might be not enough for eventually blocks
        // We tie this maximum time to the `reconciliationInterval` because If the acs commitment computation is
        // more than the reconciliation time window, we should know about it (=timeout error) as it can lead to outstanding
        // state and later even other failures (even if the slow participants catches up with skipping computations).
        // This can also impact pruning.
        val maximumAcsComputationWaitTime =
          Math.max(reconciliationInterval.duration.toMillis, 1.minutes.toMillis)

        val computationStartedAt = System.nanoTime()
        val (commitmentPeriod, _, commitment) =
          // so the minimum eventually time until success is 1 minute
          jfrPhase("acs-commitment-compute-after-deploy") {
            eventually(timeUntilSuccess = maximumAcsComputationWaitTime.millis) {
              val lastComputed = participant1.commitments
                .computed(
                  daName,
                  tick1.toInstant,
                  tick2.toInstant,
                  Some(participant2),
                )
                .lastOption
                .getOrElse(fail("We should have at least one computed ACS commitment!"))

              // We are not interested in commitments for T0-T1, because the contracts are created in T1-T2
              lastComputed._1.toInclusive shouldEqual tick2

              lastComputed
            }
          }
        val computationEndedAt = System.nanoTime()
        logger.info(
          s"Poll rounded (from eventually) commitment computation for the loaded $testAcsSize ACS took ${TimeUnit.NANOSECONDS
              .toMillis(computationEndedAt - computationStartedAt)}ms!"
        )

        IouCommitment(commitmentPeriod, commitment)
      case Failure(ex) => fail(s"Loading or creating the contracts failed with ${ex.getMessage}")
    }
  }

  override protected def restoreState(
      sequencers: Seq[LocalSequencerReference],
      mediators: Seq[LocalMediatorReference],
      participants: Seq[LocalParticipantReference],
      networkTopologyDescriptions: Seq[NetworkTopologyDescription],
  )(implicit env: TestConsoleEnvironment, executionContext: ExecutionContext): Unit =
    handleStartupLogs {
      val nodes = env.mergeLocalInstances(participants, sequencers, mediators)
      nodes.foreach(_.stop())

      clue("Restoring database") {
        jfrPhase("restoring-database") {
          Await.result(MonadUtil.sequentialTraverse_(nodes)(restoreSnapshot(_)), 15.minutes)
        }
      }

      clue("Starting all nodes") {
        nodes.foreach(_.start())
        new NetworkBootstrapper(networkTopologyDescriptions*).bootstrap()
        eventually(timeUntilSuccess = 2.minutes, maxPollInterval = 15.seconds) {
          nodes.forall(_.is_initialized)
        }
      }

      participants.foreach(_.synchronizers.reconnect_all())

      clue(
        "Ensure that all participants are up-to-date with the state of the topology " +
          "at the given time as returned by the synchronizer"
      )(
        participants.foreach(_.testing.fetch_synchronizer_times())
      )
      // we don't have the ping here, just to avoid adding one more contract (ping means adding a contract to the involved participant(s))
    }

  override protected def localSequencerToParticipantRefsMap(implicit
      env: TestConsoleEnvironment
  ): Map[SequencerReference, List[LocalParticipantReference]] = Map(
    env.sequencer1 -> List(env.participant1, env.participant2)
  )

  override protected def networkTopologyDescription(implicit
      env: TestConsoleEnvironment
  ): List[NetworkTopologyDescription] = List(
    EnvironmentDefinition.S1M1(env)
  )

  override protected def createContracts()(implicit env: TestConsoleEnvironment): Unit = {
    import env.*

    // Enable parties, P1 has the Bank party
    val bank = participant1.parties.enable("Bank")
    val ownersCount = 20
    // P2 has the owner parties
    val owners = (0 until ownersCount).map(i => participant2.parties.enable(s"Owner-$i")).toVector

    // Create contracts
    val contractsDataset = Range.inclusive(1, testSet.acsSize.value)
    val batches = contractsDataset.grouped(testSet.creationBatchSize.value).toList
    val batchesCount = batches.size
    val temporaryContractsPerBatch =
      Math.ceil(testSet.temporaryContracts.value.toDouble / batchesCount).toInt

    // Round-robin on the owners
    val ownerIdx = new AtomicInteger(0)

    batches.foreach { batch =>
      val iouCommands = batch.map { amount =>
        val owner = owners(ownerIdx.getAndIncrement() % ownersCount)

        IouSyntax.testIou(bank, owner, amount.toDouble).create.commands.loneElement
      }
      participant1.ledger_api.javaapi.commands.submit(Seq(bank), iouCommands)
      val ledgerEnd = participant1.ledger_api.state.end()
      logger.debug(s"Ledger end time is $ledgerEnd")

      // Temporary contracts - even though we have 0 temporary contracts env setup, we let this in case later we change this
      if (temporaryContractsPerBatch > 0) {
        val temporaryContractsCreateCommands =
          Seq.fill(temporaryContractsPerBatch)(100.0).map { amount =>
            val owner = owners(ownerIdx.getAndIncrement() % ownersCount)
            IouSyntax.testIou(bank, owner, amount).create.commands.loneElement
          }
        val chip = JavaDecodeUtil.decodeAllCreated(M.iou.Iou.COMPANION)(
          participant1.ledger_api.javaapi.commands
            .submit(Seq(bank), temporaryContractsCreateCommands)
        )

        val archiveCommands = chip.map(_.id.exerciseArchive().commands().loneElement)

        participant1.ledger_api.javaapi.commands.submit(Seq(bank), archiveCommands)
      }
    }
  }

  protected def verifySentCommitmentState(
      p1LastSentCommitmentFrom: CantonTimestampSecond,
      p2LastSentCommitmentFrom: CantonTimestampSecond,
      p1ReceivedPeriod: CommitmentPeriod,
      p2ReceivedPeriod: CommitmentPeriod,
      verifyState: SentCmtState,
  )(implicit env: TestConsoleEnvironment) = {
    import env.*

    logger.debug(
      s"We should be able to see the ACS commitment that was sent from P1 to P2 and it should have a state $verifyState"
    )
    // We would like to verify the last sent ACS commitment of P1
    val p1SentAcsCommitment = eventually() {
      participant1.commitments
        .lookup_sent_acs_commitments(
          synchronizerTimeRanges = Seq(
            SynchronizerTimeRange(
              daId,
              Some(
                TimeRange(
                  p1LastSentCommitmentFrom.forgetRefinement.immediatePredecessor,
                  p1ReceivedPeriod.toInclusive.forgetRefinement,
                )
              ),
            )
          ),
          counterParticipants = Seq(participant2),
          commitmentState = Seq.empty,
          verboseMode = true,
        )
        .getOrElse(
          daId,
          fail(s"P1 sent ACS commitment should have element for '$daId' synchronizer"),
        )
        .lastOption
        .getOrElse(fail("P1 sent ACS commitment should have at least one element"))
    }

    p1SentAcsCommitment.sentCommitment.nonEmpty shouldBe true
    p1SentAcsCommitment.state shouldEqual (verifyState)

    logger.debug(
      "We should be able to see the ACS commitment that was sent from P2 to P1 and it should have a state match"
    )
    // We would like to verify the last sent ACS for P2
    val p2SentAcsCommitment = eventually() {
      participant2.commitments
        .lookup_sent_acs_commitments(
          synchronizerTimeRanges = Seq(
            SynchronizerTimeRange(
              daId,
              Some(
                TimeRange(
                  p2LastSentCommitmentFrom.forgetRefinement.immediatePredecessor,
                  p2ReceivedPeriod.toInclusive.forgetRefinement,
                )
              ),
            )
          ),
          counterParticipants = Seq(participant1),
          commitmentState = Seq.empty,
          verboseMode = true,
        )
        .getOrElse(
          daId,
          fail(
            s"Sent ACS commitment from P2 should have element that was sent through '$daId' synchronizer"
          ),
        )
        .lastOption
        .getOrElse(fail("Sent ACS commitments from P2 should have at least one element"))
    }

    p2SentAcsCommitment.sentCommitment.nonEmpty shouldBe true
    p2SentAcsCommitment.state shouldEqual (verifyState)
  }

  s"After deploying #$testAcsSize ACS between P1 and P2 participants" should {
    s"have matching ACS commitment" in { implicit env =>
      import env.*

      logger.debug(
        s"Loading/creating Iou contracts between parties in P1 and P2 and get the relevant ACS commitment period"
      )
      val p1DeployedIouCommitment @ IouCommitment(computedCommitmentPeriod, _) =
        loadContractsAndGetAcsCommitment

      logger.debug(s"The $testAcsSize contracts should be loaded into P1 and P2")
      eventually() {
        jfrPhase("check-loaded-acs-size") {
          // query with overriding the default limitation, which was 1000
          participant1.ledger_api.state.acs
            .of_all(limit = PositiveInt.MaxValue)
            .size shouldEqual testAcsSize.value
          participant2.ledger_api.state.acs
            .of_all(limit = PositiveInt.MaxValue)
            .size shouldEqual testAcsSize.value
        }
      }

      // For later checks we put the computed commitment into our commitments map
      iouCommitments.put(participant1, p1DeployedIouCommitment)

      // So far we got the computed period, but not the sending (which bases on commitmentSendDelay config params)
      logger.debug(
        "We should be able to see the last ACS commitment sent from P1 to P2"
      )
      val p1LastSentCommitment = eventually() {
        participant1.commitments
          .lookup_sent_acs_commitments(
            synchronizerTimeRanges = Seq(
              SynchronizerTimeRange(
                synchronizerId = daId,
                timeRange = Some(
                  TimeRange(
                    computedCommitmentPeriod.fromExclusive.forgetRefinement,
                    computedCommitmentPeriod.toInclusive.forgetRefinement,
                  )
                ),
              )
            ),
            counterParticipants = Seq(participant2),
            commitmentState = List.empty,
            verboseMode = true,
          )
          .getOrElse(daId, fail(s"P1 last sent ACS commitment for $daId is empty!"))
          .loneElement
      }

      // So far we got the P1 computed period, but not the P2 sending (which bases on commitmentSendDelay config params)
      logger.debug(
        "We should be able to see the last ACS commitment sent from P2 to P1"
      )
      val p2LastSentCommitment = eventually() {
        participant2.commitments
          .lookup_sent_acs_commitments(
            synchronizerTimeRanges = Seq(
              SynchronizerTimeRange(
                synchronizerId = daId,
                timeRange = Some(
                  TimeRange(
                    computedCommitmentPeriod.fromExclusive.forgetRefinement,
                    computedCommitmentPeriod.toInclusive.forgetRefinement,
                  )
                ),
              )
            ),
            counterParticipants = Seq(participant1),
            commitmentState = List.empty,
            verboseMode = true,
          )
          .getOrElse(daId, fail(s"P2 last sent ACS commitment for $daId is empty!"))
          .loneElement
      }

      // Here we check whether we received it (P1) because the sending via the sequencer might take (ideally short) time
      logger.debug(
        "We should be able to see the period when the last ACS commitment was received both for P1 and P2"
      )
      val p1ReceivedPeriod = eventually() {
        val p1ReceivedAcsCommitment = participant1.commitments
          .received(
            daName,
            // We received from P2
            p2LastSentCommitment.receivedCmtPeriod.fromExclusive.toInstant,
            p2LastSentCommitment.receivedCmtPeriod.toInclusive.toInstant,
          )
          .lastOption
          .getOrElse(fail("We expect to have at least one received ACS Commitment for P1"))

        p1ReceivedAcsCommitment.message.period
      }

      // Here we check whether we received it (P2) because the sending via the sequencer might take (ideally short) time
      val p2ReceivedPeriod = eventually() {
        val p2ReceivedAcsCommitment = participant2.commitments
          .received(
            daName,
            // We received from P1
            p1LastSentCommitment.receivedCmtPeriod.fromExclusive.toInstant,
            p1LastSentCommitment.receivedCmtPeriod.toInclusive.toInstant,
          )
          .lastOption
          .getOrElse(fail("We expect to have at least one received ACS Commitment for P2"))

        p2ReceivedAcsCommitment.message.period
      }

      // now is still at T2, we have the received periods which is important because from that point when a
      // participant sent out and received the counter participant commitment, we can see the state of them
      // otherwise the ACS commitment state can be 'NotCompared'
      jfrPhase("verify-state-match") {
        verifySentCommitmentState(
          p1LastSentCommitment.receivedCmtPeriod.fromExclusive,
          p2LastSentCommitment.receivedCmtPeriod.fromExclusive,
          p1ReceivedPeriod,
          p2ReceivedPeriod,
          // We expect to have 'Match' in both P1 and P2
          verifyState = SentCmtState.Match,
        )
      }
    }
  }
}

/** On top of the contracts deployment and checking ACS commitment match state, that is defined in
  * [[AcsCommitmentScalabilityTestBase]], here we deploy a new IOU contract and purge it from P2
  * Then we check the ACS commitment mismatch between P1 and P2.
  *
  * ## Foundational test data flow
  *
  *   1. Check ACS commitment **mismatch** between **P1** and **P2**
  *      - *deploy* one more IOU contract
  *      - *purge* the newly deployed contract from **P2**
  *      - check **mismatch** between P1 and P2
  *
  * This test stores the latest ACS commitments (after deploy and purge), in map entries with the
  * key of P1 and P2
  *
  * ## Specific JFR phases
  *
  * On top of what we have in [[AcsCommitmentScalabilityTestBase]], there is one more phase here:
  *   - `verify-state-mismatch` - similar to the `verify-state-match` phase, but used to check for
  *     mismatches
  */
abstract class AcsCommitmentScalabilityTestMismatch(
    testAcsSize: PositiveInt = PositiveInt.tryCreate(1000),
    directorySuffix: String = "AcsCommitmentScalabilityTest",
    pruningCantonJournalDelay: config.NonNegativeFiniteDuration =
      config.NonNegativeFiniteDuration.ofHours(2),
    reconciliationInterval: PositiveSeconds = PositiveSeconds.tryOfSeconds(10 * 60),
) extends AcsCommitmentScalabilityTestBase(
      testAcsSize,
      directorySuffix,
      pruningCantonJournalDelay,
      reconciliationInterval,
    ) {

  s"One missing active contract in P2 compared to P1" should {
    s"cause non matching ACS commitment state" in { implicit env =>
      import env.*

      // Before the deployment of this new contract, we advance our test clock to T3+1microsecond, so the contract is deployed at T3
      // We get back the computed time for it and the test clock eventually stands at T4+1 microsecond
      logger.debug("Deploy a new Iou contract between P1 adminParty and P2 adminParty")
      val p1DeployedIouCommitment @ IouCommitmentWithContracts(
        deployedContractSeq,
        _,
        _,
        _,
      ) =
        deployOneContractAndComputeAcsCommitment(
          daId,
          daName,
          participant1,
          participant2,
        )
      val newDeployedContract = deployedContractSeq.loneElement
      iouCommitments.put(participant1, p1DeployedIouCommitment)

      val purgeOnParticipant2 = purgeOneContractAndComputeAcsCommitment(participant2) _
      // This advances two ticks again, so the purge happens T5-T6, we are now at T6+1 microsecond
      val p2PurgedComputedIouCommitment @ IouCommitment(p2PurgedCommitmentPeriod, _) =
        purgeOnParticipant2(daName, newDeployedContract)

      logger.debug("P1 should have the new Iou contract, so one more contract in their ACS than P2")
      eventually() {
        participant1.ledger_api.state.acs
          .of_all(limit = PositiveInt.MaxValue)
          .size - 1 shouldEqual participant2.ledger_api.state.acs
          .of_all(limit = PositiveInt.MaxValue)
          .size
      }

      // Now that we purged and verified it, let's store the missing contract for further checks
      maybeMissingContract.set(Option(newDeployedContract))
      iouCommitments.put(participant2, p2PurgedComputedIouCommitment)

      logger.debug(
        "We should be able to see the (after purge) ACS commitment sent from P1 to P2"
      )
      val p1LastSentCommitment = eventually() {
        participant1.commitments
          .lookup_sent_acs_commitments(
            synchronizerTimeRanges = Seq(
              SynchronizerTimeRange(
                synchronizerId = daId,
                timeRange = Some(
                  TimeRange(
                    p2PurgedCommitmentPeriod.fromExclusive.forgetRefinement,
                    p2PurgedCommitmentPeriod.toInclusive.forgetRefinement,
                  )
                ),
              )
            ),
            counterParticipants = Seq(participant2),
            commitmentState = List.empty,
            verboseMode = true,
          )
          .getOrElse(daId, fail(s"P1 last sent ACS commitment for $daId is empty!"))
          .loneElement
      }

      logger.debug(
        "We should be able to see the (after purge) ACS commitment sent from P2 to P1"
      )
      val p2LastSentCommitment = eventually() {
        participant2.commitments
          .lookup_sent_acs_commitments(
            synchronizerTimeRanges = Seq(
              SynchronizerTimeRange(
                synchronizerId = daId,
                timeRange = Some(
                  TimeRange(
                    p2PurgedCommitmentPeriod.fromExclusive.forgetRefinement,
                    p2PurgedCommitmentPeriod.toInclusive.forgetRefinement,
                  )
                ),
              )
            ),
            counterParticipants = Seq(participant1),
            commitmentState = List.empty,
            verboseMode = true,
          )
          .getOrElse(daId, fail(s"P2 last sent ACS commitment for $daId is empty!"))
          .loneElement
      }

      // At T6 we should be able to receive the new ACS commitments for both participants
      logger.debug(
        "We should be able to see the period when the last ACS commitment was received both for P1 and P2"
      )
      val p1ReceivedPeriod = eventually() {
        val p1ReceivedAcsCommitment = participant1.commitments
          .received(
            daName,
            // We received from P2
            p2LastSentCommitment.receivedCmtPeriod.fromExclusive.toInstant,
            p2LastSentCommitment.receivedCmtPeriod.toInclusive.toInstant,
          )
          .loneElement

        p1ReceivedAcsCommitment.message.period
      }

      val p2ReceivedPeriod = eventually() {
        val p2ReceivedAcsCommitment = participant2.commitments
          .received(
            daName,
            // We received from P1
            p1LastSentCommitment.receivedCmtPeriod.fromExclusive.toInstant,
            p1LastSentCommitment.receivedCmtPeriod.toInclusive.toInstant,
          )
          .loneElement

        p2ReceivedAcsCommitment.message.period
      }

      // now is still at T6, we have the received periods which is important because from that point when a
      // participant sent out and received the counter participant commitment, we can see the state of them
      // otherwise the ACS commitment state can be still 'Match'
      jfrPhase("verify-state-mismatch") {
        verifySentCommitmentState(
          p1LastSentCommitment.receivedCmtPeriod.fromExclusive,
          p2LastSentCommitment.receivedCmtPeriod.fromExclusive,
          p1ReceivedPeriod,
          p2ReceivedPeriod,
          // P1 and P2 should not be agreed due to the purged contract
          SentCmtState.Mismatch,
        )
      }
    }
  }
}

/** As its name implies this method tests whether we can open commitment after running the base test
  * in [[AcsCommitmentScalabilityTestBase]]
  *
  * ## Test data flow
  *
  * On top of the first foundational test [[AcsCommitmentScalabilityTestBase]], we define one
  * runnable test here:
  *   1. In matching state, verify
  *      - if we can open a commitment
  *      - the result's list size, obtained by `open_commitment`, should match the [[testAcsSize]]
  *
  * ## Test run script
  *
  * ## Test result
  *
  * As a reference, here are some example test run times on already exported data using *MacBook M4
  * Pro, 48GB RAM* with **JVM GraalVM 21** params
  *   - -Xmx8G
  *   - -XX:+UnlockDiagnosticVMOptions
  *   - -XX:-DoEscapeAnalysis
  *   - -XX:+DebugNonSafepoints
  *   - -XX:StartFlightRecording=filename=test-[[LargeAcsIntegrationTestBase.AcsTestSet.acsSize]].jfr,dumponexit=true,maxsize=4g,stackdepth=512,settings=profile.jfc
  *   - -Xms4G
  *     {{{
  * | ACS size | total run time   | highest heap usage (post-GC) |
  * |----------|------------------|------------------------------|
  * | 10k      | 23 s             | 183 MB                       |
  * | 100k     | 1 min 48 s       | 1.23 GB                      |
  * | 500k     | 3 min 30 s       | 3.07 GB                      |
  * | 700k     | 4 min 45 s       | 4.6 GB                       |
  * | 1m       | 6 min 43 s       | 7.32 GB                      |
  * | 1.25m    | 7 min 48 s       | 7.4 GB                       |
  * | 1.3m     | 8 min 20 s       | 7.56 GB                      |
  * | 1.5m     | 9 min 27 s       | 7.38 GB                      |
  * | 1.6m     | 10 min 18 s      | 7.48 GB                      |
  * | 1.75m    | 11 min 37 s      | 8 GB                         |
  * | 1.85m    | 11 min 41 s      | 8 GB                         |
  * | 2m       | 13 min           | 8 GB                         |
  * | 2.25m    | 43 min 1 s (died)| +8GB (OOM caused)            |
  *     }}}
  *
  * The memory bottleneck in these tests were in the phase of `open-one-commitment-phase` that held
  * a lot of protobuf byte array and gives some headaches for the GC. However, as we can see in the
  * results, the used highest heap size after GC is high starting from 1 million ACS and lasts even
  * at 2 million ACS records. This is a broad range.
  *
  * The reason why it didn't OOM for this range is that opening a commitment loads a lot of data in
  * the background that short-lived (part of TLAB) and therefore can be cleaned relatively quickly.
  * The highest gaps in the time increase reflects that the GC has more and more pauses and so the
  * GC time is increasing yet it is harder to find the amount which causes OOM as G1GC works more
  * when the allocated total memory is closer to the maximum heap size.
  *
  * In **JMC** on the relevant `jfr` file you can see the estimated allocated object sizes after
  * filtering on the times of the phase in the `Object Allocation Samples` in the Event Browser.
  * Also, the post-GC size can be checked in the Garbage Collector left side menu panel.
  *
  * With the given JVM settings the maximum size we can process is 2 million
  *
  * At 2.25 million ACS, after 16 mins run the test started to throw OOM (Out Of Memory) exceptions.
  *
  * These OOM reports eventually caused the early termination, and failure of the test run, although
  * it is worth to mention that GC struggles a lot before it "gives up". For 2.25 million ACS,
  * starting from the first OOM message the test terminated after 15 minutes spending on trying to
  * solve the memory issue.
  *
  * It is worth to wait until the end of the process as OOM issues eventually terminates the JVM
  * process with SIGINT/SIGTERM. This allows JFR to dump the recorded data at exit. On the contrary,
  * based on my experience, the test interruption in IntelliJ IDEA propagates quickly to SIGKILL
  * which does not allow a graceful termination (thus not dumping the jfr data into the file)
  *
  * ## The used JVM options explanation
  *   - `-Xmx8G` - maximum heap size
  *   - `-XX:-DoEscapeAnalysis` - helps reveal allocations
  *   - `-XX:+UnlockDiagnosticVMOptions` - gives better sampling for short/inlined methods
  *   - `-XX:+DebugNonSafepoints` - gives better sampling for short/inlined methods
  *   - `-XX:StartFlightRecording=filename=test-[[testAcsSize]].jfr,dumponexit=true,maxsize=4g,stackdepth=512,settings={path_to}/profile.jfc`
  *     \- more on this in the section below
  *   - `-Xms4G` - initial (minimum) heap size
  *
  * ## JVM performance testing with recording JFR - some explanation
  *
  * In order to be able to find the memory bottleneck of tooling, we should enable JFR (Java Flight
  * Recording). The JVM parameter that allows us to do it is `-XX:StartFlightRecording` which - in
  * our tests - contains some options. Listing here what we used - and why:
  *   - **filename=test-[[testAcsSize]].jfr** - in order to open the recorded JVM sample data in JMC
  *     (JDK Mission Control). We have to save it into a file. The file name ideally reflects the
  *     run size that is why it has the [[testAcsSize]].
  *   - **dumponexit=true** - the data is recorded into memory and dumped at the end of the process
  *     (if it is not killed, but interrupted)
  *   - **maxsize=4g** - increases total buffer (default ~250MB), which prevents early discard
  *     during memory spikes.
  *   - **stackdepth=512** - Default is 64. Many Scala call chains (e.g., `Future.map, flatMap`) are
  *     deeper than that
  *   - **settings={path-to-profile}/profile.jfc** - this file contains the JFR sampling events,
  *     options and selections. If we use simply `profile` as a value it tries to use the default
  *     `jfc` file in the used JDK. Nevertheless, some JDK doesn't have it and the default profiling
  *     doesn't allow us to see some details clearly so it was really important to use a specific
  *     configuration file.
  *
  * ### Test specific JFR phases
  *
  * On top of what we have in [[AcsCommitmentScalabilityTestBase]], there is one specific phase
  * here:
  *   - `open-one-commitment-phase` - as its name implies, this is the phase when we open ONLY one
  *     ACS commitment. Open means we load many data into memory in the process and return the
  *     shared contracts and reassignment counters.
  *
  * ## JFR settings
  *
  * Please find the comments in the under the resources/scripts/acs-scalability-test/profile.jfc
  *
  * With the settings in the referred profile.jfc file the generated `jfr` file for over a million
  * ACS can be greater than 100MB. For 2 million it reached 180 MB, but this strongly depends on the
  * test run time
  */
final class AcsCommitmentScalabilityTestOpenCommitment
    extends AcsCommitmentScalabilityTestBase(testAcsSize = PositiveInt.tryCreate(10_000)) {

  s"When we are in a matching ACS commitment state between P1 and P2" should {
    s"be able to open commitment on P1 which returns a $testAcsSize long sequence" in {
      implicit env =>
        import env.*

        // Open commitment loads the contracts into the memory
        jfrPhase("open-one-commitment-phase") {
          // get the P1 computed commitment after the initial deployment
          val p1IouCommitment = iouCommitments.getOrElse(
            participant1,
            fail("We should have recorded commitment for participant1!"),
          )

          logger.debug("P2 open commitment should NOT contain the missing contract id")
          val p1OpenedCommitment = participant1.commitments.open_commitment(
            commitment = p1IouCommitment.commitment,
            physicalSynchronizerId = synchronizer1Id,
            timestamp = p1IouCommitment.commitmentPeriod.toInclusive.forgetRefinement,
            counterParticipant = participant2,
          )

          p1OpenedCommitment.size shouldEqual testAcsSize.value
        }
    }
  }
}

/** This test simulates the method calls that an investigation about the mismatch(ed) ACS commitment
  * state involves. This is an edge case as normally an operator doesn't have permissions to open
  * both disagreeing participants' commitments.
  *
  * The investigation doesn't need to find the reason of the mismatch because we have the recorded
  * purged (names 'missing') contract stored in a value definition. In this way we just ensure that
  * calling 'open_commitment' on P1 and P2 contains - and NOT contains the missing contract id in
  * the open commitment result.
  *
  * ## Test data flow
  *
  * On top of the mismatch state defined in [[AcsCommitmentScalabilityTestMismatch]], this edge case
  * test:
  *   1. Use ACS commitment tooling (eg. `open_commitment`) to simulate an investigation
  *      - use `commitments.inspect_commitment_contracts` on the purged contract
  *      - **P1** should give back an **Active** state on it
  *      - **P2** should give back an **Active** then **Archived** status on it
  *      - use `commitments.open_commitment` on the commitment after the deployment and purge
  *        respectively on **P1** and **P2**
  *      - since this call gives back all the contracts therefore calling it on **P1** *should
  *        contain* the purged contract id from **P2**
  *      - while **P1** *should NOT contain* the same contract id
  *
  * ## Test specific JFR phases
  *
  * The following phases were specific on top of what we have in
  * [[AcsCommitmentScalabilityTestMismatch]]:
  *   - `compute-one-deployed-contract` - deploys an additional contract and computes the subsequent
  *     ACS commitment. We are mainly interested in the time aspect of this phase.
  *   - `jfrPhase("compute-on-one-purged-contract") {` - purges a contract and computes the
  *     subsequent ACS commitment. Again, we are mainly interested in the time aspect of this phase.
  *   - `open-commitments-phase` - opens commitments from both P1 and P2 (mimicking a special
  *     investigation). This represents a scenario where an operator queries both participants
  *     simultaneously (an edge case). This effectively doubles the GC effort to mark and sweep
  *     memory allocations because two `open_commitment` calls are executed.
  *
  * ## Test result
  *
  * As a reference, here are some example test run times on already exported data using MacBook M4
  * Pro, 48GB RAM with JVM (GraalVM-21) params
  * -Xmx8G
  * -XX:+UnlockDiagnosticVMOptions
  * -XX:-DoEscapeAnalysis
  * -XX:+DebugNonSafepoints
  * -XX:StartFlightRecording=filename=test-[[LargeAcsIntegrationTestBase.AcsTestSet.acsSize]].jfr,dumponexit=true,maxsize=4g,stackdepth=512,settings=profile.jfc
  * -Xms4G
  * {{{
  * | ACS size | total run time    | highest heap usage (post-GC) |
  * |----------|-------------------|------------------------------|
  * | 10k      | 30 s              | 202 MB                       |
  * | 100k     | 1 min 25 s        | 1.6 GB                       |
  * | 500k     | 5 min 15 s        | 3.29 GB                      |
  * | 700k     | 7 min 6 s         | 7.2 GB                       |
  * | 1m       | 10 min 21 s       | 7.64 GB                      |
  * | 1.25m    | 14 min 12 s       | 7.65 GB                      |
  * | 1.5m     | 15 min 58 s       | 7.43 GB                      |
  * | 1.75m    | 17 min 14 s       | 7.98 GB                      |
  * | 2m       | 23 min 54 s (died)| +8GB (OOM)                   |
  * }}}
  *
  * On the above development environment, the test starts to misbehave / expose issues starting at 2
  * millions or more active contracts.
  *
  * In this test we call `open_commitment` two times sequentially. Between the calls, the references
  * and the allocated space can be cleaned by GC. That is why the "highest heap usage (post-GC)" is
  * similar than in the [[AcsCommitmentScalabilityTestOpenCommitment]] test case. However, the GC
  * had more work to do (mismatch counting then two `open_commitment` calls) that is why it died
  * with the following OOM on a little bit smaller ACS than the
  * [[AcsCommitmentScalabilityTestOpenCommitment]] test
  *
  * About the JVM, JFR settings and explanation you can also read
  * [[AcsCommitmentScalabilityTestOpenCommitment]]
  */
final class AcsCommitmentScalabilityTestInvestigation
    extends AcsCommitmentScalabilityTestMismatch(testAcsSize = PositiveInt.tryCreate(10_000)) {
  s"When we are in a non matching ACS commitment state between P1 and P2" should {
    s"be able to investigate properly with opening the commitments and finding the non matching contract" in {
      implicit env =>
        import env.*

        val missingContractOnP2 =
          maybeMissingContract
            .get()
            .getOrElse(fail(s"We should have the purged, missing contract!"))

        // Let's inspect the state of the purged commitment on P1 and P2
        val p1InspectionOnMissingContract = participant1.commitments
          .inspect_commitment_contracts(
            contracts = List(missingContractOnP2.id.toLf),
            timestamp = environment.now,
            expectedSynchronizerId = synchronizer1Id,
          )
          .loneElement

        p1InspectionOnMissingContract.cid shouldEqual missingContractOnP2.id.toLf
        p1InspectionOnMissingContract.state.loneElement
        p1InspectionOnMissingContract.state.headOption.fold(
          fail(s"${missingContractOnP2.id} shouldn't be missing on P1")
        )(_.contractState) shouldBe a[ContractActive]

        val p2InspectionOnMissingContract = participant2.commitments
          .inspect_commitment_contracts(
            contracts = List(missingContractOnP2.id.toLf),
            timestamp = environment.now,
            expectedSynchronizerId = synchronizer1Id,
          )
          .loneElement

        p2InspectionOnMissingContract.cid shouldEqual missingContractOnP2.id.toLf
        p2InspectionOnMissingContract.state.size shouldBe 2
        p2InspectionOnMissingContract.state(0).contractState shouldBe a[ContractActive]
        p2InspectionOnMissingContract.state(1).contractState shouldBe a[ContractArchived]

        // get the P1 computed commitment after the deployment of the additional contract
        // Note: P1 never purged this new contract
        val p1IouCommitment = iouCommitments.getOrElse(
          participant1,
          fail("We should have recorded commitment for participant1!"),
        )

        // Open commitment loads the contracts into the memory, both from P1 and P2 which requires roughly twice as big
        // free memory what we had during the contract creating phase
        jfrPhase("open-commitments-phase") {
          logger.debug("P1 open commitment should contain the missing contract id from P2")
          val p1OpenedCommitment = participant1.commitments.open_commitment(
            commitment = p1IouCommitment.commitment,
            physicalSynchronizerId = synchronizer1Id,
            timestamp = p1IouCommitment.commitmentPeriod.toInclusive.forgetRefinement,
            counterParticipant = participant2,
          )

          p1OpenedCommitment.map(_.cid) should contain(missingContractOnP2.id.toLf)

          // get the P2 computed commitment after the purge of one contract
          val p2IouCommitment = iouCommitments.getOrElse(
            participant2,
            fail("We should have recorded commitment for participant2!"),
          )

          logger.debug("P2 open commitment should NOT contain the missing contract id")
          val p2OpenedCommitment = participant2.commitments.open_commitment(
            commitment = p2IouCommitment.commitment,
            physicalSynchronizerId = synchronizer1Id,
            timestamp = p2IouCommitment.commitmentPeriod.toInclusive.forgetRefinement,
            counterParticipant = participant1,
          )

          p2OpenedCommitment.map(_.cid) should not contain (missingContractOnP2.id.toLf)
        }
    }
  }
}

/** Given a mismatch state and a computed commitment on it, this test makes sure that, once we pass
  * the next prune period and deploy a new contract (in this case a ping), then we cannot open the
  * commitment that was computed before the pruning
  *
  * ## Test data flow
  *
  * On top of the mismatch state defined in [[AcsCommitmentScalabilityTestMismatch]], there is this
  * edge case test:
  *   1. Wait for a prune period, add a contract and try to open an old ACS commitment that was
  *      computed before the pruning
  *      - this checks `open_commitment`'s correct behavior (failure) after pruning and deploying
  *        new contract
  *      - advance after pruning time (post prune time with a few seconds is enough)
  *      - deploy a `ping` contract from **P1** to **P2**
  *      - using `commitments.open_commitment` on an old commitment should raise a specific
  *        `CommandFailure` error
  *
  * ## Test specific JFR phases
  *
  * On top of what phases we have in [[AcsCommitmentScalabilityTestMismatch]], there is one more
  * phase here:
  *   - `open-commitment-after-prune` - after pruning and deploying an additional (ping) contract,
  *     running `open_commitment` on a pre-prune commitment should fail. This phase is mainly
  *     interesting from a run time perspective. However, if `open_commitment` succeeds before
  *     pruning and/or additional contract deployment, it can still cause significant memory
  *     allocation.
  */
class AcsCommitmentScalabilityTestPruning
    extends AcsCommitmentScalabilityTestMismatch(testAcsSize = PositiveInt.tryCreate(10_000)) {
  s"After we pass the prune period and we deploy new contracts" should {
    s"not be able to open a pre-prune commitment" in { implicit env =>
      import env.*

      // Wait for pruning time (add extra seconds for making sure the pruning has time to complete)
      val testClock =
        environment.simClock.getOrElse(
          fail("No simClock in the test environment! Hint: set useStatic in the test config?")
        )
      testClock.advance(pruningCantonJournalDelay.plusSeconds(1).asJava)

      participant1.parties.enable(UUID.randomUUID().toString, synchronizer = Some(daName))
      participant1.testing.fetch_synchronizer_times()
      participant2.testing.fetch_synchronizer_times()

      // we need to deploy at least one more contract to see if the open commitment fails
      // this should succeed
      eventually() {
        participant1.health.maybe_ping(participant2).nonEmpty shouldBe true
      }

      // get the P1 computed commitment after the deployment of the additional contract that is purged from P2
      val p1IouCommitment = iouCommitments.getOrElse(
        participant1,
        fail("We should have recorded commitment for participant1!"),
      )

      logger.debug("P1 open commitment should fail due to the performed pruning")
      jfrPhase("open-commitment-after-prune") {
        eventually() {
          loggerFactory.assertThrowsAndLogsSeq[CommandFailure](
            participant1.commitments.open_commitment(
              commitment = p1IouCommitment.commitment,
              physicalSynchronizerId = synchronizer1Id,
              timestamp = p1IouCommitment.commitmentPeriod.toInclusive.forgetRefinement,
              counterParticipant = participant2,
            ),
            logEntries => {
              logEntries.exists(entry =>
                entry.errorMessage.contains(
                  "Active contract store for synchronizer"
                ) && entry.errorMessage.contains(
                  "has been pruned up"
                )
              ) shouldBe true
            },
          )
        }
      }
    }
  }
}
