// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.manual.acs

import better.files.*
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.config
import com.digitalasset.canton.config.RequireTypes
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.console.{LocalParticipantReference, SequencerReference}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.examples.java as M
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.bootstrap.NetworkTopologyDescription
import com.digitalasset.canton.integration.tests.acs.LargeAcsIntegrationTestBase
import com.digitalasset.canton.integration.tests.acs.LargeAcsIntegrationTestBase.AcsTestSet
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.util.PartyToParticipantDeclarative
import com.digitalasset.canton.logging.NodeLoggingUtil
import com.digitalasset.canton.participant.admin.data.ContractImportMode
import com.digitalasset.canton.participant.ledger.api.client.JavaDecodeUtil
import com.digitalasset.canton.time.PositiveSeconds
import com.digitalasset.canton.topology.transaction.ParticipantPermission as PP
import com.digitalasset.canton.util.FutureInstances.parallelFuture
import com.digitalasset.canton.util.{MonadUtil, SingleUseCell}
import monocle.Monocle.toAppliedFocusOps
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.time.{Minutes, Span}

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

/** Tests the export and import of a large Active Contract Set (ACS).
  *
  * IMPORTANT: This does NOT implement a proper offline party replication, as that is not the focus
  * of this test.
  *
  * The tooling supports generating "temporary contracts" - contracts that are created and archived
  * immediately (and thus do not appear in the ACS snapshot). This is useful for measuring how
  * archived, not-yet-pruned contracts impact export performance. Therefore, background pruning is
  * explicitly disabled in this environment.
  *
  * Raison d'être: Maintain readily available test code in the repository for on-demand performance
  * investigations. We are not aiming to assert a specific performance target in a regular CI
  * pipeline; rather, this provides a sandbox for bulk ACS operations.
  *
  * Test setup:
  *   - Topology: 3 Participants (P1, P2, P3) with a single mediator and a single sequencer.
  *   - P1 hosts party `Bank`; P2 hosts parties `Owner-0` through `Owner-19`.
  *   - `Bank` creates a specified number of active IOU contracts with the owners.
  *   - `Bank` is subsequently authorized to be hosted on P3.
  *   - P1 exports `Bank`'s ACS to a file.
  *   - P3 imports `Bank`'s ACS from the file.
  *
  * The core execution flow is implemented by [[LargeAcsExportAndImportTest]].
  *
  * Generating a massive ACS from scratch is time-consuming. To accelerate repeated test executions,
  * you can use [[LargeAcsCreateContractsTest]] to pre-generate the contracts (based on
  * [[AcsTestSet.acsSize]]) and persist the nodes' states as database dump files. When dump files
  * exist for a given [[AcsTestSet.name]], [[LargeAcsExportAndImportTest]] will bypass contract
  * creation and restore the database from these dumps instead. Without existing dumps, the test
  * defaults to creating the contracts from scratch (which is how it executes in CI).
  *
  * Hint: For testing with an ACS size of 10,000 active contracts or larger, you definitively want
  * to use a previously created database dump of the test network.
  *
  * Example generation times (developer notebook, 28 Jul 2026):
  * {{{
  * Contract size |             create [s] |            DB dump [s] |
  *          1000 |                     22 |                     11 |
  *        10_000 |                     31 |                     13 |
  *       100_000 |                    102 |                     33 |
  *       150_000 |                    121 |                     41 |
  *       200_000 |                    158 |                     48 |
  *       250_000 |                    206 |                     60 |
  *       300_000 |                    222 |                     68 |
  *       400_000 |                    281 |                     87 |
  *       500_000 |                    349 |                    102 |
  *     1_000_000 |                    767 |                    193 |
  *             N | T ≈ 0.00073 * N + 12.8 | T ≈ 0.00018 * N + 12.8 |
  * }}}
  *
  * Example execution times (developer notebook, 28 Jul 2026):
  * {{{
  *  ACS size | dump restore [s] | acs_export [s] | acs_import [s] | reconnect [s] |
  *    10_000 |               25 |              1 |              2 |             1 |
  *   100_000 |               40 |              5 |              6 |            10 |
  *   150_000 |               40 |              5 |              9 |            22 |
  *   200_000 |               43 |              9 |             14 |            43 |
  *   250_000 |               47 |              8 |             14 |            57 |
  *   300_000 |               51 |             10 |             17 |            75 |
  *   400_000 |               62 |             13 |             22 |           118 |
  *   500_000 |               68 |             17 |             26 |           162 |
  * 1_000_000 |              106 |             35 |             54 |           762 |
  *
  * Approximations:
  *   dump restore: T ≈ 0.000079 * N + 28.0
  *     acs_export: T ≈ 0.000034 * N + 0.5
  *     acs_import: T ≈ 0.000052 * N + 1.4
  *      reconnect: T ≈ 7.6e-10 * N^2 (Exhibits O(N^2) complexity)
  * }}}
  */
protected abstract class LargeAcsExportAndImportTestBase extends LargeAcsIntegrationTestBase {

  val numThreads = Threading.detectNumberOfThreads(noTracingLogger)

  // TODO(#27707) - Remove when ACS commitments consider the onboarding flag
  // A party replication is involved, and we want to minimize the risk of warnings related to acs commitment mismatches
  override protected val reconciliationInterval = PositiveSeconds.tryOfDays(365 * 10)

  override protected def forceLocalPostgres: Boolean = true

  override protected def localSequencerToParticipantRefsMap(implicit
      env: TestConsoleEnvironment
  ): Map[SequencerReference, List[
    LocalParticipantReference
  ]] = Map(
    env.sequencer1 -> List(env.participant1, env.participant2, env.participant3)
  )

  override protected def networkTopologyDescription(implicit
      env: TestConsoleEnvironment
  ): List[NetworkTopologyDescription] = List(
    EnvironmentDefinition.S1M1(env)
  )

  override protected val baseEnvironmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3S1M1_Manual
      .addConfigTransforms(ConfigTransforms.allDefaultsButGloballyUniquePorts*)
      .addConfigTransforms(
        ConfigTransforms.disableAdditionalConsistencyChecks
      )
      .addConfigTransforms(
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
        ConfigTransforms.updateParticipantConfig("participant3")(cfg =>
          cfg
            .focus(_.ledgerApi.internalPort)
            .replace(Some(RequireTypes.Port.tryCreate(9031)))
            .focus(_.adminApi.internalPort)
            .replace(Some(RequireTypes.Port.tryCreate(9032)))
        ),
        // Disable background pruning
        ConfigTransforms.updateAllParticipantConfigs_(
          _.focus(_.parameters.journalGarbageCollectionDelay)
            .replace(config.NonNegativeFiniteDuration.ofDays(365 * 100))
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
        // Disable the warnings for enabled consistency checks as we're importing a large ACS
        ConfigTransforms.updateAllParticipantConfigs_(
          _.focus(_.parameters.activationFrequencyForWarnAboutConsistencyChecks)
            .replace(Long.MaxValue)
        ),
      )
      // Disabling LAPI verification to reduce test termination time
      .updateTestingConfig(
        _.focus(_.participantsWithoutLapiVerification).replace(
          Set(
            "participant1",
            "participant2",
            "participant3",
          )
        )
      )

  override protected def createContracts()(implicit
      env: TestConsoleEnvironment
  ): Unit = {
    import env.*

    // Enable parties
    val bank = participant1.parties.enable("Bank")
    val ownersCount = 20
    val owners = (0 until ownersCount).map(i => participant2.parties.enable(s"Owner-$i")).toVector

    // Create contracts
    val contractsDataset = Range.inclusive(1, testSet.acsSize.value)
    val batchesCount =
      Math.ceil(contractsDataset.size.toDouble / testSet.creationBatchSize.value).toInt
    val temporaryContractsPerBatch =
      Math.ceil(testSet.temporaryContracts.value.toDouble / batchesCount).toInt

    // Round-robin on the owners
    val ownerIdx = new AtomicInteger(0)

    val parallelism = numThreads
    val chunkSize = testSet.creationBatchSize

    val processBatch = { (batch: Seq[Int]) =>
      Future {
        scala.concurrent.blocking {
          val start = System.nanoTime()
          val iousCommands = batch.map { amount =>
            val owner = owners(ownerIdx.getAndIncrement() % ownersCount)
            IouSyntax.testIou(bank, owner, amount.toDouble).create.commands.loneElement
          }

          participant1.ledger_api.javaapi.commands.submit(Seq(bank), iousCommands)

          // Temporary contracts
          if (temporaryContractsPerBatch > 0) {
            val temporaryContractsCreateCmds =
              Seq.fill(temporaryContractsPerBatch)(100.0).map { amount =>
                val owner = owners(ownerIdx.getAndIncrement() % ownersCount)
                IouSyntax.testIou(bank, owner, amount).create.commands.loneElement
              }
            val chip = JavaDecodeUtil.decodeAllCreated(M.iou.Iou.COMPANION)(
              participant1.ledger_api.javaapi.commands
                .submit(Seq(bank), temporaryContractsCreateCmds)
            )

            val archiveCmds = chip.map(_.id.exerciseArchive().commands().loneElement)

            participant1.ledger_api.javaapi.commands.submit(Seq(bank), archiveCmds)
          }

          val ledgerEnd = participant1.ledger_api.state.end()
          val end = System.nanoTime()
          logger.info(
            s"Batch: ${batch.head} to ${batch.last} took ${TimeUnit.NANOSECONDS
                .toMillis(end - start)}ms and ledger end = $ledgerEnd"
          )
        }
      }
        .map(_ => ())
    }

    val futureResult =
      MonadUtil.batchedSequentialTraverse_(parallelism, chunkSize)(contractsDataset)(processBatch)

    // Await completion of all concurrent batches
    futureResult.futureValue(Timeout(Span(30, Minutes)))

    // Sanity checks
    participant1.ledger_api.state.acs
      .of_party(bank, limit = PositiveInt.MaxValue)
      .size shouldBe testSet.acsSize.value
    participant2.ledger_api.state.acs
      .count() shouldBe testSet.acsSize.value
  }
}

/** A "test" that first creates an ACS for Bank on P1 and the owners on P2, and then stores that
  * state as a database dump.
  *
  * Restoring a database dump for an ACS with 10'000 or more contracts is much faster than
  * (re)creating those active contracts for every test run (see [[LargeAcsExportAndImportTest]]).
  *
  * The number of created active contracts is defined by the [[AcsTestSet]].
  */
protected abstract class DumpTestSet extends LargeAcsExportAndImportTestBase {

  // Use INFO log level to save storage space, switch to DEBUG as needed
  NodeLoggingUtil.setLevel(level = "INFO")

  override protected def environmentDefinition: EnvironmentDefinition =
    baseEnvironmentDefinition.withSetup { implicit env =>
      testSetup()
    }

  s"create ${testSet.acsSize.value} active contracts" in { implicit env =>
    createContracts()
  }

  "create database dump" in { implicit env =>
    import env.*

    createSnapshot(
      Seq(sequencer1),
      Seq(mediator1),
      Seq(participant1, participant2, participant3),
    )
  }
}

protected abstract class EstablishTestSet extends LargeAcsExportAndImportTestBase {

  protected def testContractIdImportMode: ContractImportMode

  // Use INFO log level to save storage space, switch to DEBUG as needed
  NodeLoggingUtil.setLevel(level = "INFO")

  // Replicate Bank from P1 to P3
  private val acsExportFile = new SingleUseCell[File]
  private val ledgerOffsetBeforePartyOnboarding = new SingleUseCell[Long]

  s"restore state from database dump or create contracts for ${testSet.name}" in { implicit env =>
    import env.*

    loadOrCreateContracts(
      sequencers = Seq(sequencer1),
      mediators = Seq(mediator1),
      participants = Seq(participant1, participant2, participant3),
      Seq(EnvironmentDefinition.S1M1),
    )
  }

  "authorize Bank on P3" in { implicit env =>
    import env.*
    val bank = grabPartyId(participant1, "Bank")

    ledgerOffsetBeforePartyOnboarding.putIfAbsent(participant1.ledger_api.state.end())

    PartyToParticipantDeclarative.forParty(Set(participant1, participant3), daId)(
      participant1,
      bank,
      PositiveInt.one,
      Set(
        (participant1, PP.Submission),
        (participant3, PP.Submission),
      ),
    )
  }

  // Replicate Bank from P1 to P3
  "export ACS for Bank from P1" in { implicit env =>
    import env.*

    val bank = grabPartyId(participant1, "Bank")

    acsExportFile.putIfAbsent(
      File.newTemporaryFile(
        parent = Some(testSet.exportDirectory),
        prefix = "LargeAcsTest_Bank_",
      )
    )

    val bankAddedOnP3Offset = participant1.parties.find_party_max_activation_offset(
      partyId = bank,
      synchronizerId = daId,
      participantId = participant3.id,
      beginOffsetExclusive = ledgerOffsetBeforePartyOnboarding.getOrElse(
        throw new RuntimeException("missing begin offset")
      ),
      completeAfter = PositiveInt.one,
      onboarding = false,
    )

    acsExportFile.get.foreach { acsExport =>
      participant1.repair.export_acs(
        parties = Set(bank),
        exportFilePath = acsExport.canonicalPath,
        ledgerOffset = bankAddedOnP3Offset,
      )
    }

  }

  "import ACS for Bank on P3" in { implicit env =>
    import env.*

    val synchronizerId = participant1.synchronizers.list_registered().loneElement._2.toOption.value

    participant1.stop()
    participant2.stop()

    participant3.synchronizers.disconnect_all()

    acsExportFile.get.foreach { acsExportFile =>
      val startImport = System.nanoTime()

      participant3.repair.import_acs(
        synchronizerId,
        acsExportFile.canonicalPath,
        contractImportMode = testContractIdImportMode,
      )

      val importDurationMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startImport)

      importDurationMs should be < testSet.acsImportDurationBoundMs
    }
  }

  "reconnect P3" in { implicit env =>
    import env.*
    participant3.synchronizers.reconnect(daName)
  }

  "assert ACS on P3" in { implicit env =>
    import env.*

    participant3.testing.state_inspection
      .contractCountInAcs(daName, CantonTimestamp.now())
      .futureValueUS shouldBe Some(testSet.acsSize.value)
  }
}

/** Use this test to create a large ACS, and dump the test environment to file for subsequent
  * testing. Note: The dump files are imported based on the directory name so change the directory
  * suffix accordingly
  */
final class LargeAcsCreateContractsTest extends DumpTestSet {
  override protected def testSet: AcsTestSet =
    AcsTestSet(
      PositiveInt.tryCreate(10_000),
      temporaryContracts = NonNegativeInt.zero,
      directorySuffix = "LargeAcsExportAndImportTest",
    )
}

/** The actual test */
final class LargeAcsExportAndImportTest extends EstablishTestSet {
  override protected def testSet: AcsTestSet =
    AcsTestSet(
      PositiveInt.tryCreate(10_000),
      temporaryContracts = NonNegativeInt.zero,
      directorySuffix = "LargeAcsExportAndImportTest",
    )

  override protected def testContractIdImportMode: ContractImportMode =
    ContractImportMode.Validation
}
