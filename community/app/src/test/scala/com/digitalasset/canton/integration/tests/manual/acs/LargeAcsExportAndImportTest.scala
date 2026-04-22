// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.manual.acs

import better.files.*
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
import com.digitalasset.canton.participant.admin.data.ContractImportMode
import com.digitalasset.canton.participant.ledger.api.client.JavaDecodeUtil
import com.digitalasset.canton.time.PositiveSeconds
import com.digitalasset.canton.topology.transaction.ParticipantPermission as PP
import com.digitalasset.canton.util.SingleUseCell
import monocle.Monocle.toAppliedFocusOps

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration.DurationInt

/** Given a large active contract set (ACS), we want to test the ACS export and import.
  *
  * IMPORTANT: This does NOT implement a proper offline party replication as this is NOT the test
  * focus.
  *
  * The tooling also allows to generate "temporary contracts": these are contracts that are created
  * and archived immediately (and thus don't show up in the ACS snapshot). This is useful to check
  * how such archived but not-yet pruned contracts impact performance of the export. Hence, it is
  * important to disable (background pruning).
  *
  * Raison d'être – Have this test code readily available in the repository for on-demand
  * performance investigations. Thus, we're not so much interested in actually creating a large ACS
  * and asserting a particular performance target; executed regularly on CI.
  *
  * Test setup:
  *   - Topology: 3 Participants (P1, P2, P3) with a single mediator and a single sequencer
  *   - P1 hosts party Bank, P2 hosts party Owner-0, ..., Owner-19
  *   - Bank creates a specified number of active IOU contracts with the owners
  *   - Bank authorizes to be hosted on P3
  *   - P1 exports Bank's ACS to a file
  *   - P3 imports Bank's ACS from the file
  *
  * Above is implemented by [[LargeAcsExportAndImportTest]].
  *
  * Creating a large ACS is time-consuming. There's a commented-out test,
  * `LargeAcsCreateContractsTest`, that creates active contracts as specified by
  * [[AcsTestSet.acsSize]], and then dumps the nodes' persisted state as database dump files. When
  * dump files are present for a [[AcsTestSet.name]], then the [[LargeAcsExportAndImportTest]]
  * restores them at the beginning of its test execution. This allows for faster, repeated test
  * executions. Without dump files, the test creates the active contracts as required by
  * [[AcsTestSet.acsSize]]. – That's being executed on CI.
  *
  * Hint: For testing with an ACS size of 10'000 active contracts or larger, you definitively want
  * to use a previously created database dump of the test network. Some example creation times
  * (developer notebook):
  *   - 1s for 1000 active contracts
  *   - 72s (1min 12s) for 10_000
  *   - 437s (7min 17s) for 100_000
  *   - 817s (13min 37s) for 200_000
  *   - 1229s (20min 29s) for 300_000
  *   - extrapolation: T ≈ 0.00404 * N + 17.5, where N = desired number of active contracts and T is
  *     in seconds
  *
  * Empirically, the software starts to misbehave / expose issues starting at 100'000 or more active
  * contracts.
  *
  * Some example times (developer notebook) as reference:
  * {{{
  * ACS size | dump restore [s] | acs_export [s] |           acs_import [s] |
  * 1000     |               17 |            0.1 |                        1 |
  * 10_000   |               21 |            0.7 |                        6 |
  * 100_000  |               42 |              4 |                      111 |
  * 150_000  |               53 |              7 |                      126 |
  * 200_000  |               65 |              9 |                      142 |
  * 300_000  |               87 |             15 |                      174 |
  * N        |                  |                |   T ≈ 0.00059 * N + 18.5 |
  * }}}
  */
protected abstract class LargeAcsExportAndImportTestBase extends LargeAcsIntegrationTestBase {
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
    val batches = contractsDataset.grouped(testSet.creationBatchSize.value).toList
    val batchesCount = batches.size
    val temporaryContractsPerBatch =
      Math.ceil(testSet.temporaryContracts.value.toDouble / batchesCount).toInt

    // Round-robin on the owners
    val ownerIdx = new AtomicInteger(0)

    batches.foreach { batch =>
      val start = System.nanoTime()
      val iousCommands = batch.map { amount =>
        val owner = owners(ownerIdx.getAndIncrement() % ownersCount)

        IouSyntax.testIou(bank, owner, amount.toDouble).create.commands.loneElement
      }
      participant1.ledger_api.javaapi.commands.submit(Seq(bank), iousCommands)
      val ledgerEnd = participant1.ledger_api.state.end()
      val end = System.nanoTime()
      logger.info(
        s"Batch: ${batch.head} to ${batch.head + testSet.creationBatchSize.value} took ${TimeUnit.NANOSECONDS
            .toMillis(end - start)}ms and ledger end = $ledgerEnd"
      )

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
    }

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
      PositiveInt.tryCreate(1000),
      temporaryContracts = NonNegativeInt.zero,
      directorySuffix = "LargeAcsExportAndImportTest",
    )
}

/** The actual test */
final class LargeAcsExportAndImportTest extends EstablishTestSet {
  override protected def testSet: AcsTestSet =
    AcsTestSet(
      PositiveInt.tryCreate(1000),
      temporaryContracts = NonNegativeInt.zero,
      directorySuffix = "LargeAcsExportAndImportTest",
    )

  override protected def testContractIdImportMode: ContractImportMode =
    ContractImportMode.Validation
}
