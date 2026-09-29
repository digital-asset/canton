// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi

import better.files.File
import com.daml.ledger.api.v2.transaction_filter.CumulativeFilter.IdentifierFilter.WildcardFilter
import com.daml.ledger.api.v2.transaction_filter.TransactionShape.TRANSACTION_SHAPE_ACS_DELTA
import com.daml.ledger.api.v2.transaction_filter.{
  CumulativeFilter,
  EventFormat,
  Filters,
  TransactionFormat,
  UpdateFormat,
}
import com.digitalasset.canton.admin.api.client.commands.LedgerApiCommands.UpdateService.TransactionWrapper
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.integration.plugins.UseReferenceBlockSequencer.MultiSynchronizer
import com.digitalasset.canton.integration.plugins.{
  UsePostgres,
  UseProgrammableSequencer,
  UseReferenceBlockSequencer,
}
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.tests.manual.topology.TopologyOperations.RichIterable
import com.digitalasset.canton.integration.util.{EntitySyntax, PartiesAllocator}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.synchronizer.sequencer.HasProgrammableSequencer
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import monocle.macros.syntax.lens.*

import java.time.Duration
import scala.annotation.unused
import scala.concurrent.duration.*

class RecordTimeToOffsetIT
    extends CommunityIntegrationTest
    with SharedEnvironment
    with EntitySyntax
    with HasProgrammableSequencer {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(
    new UseReferenceBlockSequencer[DbConfig.Postgres](
      loggerFactory,
      MultiSynchronizer.tryCreate(Set("sequencer1"), Set("sequencer2")),
    )
  )
  registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))

  private var alice: PartyId = _
  private var bob: PartyId = _
  private var charlie: PartyId = _

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3_S1M1_S1M1
      .addConfigTransforms(
        ConfigTransforms.useStaticTime,
        ConfigTransforms.updateAllParticipantConfigs_(
          ConfigTransforms.useTestingTimeService.andThen(
            _.focus(_.parameters.batching.maxAcsImportBatchSize)
              .replace(PositiveInt.one)
          )
        ),
      )
      .withSetup { implicit env =>
        import env.*
        participant1.synchronizers.connect_local(sequencer1, alias = daName)
        participant1.synchronizers.connect_local(sequencer2, alias = acmeName)
        participant1.dars.upload(CantonExamplesPath, synchronizerId = daId)
        participant1.dars.upload(CantonExamplesPath, synchronizerId = acmeId)
        participant2.synchronizers.connect_local(sequencer1, alias = daName)
        participant2.synchronizers.connect_local(sequencer2, alias = acmeName)
        participant2.dars.upload(CantonExamplesPath, synchronizerId = daId)
        participant2.dars.upload(CantonExamplesPath, synchronizerId = acmeId)

        PartiesAllocator(Set(participant1, participant2))(
          newParties =
            Seq("Alice" -> participant1, "Bob" -> participant1, "Charlie" -> participant2),
          targetTopology = Map(
            "Alice" -> Map(
              daId -> (PositiveInt.one, Set(participant1.id -> ParticipantPermission.Submission))
            ),
            "Bob" -> Map(
              daId -> (PositiveInt.one, Set(participant1.id -> ParticipantPermission.Submission))
            ),
            "Charlie" -> Map(
              daId -> (PositiveInt.one, Set(participant2.id -> ParticipantPermission.Submission)),
              acmeId -> (PositiveInt.one, Set(participant2.id -> ParticipantPermission.Submission)),
            ),
          ),
        )

        alice = "Alice".toPartyId(participant1)
        bob = "Bob".toPartyId(participant2)
        charlie = "Charlie".toPartyId(participant1)

      }

  "RecordTimeToOffset" should {

    s"return a record offset for an imported contract" in { implicit env =>
      import env.*
      val clock = environment.simClock.value
      @unused
      val contractsDa = (1 to 5).map { _ =>
        clock.advance(Duration.ofSeconds(1))
        IouSyntax.createIouComplete(participant1, synchronizerId = Some(daId))(alice, bob)
      }

      // Ensure that participant2 haven't received any updates
      always(200.milliseconds) {
        participant2.ledger_api.updates.updates(
          transactionAndReassignmentsFormat,
          completeAfter = PositiveInt.tryCreate(20),
        ) should be(empty)
      }

      // Sequence a contract create before the import
      @unused
      val beforeImport =
        IouSyntax.createIouComplete(participant2, synchronizerId = Some(daId))(charlie, bob)

      // Ensure that participant2 knows only about the transaction above
      participant2.ledger_api.updates
        .updates(
          transactionAndReassignmentsFormat,
          completeAfter = PositiveInt.tryCreate(20),
        )
        .loneElement("should contain only one transaction")
        .asInstanceOf[TransactionWrapper]
        .transaction
        .offset shouldEqual beforeImport._2.offset

      File.usingTemporaryFile() { acsSnapshot =>
        participant1.repair.export_acs(
          parties = Set(alice),
          exportFilePath = acsSnapshot.canonicalPath,
          synchronizerId = None,
          ledgerOffset = participant1.ledger_api.state.end(),
        )

        participant2.synchronizers.disconnect_all()

        participant2.repair.import_acs(daId, acsSnapshot.canonicalPath)
      }
      clock.advance(Duration.ofSeconds(1))

      eventually() {
        // Verify ALL contracts from both synchronizers were imported
        participant2.ledger_api.state.acs.active_contracts_of_party(alice) should have length 5
      }
      participant2.synchronizers.reconnect_all()

      val importedContracts = participant2.ledger_api.state.acs
        .active_contracts_of_party(alice)

      // Sequence a contract create after the import
      val iouAfterRepair =
        IouSyntax.createIouComplete(participant2, synchronizerId = Some(daId))(charlie, bob)

      val (
        transactionBeforeRepair,
        repairTransaction1,
        repairTransaction2,
        repairTransaction3,
        repairTransaction4,
        repairTransaction5,
        transactionAfterRepair,
      ) =
        participant2.ledger_api.updates.updates(
          transactionAndReassignmentsFormat,
          completeAfter = PositiveInt.tryCreate(20),
        ) match {
          case Seq(
                before: TransactionWrapper,
                rep1: TransactionWrapper,
                rep2: TransactionWrapper,
                rep3: TransactionWrapper,
                rep4: TransactionWrapper,
                rep5: TransactionWrapper,
                after: TransactionWrapper,
              ) =>
            (before, rep1, rep2, rep3, rep4, rep5, after)
          case seq =>
            fail(
              s"Expected exactly 7 transactions wrappers: create before, 5 from repair, create after, got: $seq"
            )
        }

      // Verify that test setup is what we expected
      transactionBeforeRepair.transaction.offset shouldBe (beforeImport._2.offset)
      transactionBeforeRepair.transaction.events
        .loneElement("should be single event")
        .event
        .created
        .value
        .contractId shouldBe (beforeImport._1.id.contractId)
      transactionAfterRepair.transaction.offset shouldBe (iouAfterRepair._2.offset)
      transactionAfterRepair.transaction.events
        .loneElement("should be single event")
        .event
        .created
        .value
        .contractId shouldBe (iouAfterRepair._1.id.contractId)
      repairTransaction1.transaction.events
        .loneElement("should be single event")
        .event
        .created
        .value
        .contractId shouldBe contractsDa(0)._1.id.contractId
      repairTransaction2.transaction.events
        .loneElement("should be single event")
        .event
        .created
        .value
        .contractId shouldBe contractsDa(1)._1.id.contractId
      repairTransaction3.transaction.events
        .loneElement("should be single event")
        .event
        .created
        .value
        .contractId shouldBe contractsDa(2)._1.id.contractId
      repairTransaction4.transaction.events
        .loneElement("should be single event")
        .event
        .created
        .value
        .contractId shouldBe contractsDa(3)._1.id.contractId
      repairTransaction5.transaction.events
        .loneElement("should be single event")
        .event
        .created
        .value
        .contractId shouldBe contractsDa(4)._1.id.contractId

      // Ensure that all imported contracts share record time.
      repairTransaction1
        .asInstanceOf[TransactionWrapper]
        .transaction
        .recordTime
        .value shouldEqual repairTransaction5
        .asInstanceOf[TransactionWrapper]
        .transaction
        .recordTime
        .value

      // Do actual assertions

      // Convert the first imported contract's record time to offset.
      // Should yield offset of the contract sequenced directly before the import. (First at given rt)
      val rto1 = participant2.ledger_api.state.offsetForRecordTime(
        daId,
        repairTransaction1.transaction.recordTime.value,
      )
      rto1.unwrap shouldEqual importedContracts(0).createdEvent.value.offset

      // Test "last before rt". Get record time of the contract after the import reduced by a ms.
      val rt6 = transactionAfterRepair.transaction.recordTime.value
      val offsetBeforeRt5 =
        participant2.ledger_api.state.offsetForRecordTime(daId, rt6.immediatePredecessor)

      val lastOfImportBatchOffset =
        repairTransaction5.asInstanceOf[TransactionWrapper].transaction.offset
      offsetBeforeRt5.unwrap should equal(
        lastOfImportBatchOffset
      ) // Last offset before should be the last offset of imported contracts
    }

    "return an offset for a topology transaction record time" in { implicit env =>
      import env.*
      @unused
      val clock = environment.simClock.value
      // Sequence a create before topology changes
      val iou1 =
        IouSyntax.createIouComplete(participant1, synchronizerId = Some(daId))(alice, alice)
      @unused
      val joe = participant1.parties.enable("joe", synchronizer = Some(daName))
      val updates = participant1.ledger_api.updates.topology_transactions(
        completeAfter = PositiveInt.one,
        beginOffsetExclusive = iou1._2.offset,
      )
      val topologyTransaction = updates.loneElement("Should fetch a single topology transaction")

      clock.advance(java.time.Duration.ofHours(1))

      val iou2 =
        IouSyntax.createIouComplete(participant1, synchronizerId = Some(daId))(alice, alice)

      // Check exact query
      participant1.ledger_api.state
        .offsetForRecordTime(daId, topologyTransaction.topologyTransaction.recordTime.value)
        .unwrap shouldBe topologyTransaction.topologyTransaction.offset

      // Check timestamp after
      participant1.ledger_api.state
        .offsetForRecordTime(daId, iou2._2.recordTime.value.addMicros(-100))
        .unwrap shouldBe topologyTransaction.topologyTransaction.offset

      // Make sure that for regular transactions record times we return proper offsets
      participant1.ledger_api.state
        .offsetForRecordTime(
          daId,
          topologyTransaction.topologyTransaction.recordTime.value.addMicros(-1),
        )
        .unwrap shouldBe iou1._2.offset
      participant1.ledger_api.state
        .offsetForRecordTime(daId, iou2._2.recordTime.value)
        .unwrap shouldBe iou2._2.offset

    }
  }

  private val transactionAndReassignmentsFormat = UpdateFormat(
    includeTransactions = Some(
      TransactionFormat(
        eventFormat = Some(
          EventFormat(
            filtersByParty = Map(),
            filtersForAnyParty = Some(
              Filters(
                cumulative = Seq(
                  CumulativeFilter(
                    identifierFilter = WildcardFilter(
                      com.daml.ledger.api.v2.transaction_filter.WildcardFilter(false)
                    )
                  )
                )
              )
            ),
            verbose = false,
          )
        ),
        transactionShape = TRANSACTION_SHAPE_ACS_DELTA,
      )
    ),
    includeReassignments = Some(
      EventFormat(
        filtersByParty = Map(),
        filtersForAnyParty = Some(
          Filters(
            Seq(
              CumulativeFilter(
                identifierFilter = WildcardFilter(
                  com.daml.ledger.api.v2.transaction_filter.WildcardFilter(false)
                )
              )
            )
          )
        ),
        verbose = false,
      )
    ),
    includeTopologyEvents = None,
  )
}
