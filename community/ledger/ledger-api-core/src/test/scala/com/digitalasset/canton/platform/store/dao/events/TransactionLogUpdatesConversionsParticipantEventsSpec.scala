// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.dao.events

import com.daml.ledger.api.v2.reassignment.{Reassignment, ReassignmentEvent, UnassignedEvent}
import com.daml.ledger.api.v2.update_service.GetUpdateResponse
import com.digitalasset.canton.data.{CantonTimestamp, Counter, Offset}
import com.digitalasset.canton.ledger.api.TransactionShape
import com.digitalasset.canton.ledger.api.util.{LfEngineToApi, TimestampConversion}
import com.digitalasset.canton.ledger.participant.state.ParticipantEvent
import com.digitalasset.canton.ledger.participant.state.Update.RepairParticipantEventUpdate
import com.digitalasset.canton.ledger.participant.state.Update.TransactionAccepted.RepresentativePackageId.SameAsContractPackageId
import com.digitalasset.canton.ledger.participant.state.index.IndexUpdateService.UpdateResponse
import com.digitalasset.canton.logging.LoggingContextWithTrace
import com.digitalasset.canton.metrics.LedgerApiServerMetrics
import com.digitalasset.canton.participant.store.PersistedContractInstance
import com.digitalasset.canton.platform.store.backend.PersistentParticipantEventUpdateCategory
import com.digitalasset.canton.platform.store.backend.PersistentParticipantEventUpdateCategory.{
  AcsImport,
  AcsImportTxStyle,
  PurgeTxStyle,
}
import com.digitalasset.canton.platform.store.dao.EventProjectionProperties
import com.digitalasset.canton.platform.store.dao.EventProjectionProperties.UseOriginalViewPackageId
import com.digitalasset.canton.platform.store.interfaces.TransactionLogUpdate
import com.digitalasset.canton.platform.{
  InternalEventFormat,
  InternalTransactionFormat,
  InternalUpdateFormat,
  TemplatePartiesFilter,
}
import com.digitalasset.canton.protocol.{LfContractId, ReassignmentId, TestUpdateId}
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.SerializableTraceContextConverter.SerializableTraceContextExtension
import com.digitalasset.canton.tracing.{SerializableTraceContext, TraceContext}
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.digitalasset.canton.{BaseTest, HasExecutionContext, LfValue}
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.digitalasset.daml.lf.data.{Bytes, ImmArray, Ref}
import com.digitalasset.daml.lf.transaction.{
  CreationTime,
  FatContractInstance,
  Node,
  SerializationVersion,
}
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.Future

class TransactionLogUpdatesConversionsParticipantEventsSpec
    extends AsyncFlatSpec
    with Matchers
    with BaseTest
    with HasExecutionContext {
  import TransactionLogUpdatesConversionsParticipantEventsSpec.*

  private implicit val loggingContext: LoggingContextWithTrace = LoggingContextWithTrace.empty

  private val lfValueTranslation = new LfValueTranslation(
    metrics = LedgerApiServerMetrics.ForTesting,
    engineO = None,
    loadPackage = (_, _) => Future.successful(None),
    loggerFactory = loggerFactory,
  )

  behavior of "TransactionLogUpdatesConversions.filter for ParticipantEvents"

  it should "drop ParticipantEventUpdates when reassignments are not requested" in {
    TransactionLogUpdatesConversions
      .filter(transactionsOnlyUpdateFormat)
      .apply(bufferedRemove.copy(update = purgeUpdate)) shouldBe None
  }

  it should "keep only the ParticipantEvents matching the reassignment filter" in {
    val update = bufferedRemove.copy(update =
      purgeUpdate.copy(
        events = Seq(removed(cid1, Set(alice)), removed(cid2, Set(bob)))
      )
    )

    TransactionLogUpdatesConversions
      .filter(
        updateFormat(reassignments = Some(Set(alice)))
      )
      .apply(update) shouldBe Some(
      update.copy(update = purgeUpdate.copy(events = Seq(removed(cid1, Set(alice)))))
    )
  }

  it should "drop the ParticipantEventUpdate if no event matches the reassignment filter" in {
    TransactionLogUpdatesConversions
      .filter(
        updateFormat(reassignments = Some(Set(charlie)))
      )
      .apply(bufferedRemove.copy(update = purgeUpdate)) shouldBe None
  }

  it should "keep all ParticipantEvents for a party-wildcard filter" in {
    val purge = bufferedRemove.copy(update = purgeUpdate)
    TransactionLogUpdatesConversions
      .filter(
        updateFormat(reassignmentsWildcard = true)
      )
      .apply(purge) shouldBe Some(purge)

  }

  behavior of "TransactionLogUpdatesConversions.toUpdateResponse ParticipantEventUpdates"

  it should "render buffered ParticipantEventUpdates identically to persisted ones" in {
    TransactionLogUpdatesConversions
      .toUpdateResponse(
        updateFormat(reassignments = Some(Set(alice))),
        lfValueTranslation,
      )
      .apply(bufferedRemove.copy(update = purgeUpdate))
      .map {
        case UpdateResponse.ProtoUpdate(Some(response), _) =>
          response shouldBe GetUpdateResponse(
            GetUpdateResponse.Update.Reassignment(expectedReassignment)
          )
        case other => fail(s"Unexpected response: $other")
      }
  }

  it should "render buffered ParticipantEventUpdates on the pointwise endpoint" in {
    TransactionLogUpdatesConversions
      .toGetUpdateResponse(
        transactionLogUpdate = bufferedRemove.copy(update = purgeUpdate),
        internalUpdateFormat = updateFormat(reassignments = Some(Set(alice))),
        lfValueTranslation = lfValueTranslation,
      )
      .map(
        _ shouldBe Some(
          GetUpdateResponse(GetUpdateResponse.Update.Reassignment(expectedReassignment))
        )
      )
  }

  it should "drop ParticipantEventUpdates when transactions are not requested" in {
    TransactionLogUpdatesConversions
      .filter(
        updateFormat(reassignments = Some(Set(alice)))
      )
      .apply(bufferedParticipantUpdate()) shouldBe None
  }

  it should "apply the transaction filter" in {
    TransactionLogUpdatesConversions
      .filter(
        transactionFormat(Set(charlie), TransactionShape.AcsDelta)
      )
      .apply(bufferedParticipantUpdate()) shouldBe None

    val result = TransactionLogUpdatesConversions
      .filter(transactionFormat(Set(alice), TransactionShape.AcsDelta))
      .apply(bufferedParticipantUpdate())

    result shouldBe Some(bufferedParticipantUpdate())
  }

  Seq[TransactionShape](TransactionShape.AcsDelta, TransactionShape.LedgerEffects).foreach {
    shape =>
      it should s"render ParticipantEventUpdates identically with ($shape)" in {
        for {
          buffered <- TransactionLogUpdatesConversions
            .toUpdateResponse(
              transactionFormat(Set(alice), shape),
              lfValueTranslation,
            )
            .apply(bufferedParticipantUpdate(AcsImportTxStyle))
        } yield buffered match {
          case UpdateResponse.ProtoUpdate(Some(response), _) =>
            response.update.isReassignment shouldBe false
          case other => fail(s"Unexpected response: $other")
        }
      }
      it should s"render ParticipantEventUpdates as reassignemnts identically with ($shape)" in {
        for {
          buffered <- TransactionLogUpdatesConversions
            .toUpdateResponse(
              updateFormat(reassignmentsWildcard = true),
              lfValueTranslation,
            )
            .apply(bufferedParticipantUpdate(AcsImport))
        } yield buffered match {
          case UpdateResponse.ProtoUpdate(Some(response), _) =>
            response.update.isReassignment shouldBe true
          case other => fail(s"Unexpected response: $other")
        }
      }

      it should s"render buffered ParticipantEventUpdates on the pointwise endpoint ($shape)" in {
        for {
          buffered <- TransactionLogUpdatesConversions
            .toGetUpdateResponse(
              transactionLogUpdate = bufferedParticipantUpdate(AcsImportTxStyle),
              internalUpdateFormat = transactionFormat(Set(alice), shape),
              lfValueTranslation = lfValueTranslation,
            )
        } yield buffered match {
          case Some(response) => response.update.isReassignment shouldBe false
          case None => fail(s"unexpected response None")
        }
      }
      it should s"render buffered ParticipantEventUpdates on the pointwise endpoint ($shape) as reassignments" in {
        for {
          buffered <- TransactionLogUpdatesConversions
            .toGetUpdateResponse(
              transactionLogUpdate = bufferedParticipantUpdate(AcsImport),
              internalUpdateFormat = updateFormat(reassignmentsWildcard = true),
              lfValueTranslation = lfValueTranslation,
            )
        } yield buffered match {
          case Some(response) =>
            response.update.isReassignment shouldBe true
          case None => fail(s"unexpected response None")
        }
      }
  }
}

object TransactionLogUpdatesConversionsParticipantEventsSpec {

  private val alice = Ref.Party.assertFromString("alice")
  private val bob = Ref.Party.assertFromString("bob")
  private val charlie = Ref.Party.assertFromString("charlie")
  private val synchronizerId = SynchronizerId.tryFromString("x::synchronizer")
  private val cid1 = LfContractId.assertFromString("00" + "01" * 32)
  private val cid2 = LfContractId.assertFromString("00" + "02" * 32)
  private val templateId = Ref.Identifier.assertFromString("pkg:Mod:Template")
  private val packageName = Ref.PackageName.assertFromString("pkg-name")
  private val updateId = TestUpdateId("add-remove").toHexString
  private val offset = 15L
  private val recordTime = Timestamp.assertFromLong(100L)
  private val traceContext = TraceContext.withNewTraceContext("add-remove")(identity)

  private val eventProjectionProperties =
    EventProjectionProperties(verbose = false)(interfaceViewPackageUpgrade =
      UseOriginalViewPackageId
    )

  private def eventFormat(parties: Option[Set[Ref.Party]]) = InternalEventFormat(
    eventFilter = TemplatePartiesFilter(
      relation = Map.empty,
      templateWildcardParties = parties,
    ),
    eventProjectionProperties = eventProjectionProperties,
  )

  private def updateFormat(
      reassignments: Option[Set[Ref.Party]] = None,
      reassignmentsWildcard: Boolean = false,
  ): InternalUpdateFormat =
    InternalUpdateFormat(
      includeTransactions = None,
      includeReassignments =
        if (reassignmentsWildcard) Some(eventFormat(None))
        else reassignments.map(parties => eventFormat(Some(parties))),
      includeTopologyEvents = None,
      includeAcsCommitments = None,
    )

  private def transactionFormat(
      parties: Set[Ref.Party],
      shape: TransactionShape,
  ): InternalUpdateFormat =
    updateFormat().copy(includeTransactions =
      Some(
        InternalTransactionFormat(
          internalEventFormat = eventFormat(Some(parties)),
          transactionShape = shape,
        )
      )
    )

  private val transactionsOnlyUpdateFormat: InternalUpdateFormat =
    updateFormat().copy(includeTransactions =
      Some(
        InternalTransactionFormat(
          internalEventFormat = eventFormat(Some(Set(alice, bob))),
          transactionShape = TransactionShape.AcsDelta,
        )
      )
    )

  private val removeNodeId = 3

  private def removed(
      contractId: LfContractId,
      stakeholders: Set[Ref.Party],
      reassignmentCounter: Long = 0L,
  ) =
    ParticipantEvent.Removed(
      contractId = contractId,
      templateId = templateId,
      packageName = packageName,
      stakeholders = stakeholders,
      reassignmentCounter = reassignmentCounter,
      nodeId = removeNodeId,
      keyOpt = None,
      signatories = Some(Set(alice)),
    )

  private val purgeTxStyleUpdate = RepairParticipantEventUpdate(
    updateId = TestUpdateId("add-remove"),
    workflowId = Some(Ref.WorkflowId.assertFromString("workflow-id")),
    recordTime = CantonTimestamp(recordTime),
    synchronizerId = synchronizerId,
    events = Seq(removed(cid1, Set(alice, bob))),
    category = PurgeTxStyle,
    traceContext = traceContext,
    repairCounter = Counter.MinValue,
  )

  private val purgeUpdate = RepairParticipantEventUpdate(
    updateId = TestUpdateId("add-remove"),
    workflowId = Some(Ref.WorkflowId.assertFromString("workflow-id")),
    recordTime = CantonTimestamp(recordTime),
    synchronizerId = synchronizerId,
    events = Seq(removed(cid1, Set(alice, bob))),
    category = PersistentParticipantEventUpdateCategory.Purge,
    traceContext = traceContext,
    repairCounter = Counter.MinValue,
  )

  private val bufferedRemove: TransactionLogUpdate.ParticipantEventUpdate =
    TransactionLogUpdate.ParticipantEventUpdate(
      offset = Offset.tryFromLong(offset),
      update = purgeTxStyleUpdate,
    )

  private val addedContract = FatContractInstance.fromCreateNode(
    Node.Create(
      templateId = templateId,
      packageName = packageName,
      coid = cid1,
      arg = LfValue.ValueRecord(None, ImmArray.empty),
      signatories = Set(alice),
      stakeholders = Set(alice, bob),
      keyOpt = None,
      version = SerializationVersion.minVersion,
    ),
    CreationTime.CreatedAt(recordTime),
    Bytes.Empty,
  )

  private val addNodeId = 2
  private def bufferedParticipantUpdate(
      category: PersistentParticipantEventUpdateCategory = AcsImportTxStyle
  ): TransactionLogUpdate.ParticipantEventUpdate =
    TransactionLogUpdate.ParticipantEventUpdate(
      offset = Offset.tryFromLong(offset),
      update = purgeTxStyleUpdate.copy(
        events = Seq(
          ParticipantEvent.Added(
            nodeId = addNodeId,
            contractInstance = PersistedContractInstance(1L, addedContract),
            reassignmentCounter = 0L,
            representativePackageId = SameAsContractPackageId,
          ),
          removed(cid2, Set(alice, bob), 0L),
        ),
        category = category,
      ),
    )

  private def expectedReassignment: Reassignment = Reassignment(
    updateId = updateId,
    commandId = "",
    workflowId = "workflow-id",
    offset = offset,
    events = Seq(
      ReassignmentEvent(
        ReassignmentEvent.Event.Unassigned(
          UnassignedEvent(
            offset = offset,
            nodeId = 3,
            contractId = cid1.coid,
            templateId = Some(LfEngineToApi.toApiIdentifier(templateId)),
            packageName = packageName,
            source = synchronizerId.toProtoPrimitive,
            target = synchronizerId.toProtoPrimitive,
            reassignmentId = ReassignmentId(
              source = Source(synchronizerId),
              target = Target(synchronizerId),
              unassignmentTs = CantonTimestamp(recordTime),
              contractIdCounters = Map(),
            ).toProtoPrimitive,
            reassignmentCounter = 0L,
            witnessParties = Seq(alice.toString),
            assignmentExclusivity = None,
            submitter = "",
          )
        )
      )
    ),
    recordTime = Some(TimestampConversion.fromLf(recordTime)),
    traceContext = SerializableTraceContext(traceContext).toDamlProto,
    synchronizerId = synchronizerId.toProtoPrimitive,
    paidTrafficCost = None,
  )
}
