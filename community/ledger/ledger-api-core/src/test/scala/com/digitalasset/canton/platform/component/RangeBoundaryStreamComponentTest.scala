// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.component

import com.daml.ledger.api.v2.update_service.GetUpdatesResponse
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.ledger.api.*
import com.digitalasset.canton.ledger.api.TransactionShape.LedgerEffects
import com.digitalasset.canton.ledger.api.messages.state.{
  AcsContinuationPointerActiveContracts,
  AcsRangeInfo,
}
import com.digitalasset.canton.ledger.participant.state.InternalIndexService.AcsUpdate
import com.digitalasset.canton.ledger.participant.state.index.IndexUpdateService.UpdatesResponse
import com.digitalasset.canton.ledger.participant.state.{
  InternalIndexServiceImpl,
  TestAcsChangeFactory,
  Update,
}
import com.digitalasset.canton.platform.component.IndexComponentTest.ServiceParams
import com.digitalasset.canton.platform.config.IndexServiceConfig
import com.digitalasset.canton.protocol.{ContractInstance, TestUpdateId}
import com.google.protobuf.ByteString
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.{Sink, Source}
import org.scalatest.wordspec.AnyWordSpec

trait RangeBoundaryStreamComponentTest
    extends AnyWordSpec
    with IndexComponentTest
    with PersistenceSqlQueries {

  private val nextRecordTime = new SingleStepIncreasingRecordTime

  private lazy val internalIndexService = new InternalIndexServiceImpl(index)

  private def transactionUpdateFormat(
      includeTopologyEvents: Option[TopologyFormat] = None
  ): UpdateFormat =
    UpdateFormat(
      includeTransactions = Some(
        TransactionFormat(
          eventFormat = EventFormat(
            filtersByParty = Map(dsoParty.value -> CumulativeFilter.templateWildcardFilter(true)),
            filtersForAnyParty = None,
            verbose = false,
          ),
          transactionShape = LedgerEffects,
        )
      ),
      includeReassignments = None,
      includeTopologyEvents = includeTopologyEvents,
      includeAcsCommitments = None,
      includeAcsChanges = None,
    )

  private def readUpdates(
      fromExclusive: Option[Offset],
      endAt: Offset,
      updateFormat: UpdateFormat,
      descendingOrder: Boolean,
  ) =
    index
      .updates(
        begin = fromExclusive,
        endAt = Some(endAt),
        updateFormat = updateFormat,
        descendingOrder = descendingOrder,
        skipPruningChecks = false,
      )
      .collect { case UpdatesResponse.ProtoUpdates(Some(response), _) => response }
      .runWith(Sink.seq)
      .futureValue

  private def createdContractIds(
      responses: Seq[GetUpdatesResponse]
  ): Seq[String] =
    responses.flatMap(_.update.transaction.value.events.map(_.getCreated.contractId))

  private def acsCommitment(payload: ByteString): Update.ReceivedAcsCommitment =
    Update.ReceivedAcsCommitment(
      synchronizerId = synchronizer1,
      recordTime = nextRecordTime(),
      payload = payload,
      updateId = TestUpdateId("ReceivedAcsCommitment"),
    )

  private def acsUpdates(fromExclusive: Option[Offset]): Source[AcsUpdate, NotUsed] =
    internalIndexService
      .acsUpdates(synchronizer1, fromExclusive)
      .map(_.acsUpdate)
      .filter(_ != AcsUpdate.OffsetCheckpoint)

  private def witnessedCreates(
      num: Int
  ): (Update.SequencedTransactionAccepted, Vector[ContractInstance]) = {
    val (accepted, contracts) = creates(nextRecordTime, 10)(size = num)
    accepted.copy(acsChangeFactory =
      TestAcsChangeFactory(contractActivenessChanged = false)
    ) -> contracts
  }

  "updates (activate_event table)" should {
    Seq(false, true).foreach { descendingOrder =>
      s"exclude the transaction sitting exactly on the exclusive lower bound (descendingOrder=$descendingOrder)" in {
        val creates1 = creates(nextRecordTime, 10)(size = 2)
        val creates2 = creates(nextRecordTime, 10)(size = 2)

        // creates1 is ingested first (offset N), creates2 second (offset N+1).
        val boundary = ingestUpdates(creates1)
        val rangeEnd = ingestUpdates(creates2)

        val updates = readUpdates(
          fromExclusive = Some(boundary),
          endAt = rangeEnd,
          updateFormat = transactionUpdateFormat(),
          descendingOrder = descendingOrder,
        )

        val expected = creates2._2.map(_.contractId.coid)
        createdContractIds(updates) should contain theSameElementsAs expected
      }

      s"include the transaction sitting exactly on the inclusive upper bound (descendingOrder=$descendingOrder)" in {
        val startExclusive = index.currentLedgerEnd().value.lastOffset
        val creates1 = creates(nextRecordTime, 10)(size = 2)
        val creates2 = creates(nextRecordTime, 10)(size = 2)

        val boundary = ingestUpdates(creates1)
        ingestUpdates(creates2)

        val updates = readUpdates(
          fromExclusive = Some(startExclusive),
          endAt = boundary,
          updateFormat = transactionUpdateFormat(),
          descendingOrder = descendingOrder,
        )

        val expected = creates1._2.map(_.contractId.coid)
        createdContractIds(updates) should contain theSameElementsAs expected
      }
    }
  }

  "updates with witnessed events" should {
    Seq(false, true).foreach { descendingOrder =>
      s"exclude the transaction sitting exactly on the exclusive lower bound (descendingOrder=$descendingOrder)" in {
        val witnessed1 = witnessedCreates(num = 2)
        val witnessed2 = witnessedCreates(num = 2)

        val boundary = ingestUpdates(witnessed1)
        val rangeEnd = ingestUpdates(witnessed2)

        val updates = readUpdates(
          fromExclusive = Some(boundary),
          endAt = rangeEnd,
          updateFormat = transactionUpdateFormat(),
          descendingOrder = descendingOrder,
        )

        val expected = witnessed2._2.map(_.contractId.coid)
        createdContractIds(updates) should contain theSameElementsAs expected
      }

      s"include the transaction sitting exactly on the inclusive upper bound (descendingOrder=$descendingOrder)" in {
        val startExclusive = index.currentLedgerEnd().value.lastOffset
        val witnessed1 = witnessedCreates(num = 2)
        val witnessed2 = witnessedCreates(num = 2)

        val boundary = ingestUpdates(witnessed1)
        ingestUpdates(witnessed2)

        val updates = readUpdates(
          fromExclusive = Some(startExclusive),
          endAt = boundary,
          updateFormat = transactionUpdateFormat(),
          descendingOrder = descendingOrder,
        )

        val expected = witnessed1._2.map(_.contractId.coid)
        createdContractIds(updates) should contain theSameElementsAs expected
      }
    }
  }

  "updates with topology transactions" should {
    Seq(false, true).foreach { descendingOrder =>
      s"exclude the topology transaction sitting exactly on the exclusive lower bound (descendingOrder=$descendingOrder)" in {
        val updateFormat = transactionUpdateFormat(includeTopologyEvents =
          Some(
            TopologyFormat(
              participantAuthorizationFormat = Some(ParticipantAuthorizationFormat(None)),
              synchronizerParametersFormat = false,
              synchronizerId = None,
            )
          )
        )

        val boundary =
          ingestTopologyEvents(parties = Set("boundary-party-1"), recordTime = nextRecordTime())
        ingestTopologyEvents(Set("boundary-party-2"), recordTime = nextRecordTime())
        val rangeEnd = index.currentLedgerEnd().value.lastOffset

        val updates = readUpdates(
          fromExclusive = Some(boundary),
          endAt = rangeEnd,
          updateFormat = updateFormat,
          descendingOrder = descendingOrder,
        )

        val partyIds = updates.flatMap(
          _.update.topologyTransaction.toList.flatMap(
            _.events.map(_.getParticipantAuthorizationOnboarding.partyId)
          )
        )
        partyIds should contain theSameElementsAs Seq("boundary-party-2")
      }

      s"include the topology transaction sitting exactly on the inclusive upper bound (descendingOrder=$descendingOrder)" in {
        val updateFormat = transactionUpdateFormat(includeTopologyEvents =
          Some(
            TopologyFormat(
              participantAuthorizationFormat = Some(ParticipantAuthorizationFormat(None)),
              synchronizerParametersFormat = false,
              synchronizerId = None,
            )
          )
        )

        val startExclusive = index.currentLedgerEnd().value.lastOffset
        val boundary =
          ingestTopologyEvents(Set("upper-boundary-party-1"), recordTime = nextRecordTime())
        ingestTopologyEvents(Set("upper-boundary-party-2"), recordTime = nextRecordTime())

        val updates = readUpdates(
          fromExclusive = Some(startExclusive),
          endAt = boundary,
          updateFormat = updateFormat,
          descendingOrder = descendingOrder,
        )

        val partyIds = updates.flatMap(
          _.update.topologyTransaction.toList.flatMap(
            _.events.map(_.getParticipantAuthorizationOnboarding.partyId)
          )
        )
        partyIds should contain theSameElementsAs Seq("upper-boundary-party-1")
      }
    }
  }

  "acs commitments (acsUpdates)" should {
    "exclude the commitment sitting exactly on the exclusive lower bound" in {
      val payload1 = ByteString.copyFromUtf8("boundary-commitment-1")
      val payload2 = ByteString.copyFromUtf8("boundary-commitment-2")

      val boundary = ingestUpdateSync(acsCommitment(payload1))
      ingestUpdateSync(acsCommitment(payload2))

      val acsUpdate = acsUpdates(fromExclusive = Some(boundary))
        .take(1)
        .runWith(Sink.seq)
        .futureValue
        .loneElement

      acsUpdate shouldBe AcsUpdate.AcsCommitment(payload2)
    }
  }

  "acs paged (getActiveContracts)" should {
    "page from the exclusive start sequential id, excluding the boundary contracts" in {
      val (create1, contracts1) = creates(nextRecordTime, 10)(size = 2)
      val (create2, contracts2) = creates(nextRecordTime, 10)(size = 2)

      val firstBatchOffset = ingestUpdates(create1 -> contracts1).unwrap
      val activeAt = ingestUpdates(create2 -> contracts2).unwrap

      val startSequentialIdExclusive = eventSeqIdForActivateContractOffset(firstBatchOffset)
      val pagedRangeInfo = AcsRangeInfo.empty.copy(continuationPointer =
        Some(AcsContinuationPointerActiveContracts(startSequentialIdExclusive))
      )

      val pagedContractIds = activeContractIds(activeAt, pagedRangeInfo).map(_._1)

      pagedContractIds should contain noElementsOf contracts1.map(_.contractId.coid)
      pagedContractIds should contain allElementsOf contracts2.map(_.contractId.coid)
    }

    "include the contracts created exactly at the inclusive activeAt and exclude the next ones" in {
      val (create1, contracts1) = creates(nextRecordTime, 10)(size = 2)
      val (create2, contracts2) = creates(nextRecordTime, 10)(size = 2)

      // contracts1 are active at their own offset (activeAt is inclusive), contracts2 are not yet.
      val activeAt = ingestUpdates(create1 -> contracts1).unwrap
      ingestUpdates(create2 -> contracts2)

      val activeContractIdsAtBoundary = activeContractIds(activeAt).map(_._1)

      activeContractIdsAtBoundary should contain allElementsOf contracts1.map(_.contractId.coid)
      activeContractIdsAtBoundary should contain noElementsOf contracts2.map(_.contractId.coid)
    }
  }
}

final class RangeBoundaryStreamComponentTestCachesDisabledPostgres
    extends RangeBoundaryStreamComponentTest
    with IndexComponentTest.WithPostgres {
  override protected def serviceParams: ServiceParams =
    super.serviceParams
      .copy(indexServiceConfig = IndexServiceConfig(maxTransactionsInMemoryFanOutBufferSize = 0))
}

final class RangeBoundaryStreamComponentTestDefaultPostgres
    extends RangeBoundaryStreamComponentTest
    with IndexComponentTest.WithPostgres {
  override protected def serviceParams: ServiceParams =
    super.serviceParams.copy(indexServiceConfig = IndexServiceConfig())
}
