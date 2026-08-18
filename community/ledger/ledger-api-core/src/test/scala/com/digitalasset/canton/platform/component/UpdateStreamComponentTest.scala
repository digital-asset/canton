// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.component

import com.digitalasset.canton.ledger.api.*
import com.digitalasset.canton.ledger.api.TransactionShape.LedgerEffects
import com.digitalasset.canton.ledger.participant.state.index.IndexUpdateService.UpdatesResponse
import com.google.protobuf.ByteString
import org.apache.pekko.stream.scaladsl.Sink
import org.scalatest.wordspec.AnyWordSpec

class UpdateStreamComponentTest extends AnyWordSpec with IndexComponentTest {
  def updateFormat(transactionShape: TransactionShape) = UpdateFormat(
    includeTransactions = Some(
      TransactionFormat(
        eventFormat = EventFormat(
          filtersByParty = Map(dsoParty.value -> CumulativeFilter.templateWildcardFilter(true)),
          filtersForAnyParty = None,
          verbose = false,
        ),
        transactionShape = transactionShape,
      )
    ),
    includeReassignments = None,
    includeTopologyEvents = None,
    includeAcsCommitments = None,
    includeAcsChanges = None,
  )
  private val nextRecordTime = new SingleStepIncreasingRecordTime

  "update stream in reverse order" should {
    "stream create transactions" in {
      val rangeStart = index.currentLedgerEnd().map(_.lastOffset)
      val createContracts =
        Vector.tabulate(10)(_ => creates(nextRecordTime, 10)(1))
      val rangeEnd = ingestUpdates(createContracts*)
      val updatesStream = index.updates(
        begin = rangeStart,
        endAt = Some(rangeEnd),
        updateFormat = updateFormat(LedgerEffects),
        descendingOrder = true,
        skipPruningChecks = false,
      )
      val updates = updatesStream
        .collect { case UpdatesResponse.ProtoUpdates(Some(response), _) => response }
        .runWith(Sink.seq)
        .futureValue
      updates.flatMap(
        _.update.transaction.value.events.map(_.getCreated.contractId)
      ) should contain theSameElementsInOrderAs (createContracts.reverse.flatMap(
        _._2.map(_.contractId.coid)
      ))
    }

    "preserve order of events inside a transaction" in {
      val rangeStart = index.currentLedgerEnd().map(_.lastOffset)
      val createContracts =
        Vector.tabulate(10)(_ => creates(nextRecordTime, 10)(5))
      val rangeEnd = ingestUpdates(createContracts*)
      val updatesStream = index.updates(
        begin = rangeStart,
        endAt = Some(rangeEnd),
        updateFormat = updateFormat(LedgerEffects),
        descendingOrder = true,
        skipPruningChecks = false,
      )
      val updates = updatesStream
        .collect { case UpdatesResponse.ProtoUpdates(Some(response), _) => response }
        .runWith(Sink.seq)
        .futureValue
      updates.map(
        _.update.transaction.value.events.map(_.getCreated.contractId)
      ) should contain theSameElementsInOrderAs (createContracts.reverse.map(
        _._2.map(_.contractId.coid)
      ))
    }

    "properly order topology events interleaved with create events" in {
      val rangeStart = index.currentLedgerEnd().map(_.lastOffset)
      val createContractsFirst =
        Vector.tabulate(3)(_ => creates(nextRecordTime, 10)(1))
      ingestUpdates(createContractsFirst*)
      ingestTopologyEvents(parties = Set("new-party-1"), recordTime = nextRecordTime())
      ingestTopologyEvents(parties = Set("new-party-2"), recordTime = nextRecordTime())
      val createContractsSecond =
        Vector.tabulate(2)(_ => creates(nextRecordTime, 10)(1))
      val rangeEnd = ingestUpdates(createContractsSecond*)

      val updatesStream = index.updates(
        begin = rangeStart,
        endAt = Some(rangeEnd),
        updateFormat = updateFormat(LedgerEffects).copy(includeTopologyEvents =
          Some(
            TopologyFormat(
              Some(ParticipantAuthorizationFormat(None)),
              synchronizerParametersFormat = false,
              synchronizerId = None,
            )
          )
        ),
        descendingOrder = true,
        skipPruningChecks = false,
      )

      val updates = updatesStream
        .collect { case UpdatesResponse.ProtoUpdates(Some(response), _) => response }
        .runWith(Sink.seq)
        .futureValue

      updates.map(_.update.isTopologyTransaction) should contain theSameElementsInOrderAs (Seq(
        false, false, true, true, false, false, false))
    }

    "deliver synchronizer parameters as an empty update with a synchronizer parameters response" in {
      val rangeStart = index.currentLedgerEnd().map(_.lastOffset)
      ingestTopologyEvents(
        synchronizerParametersPayloads = Seq("synchronizer-parameters"),
        synchronizerId = synchronizer1,
        recordTime = nextRecordTime(),
      )
      val rangeEnd = index.currentLedgerEnd().value.lastOffset

      val updatesStream = index.updates(
        begin = rangeStart,
        endAt = Some(rangeEnd),
        updateFormat = updateFormat(LedgerEffects).copy(includeTopologyEvents =
          Some(
            TopologyFormat(
              Some(ParticipantAuthorizationFormat(None)),
              synchronizerParametersFormat = true,
              synchronizerId = Some(synchronizer1),
            )
          )
        ),
        descendingOrder = false,
        skipPruningChecks = false,
      )

      val responses = updatesStream
        .collect { case response: UpdatesResponse.ProtoUpdates => response }
        .runWith(Sink.seq)
        .futureValue

      val synchronizerParamsResponse = responses.loneElement
      synchronizerParamsResponse.response shouldBe empty
      synchronizerParamsResponse.synchronizerParametersResponse should not be empty
    }

    "property order create events interleaved with reassignments" in {
      val rangeStart = index.currentLedgerEnd().map(_.lastOffset)
      val create1 = creates(nextRecordTime, 10)(1)
      val create2 = creates(nextRecordTime, 10)(1)

      ingestUpdates(create1)
      ingestUpdates(create2)

      val reassignment1 = mkReassignmentAccepted(
        dsoParty.value,
        "upd-id-ra-1",
        withAcsChange = true,
        create1._2,
      )
      ingestUpdateSync(reassignment1)

      val reassignment2 = mkReassignmentAccepted(
        dsoParty.value,
        "upd-id-ra-2",
        withAcsChange = true,
        create2._2,
      )
      ingestUpdateSync(reassignment2)

      val create3 = creates(nextRecordTime, 10)(1)
      val rangeEnd = ingestUpdates(create3)

      val updatesStream = index.updates(
        begin = rangeStart,
        endAt = Some(rangeEnd),
        updateFormat = updateFormat(LedgerEffects).copy(includeReassignments =
          Some(
            EventFormat(
              filtersByParty = Map(),
              filtersForAnyParty = Some(CumulativeFilter.templateWildcardFilter(false)),
              verbose = false,
            )
          )
        ),
        descendingOrder = true,
        skipPruningChecks = false,
      )

      val updates = updatesStream
        .collect { case UpdatesResponse.ProtoUpdates(Some(response), _) => response }
        .runWith(Sink.seq)
        .futureValue

      updates should have size 5

      updates.map(_.update.isReassignment) should contain theSameElementsInOrderAs Seq(
        false, true, true, false, false,
      )

      updates(
        1
      ).update.reassignment.value.events.loneElement.event.assigned.value.createdEvent.value.contractId shouldEqual create2._2.loneElement.contractId.coid

      updates(
        2
      ).update.reassignment.value.events.loneElement.event.assigned.value.createdEvent.value.contractId shouldEqual create1._2.loneElement.contractId.coid

    }

    "preserve order of updates interleaved" in {
      val rangeStart = index.currentLedgerEnd().map(_.lastOffset)
      val create1 = creates(nextRecordTime, 10)(1)

      ingestUpdates(create1)
      ingestTopologyEvents(parties = Set("new-party-1"), recordTime = nextRecordTime())
      val reassignment1 = mkReassignmentAccepted(
        dsoParty.value,
        "upd-id-ra-interleave-1",
        withAcsChange = true,
        create1._2,
      )
      ingestUpdateSync(reassignment1)

      val create2 = creates(nextRecordTime, 10)(1)
      ingestUpdates(create2)
      ingestTopologyEvents(
        parties = Set("new-party-2"),
        synchronizerParametersPayloads = Seq("synchronizer-parameters-2"),
        recordTime = nextRecordTime(),
      )
      val reassignment2 = mkReassignmentAccepted(
        dsoParty.value,
        "upd-id-ra-interleave-2",
        withAcsChange = true,
        create2._2,
      )
      ingestUpdateSync(reassignment2)

      ingestTopologyEvents(
        synchronizerParametersPayloads = Seq("synchronizer-parameters-3"),
        recordTime = nextRecordTime(),
      )

      val rangeEnd = index.currentLedgerEnd().value.lastOffset
      val updatesStream = index.updates(
        begin = rangeStart,
        endAt = Some(rangeEnd),
        updateFormat = updateFormat(LedgerEffects)
          .copy(
            includeReassignments = Some(
              EventFormat(
                filtersByParty = Map(),
                filtersForAnyParty = Some(CumulativeFilter.templateWildcardFilter(false)),
                verbose = false,
              )
            ),
            includeTopologyEvents = Some(
              TopologyFormat(
                Some(ParticipantAuthorizationFormat(parties = None)),
                synchronizerParametersFormat = true,
                synchronizerId = Some(synchronizer1),
              )
            ),
          ),
        descendingOrder = true,
        skipPruningChecks = false,
      )

      val updatesWithSynchronizerParameters = updatesStream
        .collect { case u: UpdatesResponse.ProtoUpdates => u }
        .runWith(Sink.seq)
        .futureValue

      updatesWithSynchronizerParameters should have size 7
      updatesWithSynchronizerParameters.map { u =>
        val responseKind = u.response.map { r =>
          val upd = r.update
          (upd.isReassignment, upd.isTopologyTransaction, upd.transaction.isDefined)
        }
        (responseKind, u.synchronizerParametersResponse.isDefined)
      } should contain theSameElementsInOrderAs Seq(
        (None, true), // synchronizer parameters only
        (Some((true, false, false)), false), // reassignment2
        (Some((false, true, false)), true), // topology2 (party events + synchronizer parameters)
        (Some((false, false, true)), false), // create2
        (Some((true, false, false)), false), // reassignment1
        (Some((false, true, false)), false), // topology1 (party events only)
        (Some((false, false, true)), false), // create1
      )

      updatesWithSynchronizerParameters(
        0
      ).synchronizerParametersResponse.value.synchronizerParametersState.payload shouldEqual ByteString
        .copyFromUtf8("synchronizer-parameters-3")
      updatesWithSynchronizerParameters(
        2
      ).synchronizerParametersResponse.value.synchronizerParametersState.payload shouldEqual ByteString
        .copyFromUtf8("synchronizer-parameters-2")

      // Event-level assertions
      updatesWithSynchronizerParameters(
        1
      ).response.value.update.reassignment.value.events.loneElement.event.assigned.value.createdEvent.value.contractId shouldEqual create2._2.loneElement.contractId.coid
      updatesWithSynchronizerParameters(
        2
      ).response.value.update.topologyTransaction.value.events.loneElement.getParticipantAuthorizationOnboarding.partyId shouldEqual "new-party-2"
      updatesWithSynchronizerParameters(
        3
      ).response.value.update.transaction.value.events.loneElement.getCreated.contractId shouldEqual create2._2.loneElement.contractId.coid
      updatesWithSynchronizerParameters(
        4
      ).response.value.update.reassignment.value.events.loneElement.event.assigned.value.createdEvent.value.contractId shouldEqual create1._2.loneElement.contractId.coid
      updatesWithSynchronizerParameters(
        5
      ).response.value.update.topologyTransaction.value.events.loneElement.getParticipantAuthorizationOnboarding.partyId shouldEqual "new-party-1"
      updatesWithSynchronizerParameters(
        6
      ).response.value.update.transaction.value.events.loneElement.getCreated.contractId shouldEqual create1._2.loneElement.contractId.coid
    }
  }
}
