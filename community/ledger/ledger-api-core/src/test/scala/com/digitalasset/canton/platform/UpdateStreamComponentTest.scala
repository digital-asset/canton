// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.ledger.api.TransactionShape.LedgerEffects
import com.digitalasset.canton.ledger.api.{
  CumulativeFilter,
  EventFormat,
  ParticipantAuthorizationFormat,
  TopologyFormat,
  TransactionFormat,
  TransactionShape,
  UpdateFormat,
}
import org.apache.pekko.stream.scaladsl.Sink
import org.scalatest.wordspec.AnyWordSpec

import java.util.concurrent.atomic.AtomicReference

class UpdateStreamComponentTest extends AnyWordSpec with IndexComponentTest {
  def updateFormat(transactionShape: TransactionShape) = UpdateFormat(
    includeTransactions = Some(
      TransactionFormat(
        eventFormat = EventFormat(
          filtersByParty = Map(dsoParty -> CumulativeFilter.templateWildcardFilter(true)),
          filtersForAnyParty = None,
          verbose = false,
        ),
        transactionShape = transactionShape,
      )
    ),
    includeReassignments = None,
    includeTopologyEvents = None,
  )

  private val recordTimeRef = new AtomicReference(CantonTimestamp.now())
  private val nextRecordTime: () => CantonTimestamp =
    () => recordTimeRef.updateAndGet(_.immediateSuccessor)

  "update stream in reverse order" should {
    "stream create transactions" in {
      val rangeStart = index.currentLedgerEnd().futureValue
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
      val updates = updatesStream.runWith(Sink.seq).futureValue
      updates.flatMap(
        _.update.transaction.value.events.map(_.getCreated.contractId)
      ) should contain theSameElementsInOrderAs (createContracts.reverse.flatMap(
        _._2.map(_.contractId.coid)
      ))
    }

    "preserve order of events inside a transaction" in {
      val rangeStart = index.currentLedgerEnd().futureValue
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
      val updates = updatesStream.runWith(Sink.seq).futureValue
      updates.map(
        _.update.transaction.value.events.map(_.getCreated.contractId)
      ) should contain theSameElementsInOrderAs (createContracts.reverse.map(
        _._2.map(_.contractId.coid)
      ))
    }

    "properly order topology events interleaved with create events" in {
      val rangeStart = index.currentLedgerEnd().futureValue
      val createContractsFirst =
        Vector.tabulate(3)(_ => creates(nextRecordTime, 10)(1))
      ingestUpdates(createContractsFirst*)
      ingestPartyOnboarding(Set("new-party-1"), nextRecordTime())
      ingestPartyOnboarding(Set("new-party-2"), nextRecordTime())
      val createContractsSecond =
        Vector.tabulate(2)(_ => creates(nextRecordTime, 10)(1))
      val rangeEnd = ingestUpdates(createContractsSecond*)

      val updatesStream = index.updates(
        begin = rangeStart,
        endAt = Some(rangeEnd),
        updateFormat = updateFormat(LedgerEffects).copy(includeTopologyEvents =
          Some(
            TopologyFormat(
              Some(ParticipantAuthorizationFormat(None))
            )
          )
        ),
        descendingOrder = true,
        skipPruningChecks = false,
      )

      val updates = updatesStream.runWith(Sink.seq).futureValue

      updates.map(_.update.isTopologyTransaction) should contain theSameElementsInOrderAs (Seq(
        false, false, true, true, false, false, false))
    }

    "property order create events interleaved with reassignments" in {
      val rangeStart = index.currentLedgerEnd().futureValue
      val create1 = creates(nextRecordTime, 10)(1)
      val create2 = creates(nextRecordTime, 10)(1)

      ingestUpdates(create1)
      ingestUpdates(create2)

      val reassignment1 = mkReassignmentAccepted(
        dsoParty,
        "upd-id-ra-1",
        withAcsChange = true,
        create1._2.loneElement.inst.toCreateNode,
      )
      ingestUpdateSync(reassignment1)

      val reassignment2 = mkReassignmentAccepted(
        dsoParty,
        "upd-id-ra-2",
        withAcsChange = true,
        create2._2.loneElement.inst.toCreateNode,
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

      val updates = updatesStream.runWith(Sink.seq).futureValue

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

    "preserve order of 2 creates, 2 topology events and 2 reassignments interleaved" in {
      val rangeStart = index.currentLedgerEnd().futureValue
      val create1 = creates(nextRecordTime, 10)(1)

      ingestUpdates(create1)
      ingestPartyOnboarding(Set("new-party-1"), nextRecordTime())
      val reassignment1 = mkReassignmentAccepted(
        dsoParty,
        "upd-id-ra-interleave-1",
        withAcsChange = true,
        create1._2.loneElement.inst.toCreateNode,
      )
      ingestUpdateSync(reassignment1)

      val create2 = creates(nextRecordTime, 10)(1)
      ingestUpdates(create2)
      ingestPartyOnboarding(Set("new-party-2"), nextRecordTime())
      val reassignment2 = mkReassignmentAccepted(
        dsoParty,
        "upd-id-ra-interleave-2",
        withAcsChange = true,
        create2._2.loneElement.inst.toCreateNode,
      )
      ingestUpdateSync(reassignment2)

      val rangeEnd = index.currentLedgerEnd().futureValue.value

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
              TopologyFormat(Some(ParticipantAuthorizationFormat(None)))
            ),
          ),
        descendingOrder = true,
        skipPruningChecks = false,
      )

      val updates = updatesStream.runWith(Sink.seq).futureValue

      updates should have size 6
      updates.map(u =>
        (u.update.isReassignment, u.update.isTopologyTransaction, u.update.transaction.isDefined)
      ) should contain theSameElementsInOrderAs Seq(
        (true, false, false), // reassignment2
        (false, true, false), // topology2
        (false, false, true), // create2
        (true, false, false), // reassignment1
        (false, true, false), // topology1
        (false, false, true), // create1
      )

      updates(
        0
      ).update.reassignment.value.events.loneElement.event.assigned.value.createdEvent.value.contractId shouldEqual create2._2.loneElement.contractId.coid
      updates(
        1
      ).update.topologyTransaction.value.events.loneElement.getParticipantAuthorizationOnboarding.partyId shouldEqual "new-party-2"
      updates(
        2
      ).update.transaction.value.events.loneElement.getCreated.contractId shouldEqual create2._2.loneElement.contractId.coid
      updates(
        3
      ).update.reassignment.value.events.loneElement.event.assigned.value.createdEvent.value.contractId shouldEqual create1._2.loneElement.contractId.coid
      updates(
        4
      ).update.topologyTransaction.value.events.loneElement.getParticipantAuthorizationOnboarding.partyId shouldEqual "new-party-1"
      updates(
        5
      ).update.transaction.value.events.loneElement.getCreated.contractId shouldEqual create1._2.loneElement.contractId.coid
    }
  }
}
