// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.multihostedparties

import com.daml.ledger.api.v2.event.Event
import com.daml.ledger.api.v2.transaction_filter.*
import com.daml.ledger.api.v2.transaction_filter.TransactionShape.{
  TRANSACTION_SHAPE_ACS_DELTA,
  TRANSACTION_SHAPE_LEDGER_EFFECTS,
}
import com.digitalasset.canton.admin.api.client.commands.LedgerApiCommands.UpdateService
import com.digitalasset.canton.admin.api.client.commands.LedgerApiCommands.UpdateService.TransactionWrapper
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.LocalParticipantReference
import com.digitalasset.canton.examples.java.divulgence.DivulgeIouByExercise
import com.digitalasset.canton.examples.java.iou.Iou
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.topology.{Party, PartyId}
import org.scalatest.OptionValues.*

object DivulgenceIntegrationTestHelpers {
  final case class OffsetCid(offset: Long, contractId: String)
  sealed trait EventType extends Serializable with Product
  case object Created extends EventType
  case object Consumed extends EventType
  case object NonConsumed extends EventType

  implicit class ParticipantSimpleStreamHelper(val participant: LocalParticipantReference)(implicit
      val alphaMultiSynchronizerSupport: Boolean = false
  ) {

    def acs(party: Party): Seq[OffsetCid] =
      participant.ledger_api.state.acs
        .active_contracts_of_party(party)
        .flatMap(_.createdEvent)
        .map(c => OffsetCid(c.offset, c.contractId))

    def acsDeltas(
        party: Party,
        beginOffsetExclusive: Long = 0L,
    ): Seq[(OffsetCid, EventType)] =
      updates(TRANSACTION_SHAPE_ACS_DELTA, Seq(party.partyId), beginOffsetExclusive)

    def acsDeltas(parties: Seq[Party]): Seq[(OffsetCid, EventType)] =
      updates(TRANSACTION_SHAPE_ACS_DELTA, parties.map(_.partyId), 0L)

    def ledgerEffects(
        party: Party,
        beginOffsetExclusive: Long = 0L,
    ): Seq[(OffsetCid, EventType)] =
      updates(TRANSACTION_SHAPE_LEDGER_EFFECTS, Seq(party.partyId), beginOffsetExclusive)

    def eventsWithAcsDelta(parties: Seq[PartyId]): Seq[Event] =
      updatesEvents(TRANSACTION_SHAPE_LEDGER_EFFECTS, parties, 0L).collect {
        case TransactionWrapper(tx) =>
          tx.events.filter(_.event match {
            case Event.Event.Created(created) => created.acsDelta
            case Event.Event.Exercised(ex) => ex.acsDelta
            case _ => false
          })
        case assigned: UpdateService.AssignedWrapper =>
          assigned.events.flatMap(_.createdEvent).collect {
            case createdEvent if createdEvent.acsDelta =>
              Event(Event.Event.Created(createdEvent))
          }
      }.flatten

    def createIou(payer: Party, owner: Party): (OffsetCid, Iou.Contract) = {
      val (contract, transaction, _) = IouSyntax.createIouComplete(participant)(payer, owner)
      OffsetCid(transaction.offset, contract.id.contractId) -> contract
    }

    def createDivulgeIou(
        payer: Party,
        divulgee: Party,
    ): (OffsetCid, DivulgeIouByExercise.Contract) = {
      val (contract, transaction, _) =
        IouSyntax.createDivulgeIouByExerciseComplete(participant)(payer, divulgee)
      OffsetCid(transaction.offset, contract.id.contractId) -> contract
    }

    def immediateDivulgeIou(
        payer: Party,
        divulgeContract: DivulgeIouByExercise.Contract,
    ): (OffsetCid, Iou.Contract) = {
      val (contract, transaction, _) =
        IouSyntax.immediateDivulgeIouComplete(participant)(payer, divulgeContract)
      OffsetCid(transaction.offset, contract.id.contractId) -> contract
    }

    def retroactiveDivulgeAndArchiveIou(
        payer: Party,
        divulgeContract: DivulgeIouByExercise.Contract,
        iouContractId: Iou.ContractId,
    ): OffsetCid = {
      val (transaction, _) =
        IouSyntax.retroactiveDivulgeAndArchiveIouComplete(participant)(
          payer,
          divulgeContract,
          iouContractId,
        )
      OffsetCid(transaction.offset, iouContractId.contractId)
    }

    def archiveIou(party: Party, iou: Iou.Contract): Unit =
      IouSyntax.archive(participant)(iou, party)

    private def updatesEvents(
        transactionShape: TransactionShape,
        parties: Seq[PartyId],
        beginOffsetExclusive: Long,
    ): Seq[UpdateService.UpdateWrapper] = {
      val reassignmentsFilter = if (alphaMultiSynchronizerSupport) {
        Some(
          EventFormat(
            filtersByParty = parties.map(party => party.toLf -> Filters(Nil)).toMap,
            filtersForAnyParty = if (parties.isEmpty) Some(Filters(Nil)) else None,
            verbose = false,
          )
        )
      } else None

      participant.ledger_api.updates.updates(
        updateFormat = UpdateFormat(
          includeTransactions = Some(
            TransactionFormat(
              eventFormat = Some(
                EventFormat(
                  filtersByParty = parties.map(party => party.toLf -> Filters(Nil)).toMap,
                  filtersForAnyParty = if (parties.isEmpty) Some(Filters(Nil)) else None,
                  verbose = false,
                )
              ),
              transactionShape = transactionShape,
            )
          ),
          includeReassignments = reassignmentsFilter,
          includeTopologyEvents = None,
        ),
        completeAfter = PositiveInt.tryCreate(1000000),
        endOffsetInclusive = Some(participant.ledger_api.state.end()),
        beginOffsetExclusive = beginOffsetExclusive,
      )
    }

    private def updates(
        transactionShape: TransactionShape,
        parties: Seq[PartyId],
        beginOffsetExclusive: Long,
    ): Seq[(OffsetCid, EventType)] =
      updatesEvents(transactionShape, parties, beginOffsetExclusive).collect {
        case TransactionWrapper(tx) =>
          tx.events.map(_.event).collect {
            case Event.Event.Created(event) =>
              OffsetCid(event.offset, event.contractId) -> Created
            case Event.Event.Archived(event) =>
              OffsetCid(event.offset, event.contractId) -> Consumed
            case Event.Event.Exercised(event) if event.consuming =>
              OffsetCid(event.offset, event.contractId) -> Consumed
            case Event.Event.Exercised(event) if !event.consuming =>
              OffsetCid(event.offset, event.contractId) -> NonConsumed
          }
        case assigned: UpdateService.AssignedWrapper =>
          assigned.events.map { event =>
            OffsetCid(assigned.reassignment.offset, event.createdEvent.value.contractId) -> Created
          }
        case unassigned: UpdateService.UnassignedWrapper =>
          unassigned.events.map { event =>
            OffsetCid(unassigned.reassignment.offset, event.contractId) -> Consumed
          }
      }.flatten
  }
}
