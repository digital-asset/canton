// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.testing.modelbased.projections

import com.daml.ledger.javaapi
import com.digitalasset.canton.testing.modelbased.projections.Projections.*
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.script.{EventId, IdeLedger}
import com.digitalasset.daml.lf.value.Value as V

import scala.collection.mutable
import scala.jdk.CollectionConverters.*

object ToProjection {
  type PartyIdReverseMapping = Map[Ref.Party, PartyId]
  type ContractIdReverseMapping = Map[V.ContractId, ContractId]

  def convertFromCantonProjection(
      partyIds: PartyIdReverseMapping,
      contractIds: ContractIdReverseMapping,
      trees: List[javaapi.data.Transaction],
  ): Projection =
    new FromCanton(partyIds, contractIds).fromTransactions(trees)

  def projectFromIdeLedger(
      partyIds: PartyIdReverseMapping,
      contractIds: ContractIdReverseMapping,
      ideLedger: IdeLedger,
      party: Ref.Party,
  ): Projection =
    new FromIdeLedger(partyIds, contractIds, party).fromIdeLedger(ideLedger)

  private[ToProjection] class FromCanton(
      partyIds: PartyIdReverseMapping,
      contractIds: ContractIdReverseMapping,
  ) {

    def fromTransactions(
        trees: List[javaapi.data.Transaction]
    ): Projection =
      trees.map(fromTransaction)

    private def fromTransaction(
        tree: javaapi.data.Transaction
    ): Commands =
      Commands(
        tree.getRootNodeIds.asScala.toList
          .map(fromEventId(tree, _))
      )

    private def fromEventId(
        tree: javaapi.data.Transaction,
        eventId: Integer,
    ): Action =
      tree.getEventsById.get(eventId) match {
        case create: javaapi.data.CreatedEvent =>
          Create(
            contractId = fromContractId(create.getContractId),
            signatories = fromPartyIds(create.getSignatories),
          )
        case exercise: javaapi.data.ExercisedEvent =>
          Exercise(
            kind = if (exercise.isConsuming) Consuming else NonConsuming,
            contractId = fromContractId(exercise.getContractId),
            controllers = fromPartyIds(exercise.getActingParties),
            subTransaction = tree
              .getChildNodeIds(exercise)
              .asScala
              .toList
              .map(fromEventId(tree, _)),
          )
        case event =>
          throw new IllegalArgumentException(s"Unsupported event type: $event")
      }

    private def fromContractId(contractId: String): ContractId =
      contractIds(V.ContractId.assertFromString(contractId))

    private def fromPartyId(partyId: String): PartyId =
      partyIds(Ref.Party.assertFromString(partyId))

    private def fromPartyIds(partyIds: java.lang.Iterable[String]): PartySet =
      partyIds.asScala.map(fromPartyId).toSet
  }

  private[ToProjection] class FromIdeLedger(
      partyIds: PartyIdReverseMapping,
      contractIds: ContractIdReverseMapping,
      party: Ref.Party,
  ) {

    import com.digitalasset.daml.lf.script.IdeLedger as IL
    import com.digitalasset.daml.lf.transaction.{Node, NodeId}

    def fromIdeLedger(ideLedger: IdeLedger): Projection =
      ideLedger.scriptSteps.values
        .collect { case commit: IL.Commit =>
          fromTransactionRoots(
            ideLedger.ledgerData.nodeInfos,
            commit.txId,
            commit.richTransaction.transaction.roots.toList,
          )
        }
        .flatten
        .toList

    @SuppressWarnings(Array("com.digitalasset.canton.NonUnitForEach"))
    private def fromTransactionRoots(
        nodeInfos: IL.LedgerNodeInfos,
        txId: IdeLedger.TransactionId,
        roots: List[NodeId],
    ): Option[Commands] = {
      val newRoots = mutable.Buffer.empty[Action]
      val _ =
        roots.foreach(fromTransactionNode(nodeInfos, txId, newRoots, _, parentIncluded = false))
      Option.when(newRoots.nonEmpty)(Commands(newRoots.toList))
    }

    private def fromTransactionNode(
        nodeInfos: IdeLedger.LedgerNodeInfos,
        txId: IdeLedger.TransactionId,
        newRoots: mutable.Buffer[Action],
        nodeId: NodeId,
        parentIncluded: Boolean,
    ): Option[Action] = {
      val eventId = EventId(txId.id.toLong, nodeId)
      val nodeInfo = nodeInfos(eventId)
      // Canton doesn't seem to return divulged create events
      val included = nodeInfo.disclosures.get(party).exists(_.explicit)
      val res: Option[Action] = nodeInfo.node match {
        case create: Node.Create =>
          Option.when(included) {
            Create(
              contractId = contractIds(create.coid),
              signatories = fromParties(create.signatories),
            )
          }
        case exercise: Node.Exercise =>
          // Run unconditionally in order to collect the new roots
          val subTransaction = exercise.children.toList.flatMap(
            fromTransactionNode(nodeInfos, txId, newRoots, _, included)
          )
          Option.when(included) {
            Exercise(
              kind = if (exercise.consuming) Consuming else NonConsuming,
              contractId = contractIds(exercise.targetCoid),
              controllers = fromParties(exercise.actingParties),
              subTransaction = subTransaction,
            )
          }
        case _ => None
      }
      res.foreach(action =>
        if (!parentIncluded) {
          newRoots += action
        }
      )
      res
    }

    private def fromParties(parties: Set[Ref.Party]): PartySet =
      parties.map(partyIds)
  }
}
