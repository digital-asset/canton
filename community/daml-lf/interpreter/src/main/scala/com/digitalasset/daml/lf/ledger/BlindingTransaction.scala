// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package ledger

import com.digitalasset.daml.lf.data.Ref.Party
import com.digitalasset.daml.lf.data.Relation
import com.digitalasset.daml.lf.transaction.BlindingInfo
import com.digitalasset.daml.lf.transaction.Node
import com.digitalasset.daml.lf.transaction.{NodeId, VersionedTransaction}
import com.digitalasset.daml.lf.value.Value.ContractId
import com.daml.nameof.NameOf

object BlindingTransaction {

  private object BlindState {
    val Empty = BlindState(Relation.empty, Relation.empty)
  }

  /** State to use while computing blindingInfo. */
  private final case class BlindState(
      disclosures: Relation[NodeId, Party],
      divulgences: Relation[ContractId, Party],
  ) {

    def discloseNode(
        witnesses: Set[Party],
        nid: NodeId,
    ): BlindState = {
      if (disclosures.contains(nid))
        InternalError.illegalArgumentException(
          NameOf.qualifiedNameOfCurrentFunc,
          s"discloseNode: nodeId already processed '$nid'.",
        )
      // Each node should be visible to someone
      copy(
        disclosures = disclosures.updated(nid, witnesses)
      )
    }

    def divulgeCoidTo(witnesses: Set[Party], acoid: ContractId): BlindState = {
      if (witnesses.nonEmpty) {
        copy(
          divulgences = divulgences
            .updated(acoid, witnesses union divulgences.getOrElse(acoid, Set.empty))
        )
      } else {
        this
      }
    }

  }

  /** Calculate blinding information for a transaction. */
  def calculateBlindingInfo(
      tx: VersionedTransaction
  ): BlindingInfo = {

    val initialParentExerciseWitnesses: Set[Party] = Set.empty

    val finalState = tx.foldWithPathState[BlindState, Set[Party]](
      BlindState.Empty,
      initialParentExerciseWitnesses,
    ) { case (state0, parentExerciseWitnesses, nodeId, node) =>
      node match {
        case action: Node.Action =>
          val witnesses = parentExerciseWitnesses union action.informeesOfNode
          val state = state0.discloseNode(witnesses, nodeId)

          action match {
            case _: Node.Create =>
              (state, witnesses)
            case _: Node.LookupByKey =>
              (state, witnesses)
            case fetch: Node.Fetch =>
              val state1 =
                state.divulgeCoidTo(parentExerciseWitnesses -- fetch.stakeholders, fetch.coid)
              (state1, witnesses)
            case ex: Node.Exercise =>
              val state1 =
                state.divulgeCoidTo(
                  (parentExerciseWitnesses union ex.choiceObservers) -- ex.stakeholders,
                  ex.targetCoid,
                )
              (state1, witnesses)
          }

        case _: Node.Rollback =>
          val state = state0.discloseNode(parentExerciseWitnesses, nodeId)
          (state, parentExerciseWitnesses)
      }
    }

    BlindingInfo(
      disclosure = finalState.disclosures,
      divulgence = finalState.divulgences,
    )
  }

}
