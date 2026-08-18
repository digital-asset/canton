// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.digest

import com.digitalasset.canton.participant.commitment.BaseDigestProcessor.{
  ContractChange,
  ContractChangeBatch,
}
import com.digitalasset.canton.participant.commitment.{SingleTrace, TracedLtHash16Blake3}
import com.digitalasset.canton.participant.digest.DigestOperation.{Add, Remove}
import com.digitalasset.canton.protocol.ContractIdSyntax.*
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.serialization.DeterministicEncoding
import com.digitalasset.canton.{LedgerParticipantId, LfPartyId, ReassignmentCounter}
import com.digitalasset.nonempty.NonEmpty
import com.digitalasset.nonempty.NonEmptyReturningOps.`NE Iterable Ops`

object DigestOps {

  def computeDeltas(
      contractChangeBatch: ContractChangeBatch,
      traceChanges: Boolean,
  ): Seq[DigestDelta] = {
    val partyDeltas = contractChangeBatch.changes.iterator
      .flatMap(partyDeltasForSingleContractChange(_, traceChanges))
      .toSeq
      .groupBy1(delta => (delta.partyId, delta.operation))
      .map { case ((party, operation), deltas) =>
        (party, operation) -> DigestDelta.Party(
          party,
          DigestOps.combineDigests(deltas.map(_.digest)),
          operation,
        )
      }

    val partiesByParticipant = DigestOps.invertMap(contractChangeBatch.partyHostings)

    val participantDeltas: Seq[DigestDelta] = partiesByParticipant.flatMap {
      case (counterParticipant, parties) =>
        def combineDigestsForOperation(
            operation: DigestOperation
        ): Option[DigestDelta.Participant] = {
          val digestsForCounterParticipant =
            // start with an iterator and convert to a Seq to not calculate the hashcode of potentially many digests
            parties.iterator
              .flatMap(party => partyDeltas.get((party, operation)).map(_.digest))
              .toSeq
          NonEmpty.from(digestsForCounterParticipant).map { digestsNE =>
            DigestDelta.Participant(
              participantId = counterParticipant,
              digest = DigestOps.combineDigests(digestsNE),
              operation = operation,
            )
          }
        }

        combineDigestsForOperation(Add).toList ++ combineDigestsForOperation(Remove)
    }.toSeq

    partyDeltas.values.toSeq ++ participantDeltas
  }

  def partyDeltasForSingleContractChange(
      contractChange: ContractChange,
      traceChanges: Boolean,
  ): Iterator[DigestDelta.Party] = {
    val stakeholderIds = contractChange.stakeholders
    val locallyHostedStakeholderIds =
      contractChange.locallyHostedStakeholders.toSet

    val partyPairsToCompute = for {
      local <- locallyHostedStakeholderIds
      stakeholder <- stakeholderIds

    } yield orderedPartyPair((local, stakeholder))

    val digestPerPartyPair = partyPairsToCompute.map { case (fromParty, toParty) =>
      (fromParty, toParty) -> DigestOps.singleDigest(
        contractId = contractChange.cid,
        reassignmentCounter = contractChange.rc,
        partyId1 = fromParty,
        partyId2 = toParty,
        isActivation = contractChange.isActivation,
        traceChanges = traceChanges,
      )
    }.toMap

    val digestOperation: DigestOperation =
      if (contractChange.isActivation) DigestOperation.Add else DigestOperation.Remove

    stakeholderIds.iterator.map { stakeholderId =>
      val partyPairs = locallyHostedStakeholderIds.map((_, stakeholderId)).map(orderedPartyPair)
      val digest = DigestOps.combineDigests(partyPairs.map(digestPerPartyPair))

      DigestDelta.Party(
        stakeholderId,
        digest = digest,
        operation = digestOperation,
      )
    }
  }

  def combineDigests(allDigests: Iterable[TracedLtHash16Blake3]): TracedLtHash16Blake3 =
    allDigests.foldLeft(TracedLtHash16Blake3.empty) { case (acc, digest) =>
      acc.union(digest)
      acc
    }

  def singleDigest(
      contractId: LfContractId,
      reassignmentCounter: ReassignmentCounter,
      partyId1: LfPartyId,
      partyId2: LfPartyId,
      isActivation: Boolean,
      traceChanges: Boolean,
  ): TracedLtHash16Blake3 = {
    val hash = TracedLtHash16Blake3.empty
    hash.add(
      singleDigestByteArray(contractId, reassignmentCounter, partyId1, partyId2),
      Option.when(traceChanges)(
        SingleTrace(contractId, reassignmentCounter, partyId1, partyId2, isActivation)
      ),
    )
    hash
  }

  private def singleDigestByteArray(
      contractId: LfContractId,
      reassignmentCounter: ReassignmentCounter,
      partyId1: LfPartyId,
      partyId2: LfPartyId,
  ): Array[Byte] = (
    contractId.encodeDeterministically
      concat ReassignmentCounter.encodeDeterministically(reassignmentCounter)
      concat DeterministicEncoding.encodeString(partyId1)
      concat DeterministicEncoding.encodeString(partyId2)
  ).toByteArray

  private[digest] def invertMap[A, B](inputMap: Map[A, Set[B]]): Map[B, Set[A]] =
    inputMap.toSeq
      .flatMap { case (a, bb) => bb.map(a -> _) }
      .groupMap(_._2)(_._1)
      .map { case (k, v) => k -> v.toSet }

  private def orderedPartyPair(partyPair: (LfPartyId, LfPartyId)): (LfPartyId, LfPartyId) = {
    val (partyId1, partyId2) = partyPair

    if (partyId1 < partyId2) (partyId1, partyId2)
    else (partyId2, partyId1)
  }
}

sealed trait DigestOperation extends Product with Serializable

object DigestOperation {
  case object Add extends DigestOperation
  case object Remove extends DigestOperation
}

sealed trait DigestDelta extends Product with Serializable {
  def digest: TracedLtHash16Blake3
  def operation: DigestOperation
}

object DigestDelta {

  final case class Party(
      partyId: LfPartyId,
      digest: TracedLtHash16Blake3,
      operation: DigestOperation,
  ) extends DigestDelta

  final case class Participant(
      participantId: LedgerParticipantId,
      digest: TracedLtHash16Blake3,
      operation: DigestOperation,
  ) extends DigestDelta
}
