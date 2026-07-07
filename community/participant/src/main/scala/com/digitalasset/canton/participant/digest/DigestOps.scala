// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.digest

import com.digitalasset.canton.participant.commitment.RunningDigestProcessor.AcsUpdate
import com.digitalasset.canton.participant.commitment.{SingleTrace, TracedLtHash16Blake3}
import com.digitalasset.canton.participant.store.AcsDigestStore.{
  LocalPartyFirst,
  PartyAndOrder,
  PartyOrder,
  RemotePartyFirst,
}
import com.digitalasset.canton.protocol.ContractIdSyntax.*
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.serialization.DeterministicEncoding
import com.digitalasset.canton.{LedgerParticipantId, LfPartyId, ReassignmentCounter}

object DigestOps {

  def computeDeltas(
      thisParticipantId: LedgerParticipantId,
      acsUpdate: AcsUpdate,
      traceChanges: Boolean,
  ): Seq[DigestDelta] = {
    val stakeholderIds = acsUpdate.stakeholders.keySet
    val locallyHostedStakeholderIds =
      acsUpdate.locallyHostedStakeholders.toSet

    val partiesByParticipant = DigestOps.invertMap(acsUpdate.stakeholders)

    val partyPairsToCompute = (for {
      local <- locallyHostedStakeholderIds
      stakeholder <- stakeholderIds
    } yield Set((local, stakeholder), (stakeholder, local))).flatten

    val digestPerPartyPair = partyPairsToCompute.map { case (fromParty, toParty) =>
      (fromParty, toParty) -> DigestOps.singleDigest(
        contractId = acsUpdate.cid,
        reassignmentCounter = acsUpdate.rc,
        partyId1 = fromParty,
        partyId2 = toParty,
        isActivation = acsUpdate.isActivation,
        traceChanges = traceChanges,
      )
    }.toMap

    val digestOperation: DigestOperation =
      if (acsUpdate.isActivation) DigestOperation.Add else DigestOperation.Remove

    val partyDeltas: Map[PartyAndOrder[LfPartyId], DigestDelta] =
      stakeholderIds.toSeq.flatMap { stakeholderId =>
        val partyPairsForFirst = locallyHostedStakeholderIds.map((_, stakeholderId))
        val partyPairsForSecond = locallyHostedStakeholderIds.map((stakeholderId, _))

        val digestsForFirst = DigestOps.combineDigests(partyPairsForFirst.map(digestPerPartyPair))
        val digestsForSecond = DigestOps.combineDigests(partyPairsForSecond.map(digestPerPartyPair))

        val localPartyFirst = PartyAndOrder(stakeholderId, LocalPartyFirst)
        val remotePartyFirst = PartyAndOrder(stakeholderId, RemotePartyFirst)

        Seq(
          localPartyFirst -> DigestDelta.Party(
            localPartyFirst,
            digest = digestsForFirst,
            operation = digestOperation,
          ),
          remotePartyFirst -> DigestDelta.Party(
            remotePartyFirst,
            digest = digestsForSecond,
            operation = digestOperation,
          ),
        )
      }.toMap

    val participantDeltas: Seq[DigestDelta] = partiesByParticipant.map {
      case (counterParticipant, parties) =>
        val partyOrder = PartyOrder.orderFor(thisParticipantId, counterParticipant)

        val digestsForCounterParticipant = parties.view.map { party =>
          partyDeltas(PartyAndOrder(party, partyOrder)).digest
        }
          // convert to a Seq to not calculate the hashcode of potentially many digests
          .toSeq

        val digestForParticipant = DigestOps.combineDigests(digestsForCounterParticipant)

        DigestDelta.Participant(
          participantId = counterParticipant,
          digest = digestForParticipant,
          operation = digestOperation,
        )
    }.toSeq

    partyDeltas.values.toSeq ++ participantDeltas
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
      partyAndOrder: PartyAndOrder[LfPartyId],
      digest: TracedLtHash16Blake3,
      operation: DigestOperation,
  ) extends DigestDelta

  final case class Participant(
      participantId: LedgerParticipantId,
      digest: TracedLtHash16Blake3,
      operation: DigestOperation,
  ) extends DigestDelta
}
