// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.party.acsreplication

import com.digitalasset.canton.ReassignmentCounter
import com.digitalasset.canton.crypto.LtHash16Blake3
import com.digitalasset.canton.data.{CantonTimestamp, ContractReassignment}
import com.digitalasset.canton.participant.protocol.party.acsreplication.PartyReplicationAcsDigestHelper.bytesFromReassignment
import com.digitalasset.canton.protocol.ContractIdSyntax.*
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.topology.{ParticipantId, PhysicalSynchronizerId}
import com.digitalasset.nonempty.NonEmpty
import com.google.protobuf.ByteString

/** Methods not meant to be called concurrently. The caller / class owner needs to ensure
  * non-concurrent access.
  *
  * @param getAcsArguments
  *   The arguments that define the ACS snapshot.
  * @param sourceParticipantId
  *   The source participant that the digest applies to.
  * @param agreedAt
  *   The time of the ACS replication agreement.
  * @param initialAcsHashO
  *   If contracts have been previously replicated, hold the corresponding acs digest hash.
  */
final private[party] class PartyReplicationAcsDigestHelper(
    getAcsArguments: AcsReplicationSourceParticipantMessage.GetAcsArguments,
    sourceParticipantId: ParticipantId,
    agreedAt: CantonTimestamp,
    initialAcsHashO: Option[ByteString],
    psid: PhysicalSynchronizerId,
) {
  // Mutable contract homomorphic hash
  private val contractsDigest =
    initialAcsHashO.fold(LtHash16Blake3.empty: LtHash16Blake3)(LtHash16Blake3.tryCreate)

  def addContract(lfContractId: LfContractId, reassignmentCounter: ReassignmentCounter): Unit =
    contractsDigest.add(bytesFromReassignment(lfContractId, reassignmentCounter))

  def computeDeltaDigest(
      reassignmentsNE: NonEmpty[Seq[ContractReassignment]]
  ): LtHash16Blake3 = {
    val delta = LtHash16Blake3.empty
    reassignmentsNE.foreach { case ContractReassignment(contract, _, _, counter) =>
      delta.add(bytesFromReassignment(contract.contractId, counter))
    }
    delta
  }

  def computeUnionHash(delta: LtHash16Blake3): ByteString = {
    val res = LtHash16Blake3.empty
    res.union(contractsDigest)
    res.union(delta)
    res.getByteString
  }

  def applyDelta(delta: LtHash16Blake3): Unit = contractsDigest.union(delta)

  def extractAcsDigest() =
    AcsReplicationSourceParticipantMessage.AcsDigest(
      getAcsHash,
      getAcsArguments,
      sourceParticipantId.uid,
      agreedAt,
      psid.protocolVersion,
    )

  def getAcsHash: ByteString = contractsDigest.getByteString
}

private[party] object PartyReplicationAcsDigestHelper {
  def bytesFromReassignment(
      lfContractId: LfContractId,
      reassignmentCounter: ReassignmentCounter,
  ): Array[Byte] = (
    lfContractId.encodeDeterministically
      concat ReassignmentCounter.encodeDeterministically(reassignmentCounter)
  ).toByteArray
}
