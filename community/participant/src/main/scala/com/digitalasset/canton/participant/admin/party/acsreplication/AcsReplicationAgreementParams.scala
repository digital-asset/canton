// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.party.acsreplication

import cats.syntax.either.*
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.Hash
import com.digitalasset.canton.participant.admin.party.PartyReplicationStatus
import com.digitalasset.canton.participant.admin.workflows.java.canton.internal as M
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import io.scalaland.chimney.dsl.*

/** Internal representation of party ACS agreement parameters.
  */
final case class AcsReplicationAgreementParams(
    requestId: Hash,
    partyId: PartyId,
    synchronizerId: SynchronizerId,
    sourceParticipantId: ParticipantId,
    targetParticipantId: ParticipantId,
    sequencerId: SequencerId,
    serial: PositiveInt,
    participantPermission: ParticipantPermission,
)

object AcsReplicationAgreementParams {
  def fromDaml(
      c: M.acsreplication.AcsReplicationAgreement,
      synchronizer: String,
  ): Either[String, AcsReplicationAgreementParams] =
    for {
      _ <- Either.cond(c.acsReplicationId.nonEmpty, (), "Empty ACS replication id")
      requestId <- Hash
        .fromHexString(c.acsReplicationId)
        .leftMap(err => s"Invalid ACS replication id: $err")
      partyId <-
        PartyId
          .fromProtoPrimitive(c.partyId, "partyId")
          .leftMap(err => s"Invalid partyId $err")
      sourceParticipantId <-
        PartyId
          .fromProtoPrimitive(c.sourceParticipant, "sourceParticipant")
          .bimap(
            err => s"Invalid sourceParticipant admin party $err",
            adminPartyId => ParticipantId(adminPartyId.uid),
          )
      targetParticipantId <-
        PartyId
          .fromProtoPrimitive(c.targetParticipant, "targetParticipant")
          .bimap(
            err => s"Invalid targetParticipant admin party $err",
            adminPartyId => ParticipantId(adminPartyId.uid),
          )
      sequencerId <-
        UniqueIdentifier
          .fromProtoPrimitive(c.sequencerUid, "sequencerId")
          .bimap(err => s"Invalid sequencerId $err", SequencerId(_))
      synchronizerId <-
        SynchronizerId
          .fromProtoPrimitive(synchronizer, "synchronizer")
          // The following error is impossible to trigger as the ledger-api does not emit invalid synchronizer ids
          .leftMap(err => s"Invalid synchronizerId $err")
      serialInt <- Either.cond(
        c.topologySerial.toInt.toLong == c.topologySerial,
        c.topologySerial.toInt,
        s"Non-integer serial ${c.topologySerial}",
      )
      // TODO(#35267) replace with CantonTimestamp
      serial <- PositiveInt.create(serialInt).leftMap(_.message)
      // TODO(#35267) ACS replicator shouldn't know about party party participant permission
      participantPermission = PartyParticipantPermission.fromDaml(c.participantPermission)
    } yield AcsReplicationAgreementParams(
      requestId,
      partyId,
      synchronizerId,
      sourceParticipantId,
      targetParticipantId,
      sequencerId,
      serial, // TODO(#35267) make optional and add CantonTimestamp
      participantPermission, // TODO(#35267) ACS replicator shouldn't know about party party participant permission
    )

  def fromProposal(
      proposal: AcsReplicationProposalParams,
      sourceParticipantId: ParticipantId,
      sequencerId: SequencerId,
  ): AcsReplicationAgreementParams = proposal
    .into[AcsReplicationAgreementParams]
    .withFieldConst(_.sourceParticipantId, sourceParticipantId)
    .withFieldConst(_.sequencerId, sequencerId)
    .transform

  def fromAgreedReplicationStatus(
      // TODO(#35267) switch to AcsReplicationStatus.AcsReplicationParameters
      params: PartyReplicationStatus.ReplicationParams,
      sequencerId: SequencerId,
  ): AcsReplicationAgreementParams = params
    .into[AcsReplicationAgreementParams]
    .withFieldConst(_.sequencerId, sequencerId)
    .transform
}
