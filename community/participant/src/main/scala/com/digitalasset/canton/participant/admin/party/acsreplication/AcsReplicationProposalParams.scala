// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.party.acsreplication

import cats.syntax.either.*
import cats.syntax.traverse.*
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.Hash
import com.digitalasset.canton.participant.admin.workflows.java.canton.internal as M
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.nonempty.NonEmpty

import scala.jdk.CollectionConverters.*

final case class AcsReplicationProposalParams private (
    requestId: Hash,
    partyId: PartyId,
    synchronizerId: SynchronizerId,
    targetParticipantId: ParticipantId,
    sequencerIds: NonEmpty[List[SequencerId]],
    serial: PositiveInt,
    participantPermission: ParticipantPermission,
)

object AcsReplicationProposalParams {
  def fromDaml(
      c: M.acsreplication.AcsReplicationProposal,
      synchronizer: String,
  ): Either[String, AcsReplicationProposalParams] =
    for {
      _ <- Either.cond(c.acsReplicationId.nonEmpty, (), "Empty ACS replication id")
      requestId <- Hash
        .fromHexString(c.acsReplicationId)
        .leftMap(err => s"Invalid ACS replication id: $err")
      partyId <-
        PartyId
          .fromProtoPrimitive(c.partyId, "partyId")
          .leftMap(err => s"Invalid partyId $err")
      // Check the target participant. The source participant has already been checked by the transaction filter.
      targetParticipantId <-
        PartyId
          .fromProtoPrimitive(c.targetParticipant, "targetParticipant")
          .bimap(
            err => s"Invalid targetParticipant admin party $err",
            adminPartyId => ParticipantId(adminPartyId.uid),
          )
      sequencerIds <-
        c.sequencerUids.asScala.toList.traverse(sequencerUid =>
          UniqueIdentifier
            .fromProtoPrimitive(sequencerUid, "sequencerUids")
            .bimap(err => s"Invalid unique identifier $sequencerUid: $err", SequencerId(_))
        )
      sequencerIdsNE <- NonEmpty.from(sequencerIds).toRight("Empty sequencerIds")
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
      // TODO(#35267) make optional and add CantonTimestamp
      serial <- PositiveInt.create(serialInt).leftMap(_.message)
      // TODO(#35267) ACS replicator shouldn't know about party party participant permission
      participantPermission = PartyParticipantPermission.fromDaml(c.participantPermission)
    } yield AcsReplicationProposalParams(
      requestId,
      partyId,
      synchronizerId,
      targetParticipantId,
      sequencerIdsNE,
      // TODO(#35267) replace with CantonTimestamp
      serial,
      // TODO(#35267) ACS replicator shouldn't know about party party participant permission
      participantPermission,
    )
}
