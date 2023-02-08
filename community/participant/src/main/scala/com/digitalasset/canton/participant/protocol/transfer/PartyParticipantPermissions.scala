// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.transfer

import cats.syntax.parallel.*
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.participant.protocol.transfer.PartyParticipantPermissions.PartyParticipants
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.transaction.ParticipantAttributes
import com.digitalasset.canton.topology.transaction.ParticipantPermission.{
  Confirmation,
  Disabled,
  Observation,
  Submission,
}
import com.digitalasset.canton.util.FutureInstances.*

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

private[transfer] final case class PartyParticipantPermissions(
    // partyId -> (permission on source domain, permission on target domain)
    permissions: Map[LfPartyId, (PartyParticipants, PartyParticipants)],
    sourceTs: CantonTimestamp,
    targetTs: CantonTimestamp,
)

private[transfer] object PartyParticipantPermissions {
  case class PartyParticipants(
      submission: Set[ParticipantId],
      confirmation: Set[ParticipantId],
      other: Set[ParticipantId],
  ) {
    require(
      !submission.exists(confirmation.contains),
      "submission and confirmation permissions must be disjoint.",
    )
    require(
      !submission.exists(other.contains),
      "submission and other permissions must be disjoint.",
    )
    require(
      !confirmation.exists(other.contains),
      "confirmation and other permissions must be disjoint.",
    )

    def submitters: Set[ParticipantId] = submission

    def confirmers: Set[ParticipantId] = submission ++ confirmation

    def all: Set[ParticipantId] = submission ++ confirmation ++ other
  }

  def apply(
      stakeholders: Set[LfPartyId],
      sourceIps: TopologySnapshot,
      targetIps: TopologySnapshot,
  )(implicit ec: ExecutionContext): Future[PartyParticipantPermissions] = {
    val permissions = stakeholders.toList
      .parTraverse { stakeholder =>
        val sourceF = partyParticipants(sourceIps, stakeholder)
        val targetF = partyParticipants(targetIps, stakeholder)
        for {
          source <- sourceF
          target <- targetF
        } yield (stakeholder, (source, target))
      }
      .map(_.toMap)

    permissions.map(PartyParticipantPermissions(_, sourceIps.timestamp, targetIps.timestamp))
  }

  private def partyParticipants(ips: TopologySnapshot, party: LfPartyId)(implicit
      ec: ExecutionContext
  ): Future[PartyParticipants] = {

    val submission = mutable.Set.newBuilder[ParticipantId]
    val confirmation = mutable.Set.newBuilder[ParticipantId]
    val other = mutable.Set.newBuilder[ParticipantId]

    ips.activeParticipantsOf(party).map { partyParticipants =>
      partyParticipants.foreach {
        case (participantId, ParticipantAttributes(permission, _trustLevel)) =>
          permission match {
            case Submission => submission += participantId
            case Confirmation => confirmation += participantId
            case Observation => other += participantId
            case Disabled =>
              throw new IllegalStateException(
                s"activeParticipantsOf($party) returned a disabled participant $participantId"
              )
          }
      }
      PartyParticipants(
        submission.result().toSet,
        confirmation.result().toSet,
        other.result().toSet,
      )
    }

  }
}
