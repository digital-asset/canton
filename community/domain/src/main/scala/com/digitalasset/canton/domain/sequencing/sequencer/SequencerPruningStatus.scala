// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer

import cats.syntax.traverse._
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.admin.v0
import com.digitalasset.canton.serialization.ProtoConverter.{ParsingResult, parseRequired}
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.topology.{Member, UnauthenticatedMemberId}
import com.digitalasset.canton.util.HasProtoV0

import scala.Ordering.Implicits._

case class SequencerMemberStatus(
    member: Member,
    registeredAt: CantonTimestamp,
    lastAcknowledged: Option[CantonTimestamp],
    enabled: Boolean = true,
) extends HasProtoV0[v0.SequencerMemberStatus] {
  def safePruningTimestamp: CantonTimestamp =
    lastAcknowledged.getOrElse(
      registeredAt
    )

  override def toProtoV0: v0.SequencerMemberStatus =
    v0.SequencerMemberStatus(
      member.toProtoPrimitive,
      Some(registeredAt.toProtoPrimitive),
      lastAcknowledged.map(_.toProtoPrimitive),
      enabled,
    )
}

/** Structure housing both members and instances of those members. Used to list clients that have been or need to be
  * disabled.
  */
case class SequencerClients(
    members: Set[Member] = Set.empty
)

/** Pruning status of a Sequencer.
  * @param lowerBound the earliest timestamp that can be read
  * @param now the current time of the sequencer clock
  * @param members details of registered members
  */
case class SequencerPruningStatus(
    lowerBound: CantonTimestamp,
    now: CantonTimestamp,
    members: Seq[SequencerMemberStatus],
) extends HasProtoV0[v0.SequencerPruningStatus] {

  /** Using the member details, calculate based on their acknowledgements when is the latest point we can
    * safely prune without losing any data that may still be read.
    */
  lazy val safePruningTimestamp: CantonTimestamp = {
    val earliestMemberTs = members
      .filter(_.enabled)
      .map(_.safePruningTimestamp)
      .reduceLeftOption(_ min _)

    // if there are no members (or they've all been ignored), we can technically prune everything.
    // as in practice a domain will register a IDM, Sequencer and Mediator, this will most likely never occur.
    earliestMemberTs.getOrElse(now)
  }

  lazy val disabledClients: SequencerClients = SequencerClients(
    members = members.filterNot(_.enabled).map(_.member).toSet
  )

  def unauthenticatedMembersToDisable(retentionPeriod: NonNegativeFiniteDuration): Set[Member] =
    members.foldLeft(Set.empty[Member]) { (toDisable, memberStatus) =>
      memberStatus.member match {
        case _: UnauthenticatedMemberId if memberStatus.enabled =>
          if (now.minus(retentionPeriod.unwrap) > memberStatus.safePruningTimestamp) {
            toDisable + memberStatus.member
          } else toDisable
        case _ => toDisable
      }
    }

  /** List clients that would need to be disabled to allow pruning at the given timestamp.
    */
  def clientsPreventingPruning(timestamp: CantonTimestamp): SequencerClients =
    members.foldLeft(SequencerClients()) { (disabled, memberStatus) =>
      if (memberStatus.safePruningTimestamp.isBefore(timestamp)) {
        disabled.copy(members = disabled.members + memberStatus.member)
      } else disabled
    }

  override def toProtoV0: v0.SequencerPruningStatus =
    v0.SequencerPruningStatus(
      earliestEventTimestamp = Some(lowerBound.toProtoPrimitive),
      now = Some(now.toProtoPrimitive),
      members = members.map(_.toProtoV0),
    )
}

object SequencerMemberStatus {
  def fromProtoV0(
      memberStatusP: v0.SequencerMemberStatus
  ): ParsingResult[SequencerMemberStatus] =
    for {
      member <- Member.fromProtoPrimitive(memberStatusP.member, "member")
      registeredAt <- parseRequired(
        CantonTimestamp.fromProtoPrimitive,
        "registeredAt",
        memberStatusP.registeredAt,
      )
      lastAcknowledgedO <- memberStatusP.lastAcknowledged.traverse(
        CantonTimestamp.fromProtoPrimitive
      )
    } yield SequencerMemberStatus(member, registeredAt, lastAcknowledgedO, memberStatusP.enabled)
}

object SequencerPruningStatus {

  /** Sentinel value to use for Sequencers that don't yet support the status endpoint */
  lazy val Unimplemented =
    SequencerPruningStatus(CantonTimestamp.MinValue, CantonTimestamp.MinValue, members = Seq.empty)

  def fromProtoV0(
      statusP: v0.SequencerPruningStatus
  ): ParsingResult[SequencerPruningStatus] =
    for {
      earliestEventTimestamp <- parseRequired(
        CantonTimestamp.fromProtoPrimitive,
        "earliestEventTimestamp",
        statusP.earliestEventTimestamp,
      )
      now <- parseRequired(CantonTimestamp.fromProtoPrimitive, "now", statusP.now)
      members <- statusP.members.traverse(SequencerMemberStatus.fromProtoV0)
    } yield SequencerPruningStatus(earliestEventTimestamp, now, members)
}
