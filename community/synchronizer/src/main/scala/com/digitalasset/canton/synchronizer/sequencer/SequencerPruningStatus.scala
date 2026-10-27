// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer

import cats.syntax.traverse.*
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.pretty.{
  Pretty,
  PrettyPrintingCompanion,
  PrettyPrintingFromCompanion,
}
import com.digitalasset.canton.sequencer.admin.v31.SequencerMemberStatus.Status
import com.digitalasset.canton.sequencer.admin.{v30, v31}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.synchronizer.sequencer.SequencerMemberStatus.{
  Active,
  Disabled,
  Onboarding,
}
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.validation.ProtoValidation
import com.digitalasset.canton.version.ProtocolVersionValidation

sealed trait SequencerMemberStatus extends PrettyPrintingFromCompanion {
  def member: Member

  /** The v30 message can only express a registration timestamp plus an enabled flag, so it has no
    * representation for a member without a registration timestamp: one that is still onboarding, or
    * one that was disabled while it was still onboarding. Such members must therefore not reach
    * this encoding, which is only used below
    * [[com.digitalasset.canton.version.ProtocolVersion.v37]]. This is enforced upstream by
    * [[com.digitalasset.canton.synchronizer.sequencing.traffic.TrafficPurchasedManager.addTrafficPurchased]],
    * which does not register onboarding members below that protocol version.
    */
  def toProtoV30: Either[String, v30.SequencerMemberStatus] = {
    val fieldsE = this match {
      case Onboarding(onboardingMember) =>
        Left(
          s"Cannot serialize the onboarding member '$onboardingMember' to a v30 sequencer member status."
        )
      case Active(_, registeredAt, lastAcknowledged) =>
        Right((registeredAt, lastAcknowledged, true))
      case Disabled(disabledMember, None, _) =>
        Left(
          s"Cannot serialize the disabled member '$disabledMember' to a v30 sequencer member status:" +
            s" it has no registration timestamp because it was disabled while still onboarding."
        )
      case Disabled(_, Some(registeredAt), lastAcknowledged) =>
        Right((registeredAt, lastAcknowledged, false))
    }

    fieldsE.map { case (registeredAt, lastAcknowledged, enabled) =>
      v30.SequencerMemberStatus(
        member = member.toProtoPrimitive,
        registeredAt = registeredAt.toProtoPrimitive,
        lastAcknowledged = lastAcknowledged.map(_.toProtoPrimitive),
        enabled = enabled,
      )
    }
  }

  def toProtoV31: v31.SequencerMemberStatus = {
    val statusP = this match {
      case Onboarding(_) =>
        v31.SequencerMemberStatus.Status.Onboarding(v31.SequencerMemberStatus.Onboarding())
      case Active(_, registeredAt, lastAcknowledged) =>
        v31.SequencerMemberStatus.Status.Active(
          v31.SequencerMemberStatus.Active(
            registeredAt.toProtoPrimitive,
            lastAcknowledged.map(_.toProtoPrimitive),
          )
        )
      case Disabled(_, registeredAt, lastAcknowledged) =>
        v31.SequencerMemberStatus.Status.Disabled(
          v31.SequencerMemberStatus.Disabled(
            registeredAt.map(_.toProtoPrimitive),
            lastAcknowledged.map(_.toProtoPrimitive),
          )
        )
    }
    v31.SequencerMemberStatus(member.toProtoPrimitive, statusP)
  }

  override def prettyCompanion: PrettyPrintingCompanion[SequencerMemberStatus] =
    SequencerMemberStatus
}

object SequencerMemberStatus extends PrettyPrintingCompanion[SequencerMemberStatus] {

  final case class Onboarding(member: Member) extends SequencerMemberStatus

  final case class Active(
      member: Member,
      registeredAt: CantonTimestamp,
      lastAcknowledged: Option[CantonTimestamp],
  ) extends SequencerMemberStatus {
    def safePruningTimestamp: CantonTimestamp = lastAcknowledged.getOrElse(registeredAt)
  }

  final case class Disabled(
      member: Member,
      registeredAt: Option[CantonTimestamp],
      lastAcknowledged: Option[CantonTimestamp],
  ) extends SequencerMemberStatus

  override protected val pretty: Pretty[SequencerMemberStatus] = {
    case onboarding: Onboarding =>
      prettyOfClass[Onboarding](param("member", _.member)).treeOf(onboarding)
    case active: Active =>
      prettyOfClass[Active](
        param("member", _.member),
        param("registered at", _.registeredAt),
        paramIfDefined("last acknowledged", _.lastAcknowledged),
      ).treeOf(active)
    case disabled: Disabled =>
      prettyOfClass[Disabled](
        param("member", _.member),
        param("registered at", _.registeredAt),
        paramIfDefined("last acknowledged", _.lastAcknowledged),
      ).treeOf(disabled)
  }

  def fromProtoV30(
      memberStatusP: v30.SequencerMemberStatus
  ): ParsingResult[SequencerMemberStatus] =
    for {
      member <- Member.fromProtoPrimitive(memberStatusP.member, "member")
      registeredAt <- CantonTimestamp.fromProtoPrimitive(memberStatusP.registeredAt)
      lastAcknowledged <- memberStatusP.lastAcknowledged.traverse(
        CantonTimestamp.fromProtoPrimitive
      )
    } yield
      if (memberStatusP.enabled) Active(member, registeredAt, lastAcknowledged)
      else Disabled(member, Some(registeredAt), lastAcknowledged)

  def fromProtoV31(p: v31.SequencerMemberStatus): ParsingResult[SequencerMemberStatus] =
    for {
      member <- Member.fromProtoPrimitive(p.member, "member")
      status <- p.status match {
        case Status.Onboarding(_) => Right(Onboarding(member): SequencerMemberStatus)
        case Status.Active(a) =>
          for {
            registeredAt <- CantonTimestamp.fromProtoPrimitive(a.registeredAt)
            lastAcknowledged <- a.lastAcknowledged.traverse(CantonTimestamp.fromProtoPrimitive)
          } yield Active(member, registeredAt, lastAcknowledged): SequencerMemberStatus
        case Status.Disabled(d) =>
          for {
            registeredAt <- d.registeredAt.traverse(CantonTimestamp.fromProtoPrimitive)
            lastAcknowledged <- d.lastAcknowledged.traverse(CantonTimestamp.fromProtoPrimitive)
          } yield Disabled(member, registeredAt, lastAcknowledged): SequencerMemberStatus
        case Status.Empty => Left(ProtoDeserializationError.FieldNotSet("status"))
      }
    } yield status
}

/** Structure housing both members and instances of those members. Used to list clients that have
  * been or need to be disabled.
  */
final case class SequencerClients(
    members: Set[Member] = Set.empty
)

/** Pruning status of a Sequencer.
  *
  * @param now
  *   the current time of the sequencer clock
  */
final case class SequencerPruningStatus(
    lowerBound: CantonTimestamp,
    now: CantonTimestamp,
    members: Set[SequencerMemberStatus],
) extends PrettyPrintingFromCompanion {

  /** Inactive members, for testing */
  private[sequencer] def inactiveClients: SequencerClients = SequencerClients(
    members = members
      .filter {
        case _: Onboarding => true
        case _: Active => false
        case _: Disabled => true
      }
      .map(_.member)
  )

  /** Using the member details, calculate based on their acknowledgements when is the latest point
    * we can safely prune without losing any data that may still be read.
    *
    * if there are no members (or they've all been ignored), we can technically prune everything. as
    * in practice a synchronizer will register a IDM, Sequencer and Mediator, this will most likely
    * never occur.
    */
  lazy val safePruningTimestamp: CantonTimestamp =
    members
      .collect { case a: Active => a }
      .map(_.safePruningTimestamp)
      .minOption
      .getOrElse(now)

  /** List clients that would need to be disabled to allow pruning at the given timestamp.
    */
  def clientsPreventingPruning(timestamp: CantonTimestamp): SequencerClients =
    SequencerClients(
      members
        .collect {
          case a: Active if a.safePruningTimestamp.isBefore(timestamp) => a
        }
        .map(_.member)
    )

  /** Drops the members without a registration timestamp: those still onboarding and those disabled
    * while onboarding. The v30 encoding cannot represent them, and as they have no events they do
    * not affect pruning.
    */
  def withoutUnregisteredMembers: SequencerPruningStatus =
    copy(members = members.filter {
      case _: Onboarding => false
      case _: Active => true
      case disabled: Disabled => disabled.registeredAt.isDefined
    })

  def toProtoV30: Either[String, v30.SequencerPruningStatus] =
    members.toSeq.traverse(_.toProtoV30).map { membersP =>
      v30.SequencerPruningStatus(
        earliestEventTimestamp = lowerBound.toProtoPrimitive,
        now = now.toProtoPrimitive,
        members = membersP,
      )
    }

  def toProtoV31: v31.SequencerPruningStatus =
    v31.SequencerPruningStatus(
      earliestEventTimestamp = lowerBound.toProtoPrimitive,
      now = now.toProtoPrimitive,
      members = members.toSeq.map(_.toProtoV31),
    )

  override def prettyCompanion: PrettyPrintingCompanion[SequencerPruningStatus] =
    SequencerPruningStatus
}

object SequencerPruningStatus extends PrettyPrintingCompanion[SequencerPruningStatus] {

  override protected val pretty: Pretty[SequencerPruningStatus] = prettyOfClass(
    param("lower bound", _.lowerBound),
    param("now", _.now),
    paramIfNonEmpty("members", _.members),
  )

  /** Sentinel value to use for Sequencers that don't yet support the status endpoint */
  lazy val Unimplemented: SequencerPruningStatus =
    SequencerPruningStatus(CantonTimestamp.MinValue, CantonTimestamp.MinValue, members = Set.empty)

  def fromProtoV30(
      statusP: v30.SequencerPruningStatus
  ): ParsingResult[SequencerPruningStatus] =
    // TODO(#35385): Reinstate a practical admin API members limit.
    for {
      validatedMembers <- ProtoValidation.validateLength(
        statusP.members,
        "members",
        ProtocolVersionValidation.NoValidation,
        ProtoValidation.MaxCollectionSize,
      )
      earliestEventTimestamp <- CantonTimestamp.fromProtoPrimitive(statusP.earliestEventTimestamp)
      now <- CantonTimestamp.fromProtoPrimitive(statusP.now)
      members <- validatedMembers.traverse(SequencerMemberStatus.fromProtoV30).map(_.toSet)
    } yield SequencerPruningStatus(earliestEventTimestamp, now, members)

  def fromProtoV31(
      statusP: v31.SequencerPruningStatus
  ): ParsingResult[SequencerPruningStatus] =
    // TODO(#35385): Reinstate a practical admin API members limit.
    for {
      validatedMembers <- ProtoValidation.validateLength(
        statusP.members,
        "members",
        ProtocolVersionValidation.NoValidation,
        ProtoValidation.MaxCollectionSize,
      )
      earliestEventTimestamp <- CantonTimestamp.fromProtoPrimitive(statusP.earliestEventTimestamp)
      now <- CantonTimestamp.fromProtoPrimitive(statusP.now)
      members <- validatedMembers.traverse(SequencerMemberStatus.fromProtoV31).map(_.toSet)
    } yield SequencerPruningStatus(earliestEventTimestamp, now, members)
}
