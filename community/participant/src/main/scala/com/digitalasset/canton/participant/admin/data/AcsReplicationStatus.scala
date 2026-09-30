// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.data

import cats.implicits.toTraverseOps
import com.daml.ledger.api.v2.admin.party_management_alpha_service.PartyReplicationStatus as LapiPartyReplicationStatus
import com.digitalasset.canton.admin.participant.v30
import com.digitalasset.canton.logging.pretty.{
  Pretty,
  PrettyPrintingCompanion,
  PrettyPrintingFromCompanion,
}
import com.digitalasset.canton.participant.admin.data.AcsReplicationStatus.SequencerChannelAgreement
import com.digitalasset.canton.participant.admin.party.acsreplication.AcsReplicationStatus as InternalStatus
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.{SequencerId, UniqueIdentifier}

/** External console representation of the ACS replication process. Refer to
  * party_management_service.proto AcsReplicationStatus for the semantics.
  */
final case class AcsReplicationStatus(
    agreement: Option[SequencerChannelAgreement],
    hasCompleted: Boolean,
    errorO: Option[AcsReplicationStatus.AcsReplicationError],
) extends PrettyPrintingFromCompanion {
  def toProtoV30: v30.PartyReplicationStatus.AcsReplicationStatus =
    v30.PartyReplicationStatus.AcsReplicationStatus(
      agreement.map(_.toProtoV30),
      hasCompleted,
      errorO.map(_.toProtoV30),
    )

  def toLapiProto: LapiPartyReplicationStatus = {
    val state =
      if (errorO.exists(_.isTerminal)) LapiPartyReplicationStatus.State.STATE_FAILED
      else if (hasCompleted) LapiPartyReplicationStatus.State.STATE_COMPLETED
      else LapiPartyReplicationStatus.State.STATE_IN_PROGRESS

    LapiPartyReplicationStatus(
      current = state,
      error = errorO.flatMap {
        case _: AcsReplicationStatus.AcsReplicationError.Disconnected => None
        case AcsReplicationStatus.AcsReplicationError.AcsReplicationFailed(message) =>
          Some(LapiPartyReplicationStatus.ErrorDetails(message))
      },
    )
  }

  override def prettyCompanion: PrettyPrintingCompanion[AcsReplicationStatus] =
    AcsReplicationStatus
}

object AcsReplicationStatus extends PrettyPrintingCompanion[AcsReplicationStatus] {
  override protected val pretty: Pretty[AcsReplicationStatus] =
    prettyOfClass(
      param("agreement", _.agreement),
      paramIfDefined("error", _.errorO),
      paramIfTrue("complete", _.hasCompleted),
    )

  def fromInternal: InternalStatus => AcsReplicationStatus = {
    // When new fields are added, adapt this deciding which fields may need to be exposed externally erring
    // on the side of cautious not exposing fields that don't have clear external utility and can be preserved
    // in a backward compatible way.
    case InternalStatus(
          _,
          agreementStatus,
          _,
          _,
          hasCompleted,
          errorO,
        ) =>
      AcsReplicationStatus(
        SequencerChannelAgreement.fromInternal(agreementStatus),
        hasCompleted,
        errorO.map(AcsReplicationError.fromInternal),
      )
  }

  def fromProtoV30(
      proto: v30.PartyReplicationStatus.AcsReplicationStatus
  ): ParsingResult[AcsReplicationStatus] =
    for {
      agreement <- proto.agreement.traverse(SequencerChannelAgreement.fromProtoV30)
      hasCompleted = proto.hasCompleted
      errorO <- proto.errorMessage.traverse(AcsReplicationError.fromProtoV30)
    } yield AcsReplicationStatus(
      agreement,
      hasCompleted,
      errorO,
    )

  final case class SequencerChannelAgreement(sequencerId: SequencerId)
      extends PrettyPrintingFromCompanion {
    def toProtoV30: v30.PartyReplicationStatus.AcsReplicationStatus.SequencerChannelAgreement =
      v30.PartyReplicationStatus.AcsReplicationStatus.SequencerChannelAgreement(
        sequencerId.uid.toProtoPrimitive
      )

    override def prettyCompanion: PrettyPrintingCompanion[SequencerChannelAgreement] =
      SequencerChannelAgreement
  }

  private object SequencerChannelAgreement
      extends PrettyPrintingCompanion[SequencerChannelAgreement] {
    val fromInternal: InternalStatus.AgreementStatus => Option[SequencerChannelAgreement] = {
      case InternalStatus.AgreementStatus.Exists(_, _, sequencerId) =>
        Some(SequencerChannelAgreement(sequencerId))
      case _ => None
    }

    def fromProtoV30(
        proto: v30.PartyReplicationStatus.AcsReplicationStatus.SequencerChannelAgreement
    ): ParsingResult[SequencerChannelAgreement] =
      for {
        sequencerId <- UniqueIdentifier
          .fromProtoPrimitive(proto.sequencerUid, "sequencer_uid")
          .map(SequencerId(_))
      } yield SequencerChannelAgreement(sequencerId)

    override protected val pretty: Pretty[SequencerChannelAgreement] = {
      import com.digitalasset.canton.logging.pretty.PrettyInstances.*
      prettyOfClass(param("sequencer", _.sequencerId))
    }
  }

  sealed trait AcsReplicationError extends PrettyPrintingFromCompanion {
    def message: String
    def isTerminal: Boolean
    def toProtoV30: v30.PartyReplicationStatus.AcsReplicationStatus.AcsReplicationError
    override def prettyCompanion: PrettyPrintingCompanion[AcsReplicationError] =
      AcsReplicationError
  }

  object AcsReplicationError extends PrettyPrintingCompanion[AcsReplicationError] {
    final case class Disconnected(message: String) extends AcsReplicationError {
      val isTerminal: Boolean = false
      def toProtoV30: v30.PartyReplicationStatus.AcsReplicationStatus.AcsReplicationError =
        v30.PartyReplicationStatus.AcsReplicationStatus.AcsReplicationError(message)
    }

    final case class AcsReplicationFailed(message: String) extends AcsReplicationError {
      val isTerminal: Boolean = true
      def toProtoV30: v30.PartyReplicationStatus.AcsReplicationStatus.AcsReplicationError =
        v30.PartyReplicationStatus.AcsReplicationStatus.AcsReplicationError(message)
    }

    def fromInternal: InternalStatus.AcsReplicationError => AcsReplicationError = {
      case InternalStatus.AcsReplicationFailed(errorMessage) =>
        AcsReplicationFailed(errorMessage)
      case InternalStatus.Disconnected(errorMessage) => Disconnected(errorMessage)
    }

    def fromProtoV30(
        proto: v30.PartyReplicationStatus.AcsReplicationStatus.AcsReplicationError
    ): ParsingResult[AcsReplicationError] = Right(AcsReplicationFailed(proto.errorMessage))

    override protected val pretty: Pretty[AcsReplicationError] = prettyOfString(_.message)
  }
}
