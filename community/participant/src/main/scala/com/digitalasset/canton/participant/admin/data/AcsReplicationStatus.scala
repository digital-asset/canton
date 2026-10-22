// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.data

import cats.implicits.toTraverseOps
import com.daml.ledger.api.v2.admin.party_management_alpha_service.PartyReplicationStatus as LapiPartyReplicationStatus
import com.digitalasset.canton.admin.participant.v30
import com.digitalasset.canton.config.RequireTypes.NonNegativeLong
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.pretty.{
  Pretty,
  PrettyPrintingCompanion,
  PrettyPrintingFromCompanion,
}
import com.digitalasset.canton.participant.admin.data.AcsReplicationStatus.{
  AcsReplicationParameters,
  AcsReplicationProgress,
  PartyToParticipantTopologyTransaction,
}
import com.digitalasset.canton.participant.admin.party.acsreplication.AcsReplicationStatus as InternalStatus
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.*
import com.google.protobuf.ByteString

/** External console representation of the ACS replication process. Refer to
  * party_management_service.proto AcsReplicationStatus for the semantics.
  */
final case class AcsReplicationStatus(
    parameters: AcsReplicationParameters,
    ptpTopologyTransaction: Option[PartyToParticipantTopologyTransaction],
    replicationProgress: Option[AcsReplicationProgress],
    hasCompleted: Boolean,
    errorO: Option[AcsReplicationStatus.AcsReplicationError],
) extends PrettyPrintingFromCompanion {
  def toProtoV30: v30.AcsReplicationStatus =
    v30.AcsReplicationStatus(
      Some(parameters.toProtoV30),
      ptpTopologyTransaction.map(_.toProtoV30),
      replicationProgress.map(_.toProtoV30),
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
      param("parameters", _.parameters),
      paramIfDefined("ptp topology transaction", _.ptpTopologyTransaction),
      paramIfDefined("replication progress", _.replicationProgress),
      paramIfTrue("has completed", _.hasCompleted),
      paramIfDefined("error", _.errorO),
    )

  def fromInternal: InternalStatus => AcsReplicationStatus = {
    // When new fields are added, adapt this deciding which fields may need to be exposed externally erring
    // on the side of cautious not exposing fields that don't have clear external utility and can be preserved
    // in a backward compatible way.
    case InternalStatus(
          parameters,
          _,
          ptpTopologyTransaction,
          replicationProgress,
          hasCompleted,
          errorO,
        ) =>
      AcsReplicationStatus(
        AcsReplicationParameters.fromInternal(parameters),
        ptpTopologyTransaction.map(PartyToParticipantTopologyTransaction.fromInternal),
        replicationProgress.map(AcsReplicationProgress.fromInternal),
        hasCompleted,
        errorO.map(AcsReplicationError.fromInternal),
      )
  }

  def fromProtoV30(
      proto: v30.AcsReplicationStatus
  ): ParsingResult[AcsReplicationStatus] =
    for {
      parametersP <- ProtoConverter.required("parameters", proto.parameters)
      parameters <- AcsReplicationParameters.fromProtoV30(parametersP)
      ptpTopologyTransaction <- proto.ptpTopologyTransaction.traverse(
        PartyToParticipantTopologyTransaction.fromProtoV30
      )
      replicationProgress <- proto.replicationProgress.traverse(AcsReplicationProgress.fromProtoV30)
      hasCompleted = proto.hasCompleted
      errorO <- proto.errorMessage.traverse(AcsReplicationError.fromProtoV30)
    } yield AcsReplicationStatus(
      parameters,
      ptpTopologyTransaction,
      replicationProgress,
      hasCompleted,
      errorO,
    )

  final case class AcsReplicationParameters(
      requestId: String,
      partyId: PartyId,
      synchronizerId: SynchronizerId,
      sourceParticipantId: ParticipantId,
      targetParticipantId: ParticipantId,
      timestamp: CantonTimestamp,
  ) extends PrettyPrintingFromCompanion {

    def toProtoV30: v30.AcsReplicationStatus.AcsReplicationParameters =
      v30.AcsReplicationStatus.AcsReplicationParameters(
        requestId,
        partyId.toProtoPrimitive,
        synchronizerId.uid.toProtoPrimitive,
        sourceParticipantId.uid.toProtoPrimitive,
        targetParticipantId.uid.toProtoPrimitive,
        Some(timestamp.toProtoTimestamp),
      )

    override def prettyCompanion: PrettyPrintingCompanion[AcsReplicationParameters] =
      AcsReplicationParameters
  }

  private object AcsReplicationParameters
      extends PrettyPrintingCompanion[AcsReplicationParameters] {
    def fromInternal: InternalStatus.AcsReplicationParameters => AcsReplicationParameters = {
      case InternalStatus.AcsReplicationParameters(
            requestId,
            partyId,
            synchronizerId,
            sourceParticipantId,
            targetParticipantId,
            timestamp,
            _,
            _,
          ) =>
        AcsReplicationParameters(
          requestId.toHexString,
          partyId,
          synchronizerId,
          sourceParticipantId,
          targetParticipantId,
          timestamp,
        )
    }

    def fromProtoV30(
        proto: v30.AcsReplicationStatus.AcsReplicationParameters
    ): ParsingResult[AcsReplicationParameters] = for {
      partyId <- PartyId.fromProtoPrimitive(proto.partyId, "party_id")
      synchronizerId <- SynchronizerId.fromProtoPrimitive(
        proto.synchronizerId,
        "synchronizer_id",
      )
      sourceParticipantId <- UniqueIdentifier
        .fromProtoPrimitive(
          proto.sourceParticipantUid,
          "source_participant_uid",
        )
        .map(ParticipantId(_))
      targetParticipantId <- UniqueIdentifier
        .fromProtoPrimitive(
          proto.targetParticipantUid,
          "target_participant_uid",
        )
        .map(ParticipantId(_))
      timestampP <- ProtoConverter.required("timestamp", proto.timestamp)
      timestamp <- CantonTimestamp.fromProtoTimestamp(timestampP)
    } yield AcsReplicationParameters(
      proto.requestId,
      partyId,
      synchronizerId,
      sourceParticipantId,
      targetParticipantId,
      timestamp,
    )

    override protected val pretty: Pretty[AcsReplicationParameters] = {
      import com.digitalasset.canton.logging.pretty.PrettyInstances.*
      prettyOfClass(
        param("request", _.requestId.doubleQuoted),
        param("party", _.partyId),
        param("synchronizer", _.synchronizerId),
        param("source participant", _.sourceParticipantId),
        param("target participant", _.targetParticipantId),
        param("timestamp", _.timestamp),
      )
    }
  }

  final case class PartyToParticipantTopologyTransaction(
      validFrom: CantonTimestamp
  ) extends PrettyPrintingFromCompanion {
    def toProtoV30: v30.AcsReplicationStatus.PartyToParticipantTopologyTransaction =
      v30.AcsReplicationStatus.PartyToParticipantTopologyTransaction(
        Some(validFrom.toProtoTimestamp)
      )

    override def prettyCompanion: PrettyPrintingCompanion[PartyToParticipantTopologyTransaction] =
      PartyToParticipantTopologyTransaction
  }

  object PartyToParticipantTopologyTransaction
      extends PrettyPrintingCompanion[PartyToParticipantTopologyTransaction] {
    def fromProtoV30(
        proto: v30.AcsReplicationStatus.PartyToParticipantTopologyTransaction
    ): ParsingResult[PartyToParticipantTopologyTransaction] =
      for {
        validFromP <- ProtoConverter.required("valid_from", proto.validFrom)
        validFrom <- CantonTimestamp.fromProtoTimestamp(validFromP)
      } yield PartyToParticipantTopologyTransaction(validFrom)

    def fromInternal(
        internal: InternalStatus.PartyToParticipantTopologyTransaction
    ): PartyToParticipantTopologyTransaction =
      PartyToParticipantTopologyTransaction(
        internal.validFrom.value
      )

    override protected val pretty: Pretty[PartyToParticipantTopologyTransaction] = {
      import com.digitalasset.canton.logging.pretty.PrettyInstances.*
      prettyOfClass(
        param("valid from", _.validFrom)
      )
    }
  }

  final case class AcsReplicationProgress(
      processedContractCount: NonNegativeLong,
      acsHashO: Option[ByteString],
      fullyProcessedAcs: Boolean,
  ) extends PrettyPrintingFromCompanion {

    def toProtoV30: v30.AcsReplicationStatus.AcsReplicationProgress =
      v30.AcsReplicationStatus.AcsReplicationProgress(
        processedContractCount.unwrap,
        acsHashO,
        fullyProcessedAcs,
      )

    override def prettyCompanion: PrettyPrintingCompanion[AcsReplicationProgress] =
      AcsReplicationProgress
  }

  object AcsReplicationProgress extends PrettyPrintingCompanion[AcsReplicationProgress] {
    def fromProtoV30(
        proto: v30.AcsReplicationStatus.AcsReplicationProgress
    ): ParsingResult[AcsReplicationProgress] = for {
      processedContractCount <- ProtoConverter.parseNonNegativeLong(
        "processed_contract_count",
        proto.processedContractCount,
      )
    } yield AcsReplicationProgress(
      processedContractCount,
      proto.acsHash,
      proto.fullyProcessedAcs,
    )

    def fromInternal(
        internal: InternalStatus.AcsReplicationProgress
    ): AcsReplicationProgress =
      AcsReplicationProgress(
        internal.processedContractCount,
        internal.acsHashO,
        internal.fullyProcessedAcs,
      )

    override protected val pretty: Pretty[AcsReplicationProgress] = {
      import com.digitalasset.canton.logging.pretty.PrettyInstances.*
      prettyOfClass(
        param("processed contract count", _.processedContractCount),
        paramIfDefined("acs hash", _.acsHashO),
        paramIfTrue("fully replicated", _.fullyProcessedAcs),
      )
    }
  }

  sealed trait AcsReplicationError extends PrettyPrintingFromCompanion {
    def message: String
    def isTerminal: Boolean
    def toProtoV30: v30.AcsReplicationStatus.AcsReplicationError
    override def prettyCompanion: PrettyPrintingCompanion[AcsReplicationError] =
      AcsReplicationError
  }

  object AcsReplicationError extends PrettyPrintingCompanion[AcsReplicationError] {
    final case class Disconnected(message: String) extends AcsReplicationError {
      val isTerminal: Boolean = false
      def toProtoV30: v30.AcsReplicationStatus.AcsReplicationError =
        v30.AcsReplicationStatus.AcsReplicationError(message, isTerminal = isTerminal)
    }

    final case class AcsReplicationFailed(message: String) extends AcsReplicationError {
      val isTerminal: Boolean = true
      def toProtoV30: v30.AcsReplicationStatus.AcsReplicationError =
        v30.AcsReplicationStatus.AcsReplicationError(message, isTerminal = isTerminal)
    }

    def fromInternal: InternalStatus.AcsReplicationError => AcsReplicationError = {
      case InternalStatus.AcsReplicationFailed(errorMessage) =>
        AcsReplicationFailed(errorMessage)
      case InternalStatus.Disconnected(errorMessage) => Disconnected(errorMessage)
    }

    def fromProtoV30(
        proto: v30.AcsReplicationStatus.AcsReplicationError
    ): ParsingResult[AcsReplicationError] = Right {
      if (proto.isTerminal) AcsReplicationFailed(proto.errorMessage)
      else Disconnected(proto.errorMessage)
    }

    override protected val pretty: Pretty[AcsReplicationError] = prettyOfString(_.message)
  }
}
