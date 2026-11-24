// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules

import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.logging.pretty.{
  Pretty,
  PrettyPrintingCompanion,
  PrettyPrintingFromCompanion,
}
import com.digitalasset.canton.protocol.BftOrdererLimitsView
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.serialization.ProtocolVersionedMemoizedEvidence
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.integration.canton.SupportedVersions
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BftNodeId,
  EpochNumber,
  ViewNumber,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.MessageFrom
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.v30
import com.digitalasset.canton.validation.{ProtoValidation, ValidationParam}
import com.digitalasset.canton.version.{
  HasProtocolVersionedWrapper,
  HasRepresentativeProtocolVersion,
  ProtocolVersion,
  ProtocolVersionValidation,
  RepresentativeProtocolVersion,
  VersionedProtoCodec,
  VersioningCompanionContextMemoization,
}
import com.digitalasset.nonempty.NonEmpty
import com.google.protobuf.ByteString

/** Status messages that describe how far into the consensus process a node is. This is used as part
  * of retransmissions such that receiving nodes can tell if there are messages they can retransmit
  * in order to help the originating node make progress.
  */
object ConsensusStatus {

  final case class EpochStatus private (
      from: BftNodeId,
      epochNumber: EpochNumber,
      segments: NonEmpty[Seq[SegmentStatus]],
  )(
      override val representativeProtocolVersion: RepresentativeProtocolVersion[
        EpochStatus.type
      ],
      override val deserializedFrom: Option[ByteString],
  ) extends HasRepresentativeProtocolVersion
      with ProtocolVersionedMemoizedEvidence
      with MessageFrom
      with HasProtocolVersionedWrapper[EpochStatus] {
    def toProto: v30.EpochStatus = v30.EpochStatus(epochNumber, segments.map(_.toProto).forgetNE)

    override protected val companionObj: EpochStatus.type = EpochStatus

    override protected[this] def toByteStringUnmemoized: ByteString =
      super[HasProtocolVersionedWrapper].toByteString
  }

  /** Deserialization context for BFT orderer consensus status structures */
  final case class ConsensusStatusDeserializationContext(
      from: BftNodeId,
      limits: BftOrdererLimitsView,
  )

  object EpochStatus
      extends VersioningCompanionContextMemoization[
        EpochStatus,
        ConsensusStatusDeserializationContext,
      ] {
    override def name: String = "EpochStatus"

    def create(
        from: BftNodeId,
        epochNumber: EpochNumber,
        segments: NonEmpty[Seq[SegmentStatus]],
    )(implicit
        synchronizerProtocolVersion: ProtocolVersion
    ): EpochStatus =
      EpochStatus(from, epochNumber, segments)(
        protocolVersionRepresentativeFor(synchronizerProtocolVersion),
        None,
      )

    def fromProto(
        pvv: ProtocolVersionValidation,
        context: ConsensusStatusDeserializationContext,
        protoEpochStatus: v30.EpochStatus,
    )(originalByteString: ByteString): ParsingResult[EpochStatus] = {
      val ConsensusStatusDeserializationContext(sentBy, limits) = context

      for {
        segments <- ProtoValidation.validateLengthThen(
          protoEpochStatus.segments,
          "segments",
          pvv,
          ValidationParam(ProtocolVersion.nextValidationBoundPV -> limits.maxSegments),
        )((element, _) => SegmentStatus.fromProto(pvv, limits, element))
        nonEmptySegments <- NonEmpty
          .from(segments)
          .toRight(
            ProtoDeserializationError.ValueConversionError("segments", "must not be empty")
          )
        rpv <- protocolVersionRepresentativeFor(SupportedVersions.ProtoData30)
      } yield EpochStatus(
        sentBy,
        EpochNumber(protoEpochStatus.epochNumber),
        nonEmptySegments,
      )(rpv, Some(originalByteString))
    }

    override val versioningTable: VersioningTable = VersioningTable(
      SupportedVersions.ProtoData30 ->
        VersionedProtoCodec(SupportedVersions.CantonProtocol36)(v30.EpochStatus)(
          supportedProtoVersionMemoizedPVV(_)(fromProto),
          _.toProto,
        )
    )
  }

  sealed trait SegmentStatus extends PrettyPrintingFromCompanion with Product with Serializable {
    def toProto: v30.SegmentStatus
  }

  object SegmentStatus {
    final case object Complete extends SegmentStatus {
      override val toProto: v30.SegmentStatus =
        v30.SegmentStatus(v30.SegmentStatus.Status.Complete(com.google.protobuf.empty.Empty()))

      override def prettyCompanion: PrettyPrintingCompanion[Complete.this.type] =
        CompletePrettyPrintingCompanion
    }

    private object CompletePrettyPrintingCompanion extends PrettyPrintingCompanion[Complete.type] {
      override protected val pretty: Pretty[Complete.type] = prettyOfObject[Complete.type]
    }

    sealed trait Incomplete extends SegmentStatus {
      def viewNumber: ViewNumber
      def areBlocksComplete: Seq[Boolean]
    }

    final case class InProgress(viewNumber: ViewNumber, blockStatuses: Seq[BlockStatus])
        extends Incomplete {
      override def areBlocksComplete: Seq[Boolean] = blockStatuses.map(_.isComplete)
      override def toProto: v30.SegmentStatus =
        v30.SegmentStatus(
          v30.SegmentStatus.Status.InProgress(
            v30.SegmentInProgress(viewNumber, blockStatuses.map(_.toProto))
          )
        )

      override def prettyCompanion: PrettyPrintingCompanion[InProgress] = InProgress
    }

    object InProgress extends PrettyPrintingCompanion[InProgress] {
      override protected val pretty: Pretty[InProgress] = prettyOfClass(
        param("viewNumber", _.viewNumber),
        param("blockStatuses", _.blockStatuses),
      )
    }

    final case class InViewChange(
        viewNumber: ViewNumber,
        viewChangeMessagesPresent: Seq[Boolean],
        areBlocksComplete: Seq[Boolean],
    ) extends Incomplete {
      override def toProto: v30.SegmentStatus = v30.SegmentStatus(
        v30.SegmentStatus.Status.InViewChange(
          v30.SegmentInViewChange(viewNumber, viewChangeMessagesPresent, areBlocksComplete)
        )
      )

      override def prettyCompanion: PrettyPrintingCompanion[InViewChange] = InViewChange
    }

    object InViewChange extends PrettyPrintingCompanion[InViewChange] {
      override protected val pretty: Pretty[InViewChange] =
        prettyOfClass(
          param("viewNumber", _.viewNumber),
          param("viewChangeMessagesPresent", _.viewChangeMessagesPresent),
          param("areBlocksComplete", _.areBlocksComplete),
        )
    }

    private[modules] def fromProto(
        pvv: ProtocolVersionValidation,
        limits: BftOrdererLimitsView,
        proto: v30.SegmentStatus,
    ): ParsingResult[SegmentStatus] =
      proto.status match {
        case v30.SegmentStatus.Status.InViewChange(
              v30.SegmentInViewChange(viewChange, viewChangeMessagesPresent, areBlocksComplete)
            ) =>
          for {
            viewChangeMessagesPresentBounded <- ProtoValidation.validateLength(
              viewChangeMessagesPresent,
              "view_change_messages_present",
              pvv,
              ValidationParam(
                ProtocolVersion.nextValidationBoundPV -> limits.maxViewChangeMessagesPresent
              ),
            )
            areBlocksCompleteBounded <- ProtoValidation.validateLength(
              areBlocksComplete,
              "are_blocks_complete",
              pvv,
              ValidationParam(ProtocolVersion.nextValidationBoundPV -> limits.maxAreBlocksComplete),
            )
          } yield SegmentStatus.InViewChange(
            ViewNumber(viewChange),
            viewChangeMessagesPresentBounded,
            areBlocksCompleteBounded,
          )
        case v30.SegmentStatus.Status
              .InProgress(v30.SegmentInProgress(viewNumber, blockStatuses)) =>
          for {
            blocks <- ProtoValidation.validateLengthThen(
              blockStatuses,
              "block_statuses",
              pvv,
              ValidationParam(ProtocolVersion.nextValidationBoundPV -> limits.maxBlockStatuses),
            )((element, _) => BlockStatus.fromProto(pvv, limits, element))
          } yield SegmentStatus.InProgress(
            ViewNumber(viewNumber),
            blocks,
          )
        case v30.SegmentStatus.Status.Complete(_) => Right(SegmentStatus.Complete)
        case v30.SegmentStatus.Status.Empty =>
          Left(ProtoDeserializationError.OtherError("Empty Received"))
      }

  }

  sealed trait BlockStatus extends PrettyPrintingFromCompanion {
    def isComplete: Boolean
    def toProto: v30.BlockStatus
  }

  object BlockStatus {
    final case object Complete extends BlockStatus {
      override val isComplete: Boolean = true
      override val toProto: v30.BlockStatus =
        v30.BlockStatus(v30.BlockStatus.Status.Complete(com.google.protobuf.empty.Empty()))

      override def prettyCompanion: PrettyPrintingCompanion[Complete.this.type] =
        CompletePrettyPrintingCompanion
    }

    private object CompletePrettyPrintingCompanion extends PrettyPrintingCompanion[Complete.type] {
      override protected val pretty: Pretty[Complete.type] = prettyOfObject[Complete.type]
    }

    final case class InProgress(
        prePrepared: Boolean,
        preparesPresent: Seq[Boolean],
        commitsPresent: Seq[Boolean],
    ) extends BlockStatus {
      override def isComplete: Boolean = false
      override def toProto: v30.BlockStatus = v30.BlockStatus(
        v30.BlockStatus.Status.InProgress(
          v30.BlockInProgress(prePrepared, preparesPresent, commitsPresent)
        )
      )

      override def prettyCompanion: PrettyPrintingCompanion[InProgress] = InProgress
    }

    object InProgress extends PrettyPrintingCompanion[InProgress] {
      override protected val pretty: Pretty[InProgress] = prettyOfClass(
        param("prePrepared", _.prePrepared),
        param("preparesPresent", _.preparesPresent),
        param("commitsPresent", _.commitsPresent),
      )
    }

    private[modules] def fromProto(
        pvv: ProtocolVersionValidation,
        limits: BftOrdererLimitsView,
        proto: v30.BlockStatus,
    ): ParsingResult[BlockStatus] =
      proto.status match {
        case v30.BlockStatus.Status.InProgress(
              v30.BlockInProgress(prePrepared, preparesPresent, commitsPresent)
            ) =>
          for {
            preparesPresentBounded <- ProtoValidation.validateLength(
              preparesPresent,
              "prepares_present",
              pvv,
              ValidationParam(ProtocolVersion.nextValidationBoundPV -> limits.maxPreparesPresent),
            )
            commitsPresentBounded <- ProtoValidation.validateLength(
              commitsPresent,
              "commits_present",
              pvv,
              ValidationParam(ProtocolVersion.nextValidationBoundPV -> limits.maxCommitsPresent),
            )
          } yield BlockStatus.InProgress(prePrepared, preparesPresentBounded, commitsPresentBounded)
        case v30.BlockStatus.Status.Complete(_) => Right(BlockStatus.Complete)
        case v30.BlockStatus.Status.Empty =>
          Left(ProtoDeserializationError.OtherError("Empty Received"))
      }
  }
}
