// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules

import cats.syntax.traverse.*
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.serialization.ProtocolVersionedMemoizedEvidence
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.integration.canton.SupportedVersions
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.integration.canton.crypto.CryptoProvider
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.data.EpochStore
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.data.EpochStore.Epoch
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BftNodeId,
  BlockNumber,
  EpochNumber,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.availability.OrderingBlock
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.iss.EpochInfo
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.{
  CommitCertificate,
  OrderedBlock,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.Membership
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.{
  MessageFrom,
  SignedMessage,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.dependencies.ConsensusModuleDependencies
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.{Env, Module}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.{
  v30 as v30,
  v31 as v31,
}
import com.digitalasset.canton.version.*
import com.google.protobuf.ByteString

object Consensus {

  sealed trait Message[+E] extends Product

  sealed trait Init extends Message[Nothing]
  object Init {
    case object KickOff extends Init

    final case class LatestEpochsLoaded(
        latestCompletedEpoch: EpochStore.Epoch,
        latestEpoch: EpochStore.Epoch,
    ) extends Init

    final case class EpochInitDataLoaded(
        latestCompletedEpoch: Epoch,
        latestEpoch: Epoch,
        epochInProgress: EpochStore.EpochInProgress,
        completedBlocks: Seq[EpochStore.Block],
    ) extends Init
  }

  case object Start extends Message[Nothing]

  sealed trait ProtocolMessage extends Message[Nothing]

  sealed trait LocalAvailability extends ProtocolMessage
  object LocalAvailability {
    case object NoProposalAvailableYet extends LocalAvailability
    final case class ProposalCreated(
        forBlock: BlockNumber,
        orderingBlock: OrderingBlock,
    ) extends LocalAvailability
  }

  /** The networked consensus protocol for ISS running on top of PBFT
    */
  sealed trait ConsensusMessage extends ProtocolMessage
  object ConsensusMessage {
    final case class PbftUnverifiedNetworkMessage(
        underlyingNetworkMessage: SignedMessage[
          ConsensusSegment.ConsensusMessage.PbftNetworkMessage
        ]
    ) extends ConsensusMessage

    final case class PbftVerifiedNetworkMessage(
        underlyingNetworkMessage: SignedMessage[
          ConsensusSegment.ConsensusMessage.PbftNetworkMessage
        ]
    ) extends ConsensusMessage

    final case class BlockOrdered(
        block: OrderedBlock,
        commitCertificate: CommitCertificate,
        hasCompletedLedSegment: Boolean,
    ) extends ConsensusMessage

    final case class CompleteEpochStored(epoch: Epoch, commitCertificates: Seq[CommitCertificate])
        extends ConsensusMessage

    final case class SegmentCompletedEpoch(
        segmentFirstBlockNumber: BlockNumber,
        epochNumber: EpochNumber,
    ) extends ConsensusMessage

    final case class AsyncException(e: Throwable) extends ConsensusMessage
  }

  sealed trait RetransmissionsMessage extends ProtocolMessage
  object RetransmissionsMessage {
    case object PeriodicStatusBroadcast extends RetransmissionsMessage
    final case class SegmentStatus(
        epochNumber: EpochNumber,
        segmentIndex: Int,
        status: ConsensusStatus.SegmentStatus,
    ) extends RetransmissionsMessage

    final case class UnverifiedNetworkMessage(message: RetransmissionsNetworkMessage)
        extends RetransmissionsMessage
    final case class VerifiedNetworkMessage(message: RetransmissionsNetworkMessage)
        extends RetransmissionsMessage

    sealed trait RetransmissionsNetworkMessage extends MessageFrom {
      def toProto: v30.RetransmissionMessage
    }

    final case class RetransmissionRequest(
        signedEpochStatus: SignedMessage[ConsensusStatus.EpochStatus]
    ) extends RetransmissionsNetworkMessage {
      def toProto: v30.RetransmissionMessage =
        v30.RetransmissionMessage(
          v30.RetransmissionMessage.Message.RetransmissionRequest(
            signedEpochStatus.toProtoV1
          )
        )
      override def from: BftNodeId = signedEpochStatus.from
    }

    object RetransmissionRequest {
      def fromProto(
          from: BftNodeId,
          request: v30.SignedMessage,
      ): ParsingResult[RetransmissionRequest] = SignedMessage
        .fromProto(v30.EpochStatus, SupportedVersions.ProtoData30)(
          ConsensusStatus.EpochStatus.fromProto(from, _)
        )(request)
        .map(Consensus.RetransmissionsMessage.RetransmissionRequest.apply)
    }

    final case class RetransmissionResponse(
        from: BftNodeId,
        commitCertificates: Seq[CommitCertificate],
    ) extends RetransmissionsNetworkMessage {
      def toProto: v30.RetransmissionMessage =
        v30.RetransmissionMessage(
          v30.RetransmissionMessage.Message.RetransmissionResponse(
            v30.RetransmissionResponse(commitCertificates.map(_.toProto))
          )
        )
    }

    object RetransmissionResponse {
      def fromProto(
          from: BftNodeId,
          protoRetransmissionResponse: v30.RetransmissionResponse,
      ): ParsingResult[RetransmissionResponse] =
        for {
          commitCertificates <- protoRetransmissionResponse.commitCertificates.traverse(
            CommitCertificate.fromProto(_, actualSender = Some(from))
          )
        } yield RetransmissionResponse(from, commitCertificates)
    }

  }

  sealed trait StateTransferMessage extends ProtocolMessage

  object StateTransferMessage {
    sealed trait StateTransferNetworkMessage
        extends HasRepresentativeProtocolVersion
        with ProtocolVersionedMemoizedEvidence
        with MessageFrom

    final case class BlockTransferRequest private (
        epoch: EpochNumber,
        from: BftNodeId,
    )(
        override val representativeProtocolVersion: RepresentativeProtocolVersion[
          BlockTransferRequest.type
        ],
        override val deserializedFrom: Option[ByteString],
    ) extends StateTransferNetworkMessage
        with HasProtocolVersionedWrapper[BlockTransferRequest] {

      def toProtoV30: v30.StateTransferMessage =
        v30.StateTransferMessage(
          v30.StateTransferMessage.Message.BlockRequest(
            v30.BlockTransferRequest(epoch)
          )
        )

      def toProtoV31: v31.StateTransferMessage =
        v31.StateTransferMessage(
          v31.StateTransferMessage.Message.BlockRequest(
            v30.BlockTransferRequest(epoch)
          )
        )

      override protected val companionObj: BlockTransferRequest.type = BlockTransferRequest

      override protected[this] def toByteStringUnmemoized: ByteString =
        super[HasProtocolVersionedWrapper].toByteString
    }

    object BlockTransferRequest
        extends VersioningCompanionContextMemoization[
          BlockTransferRequest,
          BftNodeId,
        ] {

      override def name: String = "BlockTransferRequest"

      def create(
          epoch: EpochNumber,
          from: BftNodeId,
      )(implicit synchronizerProtocolVersion: ProtocolVersion): BlockTransferRequest =
        BlockTransferRequest(epoch, from)(
          protocolVersionRepresentativeFor(synchronizerProtocolVersion),
          None,
        )

      def fromProto(from: BftNodeId, request: v30.BlockTransferRequest, protoVersion: ProtoVersion)(
          originalByteString: ByteString
      ): ParsingResult[BlockTransferRequest] =
        for {
          rpv <- protocolVersionRepresentativeFor(protoVersion)
        } yield BlockTransferRequest(EpochNumber(request.epoch), from)(
          rpv,
          Some(originalByteString),
        )

      private def fromProtoStateTransferMessage30(from: BftNodeId, value: v30.StateTransferMessage)(
          originalByteString: ByteString
      ): ParsingResult[BlockTransferRequest] =
        for {
          protoBlockTransferRequest <- value.message.blockRequest.toRight(
            ProtoDeserializationError.OtherError(s"Not a $name message")
          )
          result <- fromProto(from, protoBlockTransferRequest, SupportedVersions.ProtoData30)(
            originalByteString
          )
        } yield result

      private def fromProtoStateTransferMessage31(from: BftNodeId, value: v31.StateTransferMessage)(
          originalByteString: ByteString
      ): ParsingResult[BlockTransferRequest] =
        for {
          protoBlockTransferRequest <- value.message.blockRequest.toRight(
            ProtoDeserializationError.OtherError(s"Not a $name message")
          )
          result <- fromProto(from, protoBlockTransferRequest, SupportedVersions.ProtoData31)(
            originalByteString
          )
        } yield result

      override val versioningTable: VersioningTable =
        VersioningTable(
          SupportedVersions.ProtoData30 ->
            VersionedProtoCodec(SupportedVersions.CantonProtocol36)(v30.StateTransferMessage)(
              supportedProtoVersionMemoized(_)(fromProtoStateTransferMessage30),
              _.toProtoV30,
            ),
          SupportedVersions.ProtoData31 ->
            VersionedProtoCodec(SupportedVersions.CantonProtocol37)(v31.StateTransferMessage)(
              supportedProtoVersionMemoized(_)(fromProtoStateTransferMessage31),
              _.toProtoV31,
            ),
        )
    }

    /** Common supertype for the different protobuf versions of a block transfer response.
      *
      * [[BlockTransferResponse30]] (protocol versions < 37) carries at most one commit certificate
      * per message, while [[BlockTransferResponse31]] (protocol versions >= 37) reuses the
      * retransmission response message and can carry multiple commit certificates per message.
      */
    sealed trait BlockTransferResponse extends StateTransferNetworkMessage {
      def commitCertificates: Seq[CommitCertificate]
    }

    final case class BlockTransferResponse30 private (
        commitCertificate: Option[CommitCertificate],
        from: BftNodeId,
    )(
        override val representativeProtocolVersion: RepresentativeProtocolVersion[
          BlockTransferResponse30.type
        ],
        override val deserializedFrom: Option[ByteString],
    ) extends BlockTransferResponse
        with HasProtocolVersionedWrapper[BlockTransferResponse30] {

      override def commitCertificates: Seq[CommitCertificate] = commitCertificate.toList

      def toProto: v30.StateTransferMessage =
        v30.StateTransferMessage(
          v30.StateTransferMessage.Message.BlockResponse(
            v30.BlockTransferResponse(commitCertificate.map(_.toProto))
          )
        )
      override protected val companionObj: BlockTransferResponse30.type = BlockTransferResponse30

      override protected[this] def toByteStringUnmemoized: ByteString =
        super[HasProtocolVersionedWrapper].toByteString
    }

    object BlockTransferResponse30
        extends VersioningCompanionContextMemoization[
          BlockTransferResponse30,
          BftNodeId,
        ] {

      override def name: String = "BlockTransferResponse"

      def create(
          commitCertificate: Option[CommitCertificate],
          from: BftNodeId,
      )(implicit synchronizerProtocolVersion: ProtocolVersion): BlockTransferResponse30 =
        BlockTransferResponse30(
          commitCertificate,
          from,
        )(
          protocolVersionRepresentativeFor(synchronizerProtocolVersion),
          None,
        )

      private def fromProtoStateTransferMessage(from: BftNodeId, value: v30.StateTransferMessage)(
          originalByteString: ByteString
      ): ParsingResult[BlockTransferResponse30] = for {
        protoBlockTransferResponse <- value.message.blockResponse.toRight(
          ProtoDeserializationError.OtherError(s"Not a $name message")
        )
        blockTransferResponse <- fromProto(from, protoBlockTransferResponse)(originalByteString)
      } yield blockTransferResponse

      def fromProto(
          from: BftNodeId,
          protoResponse: v30.BlockTransferResponse,
      )(
          originalByteString: ByteString
      ): ParsingResult[BlockTransferResponse30] =
        for {
          commitCert <- protoResponse.commitCertificate
            .map(CommitCertificate.fromProto(_, actualSender = Some(from)))
            .sequence
          rpv <- protocolVersionRepresentativeFor(SupportedVersions.ProtoData30)
        } yield BlockTransferResponse30(commitCert, from)(rpv, Some(originalByteString))

      override val versioningTable: VersioningTable = VersioningTable(
        SupportedVersions.ProtoData30 ->
          VersionedProtoCodec(SupportedVersions.CantonProtocol36)(v30.StateTransferMessage)(
            supportedProtoVersionMemoized(_)(fromProtoStateTransferMessage),
            _.toProto,
          )
      )
    }

    final case class BlockTransferResponse31 private (
        commitCertificates: Seq[CommitCertificate],
        from: BftNodeId,
    )(
        override val representativeProtocolVersion: RepresentativeProtocolVersion[
          BlockTransferResponse31.type
        ],
        override val deserializedFrom: Option[ByteString],
    ) extends BlockTransferResponse
        with HasProtocolVersionedWrapper[BlockTransferResponse31] {

      def toProto: v31.StateTransferMessage =
        v31.StateTransferMessage(
          v31.StateTransferMessage.Message.BlockResponse(
            v30.RetransmissionResponse(commitCertificates.map(_.toProto))
          )
        )
      override protected val companionObj: BlockTransferResponse31.type = BlockTransferResponse31

      override protected[this] def toByteStringUnmemoized: ByteString =
        super[HasProtocolVersionedWrapper].toByteString
    }

    object BlockTransferResponse31
        extends VersioningCompanionContextMemoization[
          BlockTransferResponse31,
          BftNodeId,
        ] {

      override def name: String = "BlockTransferResponse"

      def create(
          commitCertificates: Seq[CommitCertificate],
          from: BftNodeId,
      )(implicit synchronizerProtocolVersion: ProtocolVersion): BlockTransferResponse31 =
        BlockTransferResponse31(
          commitCertificates,
          from,
        )(
          protocolVersionRepresentativeFor(synchronizerProtocolVersion),
          None,
        )

      private def fromProtoStateTransferMessage(from: BftNodeId, value: v31.StateTransferMessage)(
          originalByteString: ByteString
      ): ParsingResult[BlockTransferResponse31] = for {
        protoBlockTransferResponse <- value.message.blockResponse.toRight(
          ProtoDeserializationError.OtherError(s"Not a $name message")
        )
        blockTransferResponse <- fromProto(from, protoBlockTransferResponse)(originalByteString)
      } yield blockTransferResponse

      def fromProto(
          from: BftNodeId,
          protoResponse: v30.RetransmissionResponse,
      )(
          originalByteString: ByteString
      ): ParsingResult[BlockTransferResponse31] =
        for {
          commitCerts <- protoResponse.commitCertificates
            .map(CommitCertificate.fromProto(_, actualSender = Some(from)))
            .sequence
          rpv <- protocolVersionRepresentativeFor(SupportedVersions.ProtoData31)
        } yield BlockTransferResponse31(commitCerts, from)(rpv, Some(originalByteString))

      override val versioningTable: VersioningTable = VersioningTable(
        // This message doesn't exist for protocol versions < 37 (it reuses the retransmission response message to
        //  carry multiple commit certificates), so it's explicitly marked as unsupported for the minimum version.
        ProtoVersion(-1) -> UnsupportedProtoCodec(),
        SupportedVersions.ProtoData31 ->
          VersionedProtoCodec(SupportedVersions.CantonProtocol37)(v31.StateTransferMessage)(
            supportedProtoVersionMemoized(_)(fromProtoStateTransferMessage),
            _.toProto,
          ),
      )
    }

    final case class UnverifiedStateTransferMessage(
        signedMessage: SignedMessage[StateTransferMessage.StateTransferNetworkMessage]
    ) extends StateTransferMessage

    final case class VerifiedStateTransferMessage(message: StateTransferNetworkMessage)
        extends StateTransferMessage

    sealed trait StateTransferTimeout extends StateTransferMessage {
      def timeoutIsForEpoch: EpochNumber
    }

    final case class RetryBlockTransferRequest(
        request: SignedMessage[BlockTransferRequest],
        nodeThatTimedOut: Option[BftNodeId],
    ) extends StateTransferTimeout {
      override def timeoutIsForEpoch: EpochNumber = request.message.epoch
    }

    final case class InitiateSendBlockTransferRequest[E <: Env[E]](
        newEpochNumber: EpochNumber,
        nodesThatTimedOut: Seq[BftNodeId],
        reason: String,
    ) extends StateTransferTimeout {
      override def timeoutIsForEpoch: EpochNumber = newEpochNumber
    }

    final case class BlockVerified[E <: Env[E]](
        commitCertificate: CommitCertificate,
        currentEpochInfo: EpochInfo,
        from: BftNodeId,
        isForFutureEpoch: Boolean,
    ) extends StateTransferMessage

    final case class BlockStored[E <: Env[E]](
        commitCertificate: CommitCertificate,
        currentEpochInfo: EpochInfo,
        from: BftNodeId,
    ) extends StateTransferMessage
  }

  final case class StateTransferCompleted[E <: Env[E]](
      newEpochTopologyMessage: Consensus.NewEpochMembership[E]
  ) extends Message[E]

  final case class NewEpochMembership[E <: Env[E]](
      epochNumber: EpochNumber,
      membership: Membership,
      cryptoProvider: CryptoProvider[E],
  ) extends Message[E]

  final case class NewEpochStored[E <: Env[E]](
      newEpochInfo: EpochInfo,
      membership: Membership,
      cryptoProvider: CryptoProvider[E],
  ) extends Message[E]

  case object SegmentCancelledEpoch extends Message[Nothing]

  sealed trait Internal extends Message[Nothing]
  object Internal {
    case object WarnWaitingForNewEpochMembership extends Internal
  }
}

trait Consensus[E <: Env[E]] extends Module[E, Consensus.Message[E]] {
  val dependencies: ConsensusModuleDependencies[E]
}
