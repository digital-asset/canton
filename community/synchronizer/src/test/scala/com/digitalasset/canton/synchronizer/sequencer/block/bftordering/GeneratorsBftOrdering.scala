// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering

import com.digitalasset.canton.Generators.*
import com.digitalasset.canton.config.GeneratorsConfig.*
import com.digitalasset.canton.config.NonNegativeFiniteDuration
import com.digitalasset.canton.config.RequireTypes.PositiveLong
import com.digitalasset.canton.crypto.GeneratorsCrypto.*
import com.digitalasset.canton.crypto.{Hash, Signature}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.data.GeneratorsDataTime.*
import com.digitalasset.canton.serialization.ProtocolVersionedMemoizedEvidence
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.leaders.{
  BlacklistLeaderSelectionPolicyState,
  BlacklistStatus,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BftNodeId,
  BlockNumber,
  EpochNumber,
  ViewNumber,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.availability.{
  AvailabilityAck,
  BatchId,
  OrderingBlock,
  ProofOfAvailability,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.bfttime.CanonicalCommitSet
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.iss.BlockMetadata
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.{
  CommitCertificate,
  ConsensusCertificate,
  PrepareCertificate,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.BlacklistLeaderSelectionPolicyConfig.HowLongToBlacklist.{
  Exponential,
  Linear,
  LinearWithParameters,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.BlacklistLeaderSelectionPolicyConfig.{
  HowLongToBlacklist,
  HowManyCanWeBlacklist,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.SequencingParameters.SegmentLength
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.{
  BlacklistLeaderSelectionPolicyConfig,
  SequencingParameters,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.{
  MessageFrom,
  OrderingRequest,
  OrderingRequestBatch,
  SignedMessage,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.ConsensusSegment.ConsensusMessage.PrePrepare
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.{data, modules}
import com.digitalasset.canton.time.GeneratorsTime.*
import com.digitalasset.canton.time.PositiveFiniteDuration
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.version.ProtocolVersion
import com.google.protobuf.ByteString
import magnolify.scalacheck.auto.genArbitrary
import org.scalacheck.{Arbitrary, Gen}

import java.time.Instant

final class GeneratorsBftOrdering(
    protocolVersion: ProtocolVersion
) {

  implicit val segmentLengthArb: Arbitrary[SegmentLength] = Arbitrary(
    for {
      segmentLength <- Arbitrary.arbitrary[PositiveLong]
    } yield SegmentLength(segmentLength)
  )

  implicit val exponentialArb: Arbitrary[Exponential] = Arbitrary(
    for {
      maximumEpochBlacklisted <- Arbitrary.arbitrary[Option[Long]]
      initialValue <- Arbitrary.arbitrary[Long]
    } yield Exponential(maximumEpochBlacklisted, initialValue)
  )

  implicit val linearArb: Arbitrary[Linear] = Arbitrary(
    for {
      maximumEpochBlacklisted <- Arbitrary.arbitrary[Option[Long]]
    } yield Linear(maximumEpochBlacklisted)
  )

  implicit val linearWithParametersArb: Arbitrary[LinearWithParameters] = Arbitrary(
    for {
      maximumEpochBlacklisted <- Arbitrary.arbitrary[Option[Long]]
      slope <- Arbitrary.arbitrary[Long]
      initialValue <- Arbitrary.arbitrary[Long]
    } yield LinearWithParameters(maximumEpochBlacklisted, slope, initialValue)
  )

  implicit val noBlacklistingArb: Arbitrary[HowLongToBlacklist.NoBlacklisting.type] = Arbitrary(
    Gen.const(HowLongToBlacklist.NoBlacklisting)
  )

  implicit val howLongToBlacklistArb: Arbitrary[HowLongToBlacklist] = Arbitrary(
    Gen.oneOf(
      Arbitrary.arbitrary[Exponential],
      Arbitrary.arbitrary[Linear],
      Arbitrary.arbitrary[LinearWithParameters],
      Arbitrary.arbitrary[HowLongToBlacklist.NoBlacklisting.type],
    )
  )

  implicit val howManyCanWeBlacklistArb: Arbitrary[HowManyCanWeBlacklist] = Arbitrary(
    Gen.oneOf(
      Gen.const(HowManyCanWeBlacklist.NumFaultsTolerated),
      Gen.const(HowManyCanWeBlacklist.NoBlacklisting),
    )
  )

  implicit val blacklistLeaderSelectionPolicyConfigArb
      : Arbitrary[BlacklistLeaderSelectionPolicyConfig] =
    Arbitrary(
      for {
        howLongToBlacklist <- Arbitrary.arbitrary[HowLongToBlacklist]
        howManyCanWeBlacklist <- Arbitrary.arbitrary[HowManyCanWeBlacklist]
      } yield BlacklistLeaderSelectionPolicyConfig(
        howLongToBlacklist = howLongToBlacklist,
        howManyCanWeBlacklist = howManyCanWeBlacklist,
      )
    )

  implicit val sequencingParametersArb: Arbitrary[SequencingParameters] = Arbitrary(
    for {
      pbftViewChangeTimeout <- Arbitrary.arbitrary[PositiveFiniteDuration]
      segmentLength <- Arbitrary.arbitrary[SegmentLength]
      blacklistLeaderSelectionPolicyConfig <- Arbitrary
        .arbitrary[BlacklistLeaderSelectionPolicyConfig]
      maxRequestsInBatch <- Gen.choose[Short](1, 32)
      maxBatchesPerBlockProposal <- Gen.choose[Short](1, 31)
      pbftViewChangeTimeoutStep <- Arbitrary.arbitrary[NonNegativeFiniteDuration]
      pbftViewChangeTimeoutUpperBound <- Arbitrary.arbitrary[NonNegativeFiniteDuration]
      stricterDetectionOfRequestsPotentiallyChangingOrderingTopology <- Arbitrary.arbitrary[Boolean]
    } yield SequencingParameters.create(
      pbftViewChangeTimeout = pbftViewChangeTimeout,
      segmentLength = segmentLength,
      blacklistLeaderSelectionPolicyConfig = blacklistLeaderSelectionPolicyConfig,
      maxRequestsInBatch = maxRequestsInBatch,
      maxBatchesPerBlockProposal = maxBatchesPerBlockProposal,
      pbftViewChangeTimeoutStep = pbftViewChangeTimeoutStep,
      pbftViewChangeTimeoutUpperBound = pbftViewChangeTimeoutUpperBound,
      stricterDetectionOfRequestsPotentiallyChangingOrderingTopology =
        stricterDetectionOfRequestsPotentiallyChangingOrderingTopology,
    )(protocolVersion)
  )

  implicit val onTrialArb: Arbitrary[BlacklistStatus.OnTrial] = Arbitrary(
    for {
      epochLeftUntilNewTrial <- Gen.choose(1L, Long.MaxValue)
    } yield BlacklistStatus.OnTrial(epochLeftUntilNewTrial)
  )
  implicit val blacklistedArb: Arbitrary[BlacklistStatus.Blacklisted] = Arbitrary(
    for {
      failedAttemptsBefore <- Gen.choose(1L, Long.MaxValue)
      epochLeftUntilNewTrial <- Gen.choose(1L, Long.MaxValue)
    } yield BlacklistStatus.Blacklisted(failedAttemptsBefore, epochLeftUntilNewTrial)
  )
  implicit val blacklistStatusMarkArb: Arbitrary[BlacklistStatus.BlacklistStatusMark] = Arbitrary(
    Gen.oneOf[BlacklistStatus.BlacklistStatusMark](
      Arbitrary.arbitrary[BlacklistStatus.OnTrial],
      Arbitrary.arbitrary[BlacklistStatus.Blacklisted],
    )
  )
  implicit val epochNumberArb: Arbitrary[EpochNumber] = Arbitrary(
    Gen.choose(0L, Long.MaxValue).map(EpochNumber(_))
  )
  implicit val blockNumberArb: Arbitrary[BlockNumber] = Arbitrary(
    Gen.choose(0L, Long.MaxValue).map(BlockNumber(_))
  )
  implicit val viewNumberArb: Arbitrary[ViewNumber] = Arbitrary(
    Gen.choose(0L, Long.MaxValue).map(ViewNumber(_))
  )
  implicit val bftNodeIdArb: Arbitrary[BftNodeId] = Arbitrary(Gen.identifier.map(BftNodeId(_)))
  implicit val blacklistMapArb: Arbitrary[(BftNodeId, BlacklistStatus.BlacklistStatusMark)] =
    Arbitrary(
      for {
        nodeId <- Arbitrary.arbitrary[BftNodeId]
        statusMark <- Arbitrary.arbitrary[BlacklistStatus.BlacklistStatusMark]
      } yield (nodeId, statusMark)
    )

  implicit val blacklistLeaderSelectionPolicyStateArb
      : Arbitrary[BlacklistLeaderSelectionPolicyState] = Arbitrary(
    for {
      epochNumber <- Arbitrary.arbitrary[EpochNumber]
      startBlock <- Arbitrary.arbitrary[BlockNumber]
      blacklist <- boundedMapGen[BftNodeId, BlacklistStatus.BlacklistStatusMark]
    } yield BlacklistLeaderSelectionPolicyState.create(
      epochNumber = epochNumber,
      startBlockNumber = startBlock,
      blacklist = blacklist,
    )(protocolVersion)
  )

  val actualSender: BftNodeId = BftNodeId("actual-sender")

  implicit val batchIdArb: Arbitrary[BatchId] = Arbitrary(
    Arbitrary.arbitrary[Hash].map(BatchId.createFromHash)
  )

  // Availability messages

  implicit def tracedArb[T](implicit arb: Arbitrary[T]): Arbitrary[Traced[T]] = Arbitrary(
    arb.arbitrary.map(Traced(_)(TraceContext.empty))
  )

  implicit val orderingRequestArb: Arbitrary[OrderingRequest] = Arbitrary(
    for {
      tag <- Arbitrary.arbitrary[String]
      messageId <- Arbitrary.arbitrary[String]
      payload <- Arbitrary.arbitrary[ByteString]
      orderingStartInstant <- Gen.option(Arbitrary.arbitrary[Instant])
    } yield OrderingRequest(tag, messageId, payload, orderingStartInstant)
  )

  implicit val orderingRequestBatchArb: Arbitrary[OrderingRequestBatch] = Arbitrary(
    for {
      requests <- boundedListGen[Traced[OrderingRequest]]
      epochNumber <- Arbitrary.arbitrary[EpochNumber]
    } yield OrderingRequestBatch.create(requests, epochNumber)(protocolVersion)
  )

  implicit val availabilityRemoteDisseminationRemoteBatchArb
      : Arbitrary[modules.Availability.RemoteDissemination.RemoteBatch] = Arbitrary(
    for {
      batchId <- Arbitrary.arbitrary[BatchId]
      orderingRequestBatch <- Arbitrary.arbitrary[OrderingRequestBatch]
    } yield modules.Availability.RemoteDissemination.RemoteBatch.create(
      batchId,
      orderingRequestBatch,
      actualSender,
    )(protocolVersion)
  )

  implicit val availabilityRemoteDisseminationRemoteBatchAcknowledgedArb
      : Arbitrary[modules.Availability.RemoteDissemination.RemoteBatchAcknowledged] = Arbitrary(
    for {
      batchId <- Arbitrary.arbitrary[BatchId]
      signature <- Arbitrary.arbitrary[Signature]
    } yield modules.Availability.RemoteDissemination.RemoteBatchAcknowledged.create(
      batchId,
      actualSender,
      signature,
    )(protocolVersion)
  )

  implicit val availabilityRemoteOutputFetchRemoteBatchDataFetchedArb
      : Arbitrary[modules.Availability.RemoteOutputFetch.RemoteBatchDataFetched] = Arbitrary(
    for {
      batchId <- Arbitrary.arbitrary[BatchId]
      orderingRequestBatch <- Arbitrary.arbitrary[OrderingRequestBatch]
    } yield modules.Availability.RemoteOutputFetch.RemoteBatchDataFetched.create(
      actualSender,
      batchId,
      orderingRequestBatch,
    )(protocolVersion)
  )

  implicit val availabilityRemoteOutputFetchFetchRemoteBatchDataArb
      : Arbitrary[modules.Availability.RemoteOutputFetch.FetchRemoteBatchData] = Arbitrary(
    for {
      batchId <- Arbitrary.arbitrary[BatchId]
      epochNumber <- Arbitrary.arbitrary[EpochNumber]
    } yield modules.Availability.RemoteOutputFetch.FetchRemoteBatchData.create(
      batchId,
      epochNumber,
      actualSender,
    )(protocolVersion)
  )

  implicit val inProgressArb: Arbitrary[modules.ConsensusStatus.SegmentStatus.InProgress] =
    Arbitrary(
      for {
        viewNumber <- Arbitrary.arbitrary[ViewNumber]
        blockStatuses <- boundedListGen[modules.ConsensusStatus.BlockStatus]
      } yield modules.ConsensusStatus.SegmentStatus.InProgress(viewNumber, blockStatuses)
    )

  implicit val inViewChangeArb: Arbitrary[modules.ConsensusStatus.SegmentStatus.InViewChange] =
    Arbitrary(
      for {
        viewNumber <- Arbitrary.arbitrary[ViewNumber]
        viewChangeMessagesPresent <- boundedListGen[Boolean]
        areBlocksComplete <- boundedListGen[Boolean]
      } yield modules.ConsensusStatus.SegmentStatus
        .InViewChange(viewNumber, viewChangeMessagesPresent, areBlocksComplete)
    )

  implicit val segmentStatusArb: Arbitrary[modules.ConsensusStatus.SegmentStatus] = Arbitrary(
    Gen.oneOf[modules.ConsensusStatus.SegmentStatus](
      Gen.const(modules.ConsensusStatus.SegmentStatus.Complete),
      Arbitrary.arbitrary[modules.ConsensusStatus.SegmentStatus.InProgress],
      Arbitrary.arbitrary[modules.ConsensusStatus.SegmentStatus.InViewChange],
    )
  )

  implicit val epochStatusArb: Arbitrary[modules.ConsensusStatus.EpochStatus] = Arbitrary(
    for {
      epochNumber <- Arbitrary.arbitrary[EpochNumber]
      segments <- nonEmptyListGen[modules.ConsensusStatus.SegmentStatus]
    } yield modules.ConsensusStatus.EpochStatus.create(
      actualSender,
      epochNumber,
      segments,
    )(protocolVersion)
  )

  implicit def signedMessage[T <: ProtocolVersionedMemoizedEvidence & MessageFrom](implicit
      arb: Arbitrary[T]
  ): Arbitrary[SignedMessage[T]] = Arbitrary(
    for {
      message <- arb.arbitrary
      signature <- Arbitrary.arbitrary[Signature]
    } yield SignedMessage(message, signature)
  )

  implicit val blockMetadataArb: Arbitrary[BlockMetadata] = Arbitrary(
    for {
      epochNumber <- Arbitrary.arbitrary[EpochNumber]
      blockNumber <- Arbitrary.arbitrary[BlockNumber]
    } yield BlockMetadata(
      epochNumber,
      blockNumber,
    )
  )

  implicit val availabilityAckArb: Arbitrary[AvailabilityAck] = Arbitrary(
    for {
      from <- Arbitrary.arbitrary[BftNodeId]
      signature <- Arbitrary.arbitrary[Signature]
    } yield AvailabilityAck(from, signature)
  )

  implicit val proofOfAvailabilityArb: Arbitrary[ProofOfAvailability] = Arbitrary(
    for {
      batchId <- Arbitrary.arbitrary[BatchId]
      acks <- boundedListGen[AvailabilityAck]
      epochNumber <- Arbitrary.arbitrary[EpochNumber]
    } yield ProofOfAvailability(batchId, acks, epochNumber)
  )

  implicit val orderingBlockArb: Arbitrary[OrderingBlock] = Arbitrary(
    for {
      proofs <- boundedListGen[ProofOfAvailability]
    } yield OrderingBlock(proofs)
  )

  def commitArb(
      actualSender: Option[BftNodeId]
  ): Arbitrary[modules.ConsensusSegment.ConsensusMessage.Commit] = Arbitrary(
    for {
      blockMetadata <- Arbitrary.arbitrary[BlockMetadata]
      viewNumber <- Arbitrary.arbitrary[ViewNumber]
      blockHash <- Arbitrary.arbitrary[Hash]
      localTimestamp <- Arbitrary.arbitrary[CantonTimestamp]
      from <- Arbitrary.arbitrary[BftNodeId]
    } yield modules.ConsensusSegment.ConsensusMessage.Commit.create(
      blockMetadata,
      viewNumber,
      blockHash,
      localTimestamp,
      from,
      actualSender,
    )(protocolVersion)
  )

  implicit val canonicalCommitSetArb: Arbitrary[CanonicalCommitSet] = Arbitrary(
    for {
      commits <- boundedSetGen[SignedMessage[modules.ConsensusSegment.ConsensusMessage.Commit]](
        signedMessage(commitArb(None))
      )
    } yield CanonicalCommitSet(commits)
  )

  implicit val prePrepareArb: Arbitrary[PrePrepare] = Arbitrary(
    for {
      blockMetadata <- Arbitrary.arbitrary[BlockMetadata]
      viewNumber <- Arbitrary.arbitrary[ViewNumber]
      block <- Arbitrary.arbitrary[OrderingBlock]
      canonicalCommitSet <- Arbitrary.arbitrary[CanonicalCommitSet]
      from <- Arbitrary.arbitrary[BftNodeId]
    } yield PrePrepare.create(
      blockMetadata,
      viewNumber,
      block,
      canonicalCommitSet,
      from,
      Some(actualSender),
    )(protocolVersion)
  )

  implicit val prepareArb: Arbitrary[modules.ConsensusSegment.ConsensusMessage.Prepare] = Arbitrary(
    for {
      blockMetadata <- Arbitrary.arbitrary[BlockMetadata]
      viewNumber <- Arbitrary.arbitrary[ViewNumber]
      hash <- Arbitrary.arbitrary[Hash]
      from <- Arbitrary.arbitrary[BftNodeId]
    } yield modules.ConsensusSegment.ConsensusMessage.Prepare.create(
      blockMetadata,
      viewNumber,
      hash,
      from,
      Some(actualSender),
    )(protocolVersion)
  )

  implicit val commitCertificateArb: Arbitrary[CommitCertificate] = Arbitrary(
    for {
      prePrepare <- Arbitrary.arbitrary[SignedMessage[PrePrepare]]
      commits <- boundedListGen(signedMessage(commitArb(Some(actualSender))))
    } yield CommitCertificate(
      prePrepare,
      commits,
    )
  )

  implicit val prepareCertificateArb: Arbitrary[PrepareCertificate] = Arbitrary(
    for {
      prePrepare <- Arbitrary.arbitrary[SignedMessage[PrePrepare]]
      prepares <- boundedListGen(signedMessage(prepareArb))
    } yield PrepareCertificate(
      prePrepare,
      prepares,
    )
  )

  val consensusCertificateArb: Arbitrary[ConsensusCertificate] = Arbitrary(
    Gen.oneOf[ConsensusCertificate](
      commitCertificateArb.arbitrary,
      prepareCertificateArb.arbitrary,
    )
  )

  implicit val stateTransferMessageBlockTransferRequestArb
      : Arbitrary[modules.Consensus.StateTransferMessage.BlockTransferRequest] = Arbitrary(
    for {
      epochNumber <- Arbitrary.arbitrary[EpochNumber]
    } yield modules.Consensus.StateTransferMessage.BlockTransferRequest.create(
      epochNumber,
      actualSender,
    )(protocolVersion)
  )

  implicit val stateTransferMessageBlockTransferResponse30Arb
      : Arbitrary[modules.Consensus.StateTransferMessage.BlockTransferResponse30] = Arbitrary(
    for {
      commitCertificate <- Gen.option(Arbitrary.arbitrary[CommitCertificate])
    } yield modules.Consensus.StateTransferMessage.BlockTransferResponse30.create(
      commitCertificate,
      actualSender,
    )(protocolVersion)
  )

  implicit val stateTransferMessageBlockTransferResponse31Arb
      : Arbitrary[modules.Consensus.StateTransferMessage.BlockTransferResponse31] = Arbitrary(
    for {
      commitCertificates <- boundedListGen(Arbitrary.arbitrary[CommitCertificate])
    } yield modules.Consensus.StateTransferMessage.BlockTransferResponse31.create(
      commitCertificates,
      actualSender,
    )(protocolVersion)
  )

  implicit val viewChangeArb: Arbitrary[modules.ConsensusSegment.ConsensusMessage.ViewChange] =
    Arbitrary(
      for {
        blockMetadata <- Arbitrary.arbitrary[BlockMetadata]
        viewNumber <- Arbitrary.arbitrary[ViewNumber]
        consensusCerts <- boundedListGen(consensusCertificateArb)
        from <- Arbitrary.arbitrary[BftNodeId]
      } yield modules.ConsensusSegment.ConsensusMessage.ViewChange.create(
        blockMetadata,
        viewNumber,
        consensusCerts,
        from,
        Some(actualSender),
      )(protocolVersion)
    )

  implicit val newViewArb: Arbitrary[modules.ConsensusSegment.ConsensusMessage.NewView] = Arbitrary(
    for {
      blockMetadata <- Arbitrary.arbitrary[BlockMetadata]
      viewNumber <- Arbitrary.arbitrary[ViewNumber]
      viewChangeMessages <- boundedListGen[
        data.SignedMessage[modules.ConsensusSegment.ConsensusMessage.ViewChange]
      ]
      prePrepares <- boundedListGen(Arbitrary.arbitrary[SignedMessage[PrePrepare]])
      from <- Arbitrary.arbitrary[BftNodeId]
    } yield modules.ConsensusSegment.ConsensusMessage.NewView.create(
      blockMetadata,
      viewNumber,
      viewChangeMessages,
      prePrepares,
      from,
      Some(actualSender),
    )(protocolVersion)
  )
}
