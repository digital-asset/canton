// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering

import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.protocol.BftOrdererLimits
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.integration.canton.SupportedVersions
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.SignedMessage
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.iss.BlockMetadata
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.ConsensusSegment.ConsensusMessage.{
  Commit,
  ConsensusSegmentDeserializationContext,
  PrePrepare,
  Prepare,
}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.v30
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.v30.{
  CommitCertificate as ProtoCommitCertificate,
  ConsensusCertificate as ProtoConsensusCertificate,
  PrepareCertificate as ProtoPrepareCertificate,
}
import com.digitalasset.canton.validation.{ProtoValidation, ValidationParam}
import com.digitalasset.canton.version.{ProtocolVersion, ProtocolVersionValidation}

sealed trait ConsensusCertificate {
  def prePrepare: SignedMessage[PrePrepare]
  def blockMetadata: BlockMetadata = prePrepare.message.blockMetadata
}

final case class PrepareCertificate(
    override val prePrepare: SignedMessage[PrePrepare],
    prepares: Seq[SignedMessage[Prepare]],
) extends ConsensusCertificate {
  private lazy val sortedPrepares: Seq[SignedMessage[Prepare]] = prepares.sorted

  def toProto: ProtoPrepareCertificate =
    ProtoPrepareCertificate(Some(prePrepare.toProtoV1), sortedPrepares.map(_.toProtoV1))
}

final case class CommitCertificate(
    override val prePrepare: SignedMessage[PrePrepare],
    commits: Seq[SignedMessage[Commit]],
) extends ConsensusCertificate {
  private lazy val sortedCommits: Seq[SignedMessage[Commit]] = commits.sorted

  def toProto: ProtoCommitCertificate =
    ProtoCommitCertificate(Some(prePrepare.toProtoV1), sortedCommits.map(_.toProtoV1))
}

object ConsensusCertificate {
  def fromProto(
      pvv: ProtocolVersionValidation,
      limits: BftOrdererLimits,
      consensusCertificate: ProtoConsensusCertificate,
  ): ParsingResult[ConsensusCertificate] =
    consensusCertificate.certificate match {
      case ProtoConsensusCertificate.Certificate.Empty =>
        Left(ProtoDeserializationError.OtherError("consensus certificate is empty"))
      case ProtoConsensusCertificate.Certificate.PrepareCertificate(prepareCertificate) =>
        PrepareCertificate.fromProto(pvv, limits, prepareCertificate)
      case ProtoConsensusCertificate.Certificate.CommitCertificate(commitCertificate) =>
        CommitCertificate.fromProto(pvv, limits, commitCertificate)
    }
}

object PrepareCertificate {

  def apply(
      prePrepare: SignedMessage[PrePrepare],
      prepares: Seq[SignedMessage[Prepare]],
  ): PrepareCertificate = {
    // sort the prepares to ensure that the order is deterministic and consistent across different nodes
    val sortedPrepares = prepares.sorted
    new PrepareCertificate(prePrepare, sortedPrepares)
  }
  def fromProto(
      pvv: ProtocolVersionValidation,
      limits: BftOrdererLimits,
      prepareCertificate: v30.PrepareCertificate,
  ): ParsingResult[PrepareCertificate] =
    for {
      // We set the actual sender of nested messages to `None`
      //  as the sender is set in the outer message and is unused for nested messages.
      prePrepare <- ProtoConverter
        .parseRequired(
          SignedMessage.fromProtoWithNodeId(
            pvv,
            v30.ConsensusMessage,
            SupportedVersions.ProtoData30,
          )(nestedClaimedOriginator =>
            PrePrepare.fromProtoConsensusMessage(
              pvv,
              ConsensusSegmentDeserializationContext(nestedClaimedOriginator, None, limits),
              _,
            )
          ),
          "prePrepare",
          prepareCertificate.prePrepare,
        )
      prepares <- ProtoValidation.validateLengthThen(
        prepareCertificate.prepares,
        "prepares",
        pvv,
        ValidationParam(ProtocolVersion.nextValidationBoundPV -> limits.maxPrepares),
      ) { (prepare, _) =>
        SignedMessage.fromProtoWithNodeId(pvv, v30.ConsensusMessage, SupportedVersions.ProtoData30)(
          claimedOriginator => Prepare.fromProtoConsensusMessage((claimedOriginator, None), _)
        )(prepare)
      }
    } yield PrepareCertificate(prePrepare, prepares)
}

object CommitCertificate {
  implicit val ordering: Ordering[Commit] =
    Ordering.by(commit => (commit.from, commit.localTimestamp))

  def apply(
      prePrepare: SignedMessage[PrePrepare],
      commits: Seq[SignedMessage[Commit]],
  ): CommitCertificate = {
    // sort the commits to ensure that the order is deterministic and consistent across different nodes
    val sortedCommits = commits.sorted
    new CommitCertificate(prePrepare, sortedCommits)
  }

  def fromProto(
      pvv: ProtocolVersionValidation,
      limits: BftOrdererLimits,
      commitCertificate: v30.CommitCertificate,
  ): ParsingResult[CommitCertificate] =
    // We set the actual sender of nested messages to `None`
    //  as the sender is set in the outer message and is unused for nested messages.
    for {
      prePrepare <- ProtoConverter
        .parseRequired(
          SignedMessage.fromProtoWithNodeId(
            pvv,
            v30.ConsensusMessage,
            SupportedVersions.ProtoData30,
          )(nestedClaimedOriginator =>
            PrePrepare.fromProtoConsensusMessage(
              pvv,
              ConsensusSegmentDeserializationContext(nestedClaimedOriginator, None, limits),
              _,
            )
          ),
          "prePrepare",
          commitCertificate.prePrepare,
        )
      commits <- ProtoValidation.validateLengthThen(
        commitCertificate.commits,
        "commits",
        pvv,
        ValidationParam(ProtocolVersion.nextValidationBoundPV -> limits.maxCommits),
      ) { (commit, _) =>
        SignedMessage.fromProtoWithNodeId(pvv, v30.ConsensusMessage, SupportedVersions.ProtoData30)(
          claimedOriginator => Commit.fromProtoConsensusMessage((claimedOriginator, None), _)
        )(commit)
      }
    } yield CommitCertificate(prePrepare, commits)
}
