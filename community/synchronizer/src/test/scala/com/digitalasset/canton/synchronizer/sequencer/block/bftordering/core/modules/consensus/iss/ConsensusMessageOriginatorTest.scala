// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss

import com.digitalasset.canton.crypto.Signature
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.*
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.SignedMessage
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.bfttime.CanonicalCommitSet
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.{
  CommitCertificate,
  ConsensusCertificate,
  PrepareCertificate,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.ConsensusSegment.ConsensusMessage.{
  Commit,
  NewView,
  PbftNetworkMessage,
  PrePrepare,
  Prepare,
  ViewChange,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.{
  BftSequencerBaseTest,
  GeneratorsBftOrdering,
}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.v30
import org.scalatest.wordspec.AnyWordSpec

final class ConsensusMessageOriginatorTest extends AnyWordSpec with BftSequencerBaseTest {

  private val generators = new GeneratorsBftOrdering(synchronizerProtocolVersion)
  import generators.*

  private val actualSender = BftNodeId("actual-sender")
  private val claimedOriginator = BftNodeId("claimed-originator")
  private val forgedOriginator = BftNodeId("forged-originator")

  private def commitWithOriginator(message: Commit, from: BftNodeId): Commit =
    Commit.create(
      message.blockMetadata,
      message.viewNumber,
      message.hash,
      message.localTimestamp,
      from,
      Some(actualSender),
    )(synchronizerProtocolVersion)

  private def prepareWithOriginator(message: Prepare, from: BftNodeId): Prepare =
    Prepare.create(
      message.blockMetadata,
      message.viewNumber,
      message.hash,
      from,
      Some(actualSender),
    )(synchronizerProtocolVersion)

  private def prePrepareWithOriginator(message: PrePrepare, from: BftNodeId): PrePrepare =
    PrePrepare.create(
      message.blockMetadata,
      message.viewNumber,
      message.block,
      message.canonicalCommitSet,
      from,
      Some(actualSender),
    )(synchronizerProtocolVersion)

  private def viewChangeWithOriginator(message: ViewChange, from: BftNodeId): ViewChange =
    ViewChange.create(
      message.blockMetadata,
      message.viewNumber,
      message.consensusCerts,
      from,
      Some(actualSender),
    )(synchronizerProtocolVersion)

  private def newViewWithOriginator(message: NewView, from: BftNodeId): NewView =
    NewView.create(
      message.blockMetadata,
      message.viewNumber,
      message.viewChanges,
      message.prePrepares,
      from,
      Some(actualSender),
    )(synchronizerProtocolVersion)

  private def assertNetworkOriginatorBinding(message: PbftNetworkMessage): Unit = {
    message.from should not be actualSender
    val signedProto = SignedMessage(message, Signature.noSignature).toProtoV1

    IssConsensusModule.parseNetworkMessage(signedProto, actualSender).isRight shouldBe true
    IssConsensusModule
      .parseNetworkMessage(signedProto.copy(from = forgedOriginator), actualSender)
      .isLeft shouldBe true
  }

  "PBFT signed-message parsing" should {
    "bind each top-level claimed origin to the protocol message while allowing retransmission" in {
      assertNetworkOriginatorBinding(
        prePrepareWithOriginator(prePrepareArb.arbitrary.sample.value, claimedOriginator)
      )
      assertNetworkOriginatorBinding(
        prepareWithOriginator(prepareArb.arbitrary.sample.value, claimedOriginator)
      )
      assertNetworkOriginatorBinding(
        commitWithOriginator(
          commitArb(Some(actualSender)).arbitrary.sample.value,
          claimedOriginator,
        )
      )
      assertNetworkOriginatorBinding(
        viewChangeWithOriginator(viewChangeArb.arbitrary.sample.value, claimedOriginator)
      )
      assertNetworkOriginatorBinding(
        newViewWithOriginator(newViewArb.arbitrary.sample.value, claimedOriginator)
      )
    }

    "use and validate each embedded signed message's own claimed origin in a NewView" in {
      val nestedViewChange = viewChangeWithOriginator(
        viewChangeArb.arbitrary.sample.value,
        BftNodeId("nested-view-change"),
      )
      val nestedPrePrepare = prePrepareWithOriginator(
        prePrepareArb.arbitrary.sample.value,
        BftNodeId("nested-pre-prepare"),
      )
      val newView = NewView.create(
        nestedPrePrepare.blockMetadata,
        nestedPrePrepare.viewNumber,
        Seq(SignedMessage(nestedViewChange, Signature.noSignature)),
        Seq(SignedMessage(nestedPrePrepare, Signature.noSignature)),
        claimedOriginator,
        Some(actualSender),
      )(synchronizerProtocolVersion)
      val protoNewView = newView.toProto

      val parsedNewView = NewView
        .fromProtoConsensusMessage((claimedOriginator, Some(actualSender)), protoNewView)(
          newView.getCryptographicEvidence
        )
        .value
      parsedNewView.actualSender shouldBe Some(actualSender)
      parsedNewView.from shouldBe claimedOriginator
      parsedNewView.viewChanges.map(_.message.actualSender) shouldBe Seq(None)
      parsedNewView.viewChanges.map(_.from) shouldBe Seq(nestedViewChange.from)
      parsedNewView.prePrepares.map(_.message.actualSender) shouldBe Seq(None)
      parsedNewView.prePrepares.map(_.from) shouldBe Seq(nestedPrePrepare.from)

      val protoNestedNewView = protoNewView.message.newView.value
      val mismatchedViewChange = protoNewView.copy(
        message = v30.ConsensusMessage.Message.NewView(
          protoNestedNewView.copy(
            viewChanges = protoNestedNewView.viewChanges.updated(
              0,
              protoNestedNewView.viewChanges.head.copy(from = forgedOriginator),
            )
          )
        )
      )
      NewView
        .fromProtoConsensusMessage((claimedOriginator, Some(actualSender)), mismatchedViewChange)(
          newView.getCryptographicEvidence
        )
        .isLeft shouldBe true

      val mismatchedPrePrepare = protoNewView.copy(
        message = v30.ConsensusMessage.Message.NewView(
          protoNestedNewView.copy(
            prePrepares = protoNestedNewView.prePrepares.updated(
              0,
              protoNestedNewView.prePrepares.head.copy(from = forgedOriginator),
            )
          )
        )
      )
      NewView
        .fromProtoConsensusMessage((claimedOriginator, Some(actualSender)), mismatchedPrePrepare)(
          newView.getCryptographicEvidence
        )
        .isLeft shouldBe true
    }

    "validate claimed origins in prepare certificates, commit certificates, and canonical commits" in {
      val prePrepare =
        prePrepareWithOriginator(prePrepareArb.arbitrary.sample.value, claimedOriginator)
      val prepare = prepareWithOriginator(prepareArb.arbitrary.sample.value, claimedOriginator)
      val commit =
        commitWithOriginator(
          commitArb(Some(actualSender)).arbitrary.sample.value,
          claimedOriginator,
        )
      val signedPrePrepare = SignedMessage(prePrepare, Signature.noSignature)
      val signedPrepare = SignedMessage(prepare, Signature.noSignature)
      val signedCommit = SignedMessage(commit, Signature.noSignature)

      val prepareCertificate = PrepareCertificate(signedPrePrepare, Seq(signedPrepare)).toProto
      val parsedPrepareCertificate = PrepareCertificate.fromProto(prepareCertificate).value
      parsedPrepareCertificate.prePrepare.message.actualSender shouldBe None
      parsedPrepareCertificate.prePrepare.from shouldBe claimedOriginator
      parsedPrepareCertificate.prepares.map(_.message.actualSender) shouldBe Seq(None)
      parsedPrepareCertificate.prepares.map(_.from) shouldBe Seq(claimedOriginator)
      PrepareCertificate
        .fromProto(
          prepareCertificate.copy(
            prepares = prepareCertificate.prepares.updated(
              0,
              prepareCertificate.prepares.head.copy(from = forgedOriginator),
            )
          )
        )
        .isLeft shouldBe true
      PrepareCertificate
        .fromProto(
          prepareCertificate.copy(
            prePrepare = Some(prepareCertificate.prePrepare.value.copy(from = forgedOriginator))
          )
        )
        .isLeft shouldBe true

      val commitCertificate = CommitCertificate(signedPrePrepare, Seq(signedCommit)).toProto
      val parsedCommitCertificate = CommitCertificate.fromProto(commitCertificate).value
      parsedCommitCertificate.prePrepare.message.actualSender shouldBe None
      parsedCommitCertificate.prePrepare.from shouldBe claimedOriginator
      parsedCommitCertificate.commits.map(_.message.actualSender) shouldBe Seq(None)
      parsedCommitCertificate.commits.map(_.from) shouldBe Seq(claimedOriginator)
      val viewChange = ViewChange.create(
        prePrepare.blockMetadata,
        prePrepare.viewNumber,
        Seq[ConsensusCertificate](
          PrepareCertificate(signedPrePrepare, Seq(signedPrepare)),
          CommitCertificate(signedPrePrepare, Seq(signedCommit)),
        ),
        claimedOriginator,
        Some(actualSender),
      )(synchronizerProtocolVersion)
      val parsedViewChange = ViewChange
        .fromProtoConsensusMessage((claimedOriginator, Some(actualSender)), viewChange.toProto)(
          viewChange.getCryptographicEvidence
        )
        .value
      parsedViewChange.actualSender shouldBe Some(actualSender)
      parsedViewChange.from shouldBe claimedOriginator
      parsedViewChange.consensusCerts shouldBe Seq[ConsensusCertificate](
        parsedPrepareCertificate,
        parsedCommitCertificate,
      )

      CommitCertificate
        .fromProto(
          commitCertificate.copy(
            commits = commitCertificate.commits.updated(
              0,
              commitCertificate.commits.head.copy(from = forgedOriginator),
            )
          )
        )
        .isLeft shouldBe true
      CommitCertificate
        .fromProto(
          commitCertificate.copy(
            prePrepare = Some(commitCertificate.prePrepare.value.copy(from = forgedOriginator))
          )
        )
        .isLeft shouldBe true

      val canonicalCommitSet = CanonicalCommitSet(Set(signedCommit)).toProto
      val parsedCanonicalCommitSet = CanonicalCommitSet.fromProto(canonicalCommitSet).value
      parsedCanonicalCommitSet.sortedCommits.map(_.message.actualSender) shouldBe Seq(None)
      parsedCanonicalCommitSet.sortedCommits.map(_.from) shouldBe Seq(claimedOriginator)
      CanonicalCommitSet
        .fromProto(
          canonicalCommitSet.copy(
            canonicalCommits = canonicalCommitSet.canonicalCommits.updated(
              0,
              canonicalCommitSet.canonicalCommits.head.copy(from = forgedOriginator),
            )
          )
        )
        .isLeft shouldBe true
    }
  }
}
