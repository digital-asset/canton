// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.statetransfer

import com.digitalasset.canton.synchronizer.metrics.SequencerMetrics
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.BftSequencerBaseTest.FakeSigner
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.integration.canton.SupportedVersions
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.integration.canton.crypto.CryptoProvider.AuthenticatedMessageType.BftSignedStateTransferMessage
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.IssConsensusModule
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.data.Bootstrap.BootstrapEpochNumber
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.statetransfer.StateTransferTestHelpers.{
  aBlockMetadata,
  aCommit,
  aCommitCert,
  aPrePrepare,
  myId,
  otherId,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.{
  ProgrammableUnitTestContext,
  ProgrammableUnitTestEnv,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BlockNumber,
  EpochNumber,
  ViewNumber,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.SignedMessage
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.availability.OrderingBlock
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.bfttime.CanonicalCommitSet
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.CommitCertificate
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.iss.BlockMetadata
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.topology.{
  Membership,
  OrderingTopology,
  OrderingTopologyInfo,
  SequencingParameters,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.Consensus
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.Consensus.StateTransferMessage.{
  BlockTransferRequest,
  BlockTransferResponse30,
  BlockTransferResponse31,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.ConsensusSegment.ConsensusMessage.PrePrepare
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.{
  BftSequencerBaseTest,
  failingCryptoProvider,
}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.{v30, v31}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{ProtoDeserializationError, ProtocolVersionChecksAnyWordSpec}
import org.scalatest.wordspec.AnyWordSpec
import org.slf4j.event.Level

final class StateTransferMessageValidatorTest
    extends AnyWordSpec
    with BftSequencerBaseTest
    with ProtocolVersionChecksAnyWordSpec {

  import StateTransferMessageValidatorTest.*

  private val metrics = SequencerMetrics.noop(getClass.getSimpleName).bftOrdering
  private val validator =
    new StateTransferMessageValidator[ProgrammableUnitTestEnv](metrics, loggerFactory)

  "validate block transfer request" in {
    Table[BlockTransferRequest, Either[String, Unit]](
      ("request", "expected result"),
      // negative: genesis start epoch
      (
        BlockTransferRequest.create(BootstrapEpochNumber, otherId),
        Left("state transfer is supported only after genesis, but start epoch -1 received"),
      ),
      // positive
      (
        BlockTransferRequest.create(EpochNumber(1L), otherId),
        Right(()),
      ),
    ).forEvery { (request, expectedResult) =>
      validator.validateBlockTransferRequest(
        request
      ) shouldBe expectedResult
    }
  }

  "validate a single-certificate block transfer response" onlyRunLessThan ProtocolVersion.v37 in {
    Table(
      (
        "block transfer response",
        "latest locally completed epoch",
        "membership",
        "expected result",
      ),
      // negative: unexpected epoch in pre-prepare
      (
        BlockTransferResponse30.create(
          Some(
            aCommitCert().copy(prePrepare =
              aPrePrepare(BlockMetadata(BootstrapEpochNumber, BlockNumber.First))
            )
          ),
          otherId,
        ),
        EpochNumber.First,
        aMembershipWith2Nodes,
        Left(
          "received a block transfer response from 'other' containing pre-prepare(s) with unexpected epoch(s) List(-1), " +
            "expected 1"
        ),
      ),
      // negative: unexpected epoch in commits
      (
        BlockTransferResponse30.create(
          Some(
            aCommitCert(BlockMetadata(EpochNumber.First, BlockNumber.First))
              .copy(prePrepare = aPrePrepare(BlockMetadata(EpochNumber(1L), BlockNumber.First)))
          ),
          otherId,
        ),
        EpochNumber.First,
        aMembershipWith2Nodes,
        Left(
          "received a block transfer response from 'other' containing a commit certificate with the following issue: commit certificate for block 0 has the following errors: commits have epoch number 0 but it should be 1, expected at least 2 commits, but only got 1, commit from other has non-matching hash"
        ),
      ),
      // negative: duplicate senders
      (
        BlockTransferResponse30.create(
          Some(aCommitCert().copy(commits = Seq(aCommit(), aCommit()))),
          otherId,
        ),
        BootstrapEpochNumber,
        aMembershipWith2Nodes,
        Left(
          "received a block transfer response from 'other' containing a commit certificate with the following issue: commit certificate for block 0 has the following errors: there are more than one commits (2) from the same sender other, expected at least 2 commits, but only got 1, commit from other has non-matching hash"
        ),
      ),
      // negative: no strong quorum
      (
        BlockTransferResponse30.create(
          Some(aCommitCert().copy(commits = Seq(aCommit()))),
          otherId,
        ),
        BootstrapEpochNumber,
        aMembershipWith2Nodes,
        Left(
          "received a block transfer response from 'other' containing a commit certificate with the following issue: commit certificate for block 0 has the following errors: expected at least 2 commits, but only got 1, commit from other has non-matching hash"
        ),
      ),
      // positive
      (
        BlockTransferResponse30.create(Some(aCommitCert()), otherId),
        BootstrapEpochNumber,
        aMembershipWithOnlyOtherNode,
        Right(()),
      ),
    ).forEvery { (response, latestLocallyCompletedEpoch, membership, expectedResult) =>
      validator.validateBlockTransferResponse(
        response,
        latestLocallyCompletedEpoch,
        membership,
      ) shouldBe expectedResult
    }
  }

  // For protocol versions >= 37, a single response can carry multiple commit certificates (reusing the
  //  retransmission response message). Make sure every certificate is validated, not just the first one.
  "validate a multi-certificate block transfer response" onlyRunWithOrGreaterThan ProtocolVersion.v37 in {
    Table(
      (
        "block transfer response",
        "latest locally completed epoch",
        "membership",
        "expected result",
      ),
      // negative: the first certificate is valid, but a later one has an unexpected epoch
      (
        BlockTransferResponse31.create(
          Seq(
            aCommitCert(),
            aCommitCert(BlockMetadata(EpochNumber(1L), BlockNumber(1L))),
          ),
          otherId,
        ),
        BootstrapEpochNumber,
        aMembershipWithOnlyOtherNode,
        Left(
          "received a block transfer response from 'other' containing pre-prepare(s) with unexpected epoch(s) " +
            "List(1), expected 0"
        ),
      ),
      // negative: the first certificate is valid, but a later one has an invalid consensus certificate
      (
        BlockTransferResponse31.create(
          Seq(
            aCommitCert(),
            aCommitCert(BlockMetadata(EpochNumber.First, BlockNumber(1L))).copy(commits = Seq.empty),
          ),
          otherId,
        ),
        BootstrapEpochNumber,
        aMembershipWithOnlyOtherNode,
        Left(
          "received a block transfer response from 'other' containing a commit certificate with the following issue: " +
            "commit certificate for block 1 has the following errors: there are no commits"
        ),
      ),
      // positive: all certificates are valid
      (
        BlockTransferResponse31.create(
          Seq(
            aCommitCert(),
            aCommitCert(BlockMetadata(EpochNumber.First, BlockNumber(1L))),
          ),
          otherId,
        ),
        BootstrapEpochNumber,
        aMembershipWithOnlyOtherNode,
        Right(()),
      ),
    ).forEvery { (response, latestLocallyCompletedEpoch, membership, expectedResult) =>
      validator.validateBlockTransferResponse(
        response,
        latestLocallyCompletedEpoch,
        membership,
      ) shouldBe expectedResult
    }
  }

  // Over the wire, requests and single-certificate responses are protobuf-compatible across the v30 and v31
  //  state transfer messages, so selecting the decoder purely from the local protocol version would silently
  //  mislabel a v30-tagged payload as v31 (or vice versa). The embedded wire version must therefore match the
  //  one used at the runtime protocol version.
  "accept only a protocol v31 state transfer message over the wire" onlyRunWithOrGreaterThan ProtocolVersion.v37 in {
    val (wireTaggedV30, wireTaggedV31) = stateTransferRequestWireMessages()

    parseStateTransferMessageV31FromWire(wireTaggedV31).value.message shouldBe a[
      BlockTransferRequest
    ]
    inside(parseStateTransferMessageV31FromWire(wireTaggedV30)) {
      case Left(ProtoDeserializationError.OtherError(error)) =>
        error should include("Wire protobuf version 30 does not match the expected version 31")
    }
  }

  "accept only a protocol v30 state transfer message over the wire" onlyRunLessThan ProtocolVersion.v37 in {
    val (wireTaggedV30, wireTaggedV31) = stateTransferRequestWireMessages()

    parseStateTransferMessageV30FromWire(wireTaggedV30).value.message shouldBe a[
      BlockTransferRequest
    ]
    inside(parseStateTransferMessageV30FromWire(wireTaggedV31)) {
      case Left(ProtoDeserializationError.OtherError(error)) =>
        error should include("Wire protobuf version 31 does not match the expected version 30")
    }
  }

  private def stateTransferRequestWireMessages(): (v30.SignedMessage, v30.SignedMessage) = {
    val wireTaggedV30 =
      BlockTransferRequest
        .create(EpochNumber.First, otherId)(ProtocolVersion.v36)
        .fakeSign
        .toProtoV1
    val wireTaggedV31 =
      BlockTransferRequest
        .create(EpochNumber.First, otherId)(ProtocolVersion.v37)
        .fakeSign
        .toProtoV1
    (wireTaggedV30, wireTaggedV31)
  }

  private def parseStateTransferMessageV31FromWire(proto: v30.SignedMessage) =
    SignedMessage.fromProto(v31.StateTransferMessage, SupportedVersions.ProtoData31)(
      IssConsensusModule.parseStateTransferMessage31(otherId, _)
    )(proto)

  private def parseStateTransferMessageV30FromWire(proto: v30.SignedMessage) =
    SignedMessage.fromProto(v30.StateTransferMessage, SupportedVersions.ProtoData30)(
      IssConsensusModule.parseStateTransferMessage(otherId, _)
    )(proto)

  "skip old block transfer responses" in {
    implicit val context: ProgrammableUnitTestContext[Consensus.Message[ProgrammableUnitTestEnv]] =
      new ProgrammableUnitTestContext

    val blockMetadata = BlockMetadata(EpochNumber(1), BlockNumber(1))
    val prePrepare = PrePrepare.create(
      blockMetadata,
      ViewNumber.First,
      OrderingBlock.empty,
      CanonicalCommitSet.empty,
      otherId,
    )(testedProtocolVersion)
    val commitCertificate = CommitCertificate(prePrepare.fakeSign, Seq.empty)
    val orderingTopology = OrderingTopology.forTesting(Set(myId, otherId))
    val leaders = Seq(myId, otherId)
    val orderingTopologyInfo = OrderingTopologyInfo[ProgrammableUnitTestEnv](
      myId,
      currentTopology = orderingTopology,
      currentCryptoProvider = failingCryptoProvider,
      currentLeaders = leaders,
      currentBlacklistedNodes = Seq.empty,
      previousTopology = orderingTopology,
      previousCryptoProvider = failingCryptoProvider,
      previousLeaders = leaders,
      previousBlacklistedNodes = Seq.empty,
    )

    val response = BlockTransferResponse30.create(Some(commitCertificate), otherId)
    assertLogs(
      validator.validateUnverifiedStateTransferNetworkMessage(
        response.fakeSign,
        EpochNumber(1),
        orderingTopologyInfo,
      ),
      logLine => {
        logLine.level shouldBe Level.INFO
        logLine.message shouldBe "Old block transfer response from epoch 1, we have completed epoch 1, dropping..."
      },
    )

    context.extractSelfMessages() shouldBe empty
  }

  "block transfer response signature verification" when {
    "it contains None" in {
      implicit val context
          : ProgrammableUnitTestContext[Consensus.Message[ProgrammableUnitTestEnv]] =
        new ProgrammableUnitTestContext
      val cryptoProvider = spy(ProgrammableUnitTestEnv.noSignatureCryptoProvider)

      val response = BlockTransferResponse30.create(None, otherId)
      validator.verifyStateTransferMessage(
        response.fakeSign,
        aMembershipWith2Nodes,
        cryptoProvider,
      )

      context.runPipedMessages() should contain only
        Consensus.StateTransferMessage.VerifiedStateTransferMessage(response)

      verify(cryptoProvider, times(1))
        .verifySignedMessage(response.fakeSign, BftSignedStateTransferMessage)
    }

    "contains commit certificate is skipped" in {
      implicit val context
          : ProgrammableUnitTestContext[Consensus.Message[ProgrammableUnitTestEnv]] =
        new ProgrammableUnitTestContext

      val response = BlockTransferResponse30.create(Some(aCommitCert(aBlockMetadata)), otherId)
      validator.verifyStateTransferMessage(
        response.fakeSign,
        aMembershipWith2Nodes,
        failingCryptoProvider,
      )

      context.extractSelfMessages() should contain only
        Consensus.StateTransferMessage.VerifiedStateTransferMessage(response)
    }
  }

}

object StateTransferMessageValidatorTest {
  private def aMembershipWithOnlyOtherNode(implicit pv: ProtocolVersion) =
    Membership(
      myId,
      OrderingTopology.forTesting(Set(otherId), Option(SequencingParameters.Default)),
      leaders = Seq(otherId),
      blacklistedNodes = Seq.empty,
    )
  private def aMembershipWith2Nodes(implicit pv: ProtocolVersion) =
    Membership.forTesting(myId, Set(otherId))
}
