// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules

import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.protocol.BftOrdererLimits
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.BftSequencerBaseTest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.BftSequencerBaseTest.FakeSigner
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BftNodeId,
  BlockNumber,
  EpochNumber,
  ViewNumber,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.availability.OrderingBlock
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.bfttime.CanonicalCommitSet
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.CommitCertificate
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.iss.BlockMetadata
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.Consensus.RetransmissionsMessage.RetransmissionResponse
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.Consensus.StateTransferMessage.{
  BlockTransferResponse31,
  ConsensusDeserializationContext,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.ConsensusSegment.ConsensusMessage.PrePrepare
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.v30
import com.digitalasset.canton.version.{ProtocolVersion, ProtocolVersionValidation}
import com.google.protobuf.ByteString
import org.scalatest.wordspec.AnyWordSpec

class CommitCertificatesResponseLimitsTest extends AnyWordSpec with BftSequencerBaseTest {

  private val bound = 2
  // maxCommitCertificates = maxSegmentLength * maxSequencersInSynchronizer = 1 * 2
  private val limits = BftOrdererLimits.default.copy(
    maxSequencersInSynchronizer = PositiveInt.two,
    maxSegmentLength = PositiveInt.one,
  )
  private val sender = BftNodeId("sender")

  private val enforcing = ProtocolVersionValidation.PV(ProtocolVersion.dev)

  private def commitCertificate(block: Int): v30.CommitCertificate =
    CommitCertificate(
      PrePrepare
        .create(
          BlockMetadata(EpochNumber.First, BlockNumber(block.toLong)),
          ViewNumber.First,
          OrderingBlock.empty,
          CanonicalCommitSet.empty,
          sender,
        )(ProtocolVersion.dev)
        .fakeSign,
      Seq.empty,
    ).toProto

  private def response(certificates: Int) =
    v30.RetransmissionResponse(Seq.tabulate(certificates)(commitCertificate))

  private def parseRetransmission(certificates: Int, pvv: ProtocolVersionValidation) =
    RetransmissionResponse.fromProto(pvv, limits, sender, response(certificates)).map(_ => ())

  private def parseBlockTransfer31(certificates: Int, pvv: ProtocolVersionValidation) =
    BlockTransferResponse31
      .fromProto(
        pvv,
        ConsensusDeserializationContext(sender, limits),
        response(certificates),
      )(ByteString.EMPTY)
      .map(_ => ())

  private val parsers
      : Seq[(String, (Int, ProtocolVersionValidation) => Either[ProtoDeserializationError, Unit])] =
    Seq(
      "RetransmissionResponse" -> parseRetransmission,
      "BlockTransferResponse31" -> parseBlockTransfer31,
    )

  parsers.foreach { case (name, parse) =>
    name should {
      "accept exactly the bound and reject bound + 1 for commit_certificates" in {
        limits.maxCommitCertificates.value shouldBe bound
        parse(bound, enforcing) shouldBe a[Right[?, ?]]

        parse(bound + 1, enforcing) should matchPattern {
          case Left(ProtoDeserializationError.InvariantViolation(Some("commit_certificates"), _)) =>
        }
      }

      "enforce the limit from the dev protocol version and with AlwaysValidation only" in {
        parse(bound + 1, ProtocolVersionValidation.AlwaysValidation) shouldBe a[Left[?, ?]]
        parse(bound + 1, enforcing) shouldBe a[Left[?, ?]]

        // Older (stable) protocol versions and NoValidation bypass the check
        parse(bound + 1, ProtocolVersionValidation.PV(ProtocolVersion.v37)) shouldBe a[Right[?, ?]]
        parse(bound + 1, ProtocolVersionValidation.PV(ProtocolVersion.v36)) shouldBe a[Right[?, ?]]
        parse(bound + 1, ProtocolVersionValidation.NoValidation) shouldBe a[Right[?, ?]]
      }
    }
  }
}
