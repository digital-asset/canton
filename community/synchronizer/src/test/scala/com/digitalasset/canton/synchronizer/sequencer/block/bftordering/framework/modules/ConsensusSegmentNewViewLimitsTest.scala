// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules

import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.protocol.{BftOrdererLimits, BftOrdererLimitsView}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.BftSequencerBaseTest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.BftSequencerBaseTest.FakeSigner
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.SegmentStateTest.*
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  EpochNumber,
  ViewNumber,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.SignedMessage
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.availability.{
  BatchId,
  OrderingBlock,
  ProofOfAvailability,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.bfttime.CanonicalCommitSet
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.iss.BlockMetadata
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.ConsensusSegment.ConsensusMessage.{
  ConsensusSegmentDeserializationContext,
  NewView,
  PrePrepare,
  ViewChange,
}
import com.digitalasset.canton.version.{ProtocolVersion, ProtocolVersionValidation}
import com.google.protobuf.ByteString
import org.scalatest.wordspec.AnyWordSpec

class ConsensusSegmentNewViewLimitsTest extends AnyWordSpec with BftSequencerBaseTest {

  // 3 sequencers and segment length 1, so every derived limit is 3. The bound must allow the
  // 3 prepares of the certificates built by the SegmentStateTest helpers (4 nodes).
  private val bound = 3
  private val limits = BftOrdererLimitsView(
    BftOrdererLimits(
      maxSegmentLength = PositiveInt.one,
      maxOrderingRequests = PositiveInt.three,
      maxProofs = PositiveInt.three,
    ),
    maxSequencersPerSynchronizer = PositiveInt.three,
  )
  private val context = ConsensusSegmentDeserializationContext(myId, Some(myId), limits)

  private val enforcing = ProtocolVersionValidation.PV(ProtocolVersion.dev)

  private def parse(
      newView: NewView,
      pvv: ProtocolVersionValidation = enforcing,
  ) = NewView.fromProtoConsensusMessage(pvv, context, newView.toProto)(ByteString.EMPTY)

  private def viewChange(certs: Int): SignedMessage[ViewChange] =
    createViewChange(
      viewNumber = 1L,
      from = myId,
      slotsAndViewNumbers = Seq.fill(certs)(0L -> 0L),
    )

  private def prePrepare(proofs: Int): SignedMessage[PrePrepare] =
    PrePrepare
      .create(
        BlockMetadata.mk(EpochNumber.First, 0L),
        ViewNumber(1L),
        OrderingBlock(
          Seq.fill(proofs)(
            ProofOfAvailability(BatchId.createForTesting("batch"), Seq.empty, EpochNumber.First)
          )
        ),
        CanonicalCommitSet.empty,
        myId,
      )
      .fakeSign

  private def newView(
      viewChanges: Seq[SignedMessage[ViewChange]] = Seq.empty,
      prePrepares: Seq[SignedMessage[PrePrepare]] = Seq.empty,
  ) = createNewView(1L, myId, viewChanges, prePrepares).message

  private def violationOf(field: String)(result: Either[ProtoDeserializationError, ?]) =
    result should matchPattern {
      case Left(ProtoDeserializationError.InvariantViolation(Some(`field`), _)) =>
    }

  "NewView" should {
    "accept exactly the bound and reject bound + 1 for view_changes" in {
      parse(newView(viewChanges = Seq.fill(bound)(viewChange(0)))) shouldBe a[Right[?, ?]]

      violationOf("view_changes")(parse(newView(viewChanges = Seq.fill(bound + 1)(viewChange(0)))))
    }

    "accept exactly the bound and reject bound + 1 for pre_prepares" in {
      parse(newView(prePrepares = Seq.fill(bound)(prePrepare(0)))) shouldBe a[Right[?, ?]]

      violationOf("pre_prepares")(parse(newView(prePrepares = Seq.fill(bound + 1)(prePrepare(0)))))
    }

    "reject an oversized collection nested in a view change" in {
      parse(newView(viewChanges = Seq(viewChange(bound)))) shouldBe a[Right[?, ?]]

      violationOf("consensus_certs")(parse(newView(viewChanges = Seq(viewChange(bound + 1)))))
    }

    "reject an oversized collection nested in a pre-prepare" in {
      parse(newView(prePrepares = Seq(prePrepare(bound)))) shouldBe a[Right[?, ?]]

      violationOf("proofs")(parse(newView(prePrepares = Seq(prePrepare(bound + 1)))))
    }

    "enforce the limits only from the dev protocol version and with AlwaysValidation" in {
      val oversized = Seq(
        newView(viewChanges = Seq.fill(bound + 1)(viewChange(0))),
        newView(prePrepares = Seq.fill(bound + 1)(prePrepare(0))),
        newView(viewChanges = Seq(viewChange(bound + 1))),
        newView(prePrepares = Seq(prePrepare(bound + 1))),
      )

      oversized.foreach { nv =>
        parse(nv, ProtocolVersionValidation.AlwaysValidation) shouldBe a[Left[?, ?]]
        parse(nv, enforcing) shouldBe a[Left[?, ?]]

        // Older (stable) protocol versions and NoValidation bypass the checks
        parse(nv, ProtocolVersionValidation.PV(ProtocolVersion.v37)) shouldBe a[Right[?, ?]]
        parse(nv, ProtocolVersionValidation.PV(ProtocolVersion.v36)) shouldBe a[Right[?, ?]]
        parse(nv, ProtocolVersionValidation.NoValidation) shouldBe a[Right[?, ?]]
      }
    }
  }
}
