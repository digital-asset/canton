// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules

import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.protocol.{BftOrdererLimits, BftOrdererLimitsView}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.BftSequencerBaseTest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.BftNodeId
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.ConsensusStatus.ConsensusStatusDeserializationContext
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.v30
import com.digitalasset.canton.version.{ProtocolVersion, ProtocolVersionValidation}
import com.google.protobuf.ByteString
import com.google.protobuf.empty.Empty
import org.scalatest.wordspec.AnyWordSpec

class ConsensusStatusTest extends AnyWordSpec with BftSequencerBaseTest {
  private val from = BftNodeId("node0")

  "EpochStatus" should {
    "reject a wire message with an empty segments list rather than let it through, " +
      "as it can otherwise lead to a divide-by-zero when processing it" in {
        val proto = v30.EpochStatus(epochNumber = 0L, segments = Seq.empty)

        val context =
          ConsensusStatusDeserializationContext(from, limits = BftOrdererLimitsView.default)

        ConsensusStatus.EpochStatus.fromProto(
          ProtocolVersionValidation.PV(synchronizerProtocolVersion),
          context,
          proto,
        )(ByteString.EMPTY) shouldBe Left(
          ProtoDeserializationError.ValueConversionError("segments", "must not be empty")
        )
      }
  }

  "EpochStatus size limits" should {
    // Every derived limit is 2 (2 sequencers, segment length 1, so 2 blocks per epoch)
    val limits = BftOrdererLimitsView(
      BftOrdererLimits(
        maxSegmentLength = PositiveInt.one,
        maxOrderingRequests = PositiveInt.two,
        maxProofs = PositiveInt.two,
      ),
      maxSequencersPerSynchronizer = PositiveInt.two,
    )
    val bound = 2
    val context = ConsensusStatusDeserializationContext(from, limits)

    def parse(
        epochStatus: v30.EpochStatus,
        pvv: ProtocolVersionValidation = ProtocolVersionValidation.PV(ProtocolVersion.dev),
    ) = ConsensusStatus.EpochStatus.fromProto(pvv, context, epochStatus)(ByteString.EMPTY)

    val complete = v30.SegmentStatus(v30.SegmentStatus.Status.Complete(Empty()))
    def segmentWith(status: v30.SegmentStatus.Status) = v30.SegmentStatus(status)
    def epochOf(segments: v30.SegmentStatus*) =
      v30.EpochStatus(epochNumber = 0L, segments = segments)
    def blockInProgress(prepares: Int, commits: Int) = v30.BlockStatus(
      v30.BlockStatus.Status.InProgress(
        v30.BlockInProgress(
          prePrepared = true,
          preparesPresent = Seq.fill(prepares)(true),
          commitsPresent = Seq.fill(commits)(true),
        )
      )
    )
    def inProgressSegment(blocks: v30.BlockStatus*) = segmentWith(
      v30.SegmentStatus.Status.InProgress(v30.SegmentInProgress(0L, blocks))
    )
    def inViewChangeSegment(viewChangeMessages: Int, blocksComplete: Int) = segmentWith(
      v30.SegmentStatus.Status.InViewChange(
        v30.SegmentInViewChange(
          0L,
          Seq.fill(viewChangeMessages)(true),
          Seq.fill(blocksComplete)(true),
        )
      )
    )

    // Each case: field name, builder from a collection size to an epoch status
    val cases: Seq[(String, Int => v30.EpochStatus)] = Seq(
      "segments" -> (n => epochOf(Seq.fill(n)(complete)*)),
      "block_statuses" -> (n => epochOf(inProgressSegment(Seq.fill(n)(blockInProgress(0, 0))*))),
      "view_change_messages_present" -> (n => epochOf(inViewChangeSegment(n, 0))),
      "are_blocks_complete" -> (n => epochOf(inViewChangeSegment(0, n))),
      "prepares_present" -> (n => epochOf(inProgressSegment(blockInProgress(n, 0)))),
      "commits_present" -> (n => epochOf(inProgressSegment(blockInProgress(0, n)))),
    )

    cases.foreach { case (field, build) =>
      s"accept exactly the bound and reject bound + 1 for $field" in {
        parse(build(bound)) shouldBe a[Right[?, ?]]

        inside(parse(build(bound + 1))) {
          case Left(ProtoDeserializationError.InvariantViolation(Some(`field`), _)) => succeed
        }
      }

      s"enforce $field from the dev protocol version and with AlwaysValidation only" in {
        val oversized = build(bound + 1)

        parse(oversized, ProtocolVersionValidation.AlwaysValidation) shouldBe a[Left[?, ?]]
        parse(oversized, ProtocolVersionValidation.PV(ProtocolVersion.dev)) shouldBe a[Left[?, ?]]

        // Older (stable) protocol versions and NoValidation bypass the check
        parse(oversized, ProtocolVersionValidation.PV(ProtocolVersion.v37)) shouldBe a[Right[?, ?]]
        parse(oversized, ProtocolVersionValidation.PV(ProtocolVersion.v36)) shouldBe a[Right[?, ?]]
        parse(oversized, ProtocolVersionValidation.NoValidation) shouldBe a[Right[?, ?]]
      }
    }
  }
}
