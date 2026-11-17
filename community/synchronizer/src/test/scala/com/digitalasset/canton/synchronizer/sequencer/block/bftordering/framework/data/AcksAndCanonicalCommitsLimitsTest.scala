// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data

import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.Signature
import com.digitalasset.canton.protocol.BftOrdererLimits
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.BftSequencerBaseTest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.SegmentStateTest.*
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BftNodeId,
  EpochNumber,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.availability.{
  AvailabilityAck,
  BatchId,
  OrderingBlock,
  ProofOfAvailability,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.bfttime.CanonicalCommitSet
import com.digitalasset.canton.version.{ProtocolVersion, ProtocolVersionValidation}
import org.scalatest.wordspec.AnyWordSpec

class AcksAndCanonicalCommitsLimitsTest extends AnyWordSpec with BftSequencerBaseTest {

  // maxAcks = maxCanonicalCommits = maxSequencersInSynchronizer
  private val bound = 2
  private val limits = BftOrdererLimits.default.copy(maxSequencersInSynchronizer = PositiveInt.two)

  private val enforcing = ProtocolVersionValidation.PV(ProtocolVersion.dev)

  private implicit val pv: ProtocolVersion = ProtocolVersion.dev

  private def node(i: Int) = BftNodeId(s"node$i")

  private def proof(acks: Int) =
    OrderingBlock(
      Seq(
        ProofOfAvailability(
          BatchId.createForTesting("batch"),
          Seq.tabulate(acks)(i => AvailabilityAck(node(i), Signature.noSignature)),
          EpochNumber.First,
        )
      )
    ).toProto.proofs.head

  private def commitSet(commits: Int) =
    CanonicalCommitSet(
      Seq
        .tabulate(commits)(i =>
          createCommit(0L, 0L, node(i), createPrePrepare(0L, 0L, myId).message.hash)
        )
        .toSet
    ).toProto

  private val cases: Seq[(String, String, (Int, ProtocolVersionValidation) => ParsingResult[Any])] =
    Seq[(String, String, (Int, ProtocolVersionValidation) => ParsingResult[Any])](
      (
        "ProofOfAvailability",
        "acks",
        (n, pvv) => ProofOfAvailability.fromProto(pvv, limits, proof(n)),
      ),
      (
        "CanonicalCommitSet",
        "canonical_commits",
        (n, pvv) => CanonicalCommitSet.fromProto(pvv, limits, commitSet(n)),
      ),
    )

  cases.foreach { case (name, field, parse) =>
    name should {
      s"accept exactly the bound and reject bound + 1 for $field" in {
        parse(bound, enforcing) shouldBe a[Right[?, ?]]

        parse(bound + 1, enforcing) should matchPattern {
          case Left(ProtoDeserializationError.InvariantViolation(Some(`field`), _)) =>
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
