// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering

import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.protocol.BftOrdererLimits
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.BftSequencerBaseTest
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.SegmentStateTest.*
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.BftNodeId
import com.digitalasset.canton.version.{ProtocolVersion, ProtocolVersionValidation}
import org.scalatest.wordspec.AnyWordSpec

class CertificateVoteLimitsTest extends AnyWordSpec with BftSequencerBaseTest {

  // maxPrepares = maxCommits = maxSequencersInSynchronizer
  private val bound = 2
  private val limits = BftOrdererLimits.default.copy(maxSequencersInSynchronizer = PositiveInt.two)

  private val enforcing = ProtocolVersionValidation.PV(ProtocolVersion.dev)

  private implicit val pv: ProtocolVersion = ProtocolVersion.dev

  private val prePrepare = createPrePrepare(blockNumber = 0L, view = 0L, from = myId)
  private val hash = prePrepare.message.hash
  private def voter(i: Int) = BftNodeId(s"voter$i")

  private def prepareCertificate(votes: Int) =
    PrepareCertificate(
      prePrepare,
      Seq.tabulate(votes)(i => createPrepare(0L, 0L, voter(i), hash)),
    ).toProto

  private def commitCertificate(votes: Int) =
    CommitCertificate(
      prePrepare,
      Seq.tabulate(votes)(i => createCommit(0L, 0L, voter(i), hash)),
    ).toProto

  private val cases: Seq[
    (String, String, (Int, ProtocolVersionValidation) => ParsingResult[ConsensusCertificate])
  ] =
    Seq[(String, String, (Int, ProtocolVersionValidation) => ParsingResult[ConsensusCertificate])](
      (
        "PrepareCertificate",
        "prepares",
        (n, pvv) => PrepareCertificate.fromProto(pvv, limits, prepareCertificate(n)),
      ),
      (
        "CommitCertificate",
        "commits",
        (n, pvv) => CommitCertificate.fromProto(pvv, limits, commitCertificate(n)),
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
