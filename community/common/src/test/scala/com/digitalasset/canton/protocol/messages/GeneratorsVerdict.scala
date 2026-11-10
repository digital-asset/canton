// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import cats.syntax.either.*
import com.digitalasset.canton.Generators.nonEmptyListGen
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.version.ProtocolVersion
import org.scalacheck.{Arbitrary, Gen}

final case class GeneratorsVerdict(
    protocolVersion: ProtocolVersion,
    generatorsLocalVerdict: GeneratorsLocalVerdict,
) {
  import generatorsLocalVerdict.*

  implicit val mediatorRejectArb: Arbitrary[Verdict.MediatorReject] =
    Arbitrary(
      Gen.const {
        val status = com.google.rpc.status.Status(com.google.rpc.Code.CANCELLED_VALUE)
        Verdict.MediatorReject
          .create(status, isMalformed = false, protocolVersion)
          .valueOr(err => throw new IllegalStateException(err))
      }
    )

  private val verdictApproveArb: Arbitrary[Verdict.Approve] = Arbitrary(
    Gen.const(Verdict.Approve.create(protocolVersion))
  )

  private implicit val participantRejectArb: Arbitrary[Verdict.ParticipantReject] = Arbitrary(
    nonEmptyListGen[(Set[LfPartyId], ParticipantId, LocalReject)](
      participantRejectReasonArb
    ).map { reasons =>
      Verdict.ParticipantReject.create(reasons, protocolVersion)
    }
  )

  // If this pattern match is not exhaustive anymore, update the generator below
  {
    ((_: Verdict) match {
      case _: Verdict.Approve => ()
      case _: Verdict.MediatorReject => ()
      case _: Verdict.ParticipantReject => ()
    }).discard
  }
  implicit val verdictArb: Arbitrary[Verdict] = Arbitrary(
    Gen.oneOf(
      verdictApproveArb.arbitrary,
      mediatorRejectArb.arbitrary,
      participantRejectArb.arbitrary,
    )
  )
}
