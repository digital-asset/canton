// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import com.digitalasset.canton.protocol.messages.{LocalReject, Verdict}
import com.digitalasset.canton.protocol.messages.Verdict.{Approve, RejectReasons, Timeout}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{BaseTest, LfPartyId}
import org.scalatest.wordspec.AnyWordSpec

@SuppressWarnings(
  Array(
    "org.wartremover.warts.Product",
    "org.wartremover.warts.Serializable",
  )
)
class VerdictTest extends AnyWordSpec with BaseTest {
  private def party(name: String): LfPartyId = LfPartyId.assertFromString(name)

  "TransactionResult" can {
    "converting to and from proto" should {
      "result in an equal object" in {
        val exampleResults = Table(
          ("type", "value"),
          ("approve", Approve),
          (
            "reject",
            RejectReasons(
              List(
                (
                  Set(party("p1"), party("p2")),
                  LocalReject.MalformedRejects.Payloads.Reject("some error"),
                ),
                (Set(party("p3")), LocalReject.ConsistencyRejections.LockedContracts.Reject(Seq())),
              )
            ),
          ),
          ("timeout", Timeout),
        )
        forAll(exampleResults) { (resultType: String, original: Verdict) =>
          val cycled =
            Verdict.fromProtoVersioned(original.toProtoVersioned(ProtocolVersion.default)) match {
              case Left(err) => fail(err.toString)
              case Right(verdict) => verdict
            }

          assertResult(original, resultType)(cycled)
        }
      }
    }
  }
}
