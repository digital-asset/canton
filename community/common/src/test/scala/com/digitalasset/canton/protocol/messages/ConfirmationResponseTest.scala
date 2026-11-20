// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import cats.syntax.either.*
import com.digitalasset.canton.ProtoDeserializationError.InvariantViolation
import com.digitalasset.canton.crypto.TestHash
import com.digitalasset.canton.data.{CantonTimestamp, ViewPosition}
import com.digitalasset.canton.protocol.{LocalRejectError, RequestId, RootHash, v30}
import com.digitalasset.canton.serialization.HasCryptographicEvidenceTest
import com.digitalasset.canton.topology.{SynchronizerId, UniqueIdentifier}
import com.digitalasset.canton.{BaseTest, LfPartyId, topology}
import com.digitalasset.nonempty.NonEmpty
import com.google.protobuf.ByteString
import com.google.rpc.code.Code
import com.google.rpc.status.Status
import org.scalatest.wordspec.AnyWordSpec

class ConfirmationResponseTest extends AnyWordSpec with BaseTest with HasCryptographicEvidenceTest {

  private lazy val response1: ConfirmationResponses =
    ConfirmationResponses.tryCreate(
      RequestId(CantonTimestamp.now()),
      RootHash(TestHash.digest("txid1")),
      SynchronizerId(UniqueIdentifier.tryFromProtoPrimitive("da::default")).toPhysical,
      topology.ParticipantId(UniqueIdentifier.tryFromProtoPrimitive("da::p1")),
      NonEmpty.mk(
        Seq,
        ConfirmationResponse.tryCreate(
          Some(ViewPosition.root),
          LocalApprove(testedProtocolVersion),
          Set(LfPartyId.assertFromString("p1"), LfPartyId.assertFromString("p2")),
        ),
      ),
      testedProtocolVersion,
    )
  private lazy val response2: ConfirmationResponses =
    ConfirmationResponses.tryCreate(
      RequestId(CantonTimestamp.now()),
      RootHash(TestHash.digest("txid3")),
      SynchronizerId(UniqueIdentifier.tryFromProtoPrimitive("da::default")).toPhysical,
      topology.ParticipantId(UniqueIdentifier.tryFromProtoPrimitive("da::p1")),
      NonEmpty.mk(
        Seq,
        ConfirmationResponse.tryCreate(
          None,
          LocalRejectError.MalformedRejects.Payloads
            .Reject("test message")
            .toLocalReject(testedProtocolVersion),
          Set.empty,
        ),
      ),
      testedProtocolVersion,
    )

  def fromByteString(bytes: ByteString): ConfirmationResponses =
    ConfirmationResponses
      .fromByteString(testedProtocolVersion, bytes)
      .valueOr(err => fail(err.toString))

  "ConfirmationResponses" should {
    behave like hasCryptographicEvidenceSerialization(response1, response2)
    behave like hasCryptographicEvidenceDeserialization(
      response1,
      response1.getCryptographicEvidence,
      "response1",
    )(fromByteString)
    behave like hasCryptographicEvidenceDeserialization(
      response2,
      response2.getCryptographicEvidence,
      "response2",
    )(fromByteString)
  }

  "MediatorReject" should {
    val okStatus = Status(code = Code.OK.value, message = "ok")
    val abortedStatus = Status(code = Code.ABORTED.value, message = "aborted")

    "reject the status code OK" in {
      Verdict.MediatorReject.create(okStatus, isMalformed = false, testedProtocolVersion) shouldBe
        Left("Rejection must not use status code OK")
    }

    "accept a non-OK status code" in {
      Verdict.MediatorReject
        .create(abortedStatus, isMalformed = true, testedProtocolVersion)
        .value
        .reason shouldBe abortedStatus
    }

    "fail deserialization of the status code OK" in {
      Verdict.MediatorReject.fromProtoV30(
        v30.MediatorReject(reason = Some(okStatus), isMalformed = false)
      ) shouldBe Left(
        InvariantViolation(None, "Rejection must not use status code OK")
      )
    }
  }
}
