// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.admin

import com.daml.platform.v1.page_tokens.ListPartiesPageTokenPayload
import com.digitalasset.base.error.ErrorsAssertions
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors
import com.digitalasset.canton.topology.DefaultTestIdentities
import com.google.protobuf.ByteString
import org.scalatest.EitherValues
import org.scalatest.flatspec.AnyFlatSpec

import java.nio.charset.StandardCharsets
import java.util.Base64

class ListKnownPartiesPageTokenSpec
    extends AnyFlatSpec
    with BaseTest
    with EitherValues
    with ErrorsAssertions {

  private val participant1 = DefaultTestIdentities.participant1.toLf
  private val participant2 = DefaultTestIdentities.participant2.toLf
  private val party = DefaultTestIdentities.party1.toLf
  private val eventSeqId = 42L

  private def encodePayload(payload: ListPartiesPageTokenPayload): String =
    new String(Base64.getUrlEncoder.encode(payload.toByteArray), StandardCharsets.UTF_8)

  private def validPayload: ListPartiesPageTokenPayload =
    ListPartiesPageTokenPayload(
      partyIdLowerBoundExcl = party,
      validAtEventSeqId = eventSeqId,
      version = ListKnownPartiesPageToken.Version,
      participantIdChecksum = ListKnownPartiesPageToken.participantChecksum(participant1),
    )

  private def invalidPageToken(details: String) =
    RequestValidationErrors.InvalidArgument.Reject(details).asGrpcError

  behavior of "ListKnownPartiesPageToken.decode"

  it should "decode an empty token as the first page" in {
    ListKnownPartiesPageToken.decode("").value shouldBe None
  }

  it should "decode the payload without validating it" in {
    val payload = validPayload.copy(version = 0, participantIdChecksum = ByteString.EMPTY)
    ListKnownPartiesPageToken.decode(encodePayload(payload)) shouldBe Right(Some(payload))
  }

  it should "reject a token which is not base64" in {
    assertError(
      actual = ListKnownPartiesPageToken.decode("not-a-base64-string!!").left.value,
      expected = invalidPageToken("Invalid page token: failed base64 decoding"),
    )
  }

  it should "reject a token which is not a valid protobuf" in {
    val token = new String(
      Base64.getUrlEncoder.encode("not a valid proto buf".getBytes(StandardCharsets.UTF_8)),
      StandardCharsets.UTF_8,
    )
    assertError(
      actual = ListKnownPartiesPageToken.decode(token).left.value,
      expected = invalidPageToken("Invalid page token: failed proto decoding"),
    )
  }

  behavior of "ListKnownPartiesPageToken.validate"

  it should "accept a valid payload" in {
    ListKnownPartiesPageToken.validate(validPayload, participant1).value shouldBe
      ListKnownPartiesPageToken(party, eventSeqId)

  }

  it should "accept event sequence ID zero" in {
    val payload = validPayload.copy(validAtEventSeqId = 0L)
    ListKnownPartiesPageToken.validate(payload, participant1).value shouldBe
      ListKnownPartiesPageToken(party, 0L)
  }

  it should "reject a token issued by a different participant" in {
    assertError(
      actual = ListKnownPartiesPageToken.validate(validPayload, participant2).left.value,
      expected = invalidPageToken(
        "Invalid page token: the page token was issued by a different participant"
      ),
    )
  }

  it should "reject a token with an unknown version" in {
    val payload = validPayload.copy(version = ListKnownPartiesPageToken.Version + 1)
    assertError(
      actual = ListKnownPartiesPageToken.validate(payload, participant1).left.value,
      expected = invalidPageToken(
        s"Invalid page token: the page token was issued by an incompatible Canton version (token version ${ListKnownPartiesPageToken.Version + 1})"
      ),
    )
  }

  it should "reject a token with a negative event sequence ID" in {
    val payload = validPayload.copy(validAtEventSeqId = -1L)
    assertError(
      actual = ListKnownPartiesPageToken.validate(payload, participant1).left.value,
      expected = invalidPageToken("Invalid page token: invalid event sequence ID in the token"),
    )
  }

  it should "reject tokens issued before the ledger end was pinned" in {
    val payload = ListPartiesPageTokenPayload(partyIdLowerBoundExcl = party)
    assertError(
      actual = ListKnownPartiesPageToken.validate(payload, participant1).left.value,
      expected = invalidPageToken(
        "Invalid page token: the page token was issued by an incompatible Canton version (token version 0)"
      ),
    )
  }

  it should "reject a token with an invalid party" in {
    val payload = validPayload.copy(partyIdLowerBoundExcl = "invalid@party!")
    assertError(
      actual = ListKnownPartiesPageToken.validate(payload, participant1).left.value,
      expected = invalidPageToken("Invalid page token: invalid party in the token"),
    )
  }

  behavior of "ListKnownPartiesPageToken.encode"

  it should "encode the party lower bound and the pinned event sequence ID" in {
    val token = ListKnownPartiesPageToken.encode(party, eventSeqId, participant1)
    val decoded = ListKnownPartiesPageToken.decode(token).value.value
    val validated = ListKnownPartiesPageToken.validate(decoded, participant1).value
    validated shouldBe ListKnownPartiesPageToken(party, eventSeqId)
  }
}
