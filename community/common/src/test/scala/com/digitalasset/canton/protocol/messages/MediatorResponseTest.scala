// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import cats.syntax.either._
import com.digitalasset.canton.crypto.TestHash
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicPureCrypto
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.topology.{DomainId, UniqueIdentifier}
import com.digitalasset.canton.protocol.{RequestId, RootHash, ViewHash}
import com.digitalasset.canton.serialization.HasCryptographicEvidenceTest
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{BaseTest, LfPartyId, topology}
import com.google.protobuf.ByteString
import org.scalatest.wordspec.AnyWordSpec

class MediatorResponseTest extends AnyWordSpec with BaseTest with HasCryptographicEvidenceTest {
  val cryptoApi = new SymbolicPureCrypto
  val response1: MediatorResponse = MediatorResponse.tryCreate(
    RequestId(CantonTimestamp.now()),
    topology.ParticipantId(UniqueIdentifier.tryFromProtoPrimitive("da::p1")),
    Some(ViewHash(TestHash.digest("cr1"))),
    LocalApprove,
    Some(RootHash(TestHash.digest("txid1"))),
    Set(LfPartyId.assertFromString("p1"), LfPartyId.assertFromString("p2")),
    DomainId(UniqueIdentifier.tryFromProtoPrimitive("da::default")),
    ProtocolVersion.latestForTest,
  )
  val response2: MediatorResponse = MediatorResponse.tryCreate(
    RequestId(CantonTimestamp.now()),
    topology.ParticipantId(UniqueIdentifier.tryFromProtoPrimitive("da::p1")),
    None,
    LocalReject.MalformedRejects.Payloads.Reject("test message"),
    Some(RootHash(TestHash.digest("txid3"))),
    Set.empty,
    DomainId(UniqueIdentifier.tryFromProtoPrimitive("da::default")),
    ProtocolVersion.latestForTest,
  )

  def fromByteString(bytes: ByteString): MediatorResponse = {
    MediatorResponse.fromByteString(bytes).valueOr(err => fail(err.toString))
  }

  "ConfirmationResponse" should {
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
}
