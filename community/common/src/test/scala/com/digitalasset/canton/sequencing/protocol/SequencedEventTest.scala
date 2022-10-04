// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import com.digitalasset.canton.crypto.CryptoPureApi
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicCrypto
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.protocol.messages._
import com.digitalasset.canton.protocol.{RequestId, v0}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.DefaultTestIdentities
import com.digitalasset.canton.topology.DefaultTestIdentities.domainId
import com.digitalasset.canton.version.UntypedVersionedMessage
import com.digitalasset.canton.{BaseTestWordSpec, SequencerCounter}

class SequencedEventTest extends BaseTestWordSpec {
  "serialization" should {

    "correctly serialize and deserialize a deliver event" in {
      // there's no significance to this choice of message beyond it being easy to construct
      val message =
        SignedProtocolMessage(
          TransferResult
            .create(
              RequestId(CantonTimestamp.now()),
              Set.empty,
              TransferInDomainId(domainId),
              Verdict.Approve(testedProtocolVersion),
              testedProtocolVersion,
            ),
          SymbolicCrypto.emptySignature,
          testedProtocolVersion,
        )
      val batch = Batch.of(
        testedProtocolVersion,
        (message, Recipients.cc(DefaultTestIdentities.participant1)),
      )
      val deliver: Deliver[DefaultOpenEnvelope] =
        Deliver.create[DefaultOpenEnvelope](
          SequencerCounter(42),
          CantonTimestamp.now(),
          domainId,
          Some(MessageId.tryCreate("some-message-id")),
          batch,
          testedProtocolVersion,
        )
      val deliverEventPV0 = deliver.toProtoV0
      val deliverEventP = deliver.toProtoVersioned
      val deserializedEventV0 = deserializeV0(deliverEventPV0)
      val deserializedEvent = deserializeVersioned(deliverEventP)

      deserializedEventV0.value shouldBe deliver
      deserializedEvent.value shouldBe deliver
    }

    "correctly serialize and deserialize a deliver error" in {
      val deliverError: DeliverError = DeliverError.create(
        SequencerCounter(42),
        CantonTimestamp.now(),
        domainId,
        MessageId.tryCreate("some-message-id"),
        DeliverErrorReason.BatchRefused("no batches here please"),
        testedProtocolVersion,
      )
      val deliverErrorPV0 = deliverError.toProtoV0
      val deserializedEventV0 = deserializeV0(deliverErrorPV0)
      val deliverErrorP = deliverError.toProtoVersioned
      val deserializedEvent = deserializeVersioned(deliverErrorP)

      deserializedEvent.value shouldBe deliverError
      deserializedEventV0.value shouldBe deliverError
    }

    def deserializeV0(
        eventP: v0.SequencedEvent
    ): ParsingResult[SequencedEvent[DefaultOpenEnvelope]] = {
      val cryptoPureApi = mock[CryptoPureApi]
      val bytes = eventP.toByteString
      SequencedEvent.fromProtoWithV0(
        OpenEnvelope.fromProtoV0(
          EnvelopeContent.messageFromByteString(testedProtocolVersion, cryptoPureApi),
          testedProtocolVersion,
        )
      )(eventP, bytes)
    }

    def deserializeVersioned(
        eventP: UntypedVersionedMessage
    ): ParsingResult[SequencedEvent[DefaultOpenEnvelope]] = {
      val cryptoPureApi = mock[CryptoPureApi]
      val bytes = eventP.toByteString
      SequencedEvent.fromProtoWith(
        OpenEnvelope.fromProtoV0(
          EnvelopeContent.messageFromByteString(testedProtocolVersion, cryptoPureApi),
          testedProtocolVersion,
        )
      )(eventP, bytes)
    }
  }
}
