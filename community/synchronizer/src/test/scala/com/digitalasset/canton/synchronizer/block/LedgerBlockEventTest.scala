// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.block

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.ProtoDeserializationError.MaxBytesToDecompressExceeded
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.crypto.Signature
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.sequencing.protocol.ProtocolObjectTestUtils.{
  assertEnvelopeType,
  normalizeSubmissionRequest,
}
import com.digitalasset.canton.sequencing.protocol.{
  Batch,
  ClosedEnvelope,
  ClosedUncompressedEnvelope,
  Envelope,
  MessageId,
  Recipients,
  SignedContent,
  SubmissionRequest,
}
import com.digitalasset.canton.synchronizer.block.LedgerBlockEvent.Send
import com.digitalasset.canton.synchronizer.block.RawLedgerBlock.RawBlockEvent
import com.digitalasset.canton.synchronizer.sequencer.Sequencer.SignedSubmissionRequest
import com.digitalasset.canton.topology.{DefaultTestIdentities, ParticipantId, SequencerId}
import com.digitalasset.canton.util.MaxBytesToDecompress
import com.google.protobuf.ByteString
import org.scalatest.wordspec.AnyWordSpec

class LedgerBlockEventTest extends AnyWordSpec with BaseTest {
  val member: ParticipantId = ParticipantId("alice")
  val sequencer: SequencerId = DefaultTestIdentities.sequencerId
  private val submissionRequest: SubmissionRequest = SubmissionRequest.tryCreate(
    member,
    MessageId.randomMessageId(),
    Batch[ClosedEnvelope](
      List(
        ClosedUncompressedEnvelope
          .create(ByteString.EMPTY, Recipients.cc(member), Seq.empty, testedProtocolVersion)
      ),
      testedProtocolVersion,
    ),
    maxSequencingTime = CantonTimestamp.MaxValue,
    topologyTimestamp = None,
    aggregationRule = None,
    submissionCost = None,
    testedProtocolVersion,
  )
  private val signedSubmissionRequest =
    SignedContent(
      submissionRequest,
      Signature.noSignature,
      Some(CantonTimestamp.Epoch),
      testedProtocolVersion,
    )

  "helper methods" should {
    val byteString = signedSubmissionRequest.toByteString

    "deserialize submission request under the default max size limit" in {
      LedgerBlockEvent
        .deserializeSignedSubmissionRequest(
          testedProtocolVersion,
          defaultMaxBytesToDecompress,
        )(
          byteString
        )
        .map(normalizeSignedSubmissionRequest) shouldBe Right(
        normalizeSignedSubmissionRequest(signedSubmissionRequest)
      )
    }

    "not deserialize submission request over the max size limit" in {
      LedgerBlockEvent.deserializeSignedSubmissionRequest(
        testedProtocolVersion,
        MaxBytesToDecompress(NonNegativeInt.zero),
      )(
        byteString
      ) shouldBe Left(
        MaxBytesToDecompressExceeded("Max bytes to decompress is exceeded. The limit is 0 bytes.")
      )
    }

    "deserialize send" in {
      inside(
        LedgerBlockEvent
          .fromRawBlockEvent(
            testedProtocolVersion,
            defaultMaxBytesToDecompress,
          )(
            RawBlockEvent.Send(byteString, 0, sequencer.toProtoPrimitive)
          )
      ) { case Right(send: Send) =>
        send.signedSubmissionRequest.content.batch.envelopes.headOption.foreach { envelope =>
          assertEnvelopeType(envelope, testedProtocolVersion)
        }

        normalizeSend(send) shouldBe LedgerBlockEvent.Send(
          CantonTimestamp.Epoch,
          signedSubmissionRequest,
          sequencer,
          byteString.size(),
        )
      }
    }

    def normalizeSignedSubmissionRequest[T <: Envelope[?]](
        signedSubmissionRequest: SignedSubmissionRequest
    ): SignedContent[SubmissionRequest] =
      signedSubmissionRequest.copy(
        content = normalizeSubmissionRequest(signedSubmissionRequest.content)
      )

    def normalizeSend(send: Send): Send =
      send.copy(
        signedSubmissionRequest = send.signedSubmissionRequest.copy(
          content = normalizeSubmissionRequest(send.signedSubmissionRequest.content)
        )
      )

  }
}
