// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing

import com.digitalasset.canton.crypto.provider.symbolic.SymbolicCrypto
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.sequencing.protocol.{Batch, Deliver, MessageId, SignedContent}
import com.digitalasset.canton.serialization.ProtocolVersionedMemoizedEvidence
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.{DefaultTestIdentities, DomainId}
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.version.ProtocolVersion
import com.google.protobuf.ByteString

object SequencerTestUtils extends BaseTest {

  object MockMessageContent {
    private val bytes = ByteString.copyFromUtf8("serialized-mock-message")
    def toByteString: ByteString = bytes
    def fromByteString(
        bytes: ByteString
    ): ParsingResult[MockMessageContent.type] =
      Right(MockMessageContent)
  }

  def sign[M <: ProtocolVersionedMemoizedEvidence](content: M): SignedContent[M] =
    SignedContent(content, SymbolicCrypto.emptySignature, None)

  def mockDeliver(
      counter: Long = 0L,
      timestamp: CantonTimestamp = CantonTimestamp.Epoch,
      domainId: DomainId = DefaultTestIdentities.domainId,
      deserializedFrom: Option[ByteString] = None,
      messageId: Option[MessageId] = Some(MessageId.tryCreate("mock-deliver")),
  ): Deliver[Nothing] = {
    val batch = Batch(List.empty)
    new Deliver[Nothing](counter, timestamp, domainId, messageId, batch)(
      ProtocolVersion.latestForTest,
      deserializedFrom,
    )
  }

}
