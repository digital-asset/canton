// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import com.digitalasset.canton.sequencer.api.v30
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.{ProtocolVersion, ProtocolVersionValidation}

final case class SubscriptionResponse(
    signedSequencedEvent: SignedContent[SequencedEvent[CompressedBatch]],
    traceContext: TraceContext,
)

object SubscriptionResponse {

  /** The batch is intentionally left compressed here: decompression is deferred to the
    * [[com.digitalasset.canton.sequencing.client.SequencedEventValidator]], where the right
    * `maxRequestSize` bound (from the topology snapshot at the event's timestamp) is available.
    */
  def fromVersionedProtoV30(
      protocolVersion: ProtocolVersion
  )(responseP: v30.SubscriptionResponse)(implicit
      traceContext: TraceContext
  ): ParsingResult[SubscriptionResponse] = {
    val v30.SubscriptionResponse(
      signedSequencedEvent,
      _ignoredTraceContext,
    ) = responseP
    for {
      signedContent <- SignedContent.fromByteString(protocolVersion, signedSequencedEvent)
      signedSequencedEvent <- signedContent.deserializeContent(
        SequencedEvent.fromByteStringCompressed(
          ProtocolVersionValidation.PV(protocolVersion),
          _,
        )
      )
    } yield SubscriptionResponse(signedSequencedEvent, traceContext)

  }
}
