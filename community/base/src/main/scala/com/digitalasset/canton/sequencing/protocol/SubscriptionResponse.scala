// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import cats.syntax.traverse.*
import com.digitalasset.canton.domain.api.v0
import com.digitalasset.canton.serialization.ProtoConverter.{ParsingResult, required}
import com.digitalasset.canton.tracing.{SerializableTraceContext, TraceContext, Traced}

final case class SubscriptionResponse(
    signedSequencedEvent: Traced[SignedContent[SequencedEvent[ClosedEnvelope]]],
    trafficState: Option[SequencedEventTrafficState],
) {

  // We don't introduce a `VersionedSubscriptionResponse` because we assume that the subscription endpoint will also be
  // bumped if a V1 SubscriptionResponse is ever introduced
  def toProtoV0: v0.SubscriptionResponse =
    v0.SubscriptionResponse(
      signedSequencedEvent = Some(signedSequencedEvent.value.toProtoV0),
      traceContext = Some(SerializableTraceContext(signedSequencedEvent.traceContext).toProtoV0),
      trafficState = trafficState.map(_.toProtoV0),
    )
}

object SubscriptionResponse {

  /** Deserializes the SubscriptionResponse however will ignore the traceContext field and instead use the supplied value.
    * This is because we deserialize the traceContext separately from this request immediately on receiving the structure.
    */
  def fromProtoV0(responseP: v0.SubscriptionResponse)(implicit
      traceContext: TraceContext
  ): ParsingResult[SubscriptionResponse] = {
    val v0.SubscriptionResponse(
      maybeSignedSequencedEventP,
      _ignoredTraceContext,
      trafficStateP,
    ) = responseP
    for {
      signedSequencedEventP <- required(
        "SubscriptionResponse.signedSequencedEvent",
        maybeSignedSequencedEventP,
      )
      signedContent <- SignedContent.fromProtoV0(signedSequencedEventP)
      signedSequencedEvent <- signedContent.deserializeContent(SequencedEvent.fromByteString)
      trafficState <- trafficStateP.traverse(SequencedEventTrafficState.fromProtoV0)
    } yield SubscriptionResponse(Traced(signedSequencedEvent)(traceContext), trafficState)
  }
}
