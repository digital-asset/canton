// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer

import cats.data.NonEmptySet
import cats.syntax.functorFilter._
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.sequencing.sequencer.store._
import com.digitalasset.canton.sequencing.protocol.{Batch, MessageId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion

object DomainSequencingTestUtils {

  def mockDeliverStoreEvent(
      sender: SequencerMemberId = SequencerMemberId(0),
      payloadId: PayloadId = PayloadId(CantonTimestamp.Epoch),
      traceContext: TraceContext = TraceContext.empty,
  )(
      recipients: NonEmptySet[SequencerMemberId] = NonEmptySet.of(sender)
  ): DeliverStoreEvent[PayloadId] = {
    val messageId = MessageId.tryCreate("mock-deliver")
    val signingTs = None
    DeliverStoreEvent(sender, messageId, recipients, payloadId, signingTs, traceContext)
  }

  def payloadsForEvents(events: Seq[Sequenced[PayloadId]]): List[Payload] = {
    val payloadIds = events.mapFilter { s =>
      s.event match {
        case DeliverStoreEvent(
              _sender,
              _messageId,
              _members,
              payload,
              _signingTimestampO,
              _traceContext,
            ) =>
          Some(payload)
        case DeliverErrorStoreEvent(_sender, _messageId, _message, _traceContext) => None
      }
    }
    payloadIds
      .map(pid => Payload(pid, Batch.empty.toByteString(ProtocolVersion.latestForTest)))
      .toList
  }

}
