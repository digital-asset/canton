// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.metrics

import com.daml.metrics.api.MetricHandle.{Histogram, Meter}
import com.daml.metrics.api.MetricsContext
import com.google.common.base.Utf8
import org.apache.pekko.http.scaladsl.model.ws.{BinaryMessage, Message, TextMessage}
import org.apache.pekko.stream.scaladsl.Flow

// Support to capture metrics on websockets on pekko http
object WebSocketMetricsInterceptor {

  // Wraps the given flow, to capture in the given metrics, the following signals:
  //  - total number of received messages
  //  - size of the received messages
  //  - total number of sent messages
  //  - size of the sent messages
  def withRateSizeMetrics[M](
      metrics: WebSocketMetrics,
      flow: Flow[Message, Message, M],
  )(implicit mc: MetricsContext): Flow[Message, Message, M] =
    Flow[Message]
      .map(
        messageCountAndSizeReportMetric(
          _,
          metrics.messagesReceivedTotal,
          metrics.messagesReceivedBytes,
        )
      )
      .viaMat(flow)((_, mat2) => mat2)
      .map(
        messageCountAndSizeReportMetric(
          _,
          metrics.messagesSentTotal,
          metrics.messagesSentBytes,
        )
      )

  // support for message counting, and computation and report of the size of a message
  // for streaming content, creates a copy of the message, whose stream records its total size
  private def messageCountAndSizeReportMetric(
      message: Message,
      totalMetric: Meter,
      bytesMetric: Histogram,
  )(implicit mc: MetricsContext): Message = {
    totalMetric.mark()
    message match {
      case m: BinaryMessage.Strict =>
        bytesMetric.update(m.data.length.toLong)
        m
      case m: BinaryMessage.Streamed =>
        BinaryMessage.Streamed(
          StreamSizeMetric.recordTotalSize(m.dataStream, bytesMetric)(_.length.toLong)
        )
      case m: TextMessage.Strict =>
        bytesMetric.update(Utf8.encodedLength(m.text).toLong)
        m
      case m: TextMessage.Streamed =>
        TextMessage.Streamed(
          StreamSizeMetric.recordTotalSize(m.textStream, bytesMetric)(
            Utf8.encodedLength(_).toLong
          )
        )
    }
  }

}
