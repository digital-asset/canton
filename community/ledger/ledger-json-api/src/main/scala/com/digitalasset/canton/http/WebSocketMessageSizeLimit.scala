// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http

import org.apache.pekko.NotUsed
import org.apache.pekko.http.scaladsl.model.ws.{
  BinaryMessage,
  Message,
  TextMessage,
  WebSocketUpgrade,
}
import org.apache.pekko.http.scaladsl.model.{AttributeKeys, HttpResponse}
import org.apache.pekko.http.scaladsl.server.Directives.*
import org.apache.pekko.http.scaladsl.server.Route
import org.apache.pekko.stream.scaladsl.{Flow, Sink}
import org.apache.pekko.stream.{FlowShape, Graph, Materializer}

import scala.annotation.tailrec
import scala.concurrent.{ExecutionContext, Future}

/** Bounds the size of every single inbound websocket message.
  *
  * Websocket handlers (tapir) assemble each inbound message, which may be split into an unbounded
  * number of fragments, into a single in-memory string before decoding it. HTTP request bodies are
  * bounded by `max-inbound-message-size`, but websocket messages are not subject to that limit.
  *
  * This wrapper intercepts the websocket upgrade of the request and places a stage in front of the
  * handler flow that accumulates each message only up to `maxMessageSizeBytes` (UTF-8 encoded bytes
  * for text messages). A message exceeding the bound fails the inbound stream with
  * [[WebSocketMessageSizeLimit.MessageTooLargeException]] before it is fully assembled. The JSON
  * Ledger API websocket endpoints report it to the client as `JSON_API_WEBSOCKET_MESSAGE_TOO_LARGE`
  * and close the connection.
  *
  * Binary messages are not supported by any JSON Ledger API websocket endpoint, so they are
  * rejected up front with [[WebSocketMessageSizeLimit.MessageBinaryNotSupportedException]] without
  * being read. Endpoints accepting binary messages would need to bound them here instead.
  */
object WebSocketMessageSizeLimit {

  final class MessageTooLargeException(val maxMessageSizeBytes: Long)
      extends RuntimeException(
        s"Inbound websocket message exceeds the maximum size of $maxMessageSizeBytes bytes"
      )

  final class MessageBinaryNotSupportedException
      extends RuntimeException(
        "Inbound websocket message is in a binary format, which is not supported"
      )

  def apply(maxMessageSizeBytes: Long)(route: Route): Route =
    extractMaterializer { implicit mat =>
      mapRequest { request =>
        request.attribute(AttributeKeys.webSocketUpgrade) match {
          case Some(upgrade) =>
            request.addAttribute(
              AttributeKeys.webSocketUpgrade,
              new BoundedWebSocketUpgrade(upgrade, limitFlow(maxMessageSizeBytes)),
            )
          case None => request
        }
      }(route)
    }

  private final class BoundedWebSocketUpgrade(
      delegate: WebSocketUpgrade,
      limit: Flow[Message, Message, NotUsed],
  ) extends WebSocketUpgrade {
    override def requestedProtocols: Seq[String] = delegate.requestedProtocols

    override def handleMessages(
        handlerFlow: Graph[FlowShape[Message, Message], Any],
        subprotocol: Option[String],
    ): HttpResponse = delegate.handleMessages(limit.via(handlerFlow), subprotocol)
  }

  /** Replaces every inbound message by a strict one, failing as soon as a message exceeds
    * `maxBytes`. Fragments are only accumulated while within the bound.
    */
  private[http] def limitFlow(maxBytes: Long)(implicit
      mat: Materializer
  ): Flow[Message, Message, NotUsed] = {
    implicit val ec: ExecutionContext = mat.executionContext

    // Fails within the fragment stream, so that pekko's cancellation of the inbound message
    // stream also carries this exception.
    def bounded[A](weight: A => Long): Flow[A, A, NotUsed] =
      Flow[A].statefulMap(() => 0L)(
        (total, chunk) => {
          val newTotal = total + weight(chunk)
          if (newTotal > maxBytes) throw new MessageTooLargeException(maxBytes)
          (newTotal, chunk)
        },
        _ => None,
      )

    def checkStrict(size: Long, m: Message): Future[Message] =
      if (size > maxBytes) Future.failed(new MessageTooLargeException(maxBytes))
      else Future.successful(m)

    Flow[Message].mapAsync(1) {
      case m: TextMessage.Strict => checkStrict(utf8Length(m.text), m)
      case m: TextMessage.Streamed =>
        m.textStream
          .via(bounded(utf8Length))
          .runWith(Sink.fold(new java.lang.StringBuilder)(_.append(_)))
          .map(sb => TextMessage.Strict(sb.toString))
      // The JSON Ledger API websocket endpoints only accept text messages. Reject binary ones
      // without consuming their data.
      case _: BinaryMessage => Future.failed(new MessageBinaryNotSupportedException)
    }
  }

  /** Number of bytes of the UTF-8 encoding of `s`. Unlike guava's `Utf8.encodedLength`, this does
    * not throw on unpaired surrogates; each surrogate counts as 2 bytes (a pair encodes to 4).
    */
  private[http] def utf8Length(s: String): Long = {
    @tailrec def go(i: Int, acc: Long): Long =
      if (i >= s.length) acc
      else {
        val c = s.charAt(i)
        val bytes =
          if (c < 0x80) 1
          else if (c < 0x800 || Character.isSurrogate(c)) 2
          else 3
        go(i + 1, acc + bytes)
      }
    go(0, 0L)
  }
}
