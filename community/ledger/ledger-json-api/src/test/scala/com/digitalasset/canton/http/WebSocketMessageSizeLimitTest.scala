// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http

import com.daml.testing.utils.PekkoBeforeAndAfterAll
import com.digitalasset.canton.http.WebSocketMessageSizeLimit.{
  MessageBinaryNotSupportedException,
  MessageTooLargeException,
}
import org.apache.pekko.http.scaladsl.model.ws.{BinaryMessage, Message, TextMessage}
import org.apache.pekko.stream.scaladsl.{Sink, Source}
import org.apache.pekko.util.ByteString
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import java.util.concurrent.atomic.AtomicLong
import scala.concurrent.Future

class WebSocketMessageSizeLimitTest
    extends AsyncWordSpec
    with PekkoBeforeAndAfterAll
    with Matchers {

  private val maxBytes = 1024L

  private def run(messages: Message*): Future[Seq[Message]] =
    Source(messages.toList)
      .via(WebSocketMessageSizeLimit.limitFlow(maxBytes))
      .runWith(Sink.seq)

  private def expectTooLarge(messages: Message*) =
    recoverToExceptionIf[MessageTooLargeException](run(messages*)).map(
      _.maxMessageSizeBytes shouldBe maxBytes
    )

  "limitFlow" should {
    "assemble messages within the limit into strict messages" in {
      val text = "a" * (maxBytes.toInt - 2) + "é" // é is 2 bytes in UTF-8
      run(
        TextMessage.Strict(text),
        TextMessage.Streamed(Source(text.grouped(100).toList)),
      ).map(_ shouldBe Seq[Message](TextMessage.Strict(text), TextMessage.Strict(text)))
    }

    "reject a strict text message above the limit, counting UTF-8 bytes" in {
      expectTooLarge(TextMessage.Strict("a" * (maxBytes.toInt - 1) + "é"))
    }

    "reject a fragmented text message above the limit" in {
      expectTooLarge(TextMessage.Streamed(Source(("a" * (maxBytes.toInt + 1)).grouped(10).toList)))
    }

    "reject binary messages, even small ones, without consuming them" in {
      val pulledChunks = new AtomicLong(0L)
      val streamed = BinaryMessage.Streamed(
        Source.repeat(ByteString(1.toByte)).map { b =>
          pulledChunks.incrementAndGet()
          b
        }
      )
      for {
        _ <- recoverToSucceededIf[MessageBinaryNotSupportedException](
          run(BinaryMessage.Strict(ByteString(1.toByte)))
        )
        _ <- recoverToSucceededIf[MessageBinaryNotSupportedException](run(streamed))
      } yield pulledChunks.get() shouldBe 0L
    }

    "stop consuming an endless fragmented message once the limit is exceeded" in {
      val pulledBytes = new AtomicLong(0L)
      val chunk = "x" * 100
      val endless = Source.repeat(chunk).map { c =>
        pulledBytes.addAndGet(c.length.toLong)
        c
      }
      expectTooLarge(TextMessage.Streamed(endless)).map { _ =>
        pulledBytes.get() should be <= maxBytes + 2 * chunk.length
      }
    }

    "count the UTF-8 encoded length" in {
      WebSocketMessageSizeLimit.utf8Length("aé€😀") shouldBe (1L + 2 + 3 + 4)
      // an unpaired surrogate does not throw
      WebSocketMessageSizeLimit.utf8Length("\ud83d") shouldBe 2L
    }
  }
}
