// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.handlers

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicCrypto
import com.digitalasset.canton.sequencing.protocol.SignedContent
import com.digitalasset.canton.sequencing.{SequencerTestUtils, SerializedEventHandler}
import com.digitalasset.canton.serialization.ProtocolVersionedMemoizedEvidence
import com.digitalasset.canton.store.SequencedEventStore.OrdinarySequencedEvent
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.Future

case class HandlerError(message: String)

class CounterCaptureTest extends AnyWordSpec with BaseTest {
  type TestEventHandler = SerializedEventHandler[HandlerError]

  "CounterCapture" should {
    "return initial value if we've not successfully processed an event" in {
      val counterCapture = new CounterCapture(1L, loggerFactory)

      counterCapture.counter shouldBe 1L
    }

    "update the counter when we successfully process an event" in {
      val counterCapture = new CounterCapture(1L, loggerFactory)
      val handler: TestEventHandler = _ => Future.successful(Right(()))
      val capturingHandler = counterCapture(handler)

      val fut = capturingHandler(
        OrdinarySequencedEvent(sign(SequencerTestUtils.mockDeliver(counter = 42L)))(traceContext)
      )

      counterCapture.counter shouldBe 42L
      fut.futureValue shouldBe Right(())
    }

    "not update the counter when the handler fails" in {
      val counterCapture = new CounterCapture(1L, loggerFactory)
      val ex = new RuntimeException
      val handler: TestEventHandler = _ => Future.failed(ex)
      val capturingHandler = counterCapture(handler)

      val fut = capturingHandler(
        OrdinarySequencedEvent(sign(SequencerTestUtils.mockDeliver(counter = 42L)))(traceContext)
      )

      counterCapture.counter shouldBe 1L
      fut.failed.futureValue shouldBe ex
    }
  }

  private def sign[A <: ProtocolVersionedMemoizedEvidence](content: A): SignedContent[A] =
    SignedContent(content, SymbolicCrypto.emptySignature, None)
}
