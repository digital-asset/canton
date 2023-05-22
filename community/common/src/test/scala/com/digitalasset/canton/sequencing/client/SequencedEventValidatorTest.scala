// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.sequencing.client.SequencedEventValidationError.*
import com.digitalasset.canton.sequencing.protocol.{ClosedEnvelope, SequencedEvent}
import com.digitalasset.canton.store.SequencedEventStore.IgnoredSequencedEvent
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{BaseTest, HasExecutionContext, SequencerCounter}
import com.google.protobuf.ByteString
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.wordspec.FixtureAnyWordSpec
import org.scalatest.{Assertion, Outcome}

class SequencedEventValidatorTest
    extends FixtureAnyWordSpec
    with BaseTest
    with ScalaFutures
    with HasExecutionContext {

  override type FixtureParam = SequencedEventTestFixture

  override def withFixture(test: OneArgTest): Outcome = {
    val env = new SequencedEventTestFixture(loggerFactory, testedProtocolVersion, timeouts)
    withFixture(test.toNoArgTest(env))
  }
  private implicit val errorLoggingContext: ErrorLoggingContext =
    ErrorLoggingContext(logger, Map(), TraceContext.empty)

  "validate on reconnect" should {
    "accept the prior event" in { fixture =>
      import fixture.*
      val priorEvent = createEvent().futureValue
      val validator = mkValidator(priorEvent)
      validator
        .validateOnReconnect(priorEvent)
        .valueOrFail("successful reconnect")
        .failOnShutdown
        .futureValue
    }

    "accept a new signature on the prior event" in { fixture =>
      import fixture.*
      val priorEvent = createEvent().futureValue
      val validator = mkValidator(priorEvent)
      val sig = sign(
        priorEvent.signedEvent.content.getCryptographicEvidence,
        CantonTimestamp.Epoch,
      ).futureValue
      assert(sig != priorEvent.signedEvent.signature)
      val eventWithNewSig =
        priorEvent.copy(priorEvent.signedEvent.copy(signatures = NonEmpty(Seq, sig)))(
          fixture.traceContext
        )
      validator
        .validateOnReconnect(eventWithNewSig)
        .valueOrFail("event with regenerated signature")
        .failOnShutdown
        .futureValue
    }

    "accept a different serialization of the same content" in { fixture =>
      import fixture.*
      val deliver1 = createEventWithCounterAndTs(1L, CantonTimestamp.Epoch).futureValue
      val deliver2 = createEventWithCounterAndTs(
        1L,
        CantonTimestamp.Epoch,
        customSerialization = Some(ByteString.copyFromUtf8("Different serialization")),
      ).futureValue // changing serialization, but not the contents

      val validator = mkValidator(deliver1)
      validator
        .validateOnReconnect(deliver2)
        .valueOrFail("Different serialization should be accepted")
        .failOnShutdown
        .futureValue
    }

    "check the domain Id" in { fixture =>
      import fixture.*
      val incorrectDomainId = DomainId(UniqueIdentifier.tryFromProtoPrimitive("wrong-domain::id"))
      val validator = mkValidator(
        IgnoredSequencedEvent(CantonTimestamp.MinValue, SequencerCounter(updatedCounter), None)(
          fixture.traceContext
        )
      )
      val wrongDomain = createEvent(incorrectDomainId).futureValue
      val err = validator
        .validateOnReconnect(wrongDomain)
        .leftOrFail("wrong domain ID on reconnect")
        .failOnShutdown
        .futureValue

      err shouldBe BadDomainId(defaultDomainId, incorrectDomainId)
    }

    "check for a fork" in { fixture =>
      import fixture.*

      def expectLog(
          cmd: => FutureUnlessShutdown[SequencedEventValidationError]
      ): SequencedEventValidationError = {
        loggerFactory
          .assertLogs(cmd, _.shouldBeCantonErrorCode(ResilientSequencerSubscription.ForkHappened))
          .failOnShutdown
          .futureValue
      }

      val priorEvent = createEvent().futureValue
      val validator = mkValidator(priorEvent)
      val differentCounter = createEvent(counter = 43L).futureValue

      val errCounter = expectLog(
        validator
          .validateOnReconnect(differentCounter)
          .leftOrFail("fork on counter")
      )
      val differentTimestamp = createEvent(timestamp = CantonTimestamp.MaxValue).futureValue
      val errTimestamp = expectLog(
        validator
          .validateOnReconnect(differentTimestamp)
          .leftOrFail("fork on timestamp")
      )

      val differentContent = createEventWithCounterAndTs(
        counter = updatedCounter,
        CantonTimestamp.Epoch,
      ).futureValue

      val errContent = expectLog(
        validator
          .validateOnReconnect(differentContent)
          .leftOrFail("fork on content")
      )

      def assertFork(err: SequencedEventValidationError)(
          counter: SequencerCounter,
          suppliedEvent: SequencedEvent[ClosedEnvelope],
          expectedEvent: Option[SequencedEvent[ClosedEnvelope]],
      ): Assertion = {
        err match {
          case ForkHappened(counterRes, suppliedEventRes, expectedEventRes) =>
            (
              counter,
              suppliedEvent,
              expectedEvent,
            ) shouldBe (counterRes, suppliedEventRes, expectedEventRes)
          case x => fail(s"${x} is not ForkHappened")
        }
      }

      assertFork(errCounter)(
        SequencerCounter(updatedCounter),
        differentCounter.signedEvent.content,
        Some(priorEvent.signedEvent.content),
      )

      assertFork(errTimestamp)(
        SequencerCounter(updatedCounter),
        differentTimestamp.signedEvent.content,
        Some(priorEvent.signedEvent.content),
      )

      assertFork(errContent)(
        SequencerCounter(updatedCounter),
        differentContent.signedEvent.content,
        Some(priorEvent.signedEvent.content),
      )
    }

    "verify the signature" in { fixture =>
      import fixture.*
      val priorEvent = createEvent().futureValue
      val badSig =
        sign(ByteString.copyFromUtf8("not-the-message"), CantonTimestamp.Epoch).futureValue
      val badEvent = createEvent(signatureOverride = Some(badSig)).futureValue
      val validator = mkValidator(priorEvent)
      val result = validator
        .validateOnReconnect(badEvent)
        .leftOrFail("invalid signature on reconnect")
        .failOnShutdown
        .futureValue
      result shouldBe a[SignatureInvalid]
    }
  }

  "validate" should {
    "reject messages with unexpected domain ids" in { fixture =>
      import fixture.*
      val incorrectDomainId = DomainId(UniqueIdentifier.tryFromProtoPrimitive("wrong-domain::id"))
      val event = createEvent(incorrectDomainId).futureValue
      val validator = mkValidator()
      val result = validator
        .validate(event)
        .leftOrFail("wrong domain ID")
        .failOnShutdown
        .futureValue
      result shouldBe BadDomainId(`defaultDomainId`, `incorrectDomainId`)
    }

    "reject messages with invalid signatures" in { fixture =>
      import fixture.*
      val priorEvent =
        createEvent(timestamp = CantonTimestamp.Epoch.immediatePredecessor).futureValue
      val badSig =
        sign(ByteString.copyFromUtf8("not-the-message"), CantonTimestamp.Epoch).futureValue
      val badEvent = createEvent(
        signatureOverride = Some(badSig),
        counter = priorEvent.counter.v + 1L,
      ).futureValue
      val validator = mkValidator(priorEvent)
      val result = validator
        .validate(badEvent)
        .leftOrFail("invalid signature")
        .failOnShutdown
        .futureValue
      result shouldBe a[SignatureInvalid]
    }

    "validate correctly with explicit signing timestamp" in { fixture =>
      import fixture.*
      val syncCrypto = mock[DomainSyncCryptoClient]
      when(syncCrypto.pureCrypto).thenReturn(subscriberCryptoApi.pureCrypto)
      when(syncCrypto.snapshotUS(timestamp = ts(1))(fixture.traceContext))
        .thenAnswer[CantonTimestamp](tm => subscriberCryptoApi.snapshotUS(tm)(fixture.traceContext))
      when(syncCrypto.topologyKnownUntilTimestamp).thenReturn(CantonTimestamp.MaxValue)
      val validator = mkValidator(
        IgnoredSequencedEvent(ts(0), SequencerCounter(41), None)(fixture.traceContext),
        syncCryptoApi = syncCrypto,
      )
      val deliver =
        createEventWithCounterAndTs(42, ts(2), timestampOfSigningKey = Some(ts(1))).futureValue

      valueOrFail(validator.validate(deliver))("validate").failOnShutdown.futureValue
    }

    "reject the same counter-timestamp if passed in repeatedly" in { fixture =>
      import fixture.*
      val validator =
        mkValidator(
          IgnoredSequencedEvent(CantonTimestamp.MinValue, SequencerCounter(41), None)(
            fixture.traceContext
          )
        )

      val deliver = createEventWithCounterAndTs(42, CantonTimestamp.Epoch).futureValue
      validator
        .validate(deliver)
        .valueOrFail("validate1")
        .failOnShutdown
        .futureValue
      val err = validator
        .validate(deliver)
        .leftOrFail("validate2")
        .failOnShutdown
        .futureValue

      err shouldBe GapInSequencerCounter(SequencerCounter(42), SequencerCounter(42))
    }

    "fail if the counter or timestamp do not increase" in { fixture =>
      import fixture.*
      val validator =
        mkValidator(
          IgnoredSequencedEvent(CantonTimestamp.Epoch, SequencerCounter(41), None)(
            fixture.traceContext
          )
        )

      val deliver1 = createEventWithCounterAndTs(42, CantonTimestamp.MinValue).futureValue
      val deliver2 = createEventWithCounterAndTs(0L, CantonTimestamp.MaxValue).futureValue
      val deliver3 = createEventWithCounterAndTs(42L, CantonTimestamp.ofEpochSecond(2)).futureValue

      val error1 = validator
        .validate(deliver1)
        .leftOrFail("deliver1")
        .failOnShutdown
        .futureValue
      val error2 = validator
        .validate(deliver2)
        .leftOrFail("deliver2")
        .failOnShutdown
        .futureValue
      validator
        .validate(deliver3)
        .valueOrFail("deliver3")
        .failOnShutdown
        .futureValue
      val error3 = validator
        .validate(deliver2)
        .leftOrFail("deliver4")
        .failOnShutdown
        .futureValue

      error1 shouldBe NonIncreasingTimestamp(
        CantonTimestamp.MinValue,
        SequencerCounter(42),
        CantonTimestamp.Epoch,
        SequencerCounter(41),
      )
      error2 shouldBe DecreasingSequencerCounter(SequencerCounter(0), SequencerCounter(41))
      error3 shouldBe DecreasingSequencerCounter(SequencerCounter(0), SequencerCounter(42))
    }

    "fail if there is a counter cap" in { fixture =>
      import fixture.*
      val validator =
        mkValidator(
          IgnoredSequencedEvent(CantonTimestamp.Epoch, SequencerCounter(41), None)(
            fixture.traceContext
          )
        )

      val deliver1 = createEventWithCounterAndTs(43L, CantonTimestamp.ofEpochSecond(1)).futureValue
      val deliver2 = createEventWithCounterAndTs(42L, CantonTimestamp.ofEpochSecond(2)).futureValue
      val deliver3 = createEventWithCounterAndTs(44L, CantonTimestamp.ofEpochSecond(3)).futureValue

      val result1 = validator
        .validate(deliver1)
        .leftOrFail("deliver1")
        .failOnShutdown
        .futureValue

      validator
        .validate(deliver2)
        .valueOrFail("deliver2")
        .failOnShutdown
        .futureValue

      val result3 = validator
        .validate(deliver3)
        .leftOrFail("deliver3")
        .failOnShutdown
        .futureValue

      result1 shouldBe GapInSequencerCounter(SequencerCounter(43), SequencerCounter(41))
      result3 shouldBe GapInSequencerCounter(SequencerCounter(44), SequencerCounter(42))
    }
  }
}
