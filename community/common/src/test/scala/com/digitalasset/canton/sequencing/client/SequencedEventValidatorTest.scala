// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client

import cats.syntax.either._
import com.digitalasset.canton.crypto._
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.protocol.ExampleTransactionFactory
import com.digitalasset.canton.protocol.messages.{EnvelopeContent, InformeeMessage}
import com.digitalasset.canton.sequencing.client.SequencedEventValidationError._
import com.digitalasset.canton.sequencing.protocol._
import com.digitalasset.canton.sequencing.{
  OrdinarySerializedEvent,
  PossiblyIgnoredSerializedEvent,
  SequencerTestUtils,
}
import com.digitalasset.canton.store.SequencedEventStore.{
  IgnoredSequencedEvent,
  OrdinarySequencedEvent,
}
import com.digitalasset.canton.topology._
import com.digitalasset.canton.{BaseTest, HasExecutorService, SequencerCounter}
import com.google.protobuf.ByteString
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future

class SequencedEventValidatorTest extends AsyncWordSpec with BaseTest with HasExecutorService {

  private lazy val defaultDomainId: DomainId = DefaultTestIdentities.domainId
  private lazy val subscriberId: ParticipantId = ParticipantId("participant1-id")
  private lazy val sequencerId: SequencerId = DefaultTestIdentities.sequencer
  private lazy val subscriberCryptoApi: DomainSyncCryptoClient =
    TestingIdentityFactory(loggerFactory).forOwnerAndDomain(subscriberId, defaultDomainId)
  private lazy val sequencerCryptoApi: DomainSyncCryptoClient =
    TestingIdentityFactory(loggerFactory).forOwnerAndDomain(sequencerId, defaultDomainId)
  private lazy val updatedCounter: Long = 42L

  private def mkValidator(
      initialEventMetadata: PossiblyIgnoredSerializedEvent =
        IgnoredSequencedEvent(CantonTimestamp.MinValue, SequencerCounter(updatedCounter - 1), None)(
          traceContext
        ),
      syncCryptoApi: DomainSyncCryptoClient = subscriberCryptoApi,
  ): SequencedEventValidatorImpl = {
    new SequencedEventValidatorImpl(
      Some(initialEventMetadata),
      unauthenticated = false,
      optimistic = false,
      defaultDomainId,
      sequencerId,
      syncCryptoApi,
      loggerFactory,
    )(executorService)
  }

  private def createEvent(
      domainId: DomainId = defaultDomainId,
      signatureOverride: Option[Signature] = None,
      serializedOverride: Option[ByteString] = None,
      counter: Long = updatedCounter,
      timestamp: CantonTimestamp = CantonTimestamp.Epoch,
  ): Future[OrdinarySerializedEvent] = {
    import cats.syntax.option._
    val message = {
      val factory: ExampleTransactionFactory = new ExampleTransactionFactory()()(executorService)
      val fullInformeeTree = factory.MultipleRootsAndViewNestings.fullInformeeTree
      InformeeMessage(fullInformeeTree)(testedProtocolVersion)
    }
    val deliver: Deliver[ClosedEnvelope] = Deliver.create[ClosedEnvelope](
      SequencerCounter(counter),
      timestamp,
      domainId,
      MessageId.tryCreate("test").some,
      Batch(
        List(
          ClosedEnvelope(
            serializedOverride.getOrElse(
              EnvelopeContent(message, testedProtocolVersion).toByteString
            ),
            Recipients.cc(subscriberId),
          )
        ),
        testedProtocolVersion,
      ),
      testedProtocolVersion,
    )

    for {
      sig <- signatureOverride
        .map(Future.successful)
        .getOrElse(sign(deliver.getCryptographicEvidence, deliver.timestamp))
    } yield OrdinarySequencedEvent(SignedContent(deliver, sig, None))(traceContext)
  }

  private def createEventWithCounterAndTs(
      counter: Long,
      timestamp: CantonTimestamp,
      customSerialization: Option[ByteString] = None,
      messageIdO: Option[MessageId] = None,
      timestampOfSigningKey: Option[CantonTimestamp] = None,
  ): Future[OrdinarySerializedEvent] = {
    val event =
      SequencerTestUtils.mockDeliverClosedEnvelope(
        counter = counter,
        timestamp = timestamp,
        deserializedFrom = customSerialization,
        messageId = messageIdO,
      )
    for {
      signature <- sign(
        customSerialization.getOrElse(event.getCryptographicEvidence),
        event.timestamp,
      )
    } yield OrdinarySequencedEvent(SignedContent(event, signature, timestampOfSigningKey))(
      traceContext
    )
  }

  private def ts(offset: Int) = CantonTimestamp.Epoch.plusSeconds(offset.toLong)

  private def sign(bytes: ByteString, timestamp: CantonTimestamp): Future[Signature] = {
    val hash = sequencerCryptoApi.pureCrypto.digest(HashPurpose.SequencedEventSignature, bytes)
    for {
      cryptoApi <- sequencerCryptoApi.snapshot(timestamp)
      signature <- cryptoApi
        .sign(hash)
        .value
        .map(_.valueOr(err => fail(s"Failed to sign: $err")))(executorService)
    } yield signature
  }

  "validate on reconnect" should {
    "accept the prior event" in {
      for {
        priorEvent <- createEvent()
        validator = mkValidator(priorEvent)
        _ <- validator.validateOnReconnect(priorEvent).valueOrFail("successful reconnect")
      } yield succeed
    }

    "accept a new signature on the prior event" in {
      for {
        priorEvent <- createEvent()
        validator = mkValidator(priorEvent)
        sig <- sign(priorEvent.signedEvent.getCryptographicEvidence, CantonTimestamp.Epoch)
        _ = assert(sig != priorEvent.signedEvent.signature)
        eventWithNewSig =
          priorEvent.copy(priorEvent.signedEvent.copy(signature = sig))(traceContext)
        _ <- validator
          .validateOnReconnect(eventWithNewSig)
          .valueOrFail("event with regenerated signature")
      } yield succeed
    }

    "accept a different serialization of the same content" in {
      for {
        deliver1 <- createEventWithCounterAndTs(1L, CantonTimestamp.Epoch)
        deliver2 <- createEventWithCounterAndTs(
          1L,
          CantonTimestamp.Epoch,
          customSerialization = Some(ByteString.copyFromUtf8("Different serialization")),
        ) // changing serialization, but not the contents

        validator = mkValidator(deliver1)
        _ <- validator
          .validateOnReconnect(deliver2)
          .valueOrFail("Different serialization should be accepted")
      } yield succeed
    }

    "check the domain Id" in {
      val incorrectDomainId = DomainId(UniqueIdentifier.tryFromProtoPrimitive("wrong-domain::id"))
      val validator = mkValidator(
        IgnoredSequencedEvent(CantonTimestamp.MinValue, SequencerCounter(updatedCounter), None)(
          traceContext
        )
      )
      for {
        wrongDomain <- createEvent(incorrectDomainId)
        err <- validator.validateOnReconnect(wrongDomain).leftOrFail("wrong domain ID on reconnect")
      } yield err shouldBe BadDomainId(defaultDomainId, incorrectDomainId)
    }

    "check for a fork" in {
      for {
        priorEvent <- createEvent()
        validator = mkValidator(priorEvent)
        differentCounter <- createEvent(counter = 43L)
        errCounter <- validator
          .validateOnReconnect(differentCounter)
          .leftOrFail("fork on counter")
        differentTimestamp <- createEvent(timestamp = CantonTimestamp.MaxValue)
        errTimestamp <- validator
          .validateOnReconnect(differentTimestamp)
          .leftOrFail("fork on timestamp")
        differentContent <- createEventWithCounterAndTs(
          counter = updatedCounter,
          CantonTimestamp.Epoch,
        )
        errContent <- validator.validateOnReconnect(differentContent).leftOrFail("fork on content")
      } yield {
        errCounter shouldBe ForkHappened(
          SequencerCounter(updatedCounter),
          differentCounter.signedEvent.content,
          Some(priorEvent.signedEvent.content),
        )
        errTimestamp shouldBe ForkHappened(
          SequencerCounter(updatedCounter),
          differentTimestamp.signedEvent.content,
          Some(priorEvent.signedEvent.content),
        )
        errContent shouldBe ForkHappened(
          SequencerCounter(updatedCounter),
          differentContent.signedEvent.content,
          Some(priorEvent.signedEvent.content),
        )
      }
    }

    "verify the signature" in {
      for {
        priorEvent <- createEvent()
        badSig <- sign(ByteString.copyFromUtf8("not-the-message"), CantonTimestamp.Epoch)
        badEvent <- createEvent(signatureOverride = Some(badSig))
        validator = mkValidator(priorEvent)
        result <- validator
          .validateOnReconnect(badEvent)
          .leftOrFail("invalid signature on reconnect")
      } yield {
        result shouldBe a[SignatureInvalid]
      }
    }
  }

  "validate" should {
    "reject messages with unexpected domain ids" in {
      val incorrectDomainId = DomainId(UniqueIdentifier.tryFromProtoPrimitive("wrong-domain::id"))
      for {
        event <- createEvent(incorrectDomainId)
        validator = mkValidator()
        result <- validator.validate(event).leftOrFail("wrong domain ID")
      } yield result shouldBe BadDomainId(`defaultDomainId`, `incorrectDomainId`)
    }

    "reject messages with invalid signatures" in {
      for {
        priorEvent <- createEvent(timestamp = CantonTimestamp.Epoch.immediatePredecessor)
        badSig <- sign(ByteString.copyFromUtf8("not-the-message"), CantonTimestamp.Epoch)
        badEvent <- createEvent(
          signatureOverride = Some(badSig),
          counter = priorEvent.counter.v + 1L,
        )
        validator = mkValidator(priorEvent)
        result <- validator.validate(badEvent).leftOrFail("invalid signature")
      } yield {
        result shouldBe a[SignatureInvalid]
      }
    }

    "validate correctly with explicit signing timestamp" in {
      val syncCrypto = mock[DomainSyncCryptoClient]
      when(syncCrypto.pureCrypto).thenReturn(subscriberCryptoApi.pureCrypto)
      when(syncCrypto.snapshot(timestamp = ts(1))).thenAnswer[CantonTimestamp](tm =>
        subscriberCryptoApi.snapshot(tm)
      )
      when(syncCrypto.topologyKnownUntilTimestamp).thenReturn(CantonTimestamp.MaxValue)
      val validator = mkValidator(
        IgnoredSequencedEvent(ts(0), SequencerCounter(41), None)(traceContext),
        syncCryptoApi = syncCrypto,
      )
      for {
        deliver <- createEventWithCounterAndTs(42, ts(2), timestampOfSigningKey = Some(ts(1)))
        _ <- valueOrFail(validator.validate(deliver))("validate")
      } yield succeed
    }

    "reject the same counter-timestamp if passed in repeatedly" in {
      val validator =
        mkValidator(
          IgnoredSequencedEvent(CantonTimestamp.MinValue, SequencerCounter(41), None)(traceContext)
        )

      for {
        deliver <- createEventWithCounterAndTs(42, CantonTimestamp.Epoch)
        _ <- validator.validate(deliver).valueOrFail("validate1")
        err <- validator.validate(deliver).leftOrFail("validate2")
      } yield {
        err shouldBe GapInSequencerCounter(SequencerCounter(42), SequencerCounter(42))
      }
    }

    "fail if the counter or timestamp do not increase" in {
      val validator =
        mkValidator(
          IgnoredSequencedEvent(CantonTimestamp.Epoch, SequencerCounter(41), None)(traceContext)
        )

      for {
        deliver1 <- createEventWithCounterAndTs(42, CantonTimestamp.MinValue)
        deliver2 <- createEventWithCounterAndTs(0L, CantonTimestamp.MaxValue)
        deliver3 <- createEventWithCounterAndTs(42L, CantonTimestamp.ofEpochSecond(2))

        error1 <- validator.validate(deliver1).leftOrFail("deliver1")
        error2 <- validator.validate(deliver2).leftOrFail("deliver2")
        _ <- validator.validate(deliver3).valueOrFail("deliver3")
        error3 <- validator.validate(deliver2).leftOrFail("deliver4")
      } yield {
        error1 shouldBe NonIncreasingTimestamp(
          CantonTimestamp.MinValue,
          SequencerCounter(42),
          CantonTimestamp.Epoch,
          SequencerCounter(41),
        )
        error2 shouldBe DecreasingSequencerCounter(SequencerCounter(0), SequencerCounter(41))
        error3 shouldBe DecreasingSequencerCounter(SequencerCounter(0), SequencerCounter(42))
      }
    }

    "fail if there is a counter cap" in {
      val validator =
        mkValidator(
          IgnoredSequencedEvent(CantonTimestamp.Epoch, SequencerCounter(41), None)(traceContext)
        )

      for {
        deliver1 <- createEventWithCounterAndTs(43L, CantonTimestamp.ofEpochSecond(1))
        deliver2 <- createEventWithCounterAndTs(42L, CantonTimestamp.ofEpochSecond(2))
        deliver3 <- createEventWithCounterAndTs(44L, CantonTimestamp.ofEpochSecond(3))

        result1 <- validator.validate(deliver1).leftOrFail("deliver1")
        _ <- validator.validate(deliver2).valueOrFail("deliver2")
        result3 <- validator.validate(deliver3).leftOrFail("deliver3")
      } yield {
        result1 shouldBe GapInSequencerCounter(SequencerCounter(43), SequencerCounter(41))
        result3 shouldBe GapInSequencerCounter(SequencerCounter(44), SequencerCounter(42))
      }
    }
  }
}
