// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client

import cats.syntax.either._
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.crypto._
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.protocol.ExampleTransactionFactory
import com.digitalasset.canton.protocol.messages.{InformeeMessage, ProtocolMessage}
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
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{BaseTest, HasExecutionContext, SequencerCounter}
import com.google.protobuf.ByteString
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future

class SequencedEventValidatorTest extends AsyncWordSpec with BaseTest with HasExecutionContext {

  private lazy val defaultDomainId: DomainId = DefaultTestIdentities.domainId
  private lazy val subscriberId: ParticipantId = ParticipantId("participant1-id")
  private lazy val sequencerId: SequencerId = DefaultTestIdentities.sequencer
  private lazy val subscriberCryptoApi: DomainSyncCryptoClient =
    TestingIdentityFactory(loggerFactory).forOwnerAndDomain(subscriberId, defaultDomainId)
  private lazy val sequencerCryptoApi: DomainSyncCryptoClient =
    TestingIdentityFactory(loggerFactory).forOwnerAndDomain(sequencerId, defaultDomainId)
  private lazy val updatedCounter: SequencerCounter = 42L
  private lazy val dummyHash: Hash = TestHash.digest(0)

  private def mkValidator(
      initialEventMetadata: PossiblyIgnoredSerializedEvent =
        IgnoredSequencedEvent(CantonTimestamp.MinValue, updatedCounter - 1, None)(traceContext),
      syncCryptoApi: DomainSyncCryptoClient = subscriberCryptoApi,
  ): SequencedEventValidator = {
    new SequencedEventValidator(
      Some(initialEventMetadata),
      unauthenticated = false,
      optimistic = false,
      skipValidation = false,
      defaultDomainId,
      sequencerId,
      syncCryptoApi,
      FutureSupervisor.Noop,
      loggerFactory,
    )
  }

  private def createEvent(
      domainId: DomainId = defaultDomainId,
      signatureOverride: Option[Signature] = None,
      serializedOverride: Option[ByteString] = None,
      counter: SequencerCounter = updatedCounter,
  ): Future[OrdinarySerializedEvent] = {
    import cats.syntax.option._
    val message = {
      val factory: ExampleTransactionFactory = new ExampleTransactionFactory()()
      val fullInformeeTree = factory.MultipleRootsAndViewNestings.fullInformeeTree
      InformeeMessage(fullInformeeTree)
    }
    val deliver: Deliver[ClosedEnvelope] = Deliver.create[ClosedEnvelope](
      counter,
      CantonTimestamp.Epoch,
      domainId,
      MessageId.tryCreate("test").some,
      Batch(
        List(
          ClosedEnvelope(
            serializedOverride.getOrElse(
              ProtocolMessage.toEnvelopeContentByteString(message, ProtocolVersion.latestForTest)
            ),
            Recipients.cc(subscriberId),
          )
        )
      ),
    )

    for {
      sig <- signatureOverride
        .map(Future.successful)
        .getOrElse(sign(deliver.getCryptographicEvidence, deliver.timestamp))
    } yield OrdinarySequencedEvent(SignedContent(deliver, sig, None))(traceContext)
  }

  private def createEventWithCounterAndTs(
      counter: SequencerCounter,
      timestamp: CantonTimestamp,
      customSerialization: Option[ByteString] = None,
      messageIdO: Option[MessageId] = None,
      timestampOfSigningKey: Option[CantonTimestamp] = None,
  ): Future[OrdinarySerializedEvent] = {
    val event =
      SequencerTestUtils.mockDeliver(
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

  private def hash(signedEvent: OrdinarySerializedEvent): Hash =
    SignedContent.hashContent(subscriberCryptoApi.pureCrypto, signedEvent.signedEvent.content)

  private def sign(bytes: ByteString, timestamp: CantonTimestamp): Future[Signature] = {
    val hash = sequencerCryptoApi.pureCrypto.digest(HashPurpose.SequencedEventSignature, bytes)
    for {
      cryptoApi <- sequencerCryptoApi.snapshot(timestamp)
      signature <- cryptoApi.sign(hash).value.map(_.valueOr(err => fail(s"Failed to sign: $err")))
    } yield signature
  }

  "SequencedEventValidator" should {
    val incorrectDomainId = DomainId(UniqueIdentifier.tryFromProtoPrimitive("wrong-domain::id"))
    "reject messages with unexpected domain ids" in {
      for {
        event <- createEvent(incorrectDomainId)
        validator = mkValidator()
        result <- validator.validate(event).value
      } yield result.left.value should matchPattern {
        case BadDomainId(`defaultDomainId`, `incorrectDomainId`) =>
      }
    }

    "reject messages with invalid signatures" in {
      for {
        badSig <- sign(ByteString.copyFromUtf8("not-the-message"), CantonTimestamp.Epoch)
        event <- createEvent()
        badEvent <- createEvent(signatureOverride = Some(badSig))
        validator = mkValidator()
        good <- validator.validate(event).value // we'll skip the first event
        result <- validator.validate(badEvent).value
      } yield {
        good.value shouldBe ()
        result.left.value shouldBe a[SignatureInvalid]
      }
    }

    "validate correctly with explicit signing key" in {
      val syncCrypto = mock[DomainSyncCryptoClient]
      when(syncCrypto.pureCrypto).thenReturn(subscriberCryptoApi.pureCrypto)
      when(syncCrypto.awaitSnapshot(timestamp = ts(1))).thenAnswer[CantonTimestamp](tm =>
        subscriberCryptoApi.snapshot(tm)
      )
      when(syncCrypto.topologyKnownUntilTimestamp).thenReturn(CantonTimestamp.MaxValue)
      val validator = mkValidator(
        IgnoredSequencedEvent(ts(0), 41L, None)(traceContext),
        syncCryptoApi = syncCrypto,
      )
      for {
        deliver <- createEventWithCounterAndTs(42, ts(2), timestampOfSigningKey = Some(ts(1)))
        _ <- valueOrFail(validator.validate(deliver))("validate")
      } yield succeed
    }

    "allow the same counter-timestamp several times" in {
      val validator =
        mkValidator(IgnoredSequencedEvent(CantonTimestamp.MinValue, 41L, None)(traceContext))

      for {
        deliver <- createEventWithCounterAndTs(42, CantonTimestamp.Epoch)
        _ <- valueOrFail(validator.validate(deliver))("validate1")
        _ <- valueOrFail(validator.validate(deliver))("validate2")
      } yield succeed
    }

    "fail if the counter or timestamp do not increase" in {
      val validator =
        mkValidator(IgnoredSequencedEvent(CantonTimestamp.Epoch, 41L, None)(traceContext))

      for {
        deliver1 <- createEventWithCounterAndTs(42, CantonTimestamp.MinValue)
        deliver2 <- createEventWithCounterAndTs(0L, CantonTimestamp.MaxValue)
        deliver3 <- createEventWithCounterAndTs(42L, CantonTimestamp.ofEpochSecond(2))

        error1 <- validator.validate(deliver1).value
        error2 <- validator.validate(deliver2).value
        _ = {
          error1.left.value shouldBe NonIncreasingTimestamp(
            CantonTimestamp.MinValue,
            42L,
            CantonTimestamp.Epoch,
            41L,
          )
          error2.left.value shouldBe DecreasingSequencerCounter(0L, 41L)
        }
        _ <- validator.validate(deliver3).value.map(_.value)
        error3 <- validator.validate(deliver2).value
      } yield {
        error3.left.value shouldBe DecreasingSequencerCounter(0L, 42L)
      }
    }

    "fail if there is a counter cap" in {
      val validator =
        mkValidator(IgnoredSequencedEvent(CantonTimestamp.Epoch, 41L, None)(traceContext))

      for {
        deliver1 <- createEventWithCounterAndTs(43L, CantonTimestamp.ofEpochSecond(1))
        deliver2 <- createEventWithCounterAndTs(42L, CantonTimestamp.ofEpochSecond(2))
        deliver3 <- createEventWithCounterAndTs(44L, CantonTimestamp.ofEpochSecond(3))

        result1 <- validator.validate(deliver1).value
        _ <- valueOrFail(validator.validate(deliver2))("validate")
        result3 <- validator.validate(deliver3).value
      } yield {
        result1.left.value shouldBe GapInSequencerCounter(43L, 41L)
        result3.left.value shouldBe GapInSequencerCounter(44L, 42L)
      }
    }

    "fail if an event with the same counter has differing content" in {
      val validator =
        mkValidator(IgnoredSequencedEvent(CantonTimestamp.MinValue, 0L, None)(traceContext))
      for {
        deliver1 <- createEventWithCounterAndTs(1L, CantonTimestamp.Epoch)
        deliver2 <- createEventWithCounterAndTs(1L, CantonTimestamp.MaxValue)
        deliver3 <- createEventWithCounterAndTs(
          1L,
          CantonTimestamp.Epoch,
          messageIdO = Some(MessageId.tryCreate("foo")),
        ) // changing the content

        _ <- validator.validate(deliver1).value
        error1 <- validator.validate(deliver2).value
        error2 <- validator.validate(deliver3).value
      } yield {
        error1.left.value shouldBe ForkHappened(
          1L,
          deliver2.signedEvent.content,
          deliver1.signedEvent.content,
        )
        error2.left.value shouldBe ForkHappened(
          1L,
          deliver3.signedEvent.content,
          deliver1.signedEvent.content,
        )
      }
    }

    "succeed if an event with the same counter has a different serialization of the same content" in {
      val validator =
        mkValidator(IgnoredSequencedEvent(CantonTimestamp.MinValue, 0L, None)(traceContext))
      for {
        deliver1 <- createEventWithCounterAndTs(1L, CantonTimestamp.Epoch)
        deliver2 <- createEventWithCounterAndTs(
          1L,
          CantonTimestamp.Epoch,
          customSerialization = Some(ByteString.copyFromUtf8("Different serialization")),
        ) // changing serialization, but not the contents

        _ <- validator.validate(deliver1).value
        _ <- validator.validate(deliver2).valueOrFail("Different serialization should be accepted")
      } yield succeed
    }
  }
}
