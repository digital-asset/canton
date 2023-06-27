// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client

import cats.syntax.either.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.SequencerCounter
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicCrypto
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.protocol.ExampleTransactionFactory
import com.digitalasset.canton.protocol.messages.{EnvelopeContent, InformeeMessage}
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.sequencing.{
  OrdinarySerializedEvent,
  SequencerAggregator,
  SequencerTestUtils,
}
import com.digitalasset.canton.store.SequencedEventStore.OrdinarySequencedEvent
import com.digitalasset.canton.topology.DefaultTestIdentities.namespace
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion
import com.google.protobuf.ByteString
import org.scalatest.Assertions.fail

import scala.concurrent.{ExecutionContext, Future}

class SequencedEventTestFixture(
    loggerFactory: NamedLoggerFactory,
    testedProtocolVersion: ProtocolVersion,
    timeouts: ProcessingTimeout,
)(implicit val traceContext: TraceContext) {
  lazy val defaultDomainId: DomainId = DefaultTestIdentities.domainId
  private lazy val subscriberId: ParticipantId = ParticipantId("participant1-id")
  lazy val sequencerId: SequencerId = DefaultTestIdentities.sequencerId
  lazy val subscriberCryptoApi: DomainSyncCryptoClient =
    TestingIdentityFactory(loggerFactory).forOwnerAndDomain(subscriberId, defaultDomainId)
  private lazy val sequencerCryptoApi: DomainSyncCryptoClient =
    TestingIdentityFactory(loggerFactory).forOwnerAndDomain(sequencerId, defaultDomainId)
  lazy val updatedCounter: Long = 42L
  val SecondSequencerId = SequencerId(UniqueIdentifier(Identifier.tryCreate("da2"), namespace))
  val ThirdSequencerId = SequencerId(UniqueIdentifier(Identifier.tryCreate("da3"), namespace))

  val alice = ParticipantId(UniqueIdentifier.tryCreate("participant", "alice"))
  val bob = ParticipantId(UniqueIdentifier.tryCreate("participant", "bob"))
  val carlos = ParticipantId(UniqueIdentifier.tryCreate("participant", "carlos"))
  val signatureAlice = SymbolicCrypto.signature(
    ByteString.copyFromUtf8("signatureAlice1"),
    alice.uid.namespace.fingerprint,
  )
  val signatureBob = SymbolicCrypto.signature(
    ByteString.copyFromUtf8("signatureBob1"),
    bob.uid.namespace.fingerprint,
  )
  val signatureCarlos = SymbolicCrypto.signature(
    ByteString.copyFromUtf8("signatureCarlos1"),
    carlos.uid.namespace.fingerprint,
  )

  def mkAggregator(
      expectedSequencers: NonEmpty[Set[SequencerId]] = NonEmpty.mk(Set, sequencerId),
      expectedSequencersSize: Int = 1,
  ) =
    new SequencerAggregator(
      cryptoPureApi = subscriberCryptoApi.pureCrypto,
      eventInboxSize = PositiveInt.tryCreate(2),
      loggerFactory = loggerFactory,
      expectedSequencers = expectedSequencers,
      expectedSequencersSize = PositiveInt.tryCreate(expectedSequencersSize),
    )

  def mkValidator(
      syncCryptoApi: DomainSyncCryptoClient = subscriberCryptoApi
  )(implicit executionContext: ExecutionContext): SequencedEventValidatorImpl = {
    new SequencedEventValidatorImpl(
      unauthenticated = false,
      optimistic = false,
      defaultDomainId,
      testedProtocolVersion,
      syncCryptoApi,
      loggerFactory,
      timeouts,
    )(executionContext)
  }

  def createEvent(
      domainId: DomainId = defaultDomainId,
      signatureOverride: Option[Signature] = None,
      serializedOverride: Option[ByteString] = None,
      counter: Long = updatedCounter,
      timestamp: CantonTimestamp = CantonTimestamp.Epoch,
      timestampOfSigningKey: Option[CantonTimestamp] = None,
  )(implicit executionContext: ExecutionContext): Future[OrdinarySerializedEvent] = {
    import cats.syntax.option.*
    val message = {
      val factory: ExampleTransactionFactory = new ExampleTransactionFactory()()(executionContext)
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
          ClosedEnvelope.tryCreate(
            serializedOverride.getOrElse(
              EnvelopeContent.tryCreate(message, testedProtocolVersion).toByteString
            ),
            Recipients.cc(subscriberId),
            Seq.empty,
            testedProtocolVersion,
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
    } yield OrdinarySequencedEvent(
      SignedContent(deliver, sig, timestampOfSigningKey, testedProtocolVersion)
    )(
      traceContext
    )
  }

  def createEventWithCounterAndTs(
      counter: Long,
      timestamp: CantonTimestamp,
      customSerialization: Option[ByteString] = None,
      messageIdO: Option[MessageId] = None,
      timestampOfSigningKey: Option[CantonTimestamp] = None,
  )(implicit executionContext: ExecutionContext): Future[OrdinarySerializedEvent] = {
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
    } yield OrdinarySequencedEvent(
      SignedContent(event, signature, timestampOfSigningKey, testedProtocolVersion)
    )(traceContext)
  }

  def ts(offset: Int) = CantonTimestamp.Epoch.plusSeconds(offset.toLong)

  def sign(bytes: ByteString, timestamp: CantonTimestamp)(implicit
      executionContext: ExecutionContext
  ): Future[Signature] =
    for {
      cryptoApi <- sequencerCryptoApi.snapshot(timestamp)
      signature <- cryptoApi
        .sign(hash(bytes))
        .value
        .map(_.valueOr(err => fail(s"Failed to sign: $err")))(executionContext)
    } yield signature

  def hash(bytes: ByteString): Hash =
    sequencerCryptoApi.pureCrypto.digest(HashPurpose.SequencedEventSignature, bytes)

}
