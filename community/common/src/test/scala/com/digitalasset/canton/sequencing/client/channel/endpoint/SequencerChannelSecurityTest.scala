// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client.channel.endpoint

import cats.data.EitherT
import cats.syntax.either.*
import com.daml.metrics.ExecutorServiceMetrics
import com.daml.metrics.api.noop.NoOpMetricsFactory
import com.digitalasset.canton.config.CryptoProvider.Jce
import com.digitalasset.canton.config.{
  CachingConfigs,
  CryptoConfig,
  SessionEncryptionKeyCacheConfig,
  TestBatchingConfig,
}
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicCrypto
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown}
import com.digitalasset.canton.metrics.CommonMockMetrics
import com.digitalasset.canton.replica.ReplicaManager
import com.digitalasset.canton.resource.MemoryStorage
import com.digitalasset.canton.sequencer.api.v30
import com.digitalasset.canton.sequencer.api.v30.ConnectToSequencerChannelResponse.Response
import com.digitalasset.canton.sequencing.channel.errors.SequencerChannelErrors
import com.digitalasset.canton.sequencing.client.SubscriptionCloseReason
import com.digitalasset.canton.sequencing.client.channel.SequencerChannelProtocolProcessor
import com.digitalasset.canton.sequencing.client.channel.endpoint.SequencerChannelSecurity.SessionKeyRejection
import com.digitalasset.canton.sequencing.protocol.channel.{
  SequencerChannelId,
  SequencerChannelSessionKey,
  SequencerChannelSessionKeyContent,
}
import com.digitalasset.canton.serialization.DefaultDeserializationError
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.tracing.{
  NoReportingTracerProvider,
  SerializableTraceContext,
  TraceContext,
}
import com.digitalasset.canton.util.{ByteStringUtil, EitherTUtil, MaxBytesToDecompress}
import com.digitalasset.canton.version.{HasToByteString, ProtocolVersion}
import com.digitalasset.canton.{BaseTest, HasExecutionContext, ProtocolVersionChecksAnyWordSpec}
import com.google.protobuf.ByteString
import io.grpc.Context
import io.grpc.stub.StreamObserver
import org.scalatest.wordspec.AnyWordSpec

import java.util.concurrent.ConcurrentLinkedQueue
import scala.concurrent.ExecutionContext
import scala.jdk.CollectionConverters.*

class SequencerChannelSecurityTest
    extends AnyWordSpec
    with BaseTest
    with HasExecutionContext
    with ProtocolVersionChecksAnyWordSpec {

  private val tp = DefaultTestIdentities.participant1
  private val sp = DefaultTestIdentities.participant2
  private val other = DefaultTestIdentities.participant3
  private val psid = DefaultTestIdentities.physicalSynchronizerId
  private val otherPsid = PhysicalSynchronizerId(
    SynchronizerId(UniqueIdentifier.tryFromProtoPrimitive("other::default")),
    psid.serial,
    psid.protocolVersion,
  )
  private val channelId = SequencerChannelId.tryCreate("channel-1")
  private val otherChannelId = SequencerChannelId.tryCreate("channel-2")

  private lazy val crypto = SymbolicCrypto.create(
    testedReleaseProtocolVersion,
    timeouts,
    loggerFactory,
  )
  private lazy val identityFactory = TestingTopology()
    .withSynchronizers(psid, otherPsid)
    .withSimpleParticipants(tp, sp, other)
    .build(crypto, loggerFactory)

  private def security(
      member: Member,
      connectTo: Member,
      channel: SequencerChannelId = channelId,
      synchronizer: PhysicalSynchronizerId = psid,
  ): SequencerChannelSecurity =
    new SequencerChannelSecurity(
      identityFactory.forOwnerAndSynchronizer(member, synchronizer),
      testedProtocolVersion,
      channel,
      member,
      connectTo,
    )

  private def generate(owner: SequencerChannelSecurity): SequencerChannelSessionKey =
    owner.generateSessionKey().valueOrFailShutdown("generate session key").futureValue

  private def register(
      receiver: SequencerChannelSecurity,
      message: SequencerChannelSessionKey,
  ): Either[SessionKeyRejection, Unit] =
    receiver.registerSessionKey(message).value.futureValueUS

  private def snapshot(member: Member): SynchronizerSnapshotSyncCryptoApi =
    identityFactory.forOwnerAndSynchronizer(member, psid).currentSnapshotApproximation.futureValueUS

  private def sign(signer: Member, hash: Hash): Signature =
    snapshot(signer)
      .sign(hash, SigningKeyUsage.ProtocolOnly, signingTimestampOverrides = None)
      .valueOrFailShutdown("sign")
      .futureValue

  private def contentFromTpToSp(): SequencerChannelSessionKeyContent =
    SequencerChannelSessionKeyContent(
      crypto.pureCrypto.generateSymmetricKey().valueOrFail("generate symmetric key"),
      SequencerChannelSecurity.contextHash(psid, channelId, sender = tp, receiver = sp),
      testedProtocolVersion,
    )

  /** A session key message of the relay: it has the expected context and is encrypted to the
    * receiver's current encryption key, but is not signed by the sender.
    */
  private def sessionKeyOfRelay(): (SymmetricKey, SequencerChannelSessionKey) = {
    val content = contentFromTpToSp()
    val spEncryptionKey = snapshot(sp).ipsSnapshot.encryptionKey(sp).futureValueUS.value
    val encrypted = crypto.pureCrypto.encryptWith(content, spEncryptionKey).valueOrFail("encrypt")
    val signature = sign(
      other,
      SequencerChannelSecurity.signedHash(psid, channelId, sender = tp, receiver = sp, encrypted),
    )
    (content.sessionKey, SequencerChannelSessionKey(encrypted, signature, testedProtocolVersion))
  }

  "SequencerChannelSecurity" should {

    "share the session key between the owner and the expected receiver" onlyRunWithOrGreaterThan ProtocolVersion.sequencerChannel in {
      val owner = security(member = tp, connectTo = sp)
      val receiver = security(member = sp, connectTo = tp)

      register(receiver, generate(owner)) shouldBe Right(())

      val payload = ByteString.copyFromUtf8("payload")
      val encrypted = owner
        .encrypt(new HasToByteString { override def toByteString: ByteString = payload })
        .valueOrFailShutdown("encrypt")
        .futureValue
      receiver
        .decrypt(Encrypted.fromByteString[ByteString](encrypted.ciphertext))(Right(_))
        .valueOrFail("decrypt")
        .futureValue shouldBe payload
    }

    "reject a session key signed for another channel" onlyRunWithOrGreaterThan ProtocolVersion.sequencerChannel in {
      val owner = security(member = tp, connectTo = sp, channel = otherChannelId)
      val receiver = security(member = sp, connectTo = tp)

      register(receiver, generate(owner)).left.value shouldBe a[SessionKeyRejection.Tampered]
    }

    "reject a session key signed for another synchronizer" onlyRunWithOrGreaterThan ProtocolVersion.sequencerChannel in {
      val owner = security(member = tp, connectTo = sp, synchronizer = otherPsid)
      val receiver = security(member = sp, connectTo = tp)

      register(receiver, generate(owner)).left.value shouldBe a[SessionKeyRejection.Tampered]
    }

    "reject another member's encrypted session key re-signed as its own" onlyRunWithOrGreaterThan ProtocolVersion.sequencerChannel in {
      // The colluding member signs the expected sender's encrypted key as its own. The channel id
      // is the same, so that only the sender in the context hash tells the two apart.
      val honestKey = generate(security(member = tp, connectTo = sp))
      val receiverOfColluder = security(member = sp, connectTo = other)
      val colluderSignature = sign(
        other,
        SequencerChannelSecurity.signedHash(
          psid,
          channelId,
          sender = other,
          receiver = sp,
          honestKey.encryptedSessionKey,
        ),
      )

      register(
        receiverOfColluder,
        SequencerChannelSessionKey(
          honestKey.encryptedSessionKey,
          colluderSignature,
          testedProtocolVersion,
        ),
      ).left.value shouldBe a[SessionKeyRejection.Tampered]
    }

    "reject a replaced encrypted session key before decrypting it" onlyRunWithOrGreaterThan ProtocolVersion.sequencerChannel in {
      val receiver = security(member = sp, connectTo = tp)
      val signed = generate(security(member = tp, connectTo = sp))
      // Decrypting this replacement fails, which would be reported as MaybeTransient.
      val replacement = signed.encryptedSessionKey.copy[SequencerChannelSessionKeyContent](
        ciphertext = ByteString.copyFromUtf8("not a ciphertext")
      )

      register(
        receiver,
        SequencerChannelSessionKey(replacement, signed.signature, testedProtocolVersion),
      ).left.value shouldBe a[SessionKeyRejection.Tampered]
    }

    "reject a session key encrypted to an encryption key that is not current" onlyRunWithOrGreaterThan ProtocolVersion.sequencerChannel in {
      val receiver = security(member = sp, connectTo = tp)
      val notCurrentKey = crypto
        .generateEncryptionKey()
        .valueOrFailShutdown("generate encryption key")
        .futureValue
      val encrypted =
        crypto.pureCrypto.encryptWith(contentFromTpToSp(), notCurrentKey).valueOrFail("encrypt")
      val signature = sign(
        tp,
        SequencerChannelSecurity.signedHash(psid, channelId, sender = tp, receiver = sp, encrypted),
      )

      register(
        receiver,
        SequencerChannelSessionKey(encrypted, signature, testedProtocolVersion),
      ).left.value shouldBe a[SessionKeyRejection.MaybeTransient]
    }

    "compute the hashes of the specification" onlyRunWithOrGreaterThan ProtocolVersion.sequencerChannel in {
      // The expected values were computed independently of this code from the encoding: a 4-byte
      // big-endian hash purpose, then each input length-prefixed with a 4-byte big-endian length,
      // SHA-256, and the multihash prefix 0x1220. Every version must produce the same bytes: the
      // two channel members compute the hashes independently.
      val fixedPsid = PhysicalSynchronizerId.tryFromString(s"da::1220${"ab" * 32}::37-0")
      val sender = ParticipantId.tryFromProtoPrimitive(s"PAR::tp::1220${"cd" * 32}")
      val receiver = ParticipantId.tryFromProtoPrimitive(s"PAR::sp::1220${"ef" * 32}")
      val encrypted = AsymmetricEncrypted[SequencerChannelSessionKeyContent](
        ByteString.copyFromUtf8("ciphertext"),
        EncryptionAlgorithmSpec.EciesHkdfHmacSha256Aes128Cbc,
        Fingerprint.tryFromString(s"1220${"12" * 32}"),
      )
      fixedPsid.toProtoPrimitive shouldBe s"da::1220${"ab" * 32}::37-0"
      sender.toProtoPrimitive shouldBe s"PAR::tp::1220${"cd" * 32}"
      receiver.toProtoPrimitive shouldBe s"PAR::sp::1220${"ef" * 32}"

      SequencerChannelSecurity
        .contextHash(fixedPsid, channelId, sender, receiver)
        .toHexString shouldBe "12207e688f5e2d92c9aef6163e3a4d187ea308f9d76f057eb3fea9c2d311e23c653b"
      SequencerChannelSecurity
        .signedHash(fixedPsid, channelId, sender, receiver, encrypted)
        .toHexString shouldBe "122049103a7eba2f54e58206ad06b983295dffc409ffd5d0e13e95337a47ad664309"
    }

    "keep the encrypted session key within the RSA-2048 OAEP plaintext limit" onlyRunWithOrGreaterThan ProtocolVersion.sequencerChannel in {
      // RSA-2048 with OAEP-SHA256 encrypts at most 190 bytes; it is the default for KMS nodes.
      val jceCrypto = Crypto
        .create(
          CryptoConfig(provider = Jce),
          CachingConfigs.defaultKmsMetadataCache,
          SessionEncryptionKeyCacheConfig(),
          CachingConfigs.defaultPublicKeyConversionCache,
          new MemoryStorage(loggerFactory, timeouts),
          Option.empty[ReplicaManager],
          testedReleaseProtocolVersion,
          futureSupervisor,
          wallClock,
          CommonMockMetrics.cryptoMetrics,
          parallelExecutionContext,
          timeouts,
          TestBatchingConfig(),
          loggerFactory,
          NoReportingTracerProvider,
          new ExecutorServiceMetrics(NoOpMetricsFactory),
        )
        .valueOrFailShutdown("create crypto")
        .futureValue
      val rsaKey = jceCrypto
        .generateEncryptionKey(EncryptionKeySpec.Rsa2048)
        .valueOrFailShutdown("generate RSA key")
        .futureValue
      val longIdentifier = "x" * 185
      val content = SequencerChannelSessionKeyContent(
        jceCrypto.pureCrypto.generateSymmetricKey().valueOrFail("generate symmetric key"),
        SequencerChannelSecurity.contextHash(
          psid,
          channelId,
          sender = ParticipantId.tryFromProtoPrimitive(s"PAR::$longIdentifier::1220${"cd" * 32}"),
          receiver = ParticipantId.tryFromProtoPrimitive(s"PAR::$longIdentifier::1220${"ef" * 32}"),
        ),
        testedProtocolVersion,
      )

      val encrypted = jceCrypto.pureCrypto
        .encryptWith(content, rsaKey, EncryptionAlgorithmSpec.RsaOaepSha256)
        .valueOrFail("encrypt with RSA-2048")
      jceCrypto.privateCrypto
        .decrypt(encrypted)(
          SequencerChannelSessionKeyContent
            .fromByteString(testedProtocolVersion, _)
            .leftMap(error => DefaultDeserializationError(error.message))
        )
        .valueOrFailShutdown("decrypt")
        .futureValue
        .contextHash shouldBe content.contextHash
    }

    "never print the session key" onlyRunWithOrGreaterThan ProtocolVersion.sequencerChannel in {
      val content = contentFromTpToSp()
      content.toString shouldBe s"SequencerChannelSessionKeyContent(contextHash = ${content.contextHash})"
    }
  }

  "ChannelStage" should {

    def stageData(member: Member, connectTo: Member): ChannelStage.InternalData =
      ChannelStage.InternalData(
        channelId,
        security(member, connectTo),
        testedProtocolVersion,
        mock[SequencerChannelProtocolProcessor],
        MaxBytesToDecompress.HardcodedDefault,
        loggerFactory,
      )

    def receive(stage: ChannelStage, message: v30.SequencerChannelSessionKey): Unit =
      stage.handleMessage(Response.SessionKey(message)).value.futureValueUS.discard

    "reject a session key at the session key owner" onlyRunWithOrGreaterThan ProtocolVersion.sequencerChannel in {
      val ownerStage = new ChannelStageAwaitingSessionKeyAck(stageData(member = tp, connectTo = sp))
      val sessionKey = generate(security(member = sp, connectTo = tp))

      ownerStage
        .handleMessage(Response.SessionKey(sessionKey.toProtoV30))
        .value
        .futureValueUS
        .left
        .value should include("cannot process message SessionKey")
    }

    "reject an acknowledgement before the session key at the receiver" onlyRunWithOrGreaterThan ProtocolVersion.sequencerChannel in {
      val receiverStage = new ChannelStageAwaitingSessionKey(stageData(member = sp, connectTo = tp))

      receiverStage
        .handleMessage(Response.SessionKeyAcknowledgement(v30.SequencerChannelSessionKeyAck()))
        .value
        .futureValueUS
        .left
        .value should include("cannot process message SessionKeyAcknowledgement")
    }

    "raise the alarm for a tampered session key" onlyRunWithOrGreaterThan ProtocolVersion.sequencerChannel in {
      val receiverStage = new ChannelStageAwaitingSessionKey(stageData(member = sp, connectTo = tp))
      val owner = security(member = tp, connectTo = sp)
      val tampered = SequencerChannelSessionKey(
        generate(owner).encryptedSessionKey,
        generate(owner).signature,
        testedProtocolVersion,
      )

      loggerFactory.assertLogs(
        receive(receiverStage, tampered.toProtoV30),
        _.shouldBeCantonErrorCode(SequencerChannelErrors.SessionKeyRejected),
      )
    }

    "warn about a session key from an incompatible version" onlyRunWithOrGreaterThan ProtocolVersion.sequencerChannel in {
      val receiverStage = new ChannelStageAwaitingSessionKey(stageData(member = sp, connectTo = tp))
      val withoutSignature =
        generate(security(member = tp, connectTo = sp)).toProtoV30.copy(sessionKeySignature = None)

      loggerFactory.assertLogs(
        receive(receiverStage, withoutSignature),
        _.shouldBeCantonErrorCode(SequencerChannelErrors.SessionKeyNotAccepted),
      )
    }
  }

  "SequencerChannelClientEndpoint" should {

    "close and serve nothing when the relay sends its own session key" onlyRunWithOrGreaterThan ProtocolVersion.sequencerChannel in {
      val received = new ConcurrentLinkedQueue[ByteString]()
      val processor = new SequencerChannelProtocolProcessor {
        override implicit def executionContext: ExecutionContext = parallelExecutionContext
        override protected def psid: PhysicalSynchronizerId = SequencerChannelSecurityTest.this.psid
        override protected def timeouts = SequencerChannelSecurityTest.this.timeouts
        override protected def loggerFactory = SequencerChannelSecurityTest.this.loggerFactory
        override def handlePayload(payload: ByteString)(implicit
            traceContext: TraceContext
        ): EitherT[FutureUnlessShutdown, String, Unit] = {
          received.add(payload).discard
          EitherTUtil.unitUS
        }
      }
      val sent = new ConcurrentLinkedQueue[v30.ConnectToSequencerChannelRequest]()
      val endpoint = new SequencerChannelClientEndpoint(
        channelId,
        member = sp,
        connectTo = tp,
        processor,
        identityFactory.forOwnerAndSynchronizer(sp, psid),
        isSessionKeyOwner = false,
        testedProtocolVersion,
        MaxBytesToDecompress.HardcodedDefault,
        Context.ROOT.withCancellation(),
        FlagCloseable(logger, timeouts),
        timeouts,
        loggerFactory,
      )
      processor.setChannelEndpoint(endpoint).value
      endpoint.setRequestObserver(new StreamObserver[v30.ConnectToSequencerChannelRequest] {
        override def onNext(value: v30.ConnectToSequencerChannelRequest): Unit =
          sent.add(value).discard
        override def onError(t: Throwable): Unit = ()
        override def onCompleted(): Unit = ()
      })
      def relaySends(response: Response): Unit =
        endpoint.observer.onNext(
          v30.ConnectToSequencerChannelResponse(
            response,
            Some(SerializableTraceContext(TraceContext.empty).toProtoV30),
          )
        )

      relaySends(Response.Connected(v30.SequencerChannelConnectedToAllEndpoints()))
      val (relayKey, relayMessage) = sessionKeyOfRelay()
      loggerFactory.assertLogs(relaySends(Response.SessionKey(relayMessage.toProtoV30)))
      endpoint.closeReason.futureValue should matchPattern {
        case SubscriptionCloseReason.HandlerError(error: String)
            if error.contains("Rejected session key") =>
      }

      // An instruction under the relay's key does not reach the processor, and the endpoint has
      // sent nothing but the channel metadata.
      val instruction = crypto.pureCrypto
        .encryptSymmetricWith(
          new HasToByteString {
            override def toByteString: ByteString =
              ByteStringUtil.compressZstd(ByteString.copyFromUtf8("instruction"))
          },
          relayKey,
        )
        .valueOrFail("encrypt")
      relaySends(Response.Payload(instruction.ciphertext))
      received shouldBe empty
      processor.isChannelConnected shouldBe false
      sent.asScala.map(_.request.isMetadata).toSeq shouldBe Seq(true)
    }
  }
}
