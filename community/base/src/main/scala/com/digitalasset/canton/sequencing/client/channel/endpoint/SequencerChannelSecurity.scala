// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client.channel.endpoint

import cats.data.EitherT
import cats.syntax.either.*
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.EncryptionError.FailedToEncrypt
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
import com.digitalasset.canton.sequencing.client.channel.endpoint.SequencerChannelSecurity.SessionKeyRejection
import com.digitalasset.canton.sequencing.protocol.channel.{
  SequencerChannelId,
  SequencerChannelSessionKey,
  SequencerChannelSessionKeyContent,
}
import com.digitalasset.canton.serialization.{DefaultDeserializationError, DeserializationError}
import com.digitalasset.canton.topology.{Member, PhysicalSynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.SingleUseCell
import com.digitalasset.canton.version.{HasToByteString, ProtocolVersion}
import com.google.protobuf.ByteString

import scala.concurrent.{ExecutionContext, Future}

/** Encapsulates the sequencer channel security concern, in particular it stores the session key
  * in-memory.
  *
  * Instances of this class are expected to be used by
  * [[com.digitalasset.canton.sequencing.client.channel.endpoint.SequencerChannelClientEndpoint]],
  * and destroyed when such endpoint is no longer needed.
  *
  * Using the [[com.digitalasset.canton.crypto.SynchronizerCryptoClient]] as opposed to using
  * [[com.digitalasset.canton.crypto.SyncCryptoClient]] (`SyncCryptoClient[SyncCryptoApi]`) ensures
  * that the crypto schemes that the synchronizer prescribes are actually used. Doing so prevents
  * downgrading attacks where a weaker scheme is used.
  *
  * The session key owner encrypts the session key to the other member and signs the encrypted key,
  * so that the sequencer relaying the channel can neither read the key nor substitute its own:
  *   - Keys come from each member's latest topology snapshot, not from a timestamp agreed between
  *     the members: with an agreed timestamp, keys rotated after it would still be accepted. The
  *     encrypted key names the receiver's encryption key and the signature names the sender's
  *     signing key.
  *   - The encrypted plaintext contains a hash of the channel context, including the sender. It
  *     binds the session key to the member that encrypted it: the signature alone shows who signed
  *     the encrypted key, not who created it.
  *   - The receiver verifies the signature before it decrypts, so that only the expected sender can
  *     make it use its private key. It also checks that the key is encrypted to one of its current
  *     encryption keys, because decryption works with any key that is still in the private store,
  *     including rotated ones.
  *
  * @param synchronizerCryptoApi
  *   Provides the crypto API for symmetric and asymmetric encryption and for signing.
  * @param protocolVersion
  *   Used for the proto messages versioning.
  */
private[endpoint] final class SequencerChannelSecurity(
    synchronizerCryptoApi: SynchronizerCryptoClient,
    protocolVersion: ProtocolVersion,
    channelId: SequencerChannelId,
    member: Member,
    connectTo: Member,
)(implicit executionContext: ExecutionContext) {

  private val pureCrypto = synchronizerCryptoApi.pureCrypto

  private val sessionKey = new SingleUseCell[SymmetricKey]

  private def registerSessionKey(key: SymmetricKey): Unit =
    sessionKey.putIfAbsent(key).discard

  private[endpoint] def generateSessionKey()(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, SequencerChannelSessionKey] =
    for {
      snapshot <- EitherT.right(synchronizerCryptoApi.currentSnapshotApproximation)
      key <- EitherT
        .fromEither[FutureUnlessShutdown](pureCrypto.generateSymmetricKey())
        .leftMap(_.toString)
      content = SequencerChannelSessionKeyContent(
        key,
        SequencerChannelSecurity.contextHash(
          synchronizerCryptoApi.psid,
          channelId,
          sender = member,
          receiver = connectTo,
        ),
        protocolVersion,
      )
      encryptedPerMember <- snapshot
        .encryptFor(content, Seq(connectTo))
        .leftMap(_.toString)
      encrypted <- EitherT.fromOption[FutureUnlessShutdown](
        encryptedPerMember.get(connectTo),
        s"No encrypted session key found for $connectTo",
      )
      signature <- snapshot
        .sign(
          SequencerChannelSecurity.signedHash(
            synchronizerCryptoApi.psid,
            channelId,
            sender = member,
            receiver = connectTo,
            encrypted,
          ),
          SigningKeyUsage.ProtocolOnly,
          signingTimestampOverrides = None,
        )
        .leftMap(_.toString)
    } yield {
      registerSessionKey(key)
      SequencerChannelSessionKey(encrypted, signature, protocolVersion)
    }

  /** Registers the session key of the message only if the message is authenticated as sent by
    * `connectTo` for this channel.
    */
  private[endpoint] def registerSessionKey(
      message: SequencerChannelSessionKey
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SessionKeyRejection, Unit] = {
    val encrypted = message.encryptedSessionKey
    for {
      snapshot <- EitherT.right[SessionKeyRejection](
        synchronizerCryptoApi.currentSnapshotApproximation
      )
      _ <- snapshot
        .verifySignature(
          SequencerChannelSecurity.signedHash(
            synchronizerCryptoApi.psid,
            channelId,
            sender = connectTo,
            receiver = member,
            encrypted,
          ),
          connectTo,
          message.signature,
          SigningKeyUsage.ProtocolOnly,
        )
        .leftMap[SessionKeyRejection] {
          // A current key of `connectTo` signed different content.
          case error: SignatureCheckError.InvalidSignature =>
            SessionKeyRejection.Tampered(s"Invalid session key signature of $connectTo: $error")
          case error =>
            SessionKeyRejection.MaybeTransient(
              s"Session key signature of $connectTo cannot be verified with its current keys: $error"
            )
        }
      currentEncryptionKeys <- EitherT.right[SessionKeyRejection](
        snapshot.ipsSnapshot.encryptionKeys(member)
      )
      _ <- EitherT.cond[FutureUnlessShutdown](
        currentEncryptionKeys.exists(_.fingerprint == encrypted.encryptedFor),
        (),
        SessionKeyRejection.MaybeTransient(
          s"Session key is encrypted to ${encrypted.encryptedFor}, which is not a current encryption key of $member"
        ): SessionKeyRejection,
      )
      content <- snapshot
        .decrypt(encrypted)(bytes =>
          SequencerChannelSessionKeyContent
            .fromByteString(protocolVersion, bytes)
            .leftMap(error => DefaultDeserializationError(error.message))
        )
        .leftMap[SessionKeyRejection](error =>
          SessionKeyRejection.MaybeTransient(s"Cannot decrypt the session key: $error")
        )
      expectedContextHash = SequencerChannelSecurity.contextHash(
        synchronizerCryptoApi.psid,
        channelId,
        sender = connectTo,
        receiver = member,
      )
      _ <- EitherT.cond[FutureUnlessShutdown](
        content.contextHash == expectedContextHash,
        (),
        SessionKeyRejection.Tampered(
          s"Session key context does not match channel $channelId from $connectTo to $member"
        ): SessionKeyRejection,
      )
    } yield registerSessionKey(content.sessionKey)
  }

  private[endpoint] def encrypt[M <: HasToByteString](
      message: M
  ): EitherT[FutureUnlessShutdown, EncryptionError, Encrypted[M]] =
    for {
      key <- EitherT
        .fromOption[FutureUnlessShutdown](sessionKey.get, FailedToEncrypt("No session key found"))
      encryptedMessage <- EitherT.fromEither[FutureUnlessShutdown](
        pureCrypto.encryptSymmetricWith(message, key)
      )
    } yield encryptedMessage

  private[endpoint] def decrypt[M](
      encryptedMessage: Encrypted[M]
  )(
      deserialize: ByteString => Either[DeserializationError, M]
  ): EitherT[Future, String, M] =
    for {
      key <- EitherT.fromOption[Future](sessionKey.get, "No session key found")
      message <- EitherT.fromEither[Future](
        pureCrypto.decryptWith(encryptedMessage, key)(deserialize).leftMap(_.toString)
      )
    } yield message

}

private[endpoint] object SequencerChannelSecurity {

  /** Why a received session key was rejected, which decides how the rejection is reported. */
  sealed trait SessionKeyRejection extends Product with Serializable {
    def reason: String
  }

  object SessionKeyRejection {

    /** Neither a key rotation nor a version mismatch can explain it. */
    final case class Tampered(reason: String) extends SessionKeyRejection

    /** The other member or the sequencer relaying the channel runs an incompatible version. */
    final case class Incompatible(reason: String) extends SessionKeyRejection

    /** Can resolve by itself, for example once both members have processed a key rotation. */
    final case class MaybeTransient(reason: String) extends SessionKeyRejection
  }

  /** Hash inside the encrypted session key that binds the key to the member that encrypted it. */
  private[endpoint] def contextHash(
      psid: PhysicalSynchronizerId,
      channelId: SequencerChannelId,
      sender: Member,
      receiver: Member,
  ): Hash =
    Hash
      .build(HashPurpose.SequencerChannelSessionKeyContext, HashAlgorithm.Sha256)
      .addString(psid.toProtoPrimitive)
      .addString(channelId.unwrap)
      .addString(sender.toProtoPrimitive)
      .addString(receiver.toProtoPrimitive)
      .finish()

  private[endpoint] def signedHash(
      psid: PhysicalSynchronizerId,
      channelId: SequencerChannelId,
      sender: Member,
      receiver: Member,
      encryptedSessionKey: AsymmetricEncrypted[SequencerChannelSessionKeyContent],
  ): Hash =
    Hash
      .build(HashPurpose.SequencerChannelSessionKey, HashAlgorithm.Sha256)
      .addString(psid.toProtoPrimitive)
      .addString(channelId.unwrap)
      .addString(sender.toProtoPrimitive)
      .addString(receiver.toProtoPrimitive)
      .addByteString(
        encryptedSessionKey.computeHash(HashAlgorithm.Sha256).getCryptographicEvidence
      )
      .finish()
}
