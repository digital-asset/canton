// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import cats.data.EitherT
import cats.syntax.either._
import cats.syntax.traverseFilter._
import com.digitalasset.canton.config.{CacheConfig, CachingConfigs}
import com.digitalasset.canton.crypto.SignatureCheckError.{
  SignatureWithWrongKey,
  SignerHasNoValidKeys,
}
import com.digitalasset.canton.crypto.SyncCryptoError.{KeyNotAvailable, SyncCryptoEncryptionError}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.serialization.DeserializationError
import com.digitalasset.canton.topology._
import com.digitalasset.canton.topology.client.{
  DomainTopologyClient,
  IdentityProvidingServiceClient,
  TopologyClientApi,
  TopologySnapshot,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.HasVersionedToByteString
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{DomainAlias, DomainId}
import com.google.protobuf.ByteString

import scala.concurrent.{ExecutionContext, Future}

/** Crypto API Provider class
  *
  * The utility class combines the information provided by the IPSclient, the pure crypto functions
  * and the signing and decryption operations on a private key vault in order to automatically resolve
  * the right keys to use for signing / decryption based on domain and timestamp.
  */
class SyncCryptoApiProvider(
    val owner: KeyOwner,
    val ips: IdentityProvidingServiceClient,
    val crypto: Crypto,
    cachingConfigs: CachingConfigs,
    loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext) {

  require(ips != null)

  def pureCrypto: CryptoPureApi = crypto.pureCrypto

  def tryForDomain(domain: DomainId, alias: Option[DomainAlias] = None): DomainSyncCryptoClient =
    new DomainSyncCryptoClient(
      owner,
      domain,
      ips.tryForDomain(domain),
      crypto,
      cachingConfigs,
      alias.fold(loggerFactory)(alias => loggerFactory.append("domain", alias.unwrap)),
    )

  def forDomain(domain: DomainId): Option[DomainSyncCryptoClient] =
    for {
      dips <- ips.forDomain(domain)
    } yield new DomainSyncCryptoClient(owner, domain, dips, crypto, cachingConfigs, loggerFactory)
}

trait SyncCryptoClient extends TopologyClientApi[SyncCryptoApi] {

  def pureCrypto: CryptoPureApi

  /** Returns a snapshot of the current member topology for the given domain.
    * The future will log a warning and await the snapshot if the data is not there yet.
    */
  def ipsSnapshot(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[TopologySnapshot]

  /** Returns a snapshot of the current member topology for the given domain
    *
    * The future will wait for the data if the data is not there yet.
    */
  def awaitIpsSnapshot(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[TopologySnapshot]

}

/** Crypto operations on a particular domain
  */
class DomainSyncCryptoClient(
    val owner: KeyOwner,
    val domainId: DomainId,
    val ips: DomainTopologyClient,
    val crypto: Crypto,
    cacheConfigs: CachingConfigs,
    override val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends SyncCryptoClient
    with NamedLogging {

  override def pureCrypto: CryptoPureApi = crypto.pureCrypto

  override def snapshot(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[DomainSnapshotSyncCryptoApi] =
    ips.snapshot(timestamp).map(create)

  override def trySnapshot(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): DomainSnapshotSyncCryptoApi =
    create(ips.trySnapshot(timestamp))

  override def headSnapshot(implicit traceContext: TraceContext): DomainSnapshotSyncCryptoApi =
    create(ips.headSnapshot)

  override def awaitSnapshot(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[DomainSnapshotSyncCryptoApi] =
    ips.awaitSnapshot(timestamp).map(create)

  override def awaitSnapshotUS(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[SyncCryptoApi] =
    ips.awaitSnapshotUS(timestamp).map(create)

  private def create(snapshot: TopologySnapshot): DomainSnapshotSyncCryptoApi = {
    new DomainSnapshotSyncCryptoApi(
      owner,
      domainId,
      snapshot,
      crypto,
      ts => EitherT(mySigningKeyCache.get(ts)),
      cacheConfigs.keyCache,
      loggerFactory,
    )
  }

  private val mySigningKeyCache = cacheConfigs.mySigningKeyCache
    .buildScaffeine()
    .buildAsyncFuture[CantonTimestamp, Either[SyncCryptoError, Fingerprint]](
      findSigningKey(_).value
    )

  private def findSigningKey(
      referenceTime: CantonTimestamp
  ): EitherT[Future, SyncCryptoError, Fingerprint] = {
    import TraceContext.Implicits.Empty._
    for {
      snapshot <- EitherT.right(ipsSnapshot(referenceTime))
      signingKeys <- EitherT.right(snapshot.signingKeys(owner))
      existingKeys <- signingKeys.toList
        .filterA(pk => crypto.cryptoPrivateStore.existsSigningKey(pk.fingerprint))
        .leftMap[SyncCryptoError](SyncCryptoError.StoreError)
      kk <- existingKeys.lastOption
        .toRight[SyncCryptoError](
          SyncCryptoError.KeyNotAvailable(owner, KeyPurpose.Signing, snapshot.timestamp)
        )
        .toEitherT[Future]
    } yield kk.fingerprint

  }

  override def ipsSnapshot(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[TopologySnapshot] =
    ips.snapshot(timestamp)

  override def awaitIpsSnapshot(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[TopologySnapshot] =
    ips.awaitSnapshot(timestamp)

  override def snapshotAvailable(timestamp: CantonTimestamp): Boolean =
    ips.snapshotAvailable(timestamp)

  override def awaitTimestamp(
      timestamp: CantonTimestamp,
      waitForEffectiveTime: Boolean,
  )(implicit traceContext: TraceContext): Option[Future[Unit]] =
    ips.awaitTimestamp(timestamp, waitForEffectiveTime)

  override def awaitTimestampUS(timestamp: CantonTimestamp, waitForEffectiveTime: Boolean)(implicit
      traceContext: TraceContext
  ): Option[FutureUnlessShutdown[Unit]] =
    ips.awaitTimestampUS(timestamp, waitForEffectiveTime)

  override def currentSnapshotApproximation(implicit
      traceContext: TraceContext
  ): DomainSnapshotSyncCryptoApi =
    create(ips.currentSnapshotApproximation)

  override def topologyKnownUntilTimestamp: CantonTimestamp = ips.topologyKnownUntilTimestamp

  override def approximateTimestamp: CantonTimestamp = ips.approximateTimestamp

}

/** crypto operations for a (domain,timestamp) */
class DomainSnapshotSyncCryptoApi(
    val owner: KeyOwner,
    val domainId: DomainId,
    val ipsSnapshot: TopologySnapshot,
    val crypto: Crypto,
    fetchSigningKey: CantonTimestamp => EitherT[Future, SyncCryptoError, Fingerprint],
    validKeysCacheConfig: CacheConfig,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends SyncCryptoApi
    with NamedLogging {

  override val pureCrypto: CryptoPureApi = crypto.pureCrypto
  private val validKeysCache =
    validKeysCacheConfig
      .buildScaffeine()
      .buildAsyncFuture[KeyOwner, Map[Fingerprint, SigningPublicKey]](loadSigningKeysForOwner)

  /** Sign given hash with signing key for (owner, domain, timestamp)
    */
  override def sign(
      hash: Hash
  )(implicit traceContext: TraceContext): EitherT[Future, SyncCryptoError, Signature] =
    for {
      fingerprint <- fetchSigningKey(ipsSnapshot.referenceTime)
      signature <- crypto.privateCrypto
        .sign(hash, fingerprint)
        .leftMap[SyncCryptoError](SyncCryptoError.SyncCryptoSigningError)
    } yield signature

  private def loadSigningKeysForOwner(
      owner: KeyOwner
  ): Future[Map[Fingerprint, SigningPublicKey]] =
    ipsSnapshot.signingKeys(owner).map(_.map(x => (x.fingerprint, x)).toMap)

  /** Verify signature of a given owner
    *
    * Convenience method to lookup a key of a given owner, domain and timestamp and verify the result.
    */
  override def verifySignature(
      hash: Hash,
      signer: KeyOwner,
      signature: Signature,
  ): EitherT[Future, SignatureCheckError, Unit] = {
    val validKeysET = EitherT.right(validKeysCache.get(signer))
    def signatureCheckFailed(
        validKeys: Seq[SigningPublicKey]
    ): Either[SignatureCheckError, Unit] = {
      val error =
        if (validKeys.isEmpty)
          SignerHasNoValidKeys(
            s"There are no valid keys for ${signer} but received message signed with ${signature.signedBy}"
          )
        else
          SignatureWithWrongKey(
            s"Key ${signature.signedBy} used to generate signature is not a valid key for ${signer}. Valid keys are ${validKeys
              .map(_.fingerprint.unwrap)}"
          )
      Left(error)
    }

    validKeysET.flatMap { validKeys =>
      lazy val keysAsSeq = validKeys.values.toSeq
      validKeys.get(signature.signedBy) match {
        case Some(key) =>
          EitherT.fromEither(crypto.pureCrypto.verifySignature(hash, key, signature))
        case None if signer.uid == domainId.unwrap && signer.code == SequencerId.Code =>
          // skip signature verification if we are not yet initialized
          // TODO(i4639) improve the situation such that we don't have to trust TLS upon reconnect/initial connect
          ownerIsInitialized(keysAsSeq).subflatMap {
            case false => Right(())
            case true => signatureCheckFailed(keysAsSeq)
          }
        case None =>
          EitherT.fromEither(signatureCheckFailed(keysAsSeq))
      }
    }
  }

  private def ownerIsInitialized(
      validKeys: Seq[SigningPublicKey]
  ): EitherT[Future, SignatureCheckError, Boolean] =
    owner match {
      case participant: ParticipantId => EitherT.right(ipsSnapshot.isParticipantActive(participant))
      case _ => // we assume that a member other than a participant is initialised if at least one valid sequencer key is known
        EitherT.rightT(validKeys.nonEmpty)
    }

  override def decrypt[M](encryptedMessage: Encrypted[M])(
      deserialize: ByteString => Either[DeserializationError, M]
  ): EitherT[Future, SyncCryptoError, M] = {
    EitherT(
      ipsSnapshot
        // TODO(i7659) allow use of different keys and not only just first one
        .encryptionKey(owner)
        .map { keyO =>
          keyO
            .toRight(
              KeyNotAvailable(owner, KeyPurpose.Encryption, ipsSnapshot.timestamp): SyncCryptoError
            )
        }
    )
      .flatMap(key =>
        crypto.privateCrypto
          .decrypt(encryptedMessage, key.fingerprint)(deserialize)
          .leftMap(err => SyncCryptoError.SyncCryptoDecryptionError(err))
      )
  }

  /** Encrypts a message for the given key owner
    *
    * Utility method to lookup a key on an IPS snapshot and then encrypt the given message with the
    * most suitable key for the respective key owner.
    */
  override def encryptFor[M <: HasVersionedToByteString](
      message: M,
      owner: KeyOwner,
      version: ProtocolVersion,
  ): EitherT[Future, SyncCryptoError, Encrypted[M]] =
    EitherT(
      ipsSnapshot
        .encryptionKey(owner)
        .map { keyO =>
          keyO
            .toRight(KeyNotAvailable(owner, KeyPurpose.Encryption, ipsSnapshot.timestamp))
            .flatMap(k =>
              crypto.pureCrypto
                .encryptWith(message, k, version)
                .leftMap(SyncCryptoEncryptionError)
            )
        }
    )
}
