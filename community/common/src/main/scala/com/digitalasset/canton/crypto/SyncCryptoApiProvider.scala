// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.traverseFilter.*
import com.digitalasset.canton.concurrent.{FutureSupervisor, HasFutureSupervision}
import com.digitalasset.canton.config.{CacheConfig, CachingConfigs, ProcessingTimeout}
import com.digitalasset.canton.crypto.SignatureCheckError.{
  SignatureWithWrongKey,
  SignerHasNoValidKeys,
}
import com.digitalasset.canton.crypto.SyncCryptoError.{KeyNotAvailable, SyncCryptoEncryptionError}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown, Lifecycle}
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.serialization.DeserializationError
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.{
  DomainTopologyClient,
  IdentityProvidingServiceClient,
  TopologyClientApi,
  TopologySnapshot,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.LoggerUtil
import com.digitalasset.canton.version.{HasVersionedToByteString, ProtocolVersion}
import com.digitalasset.canton.{DomainAlias, checked}
import com.google.protobuf.ByteString
import org.slf4j.event.Level

import scala.concurrent.duration.*
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
    timeouts: ProcessingTimeout,
    futureSupervisor: FutureSupervisor,
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
      timeouts,
      futureSupervisor,
      alias.fold(loggerFactory)(alias => loggerFactory.append("domain", alias.unwrap)),
    )

  def forDomain(domain: DomainId): Option[DomainSyncCryptoClient] =
    for {
      dips <- ips.forDomain(domain)
    } yield new DomainSyncCryptoClient(
      owner,
      domain,
      dips,
      crypto,
      cachingConfigs,
      timeouts,
      futureSupervisor,
      loggerFactory,
    )
}

trait SyncCryptoClient[+T <: SyncCryptoApi] extends TopologyClientApi[T] {
  this: HasFutureSupervision =>

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

  def awaitIpsSnapshotUS(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[TopologySnapshot]

  def awaitIpsSnapshotUSSupervised(description: => String, warnAfter: Duration = 10.seconds)(
      timestamp: CantonTimestamp
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[TopologySnapshot] =
    supervisedUS(description, warnAfter)(awaitIpsSnapshotUS(timestamp))

}

object SyncCryptoClient {

  /** Computes the snapshot for the desired timestamp, assuming that the last (relevant) update to the
    * topology state happened at or before `previousTimestamp`.
    * If `previousTimestampO` is [[scala.None$]] and `desiredTimestamp` is currently not known
    * [[com.digitalasset.canton.topology.client.TopologyClientApi.topologyKnownUntilTimestamp]],
    * then the current approximation is returned and if `warnIfApproximate` is set a warning is logged.
    */
  def getSnapshotForTimestamp(
      client: SyncCryptoClient[SyncCryptoApi],
      desiredTimestamp: CantonTimestamp,
      previousTimestampO: Option[CantonTimestamp],
      protocolVersion: ProtocolVersion,
      warnIfApproximate: Boolean = true,
  )(implicit
      executionContext: ExecutionContext,
      loggingContext: ErrorLoggingContext,
  ): Future[SyncCryptoApi] = {
    implicit val traceContext: TraceContext = loggingContext.traceContext
    val knownUntil = client.topologyKnownUntilTimestamp
    if (desiredTimestamp <= knownUntil) {
      client.snapshot(desiredTimestamp)
    } else {
      loggingContext.logger.debug(
        s"Waiting for topology snapshot at $desiredTimestamp; known until $knownUntil; previous $previousTimestampO"
      )
      previousTimestampO match {
        case None =>
          val approximateSnapshot = client.currentSnapshotApproximation
          LoggerUtil.logAtLevel(
            if (warnIfApproximate) Level.WARN else Level.INFO,
            s"Using approximate topology snapshot at ${approximateSnapshot.ipsSnapshot.timestamp} for desired timestamp $desiredTimestamp",
          )
          Future.successful(approximateSnapshot)
        case Some(previousTimestamp) =>
          if (desiredTimestamp <= previousTimestamp.immediateSuccessor)
            client.awaitSnapshotSupervised(
              s"requesting topology snapshot at $desiredTimestamp with update timestamp $previousTimestamp and known until $knownUntil"
            )(desiredTimestamp)
          else {
            import scala.Ordered.orderingToOrdered
            for {
              previousSnapshot <- client.awaitSnapshotSupervised(
                s"searching for topology change delay at $previousTimestamp for desired timestamp $desiredTimestamp and known until $knownUntil"
              )(previousTimestamp)
              previousDomainParams <- previousSnapshot.ipsSnapshot
                .findDynamicDomainParametersOrDefault(
                  protocolVersion = protocolVersion,
                  warnOnUsingDefault = false,
                )

              delay = previousDomainParams.topologyChangeDelay
              diff = desiredTimestamp - previousTimestamp
              snapshotTimestamp =
                if (diff > delay.unwrap) {
                  // `desiredTimestamp` is larger than `previousTimestamp` plus the `delay`,
                  // so timestamps cannot overflow here
                  checked(previousTimestamp.plus(delay.unwrap).immediateSuccessor)
                } else desiredTimestamp
              desiredSnapshot <- client.awaitSnapshotSupervised(
                s"requesting topology snapshot at $snapshotTimestamp for desired timestamp $desiredTimestamp given previous timestamp $previousTimestamp with topology change delay $delay"
              )(snapshotTimestamp)
            } yield desiredSnapshot
          }
      }
    }
  }
}

/** Crypto operations on a particular domain
  */
class DomainSyncCryptoClient(
    val owner: KeyOwner,
    val domainId: DomainId,
    val ips: DomainTopologyClient,
    val crypto: Crypto,
    cacheConfigs: CachingConfigs,
    override val timeouts: ProcessingTimeout,
    override protected val futureSupervisor: FutureSupervisor,
    override val loggerFactory: NamedLoggerFactory,
)(implicit override protected val executionContext: ExecutionContext)
    extends SyncCryptoClient[DomainSnapshotSyncCryptoApi]
    with HasFutureSupervision
    with NamedLogging
    with FlagCloseable {

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
  ): FutureUnlessShutdown[DomainSnapshotSyncCryptoApi] =
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
    import TraceContext.Implicits.Empty.*
    for {
      snapshot <- EitherT.right(ipsSnapshot(referenceTime))
      signingKeys <- EitherT.right(snapshot.signingKeys(owner))
      existingKeys <- signingKeys.toList
        .filterA(pk => crypto.cryptoPrivateStore.existsSigningKey(pk.fingerprint))
        .leftMap[SyncCryptoError](SyncCryptoError.StoreError)
      kk <- existingKeys.lastOption
        .toRight[SyncCryptoError](
          SyncCryptoError
            .KeyNotAvailable(
              owner,
              KeyPurpose.Signing,
              snapshot.timestamp,
              signingKeys.map(_.fingerprint),
            )
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

  override def awaitIpsSnapshotUS(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[TopologySnapshot] =
    ips.awaitSnapshotUS(timestamp)

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

  override def onClosed(): Unit = Lifecycle.close(ips)(logger)
}

/** crypto operations for a (domain,timestamp) */
class DomainSnapshotSyncCryptoApi(
    val owner: KeyOwner,
    val domainId: DomainId,
    override val ipsSnapshot: TopologySnapshot,
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
      case _ => // we assume that a member other than a participant is initialised if at least one valid key is known
        EitherT.rightT(validKeys.nonEmpty)
    }

  override def decrypt[M](encryptedMessage: Encrypted[M])(
      deserialize: ByteString => Either[DeserializationError, M]
  ): EitherT[Future, SyncCryptoError, M] = {
    EitherT(
      ipsSnapshot
        .encryptionKey(owner)
        .map { keyO =>
          keyO
            .toRight(
              KeyNotAvailable(
                owner,
                KeyPurpose.Encryption,
                ipsSnapshot.timestamp,
                Seq.empty,
              ): SyncCryptoError
            )
        }
    )
      // TODO (error handling) better alert / error message if given key does not exist locally
      .flatMap(key =>
        crypto.privateCrypto
          .decrypt(AsymmetricEncrypted(encryptedMessage.ciphertext, key.fingerprint))(
            deserialize
          )
          .leftMap(err => SyncCryptoError.SyncCryptoDecryptionError(err))
      )
  }

  override def decrypt[M](encryptedMessage: AsymmetricEncrypted[M])(
      deserialize: ByteString => Either[DeserializationError, M]
  ): EitherT[Future, SyncCryptoError, M] = {
    crypto.privateCrypto
      .decrypt(encryptedMessage)(deserialize)
      .leftMap[SyncCryptoError](err => SyncCryptoError.SyncCryptoDecryptionError(err))
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
  ): EitherT[Future, SyncCryptoError, AsymmetricEncrypted[M]] =
    EitherT(
      ipsSnapshot
        .encryptionKey(owner)
        .map { keyO =>
          keyO
            .toRight(
              KeyNotAvailable(owner, KeyPurpose.Encryption, ipsSnapshot.timestamp, Seq.empty)
            )
            .flatMap(k =>
              // TODO (error handling): better error message if given key does not exist locally
              crypto.pureCrypto
                .encryptWith(message, k, version)
                .leftMap(SyncCryptoEncryptionError)
            )
        }
    )
}
