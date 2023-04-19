// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.store

import cats.data.EitherT
import cats.syntax.functor.*
import com.digitalasset.canton.config.CantonRequireTypes.String300
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.KeyPurpose.{Encryption, Signing}
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.store.db.{DbCryptoPrivateStore, StoredPrivateKey}
import com.digitalasset.canton.crypto.store.memory.InMemoryCryptoPrivateStore
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.resource.{DbStorage, MemoryStorage, Storage}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ReleaseProtocolVersion
import com.google.common.annotations.VisibleForTesting

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}

sealed trait PrivateKeyWithName extends Product with Serializable {
  type K <: PrivateKey
  def privateKey: K
  def name: Option[KeyName]
}

final case class SigningPrivateKeyWithName(
    override val privateKey: SigningPrivateKey,
    override val name: Option[KeyName],
) extends PrivateKeyWithName {
  type K = SigningPrivateKey
}

final case class EncryptionPrivateKeyWithName(
    override val privateKey: EncryptionPrivateKey,
    override val name: Option[KeyName],
) extends PrivateKeyWithName {
  type K = EncryptionPrivateKey
}

/** A store for cryptographic private material such as signing/encryption private keys and hmac secrets.
  *
  * The cache provides a write-through cache such that `get` operations can be served without reading from the async store.
  * Async population of the cache is done at creation time.
  */
trait CryptoPrivateStore extends AutoCloseable { this: NamedLogging =>

  implicit val ec: ExecutionContext

  protected val releaseProtocolVersion: ReleaseProtocolVersion

  // Cached values for keys and secret
  protected val signingKeyMap: TrieMap[Fingerprint, SigningPrivateKeyWithName] = TrieMap.empty
  protected val decryptionKeyMap: TrieMap[Fingerprint, EncryptionPrivateKeyWithName] = TrieMap.empty

  // Write methods that the underlying store has to implement.
  private[crypto] def writePrivateKey(
      key: StoredPrivateKey
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, CryptoPrivateStoreError, Unit]

  private[crypto] def readPrivateKey(keyId: Fingerprint, purpose: KeyPurpose)(implicit
      traceContext: TraceContext
  ): EitherT[Future, CryptoPrivateStoreError, Option[StoredPrivateKey]]

  @VisibleForTesting
  private[canton] def listPrivateKeys(purpose: KeyPurpose, encrypted: Boolean)(implicit
      traceContext: TraceContext
  ): EitherT[Future, CryptoPrivateStoreError, Set[StoredPrivateKey]]

  private[crypto] def deletePrivateKey(keyId: Fingerprint)(implicit
      traceContext: TraceContext
  ): EitherT[Future, CryptoPrivateStoreError, Unit]

  /** Replaces a set of keys transactionally to avoid an inconsistent state of the store.
    * Key ids will remain the same while replacing these keys.
    *
    * @param newKeys sequence of keys to replace
    */
  private[crypto] def replaceStoredPrivateKeys(newKeys: Seq[StoredPrivateKey])(implicit
      traceContext: TraceContext
  ): EitherT[Future, CryptoPrivateStoreError, Unit]

  def exportPrivateKey(keyId: Fingerprint)(implicit
      traceContext: TraceContext
  ): EitherT[Future, CryptoPrivateStoreError, Option[PrivateKey]] = {
    logger.info(s"Exporting private key: $keyId")
    for {
      sigKey <- signingKey(keyId).widen[Option[PrivateKey]]
      key <- sigKey.fold(decryptionKey(keyId).widen[Option[PrivateKey]])(key =>
        EitherT.rightT(Some(key))
      )
    } yield key
  }

  def existsPrivateKey(
      keyId: Fingerprint
  )(implicit traceContext: TraceContext): EitherT[Future, CryptoPrivateStoreError, Boolean] =
    for {
      existsSigKey <- existsSigningKey(keyId)
      existsDecryptKey <- existsDecryptionKey(keyId)
    } yield existsSigKey || existsDecryptKey

  def storePrivateKey(key: PrivateKey, name: Option[KeyName])(implicit
      traceContext: TraceContext
  ): EitherT[Future, CryptoPrivateStoreError, Unit] = {
    (key: @unchecked) match {
      case signingPrivateKey: SigningPrivateKey => storeSigningKey(signingPrivateKey, name)
      case encryptionPrivateKey: EncryptionPrivateKey =>
        storeDecryptionKey(encryptionPrivateKey, name)
    }
  }

  def removePrivateKey(
      keyId: Fingerprint
  )(implicit traceContext: TraceContext): EitherT[Future, CryptoPrivateStoreError, Unit] = {
    val deletedSigKey = signingKeyMap.remove(keyId)
    val deletedDecKey = decryptionKeyMap.remove(keyId)

    deletePrivateKey(keyId).leftMap { err =>
      // In case the deletion in the persistence layer failed, we have to restore the cache.
      deletedSigKey.foreach(signingKeyMap.put(keyId, _))
      deletedDecKey.foreach(decryptionKeyMap.put(keyId, _))
      err
    }
  }

  private def readAndParsePrivateKey[A <: PrivateKey, B <: PrivateKeyWithName](
      keyPurpose: KeyPurpose,
      parsingFunc: StoredPrivateKey => ParsingResult[A],
      buildKeyWithNameFunc: (A, Option[KeyName]) => B,
  )(keyId: Fingerprint)(implicit
      traceContext: TraceContext
  ): EitherT[Future, CryptoPrivateStoreError, Option[B]] =
    readPrivateKey(keyId, keyPurpose)
      .flatMap {
        case Some(storedPrivateKey) =>
          parsingFunc(storedPrivateKey) match {
            case Left(parseErr) =>
              EitherT.leftT[Future, Option[B]](
                CryptoPrivateStoreError
                  .FailedToReadKey(
                    keyId,
                    s"could not parse stored key (it can either be corrupted or encrypted): ${parseErr.toString}",
                  )
              )
            case Right(privateKey) =>
              EitherT.rightT[Future, CryptoPrivateStoreError](
                Some(buildKeyWithNameFunc(privateKey, storedPrivateKey.name))
              )
          }
        case None => EitherT.rightT[Future, CryptoPrivateStoreError](None)
      }

  private[crypto] def signingKey(signingKeyId: Fingerprint)(implicit
      traceContext: TraceContext
  ): EitherT[Future, CryptoPrivateStoreError, Option[SigningPrivateKey]] =
    retrieveAndUpdateCache(
      signingKeyMap,
      keyFingerprint =>
        readAndParsePrivateKey[SigningPrivateKey, SigningPrivateKeyWithName](
          Signing,
          key => SigningPrivateKey.fromByteString(key.data),
          (privateKey, name) => SigningPrivateKeyWithName(privateKey, name),
        )(keyFingerprint),
    )(signingKeyId)

  private[crypto] def storeSigningKey(
      key: SigningPrivateKey,
      name: Option[KeyName],
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, CryptoPrivateStoreError, Unit] =
    for {
      _ <- writePrivateKey(
        new StoredPrivateKey(
          id = key.id,
          data = key.toByteString(releaseProtocolVersion.v),
          purpose = key.purpose,
          name = name,
          wrapperKeyId = None,
        )
      )
        .map { _ =>
          signingKeyMap.put(key.id, SigningPrivateKeyWithName(key, name)).discard
        }
    } yield ()

  def existsSigningKey(signingKeyId: Fingerprint)(implicit
      traceContext: TraceContext
  ): EitherT[Future, CryptoPrivateStoreError, Boolean] =
    signingKey(signingKeyId).map(_.nonEmpty)

  private[crypto] def decryptionKey(encryptionKeyId: Fingerprint)(implicit
      traceContext: TraceContext
  ): EitherT[Future, CryptoPrivateStoreError, Option[EncryptionPrivateKey]] = {
    retrieveAndUpdateCache(
      decryptionKeyMap,
      keyFingerprint =>
        readAndParsePrivateKey[EncryptionPrivateKey, EncryptionPrivateKeyWithName](
          Encryption,
          key => EncryptionPrivateKey.fromByteString(key.data),
          (privateKey, name) => EncryptionPrivateKeyWithName(privateKey, name),
        )(keyFingerprint),
    )(encryptionKeyId)
  }

  private[crypto] def storeDecryptionKey(
      key: EncryptionPrivateKey,
      name: Option[KeyName],
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, CryptoPrivateStoreError, Unit] =
    for {
      _ <- writePrivateKey(
        new StoredPrivateKey(
          id = key.id,
          data = key.toByteString(releaseProtocolVersion.v),
          purpose = key.purpose,
          name = name,
          wrapperKeyId = None,
        )
      )
        .map { _ =>
          decryptionKeyMap.put(key.id, EncryptionPrivateKeyWithName(key, name)).discard
        }
    } yield ()

  def existsDecryptionKey(decryptionKeyId: Fingerprint)(implicit
      traceContext: TraceContext
  ): EitherT[Future, CryptoPrivateStoreError, Boolean] =
    decryptionKey(decryptionKeyId).map(_.nonEmpty)

  private def retrieveAndUpdateCache[KN <: PrivateKeyWithName](
      cache: TrieMap[Fingerprint, KN],
      readKey: Fingerprint => EitherT[Future, CryptoPrivateStoreError, Option[KN]],
  )(keyId: Fingerprint): EitherT[Future, CryptoPrivateStoreError, Option[KN#K]] =
    cache.get(keyId) match {
      case Some(value) => EitherT.rightT(Some(value.privateKey))
      case None =>
        readKey(keyId).map { keyOption =>
          keyOption.foreach(key => cache.putIfAbsent(keyId, key))
          keyOption.map(_.privateKey)
        }
    }

  /** Returns the wrapper key used to encrypt the private key
    * or None if private key is not encrypted.
    *
    * @param keyId private key fingerprint
    * @return the wrapper key used for encryption or None if key is not encrypted
    */
  private[crypto] def encrypted(keyId: Fingerprint)(implicit
      traceContext: TraceContext
  ): EitherT[Future, CryptoPrivateStoreError, Option[String300]]
}

object CryptoPrivateStore {
  trait CryptoPrivateStoreFactory {
    def create(
        storage: Storage,
        releaseProtocolVersion: ReleaseProtocolVersion,
        timeouts: ProcessingTimeout,
        loggerFactory: NamedLoggerFactory,
    )(implicit ec: ExecutionContext): EitherT[Future, CryptoPrivateStoreError, CryptoPrivateStore]
  }

  class CommunityCryptoPrivateStoreFactory extends CryptoPrivateStoreFactory {
    override def create(
        storage: Storage,
        releaseProtocolVersion: ReleaseProtocolVersion,
        timeouts: ProcessingTimeout,
        loggerFactory: NamedLoggerFactory,
    )(implicit ec: ExecutionContext): EitherT[Future, CryptoPrivateStoreError, CryptoPrivateStore] =
      storage match {
        case _: MemoryStorage =>
          EitherT.rightT[Future, CryptoPrivateStoreError](
            new InMemoryCryptoPrivateStore(releaseProtocolVersion, loggerFactory)
          )
        case jdbc: DbStorage =>
          EitherT.rightT[Future, CryptoPrivateStoreError](
            new DbCryptoPrivateStore(jdbc, releaseProtocolVersion, timeouts, loggerFactory)
          )
      }
  }
}

sealed trait CryptoPrivateStoreError extends Product with Serializable with PrettyPrinting
object CryptoPrivateStoreError {

  final case class FailedToListKeys(reason: String) extends CryptoPrivateStoreError {
    override def pretty: Pretty[FailedToListKeys] = prettyOfClass(unnamedParam(_.reason.unquoted))
  }

  final case class FailedToGetWrapperKeyId(reason: String) extends CryptoPrivateStoreError {
    override def pretty: Pretty[FailedToGetWrapperKeyId] = prettyOfClass(
      unnamedParam(_.reason.unquoted)
    )
  }

  final case class FailedToReadKey(keyId: Fingerprint, reason: String)
      extends CryptoPrivateStoreError {
    override def pretty: Pretty[FailedToReadKey] = prettyOfClass(unnamedParam(_.reason.unquoted))
  }

  final case class FailedToInsertKey(keyId: Fingerprint, reason: String)
      extends CryptoPrivateStoreError {
    override def pretty: Pretty[FailedToInsertKey] =
      prettyOfClass(param("keyId", _.keyId), param("reason", _.reason.unquoted))
  }

  final case class KeyAlreadyExists(keyId: Fingerprint, existingKeyName: Option[String])
      extends CryptoPrivateStoreError {
    override def pretty: Pretty[KeyAlreadyExists] =
      prettyOfClass(
        param("keyId", _.keyId),
        param("existingKeyName", _.existingKeyName.getOrElse("").unquoted),
      )
  }

  final case class FailedToDeleteKey(keyId: Fingerprint, reason: String)
      extends CryptoPrivateStoreError {
    override def pretty: Pretty[FailedToDeleteKey] =
      prettyOfClass(param("keyId", _.keyId), param("reason", _.reason.unquoted))
  }

  final case class FailedToReplaceKeys(keyId: Seq[Fingerprint], reason: String)
      extends CryptoPrivateStoreError {
    override def pretty: Pretty[FailedToReplaceKeys] =
      prettyOfClass(param("keyId", _.keyId), param("reason", _.reason.unquoted))
  }

  final case class EncryptedPrivateStoreError(reason: String) extends CryptoPrivateStoreError {
    override def pretty: Pretty[EncryptedPrivateStoreError] = prettyOfClass(
      unnamedParam(_.reason.unquoted)
    )
  }

  final case class WrapperKeyAlreadyInUse(reason: String) extends CryptoPrivateStoreError {
    override def pretty: Pretty[WrapperKeyAlreadyInUse] = prettyOfClass(
      unnamedParam(_.reason.unquoted)
    )
  }
}