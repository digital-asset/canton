// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.store

import cats.data.EitherT
import cats.syntax.functor._
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.KeyPurpose.{Encryption, Signing}
import com.digitalasset.canton.crypto._
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

case class SigningPrivateKeyWithName(
    override val privateKey: SigningPrivateKey,
    override val name: Option[KeyName],
) extends PrivateKeyWithName {
  type K = SigningPrivateKey
}

case class EncryptionPrivateKeyWithName(
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
  private[store] def listPrivateKeys(purpose: KeyPurpose, encrypted: Boolean)(implicit
      traceContext: TraceContext
  ): EitherT[Future, CryptoPrivateStoreError, Set[StoredPrivateKey]]

  private[crypto] def deletePrivateKey(keyId: Fingerprint)(implicit
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

  case class FailedToListKeys(reason: String) extends CryptoPrivateStoreError {
    override def pretty: Pretty[FailedToListKeys] = prettyOfClass(unnamedParam(_.reason.unquoted))
  }

  case class FailedToGetWrapperKeyId(reason: String) extends CryptoPrivateStoreError {
    override def pretty: Pretty[FailedToGetWrapperKeyId] = prettyOfClass(
      unnamedParam(_.reason.unquoted)
    )
  }

  case class FailedToReadKey(keyId: Fingerprint, reason: String) extends CryptoPrivateStoreError {
    override def pretty: Pretty[FailedToReadKey] = prettyOfClass(unnamedParam(_.reason.unquoted))
  }

  case class FailedToInsertKey(keyId: Fingerprint, reason: String) extends CryptoPrivateStoreError {
    override def pretty: Pretty[FailedToInsertKey] =
      prettyOfClass(param("keyId", _.keyId), param("reason", _.reason.unquoted))
  }

  case class KeyAlreadyExists(keyId: Fingerprint, existingKeyName: Option[String])
      extends CryptoPrivateStoreError {
    override def pretty: Pretty[KeyAlreadyExists] =
      prettyOfClass(
        param("keyId", _.keyId),
        param("existingKeyName", _.existingKeyName.getOrElse("").unquoted),
      )
  }

  case class FailedToDeleteKey(keyId: Fingerprint, reason: String) extends CryptoPrivateStoreError {
    override def pretty: Pretty[FailedToDeleteKey] =
      prettyOfClass(param("keyId", _.keyId), param("reason", _.reason.unquoted))
  }

  case class EncryptedPrivateStoreError(reason: String) extends CryptoPrivateStoreError {
    override def pretty: Pretty[EncryptedPrivateStoreError] = prettyOfClass(
      unnamedParam(_.reason.unquoted)
    )
  }
}
