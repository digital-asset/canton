// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.store

import java.util.concurrent.atomic.AtomicReference

import cats.data.EitherT
import cats.syntax.functor._
import com.digitalasset.canton.crypto.KeyName
import com.digitalasset.canton.crypto.store.db.DbCryptoPrivateStore
import com.digitalasset.canton.crypto.store.memory.InMemoryCryptoPrivateStore
import com.digitalasset.canton.crypto._
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.resource.{DbStorage, MemoryStorage, Storage}
import com.digitalasset.canton.tracing.TraceContext
import com.google.common.annotations.VisibleForTesting
import slick.jdbc.GetResult

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

object SigningPrivateKeyWithName {
  implicit def getResultSigningPrivateKeyWithName(implicit
      getResultByteArray: GetResult[Array[Byte]]
  ): GetResult[SigningPrivateKeyWithName] =
    GetResult { r =>
      SigningPrivateKeyWithName(r.<<, r.<<)
    }
}

case class EncryptionPrivateKeyWithName(
    override val privateKey: EncryptionPrivateKey,
    override val name: Option[KeyName],
) extends PrivateKeyWithName {
  type K = EncryptionPrivateKey
}

object EncryptionPrivateKeyWithName {
  implicit def getResultEncryptionPrivateKeyWithName(implicit
      getResultByteArray: GetResult[Array[Byte]]
  ): GetResult[EncryptionPrivateKeyWithName] =
    GetResult { r =>
      EncryptionPrivateKeyWithName(r.<<, r.<<)
    }
}

/** A store for cryptographic private material such as signing/encryption private keys and hmac secrets.
  *
  * The cache provides a write-through cache such that `get` operations can be served without reading from the async store.
  * Async population of the cache is done at creation time.
  */
trait CryptoPrivateStore { this: NamedLogging =>

  implicit val ec: ExecutionContext

  // Cached values for keys and secret
  protected val signingKeyMap: TrieMap[Fingerprint, SigningPrivateKeyWithName] = TrieMap.empty
  protected val decryptionKeyMap: TrieMap[Fingerprint, EncryptionPrivateKeyWithName] = TrieMap.empty
  protected val hmacSecretRef: AtomicReference[Option[HmacSecret]] = new AtomicReference(None)

  // Write methods that the underlying store has to implement.
  protected def writeSigningKey(key: SigningPrivateKey, name: Option[KeyName])(implicit
      traceContext: TraceContext
  ): EitherT[Future, CryptoPrivateStoreError, Unit]
  protected def writeDecryptionKey(key: EncryptionPrivateKey, name: Option[KeyName])(implicit
      traceContext: TraceContext
  ): EitherT[Future, CryptoPrivateStoreError, Unit]
  protected def writeHmacSecret(hmacSecret: HmacSecret)(implicit
      traceContext: TraceContext
  ): EitherT[Future, CryptoPrivateStoreError, Unit]

  @VisibleForTesting
  private[store] def listSigningKeys(implicit
      traceContext: TraceContext
  ): EitherT[Future, CryptoPrivateStoreError, Set[SigningPrivateKeyWithName]]
  @VisibleForTesting
  private[store] def listDecryptionKeys(implicit
      traceContext: TraceContext
  ): EitherT[Future, CryptoPrivateStoreError, Set[EncryptionPrivateKeyWithName]]
  @VisibleForTesting
  private[store] def loadHmacSecret()(implicit
      traceContext: TraceContext
  ): EitherT[Future, CryptoPrivateStoreError, Option[HmacSecret]]

  protected def deletePrivateKey(keyId: Fingerprint)(implicit
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

  private[crypto] def signingKey(signingKeyId: Fingerprint)(implicit
      traceContext: TraceContext
  ): EitherT[Future, CryptoPrivateStoreError, Option[SigningPrivateKey]] =
    retrieveAndUpdateCache(signingKeyMap, readSigningKey(_))(signingKeyId)

  protected def readSigningKey(signingKeyId: Fingerprint)(implicit
      traceContext: TraceContext
  ): EitherT[Future, CryptoPrivateStoreError, Option[SigningPrivateKeyWithName]]

  def existsSigningKey(signingKeyId: Fingerprint)(implicit
      traceContext: TraceContext
  ): EitherT[Future, CryptoPrivateStoreError, Boolean] =
    signingKey(signingKeyId).map(_.nonEmpty)

  def storeSigningKey(key: SigningPrivateKey, name: Option[KeyName])(implicit
      traceContext: TraceContext
  ): EitherT[Future, CryptoPrivateStoreError, Unit] =
    writeSigningKey(key, name).map { _ =>
      val _ = signingKeyMap.put(key.id, SigningPrivateKeyWithName(key, name))
    }

  private[crypto] def decryptionKey(encryptionKeyId: Fingerprint)(implicit
      traceContext: TraceContext
  ): EitherT[Future, CryptoPrivateStoreError, Option[EncryptionPrivateKey]] =
    retrieveAndUpdateCache(decryptionKeyMap, readDecryptionKey(_))(encryptionKeyId)

  protected def readDecryptionKey(encryptionKeyId: Fingerprint)(implicit
      traceContext: TraceContext
  ): EitherT[Future, CryptoPrivateStoreError, Option[EncryptionPrivateKeyWithName]]

  def existsDecryptionKey(decryptionKeyId: Fingerprint)(implicit
      traceContext: TraceContext
  ): EitherT[Future, CryptoPrivateStoreError, Boolean] =
    decryptionKey(decryptionKeyId).map(_.nonEmpty)

  def storeDecryptionKey(key: EncryptionPrivateKey, name: Option[KeyName])(implicit
      traceContext: TraceContext
  ): EitherT[Future, CryptoPrivateStoreError, Unit] =
    writeDecryptionKey(key, name)
      .map { _ =>
        val _ = decryptionKeyMap.put(key.id, EncryptionPrivateKeyWithName(key, name))
      }

  private[crypto] def hmacSecret(implicit
      traceContext: TraceContext
  ): EitherT[Future, CryptoPrivateStoreError, Option[HmacSecret]] =
    hmacSecretRef.get() match {
      case hmacSecret @ Some(_) => EitherT.rightT(hmacSecret)
      case None =>
        loadHmacSecret().map { result =>
          hmacSecretRef.set(result)
          result
        }
    }

  def storeHmacSecret(hmacSecret: HmacSecret)(implicit
      traceContext: TraceContext
  ): EitherT[Future, CryptoPrivateStoreError, Unit] =
    writeHmacSecret(hmacSecret).map(_ => hmacSecretRef.set(Some(hmacSecret)))

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
  def create(storage: Storage, loggerFactory: NamedLoggerFactory)(implicit
      ec: ExecutionContext
  ): CryptoPrivateStore =
    storage match {
      case _: MemoryStorage => new InMemoryCryptoPrivateStore(loggerFactory)
      case jdbc: DbStorage => new DbCryptoPrivateStore(jdbc, loggerFactory)
    }
}

sealed trait CryptoPrivateStoreError extends Product with Serializable with PrettyPrinting
object CryptoPrivateStoreError {

  case class FailedToLoadHmacSecret(reason: String) extends CryptoPrivateStoreError {
    override def pretty: Pretty[FailedToLoadHmacSecret] = prettyOfClass(
      unnamedParam(_.reason.unquoted)
    )
  }

  case object HmacSecretAlreadyExists extends CryptoPrivateStoreError {
    override def pretty: Pretty[HmacSecretAlreadyExists.type] =
      prettyOfObject[HmacSecretAlreadyExists.type]
  }

  case class FailedToInsertHmacSecret(reason: String) extends CryptoPrivateStoreError {
    override def pretty: Pretty[FailedToInsertHmacSecret] = prettyOfClass(
      unnamedParam(_.reason.unquoted)
    )
  }

  case class FailedToListKeys(reason: String) extends CryptoPrivateStoreError {
    override def pretty: Pretty[FailedToListKeys] = prettyOfClass(unnamedParam(_.reason.unquoted))
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
}
