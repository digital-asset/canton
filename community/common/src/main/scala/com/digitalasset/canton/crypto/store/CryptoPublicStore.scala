// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.store

import cats.data.EitherT
import cats.syntax.functor._
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.KeyName
import com.digitalasset.canton.crypto._
import com.digitalasset.canton.crypto.store.db.DbCryptoPublicStore
import com.digitalasset.canton.crypto.store.memory.InMemoryCryptoPublicStore
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.resource.{DbStorage, MemoryStorage, Storage}
import com.digitalasset.canton.tracing.TraceContext
import com.google.common.annotations.VisibleForTesting

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}

/** Store for all public cryptographic material such as certificates or public keys. */
trait CryptoPublicStore extends AutoCloseable {

  implicit val ec: ExecutionContext

  // Cached values for public keys with names
  protected val signingKeyMap: TrieMap[Fingerprint, SigningPublicKeyWithName] = TrieMap.empty
  protected val encryptionKeyMap: TrieMap[Fingerprint, EncryptionPublicKeyWithName] = TrieMap.empty

  // Write methods that the underlying store has to implement for the caching
  protected def writeSigningKey(key: SigningPublicKey, name: Option[KeyName])(implicit
      traceContext: TraceContext
  ): EitherT[Future, CryptoPublicStoreError, Unit]
  protected def writeEncryptionKey(key: EncryptionPublicKey, name: Option[KeyName])(implicit
      traceContext: TraceContext
  ): EitherT[Future, CryptoPublicStoreError, Unit]

  @VisibleForTesting
  private[store] def listSigningKeys(implicit
      traceContext: TraceContext
  ): EitherT[Future, CryptoPublicStoreError, Set[SigningPublicKeyWithName]]
  @VisibleForTesting
  private[store] def listEncryptionKeys(implicit
      traceContext: TraceContext
  ): EitherT[Future, CryptoPublicStoreError, Set[EncryptionPublicKeyWithName]]

  def storePublicKey(publicKey: PublicKey, name: Option[KeyName])(implicit
      traceContext: TraceContext
  ): EitherT[Future, CryptoPublicStoreError, Unit] =
    (publicKey: @unchecked) match {
      case sigKey: SigningPublicKey => storeSigningKey(sigKey, name)
      case encKey: EncryptionPublicKey => storeEncryptionKey(encKey, name)
    }

  def publicKey(keyId: Fingerprint)(implicit
      traceContext: TraceContext
  ): EitherT[Future, CryptoPublicStoreError, Option[PublicKey]] =
    publicKeyWithName(keyId).map(_.map(_.publicKey))

  def publicKeyWithName(keyId: Fingerprint)(implicit
      traceContext: TraceContext
  ): EitherT[Future, CryptoPublicStoreError, Option[PublicKeyWithName]] =
    for {
      sigKeyOption <- readSigningKey(keyId)
      pubKeyOption <- sigKeyOption.fold(readEncryptionKey(keyId).widen[Option[PublicKeyWithName]])(
        key => EitherT.rightT(Some(key))
      )
    } yield pubKeyOption

  def existsPublicKey(keyId: Fingerprint, purpose: KeyPurpose)(implicit
      traceContext: TraceContext
  ): EitherT[Future, CryptoPublicStoreError, Boolean] =
    purpose match {
      case KeyPurpose.Signing => signingKey(keyId).map(_.nonEmpty)
      case KeyPurpose.Encryption => encryptionKey(keyId).map(_.nonEmpty)
    }

  def publicKeysWithName(implicit
      traceContext: TraceContext
  ): EitherT[Future, CryptoPublicStoreError, Set[PublicKeyWithName]] =
    for {
      sigKeys <- listSigningKeys
      encKeys <- listEncryptionKeys
    } yield sigKeys.toSet[PublicKeyWithName] ++ encKeys.toSet[PublicKeyWithName]

  def signingKey(signingKeyId: Fingerprint)(implicit
      traceContext: TraceContext
  ): EitherT[Future, CryptoPublicStoreError, Option[SigningPublicKey]] =
    retrieveKeyAndUpdateCache(signingKeyMap, readSigningKey(_))(signingKeyId)

  protected def readSigningKey(signingKeyId: Fingerprint)(implicit
      traceContext: TraceContext
  ): EitherT[Future, CryptoPublicStoreError, Option[SigningPublicKeyWithName]]

  def signingKeys(implicit
      traceContext: TraceContext
  ): EitherT[Future, CryptoPublicStoreError, Set[SigningPublicKey]] =
    retrieveKeysAndUpdateCache(listSigningKeys, signingKeyMap)

  def storeSigningKey(key: SigningPublicKey, name: Option[KeyName] = None)(implicit
      traceContext: TraceContext
  ): EitherT[Future, CryptoPublicStoreError, Unit] =
    writeSigningKey(key, name).map { _ =>
      val _ = signingKeyMap.put(key.id, SigningPublicKeyWithName(key, name))
    }

  def encryptionKey(encryptionKeyId: Fingerprint)(implicit
      traceContext: TraceContext
  ): EitherT[Future, CryptoPublicStoreError, Option[EncryptionPublicKey]] =
    retrieveKeyAndUpdateCache(encryptionKeyMap, readEncryptionKey(_))(encryptionKeyId)

  protected def readEncryptionKey(encryptionKeyId: Fingerprint)(implicit
      traceContext: TraceContext
  ): EitherT[Future, CryptoPublicStoreError, Option[EncryptionPublicKeyWithName]]

  def encryptionKeys(implicit
      traceContext: TraceContext
  ): EitherT[Future, CryptoPublicStoreError, Set[EncryptionPublicKey]] =
    retrieveKeysAndUpdateCache(listEncryptionKeys, encryptionKeyMap)

  def storeEncryptionKey(key: EncryptionPublicKey, name: Option[KeyName] = None)(implicit
      traceContext: TraceContext
  ): EitherT[Future, CryptoPublicStoreError, Unit] =
    writeEncryptionKey(key, name)
      .map { _ =>
        val _ = encryptionKeyMap.put(key.id, EncryptionPublicKeyWithName(key, name))
      }

  def storeCertificate(cert: X509Certificate)(implicit
      traceContext: TraceContext
  ): EitherT[Future, CryptoPublicStoreError, Unit]
  def listCertificates()(implicit
      traceContext: TraceContext
  ): EitherT[Future, CryptoPublicStoreError, Set[X509Certificate]]

  private def retrieveKeyAndUpdateCache[KN <: PublicKeyWithName](
      cache: TrieMap[Fingerprint, KN],
      readKey: Fingerprint => EitherT[Future, CryptoPublicStoreError, Option[KN]],
  )(keyId: Fingerprint): EitherT[Future, CryptoPublicStoreError, Option[KN#K]] =
    cache.get(keyId) match {
      case Some(value) => EitherT.rightT(Some(value.publicKey))
      case None =>
        readKey(keyId).map { keyOption =>
          keyOption.foreach(key => cache.putIfAbsent(keyId, key))
          keyOption.map(_.publicKey)
        }
    }

  private def retrieveKeysAndUpdateCache[KN <: PublicKeyWithName](
      keysFromDb: EitherT[Future, CryptoPublicStoreError, Set[KN]],
      cache: TrieMap[Fingerprint, KN],
  ): EitherT[Future, CryptoPublicStoreError, Set[KN#K]] =
    for {
      // we always rebuild the cache here just in case new keys have been added by another process
      // this should not be a problem since these operations to get all keys are infrequent and typically
      // typically the number of keys is not very large
      storedKeys <- keysFromDb
      _ = cache ++= storedKeys.map(k => k.publicKey.id -> k)
    } yield storedKeys.map(_.publicKey)
}

object CryptoPublicStore {
  def create(storage: Storage, timeouts: ProcessingTimeout, loggerFactory: NamedLoggerFactory)(
      implicit ec: ExecutionContext
  ): CryptoPublicStore = {
    storage match {
      case _: MemoryStorage => new InMemoryCryptoPublicStore
      case dbStorage: DbStorage => new DbCryptoPublicStore(dbStorage, timeouts, loggerFactory)
    }
  }
}

sealed trait CryptoPublicStoreError extends Product with Serializable with PrettyPrinting
object CryptoPublicStoreError {

  case class FailedToListKeys(reason: String) extends CryptoPublicStoreError {
    override def pretty: Pretty[FailedToListKeys] = prettyOfClass(unnamedParam(_.reason.unquoted))
  }

  case class FailedToReadKey(keyId: Fingerprint, reason: String) extends CryptoPublicStoreError {
    override def pretty: Pretty[FailedToReadKey] = prettyOfClass(unnamedParam(_.reason.unquoted))
  }

  case class FailedToInsertKey(keyId: Fingerprint, reason: String) extends CryptoPublicStoreError {
    override def pretty: Pretty[FailedToInsertKey] =
      prettyOfClass(param("keyId", _.keyId), param("reason", _.reason.unquoted))
  }

  case class FailedToInsertCertificate(certId: CertificateId, reason: String)
      extends CryptoPublicStoreError {
    override def pretty: Pretty[FailedToInsertCertificate] =
      prettyOfClass(param("certId", _.certId), param("reason", _.reason.unquoted))
  }

  case class FailedToListCertificates(reason: String) extends CryptoPublicStoreError {
    override def pretty: Pretty[FailedToListCertificates] = prettyOfClass(
      unnamedParam(_.reason.unquoted)
    )
  }

  case class CertificateAlreadyExists(certId: CertificateId) extends CryptoPublicStoreError {
    override def pretty: Pretty[CertificateAlreadyExists] = prettyOfClass(param("certId", _.certId))
  }

  case class KeyAlreadyExists(keyId: Fingerprint, existingKeyName: Option[String])
      extends CryptoPublicStoreError {
    override def pretty: Pretty[KeyAlreadyExists] =
      prettyOfClass(
        param("keyId", _.keyId),
        param("existingKeyName", _.existingKeyName.getOrElse("").unquoted),
      )
  }

}
