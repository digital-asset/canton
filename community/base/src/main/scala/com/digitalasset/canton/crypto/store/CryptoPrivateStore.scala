// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.store

import cats.data.EitherT
import com.digitalasset.canton.config.CantonRequireTypes.String300
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.store.db.DbCryptoPrivateStore
import com.digitalasset.canton.crypto.store.memory.InMemoryCryptoPrivateStore
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.resource.{DbStorage, MemoryStorage, Storage}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ReleaseProtocolVersion

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
  * It encapsulates only existence checks/delete operations so it can be extendable to an external
  * crypto private store (e.g. an AWS KMS store).
  */
trait CryptoPrivateStore extends AutoCloseable {

  def removePrivateKey(
      keyId: Fingerprint
  )(implicit traceContext: TraceContext): EitherT[Future, CryptoPrivateStoreError, Unit]

  def existsPrivateKey(
      keyId: Fingerprint
  )(implicit traceContext: TraceContext): EitherT[Future, CryptoPrivateStoreError, Boolean]

  def existsSigningKey(signingKeyId: Fingerprint)(implicit
      traceContext: TraceContext
  ): EitherT[Future, CryptoPrivateStoreError, Boolean]

  def existsDecryptionKey(decryptionKeyId: Fingerprint)(implicit
      traceContext: TraceContext
  ): EitherT[Future, CryptoPrivateStoreError, Boolean]

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
    )(implicit
        ec: ExecutionContext
    ): EitherT[Future, CryptoPrivateStoreError, CryptoPrivateStore]
  }

  class CommunityCryptoPrivateStoreFactory extends CryptoPrivateStoreFactory {
    override def create(
        storage: Storage,
        releaseProtocolVersion: ReleaseProtocolVersion,
        timeouts: ProcessingTimeout,
        loggerFactory: NamedLoggerFactory,
    )(implicit
        ec: ExecutionContext
    ): EitherT[Future, CryptoPrivateStoreError, CryptoPrivateStore] =
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
