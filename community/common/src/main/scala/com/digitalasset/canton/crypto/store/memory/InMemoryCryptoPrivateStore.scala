// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.store.memory

import cats.data.EitherT
import cats.syntax.either._
import com.digitalasset.canton.crypto.store.{
  CryptoPrivateStore,
  CryptoPrivateStoreError,
  EncryptionPrivateKeyWithName,
  PrivateKeyWithName,
  SigningPrivateKeyWithName,
}
import com.digitalasset.canton.crypto.{EncryptionPrivateKey, Fingerprint, SigningPrivateKey}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.TrieMapUtil

import com.digitalasset.canton.crypto.KeyName

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}

/** The in-memory store does not provide any persistence and keys during runtime are stored in the generic caching layer.
  */
class InMemoryCryptoPrivateStore(override protected val loggerFactory: NamedLoggerFactory)(
    override implicit val ec: ExecutionContext
) extends CryptoPrivateStore
    with NamedLogging {

  private val storedSigningKeyMap: TrieMap[Fingerprint, SigningPrivateKeyWithName] = TrieMap.empty
  private val storedDecryptionKeyMap: TrieMap[Fingerprint, EncryptionPrivateKeyWithName] =
    TrieMap.empty

  private def errorDuplicate[K <: PrivateKeyWithName](
      keyId: Fingerprint,
      oldKey: K,
      newKey: K,
  ): CryptoPrivateStoreError =
    CryptoPrivateStoreError.KeyAlreadyExists(keyId, oldKey.name.map(_.unwrap))

  override def readSigningKey(signingKeyId: Fingerprint)(implicit
      traceContext: TraceContext
  ): EitherT[Future, CryptoPrivateStoreError, Option[SigningPrivateKeyWithName]] =
    EitherT.rightT(storedSigningKeyMap.get(signingKeyId))

  override def readDecryptionKey(encryptionKeyId: Fingerprint)(implicit
      traceContext: TraceContext
  ): EitherT[Future, CryptoPrivateStoreError, Option[EncryptionPrivateKeyWithName]] =
    EitherT.rightT(storedDecryptionKeyMap.get(encryptionKeyId))

  override protected def writeSigningKey(key: SigningPrivateKey, name: Option[KeyName])(implicit
      traceContext: TraceContext
  ): EitherT[Future, CryptoPrivateStoreError, Unit] = {
    TrieMapUtil
      .insertIfAbsent(
        storedSigningKeyMap,
        key.id,
        SigningPrivateKeyWithName(key, name),
        errorDuplicate[SigningPrivateKeyWithName] _,
      )
      .toEitherT
  }

  override protected def writeDecryptionKey(key: EncryptionPrivateKey, name: Option[KeyName])(
      implicit traceContext: TraceContext
  ): EitherT[Future, CryptoPrivateStoreError, Unit] = {
    TrieMapUtil
      .insertIfAbsent(
        storedDecryptionKeyMap,
        key.id,
        EncryptionPrivateKeyWithName(key, name),
        errorDuplicate[EncryptionPrivateKeyWithName] _,
      )
      .toEitherT
  }

  override private[store] def listSigningKeys(implicit
      traceContext: TraceContext
  ): EitherT[Future, CryptoPrivateStoreError, Set[SigningPrivateKeyWithName]] =
    EitherT.rightT(storedSigningKeyMap.values.toSet)

  override private[store] def listDecryptionKeys(implicit
      traceContext: TraceContext
  ): EitherT[Future, CryptoPrivateStoreError, Set[EncryptionPrivateKeyWithName]] =
    EitherT.rightT(storedDecryptionKeyMap.values.toSet)

  override protected def deletePrivateKey(
      keyId: Fingerprint
  )(implicit traceContext: TraceContext): EitherT[Future, CryptoPrivateStoreError, Unit] = {
    storedSigningKeyMap.remove(keyId)
    storedDecryptionKeyMap.remove(keyId)
    EitherT.rightT(())
  }

  override def close(): Unit = ()
}
