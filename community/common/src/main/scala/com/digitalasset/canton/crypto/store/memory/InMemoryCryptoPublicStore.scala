// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.store.memory

import cats.data.EitherT
import cats.syntax.bifunctor._
import cats.syntax.either._
import com.digitalasset.canton.crypto.store.{CryptoPublicStore, CryptoPublicStoreError}
import com.digitalasset.canton.crypto.{KeyName, _}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.TrieMapUtil

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}

class InMemoryCryptoPublicStore(override implicit val ec: ExecutionContext)
    extends CryptoPublicStore {

  private val storedSigningKeyMap: TrieMap[Fingerprint, SigningPublicKeyWithName] = TrieMap.empty
  private val storedEncryptionKeyMap: TrieMap[Fingerprint, EncryptionPublicKeyWithName] =
    TrieMap.empty
  private val certificateKeyMap: TrieMap[CertificateId, X509Certificate] = TrieMap.empty

  private def errorKeyDuplicate[K <: PublicKeyWithName](
      keyId: Fingerprint,
      oldKey: K,
      newKey: K,
  ): CryptoPublicStoreError =
    CryptoPublicStoreError.KeyAlreadyExists(keyId, oldKey.name.map(_.unwrap))

  override protected def writeSigningKey(key: SigningPublicKey, name: Option[KeyName])(implicit
      traceContext: TraceContext
  ): EitherT[Future, CryptoPublicStoreError, Unit] = {
    TrieMapUtil
      .insertIfAbsent(
        storedSigningKeyMap,
        key.id,
        SigningPublicKeyWithName(key, name),
        errorKeyDuplicate[SigningPublicKeyWithName] _,
      )
      .toEitherT
  }

  override def readSigningKey(signingKeyId: Fingerprint)(implicit
      traceContext: TraceContext
  ): EitherT[Future, CryptoPublicStoreError, Option[SigningPublicKeyWithName]] =
    EitherT.rightT(storedSigningKeyMap.get(signingKeyId))

  override def readEncryptionKey(encryptionKeyId: Fingerprint)(implicit
      traceContext: TraceContext
  ): EitherT[Future, CryptoPublicStoreError, Option[EncryptionPublicKeyWithName]] =
    EitherT.rightT(storedEncryptionKeyMap.get(encryptionKeyId))

  override protected def writeEncryptionKey(key: EncryptionPublicKey, name: Option[KeyName])(
      implicit traceContext: TraceContext
  ): EitherT[Future, CryptoPublicStoreError, Unit] = {
    TrieMapUtil
      .insertIfAbsent(
        storedEncryptionKeyMap,
        key.id,
        EncryptionPublicKeyWithName(key, name),
        errorKeyDuplicate[EncryptionPublicKeyWithName] _,
      )
      .toEitherT
  }

  override private[store] def listSigningKeys(implicit
      traceContext: TraceContext
  ): EitherT[Future, CryptoPublicStoreError, Set[SigningPublicKeyWithName]] =
    EitherT.rightT(storedSigningKeyMap.values.toSet)

  override private[store] def listEncryptionKeys(implicit
      traceContext: TraceContext
  ): EitherT[Future, CryptoPublicStoreError, Set[EncryptionPublicKeyWithName]] =
    EitherT.rightT(storedEncryptionKeyMap.values.toSet)

  override def storeCertificate(
      cert: X509Certificate
  )(implicit traceContext: TraceContext): EitherT[Future, CryptoPublicStoreError, Unit] = {
    TrieMapUtil
      .insertIfAbsent(
        certificateKeyMap,
        cert.id,
        cert,
        CryptoPublicStoreError.CertificateAlreadyExists(cert.id),
      )
      .toEitherT[Future]
      .leftWiden[CryptoPublicStoreError]
  }

  override def listCertificates()(implicit
      traceContext: TraceContext
  ): EitherT[Future, CryptoPublicStoreError, Set[X509Certificate]] =
    EitherT.rightT(certificateKeyMap.values.toSet)

  override def close(): Unit = ()
}
