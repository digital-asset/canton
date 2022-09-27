// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.store.memory

import cats.data.EitherT
import cats.syntax.either._
import com.digitalasset.canton.crypto.KeyPurpose.{Encryption, Signing}
import com.digitalasset.canton.crypto.store.db.StoredPrivateKey
import com.digitalasset.canton.crypto.store.{
  CryptoPrivateStore,
  CryptoPrivateStoreError,
  EncryptionPrivateKeyWithName,
  PrivateKeyWithName,
  SigningPrivateKeyWithName,
}
import com.digitalasset.canton.crypto.{
  EncryptionPrivateKey,
  Fingerprint,
  KeyName,
  KeyPurpose,
  PrivateKey,
  SigningPrivateKey,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.TrieMapUtil

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

  override private[crypto] def readPrivateKey(keyId: Fingerprint, purpose: KeyPurpose)(implicit
      traceContext: TraceContext
  ): EitherT[Future, CryptoPrivateStoreError, Option[StoredPrivateKey]] =
    EitherT.rightT {
      purpose match {
        case Signing =>
          storedSigningKeyMap
            .get(keyId)
            .map(_.toStored)

        case Encryption =>
          storedDecryptionKeyMap
            .get(keyId)
            .map(_.toStored)
      }
    }

  override private[crypto] def writePrivateKey(
      key: StoredPrivateKey
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, CryptoPrivateStoreError, Unit] = {

    def parseAndWritePrivateKey[A <: PrivateKey, B <: PrivateKeyWithName](
        privateKey: ParsingResult[A],
        cache: TrieMap[Fingerprint, B],
        buildKeyWithNameFunc: (A, Option[KeyName]) => B,
    ): EitherT[Future, CryptoPrivateStoreError, Unit] = {
      privateKey match {
        case Left(parseErr) =>
          EitherT.leftT[Future, Unit](
            CryptoPrivateStoreError.FailedToInsertKey(
              key.id,
              s"could not parse stored key (it can either be corrupted or encrypted): ${parseErr.toString}",
            )
          )
        case Right(pk) =>
          TrieMapUtil
            .insertIfAbsent(
              cache,
              key.id,
              buildKeyWithNameFunc(pk, key.name),
              errorDuplicate[B] _,
            )
            .toEitherT
      }
    }

    key.purpose match {
      case Signing =>
        parseAndWritePrivateKey[SigningPrivateKey, SigningPrivateKeyWithName](
          SigningPrivateKey.fromStored(key),
          storedSigningKeyMap,
          (privateKey, name) => SigningPrivateKeyWithName(privateKey, name),
        )
      case Encryption =>
        parseAndWritePrivateKey[EncryptionPrivateKey, EncryptionPrivateKeyWithName](
          EncryptionPrivateKey.fromStored(key),
          storedDecryptionKeyMap,
          (privateKey, name) => EncryptionPrivateKeyWithName(privateKey, name),
        )
    }
  }

  override private[store] def listPrivateKeys(purpose: KeyPurpose, encrypted: Boolean)(implicit
      traceContext: TraceContext
  ): EitherT[Future, CryptoPrivateStoreError, Set[StoredPrivateKey]] =
    EitherT.rightT(
      purpose match {
        case Signing =>
          storedSigningKeyMap.values.toSet
            .map((x: SigningPrivateKeyWithName) => x.privateKey.toStored(x.name, None))

        case Encryption =>
          storedDecryptionKeyMap.values.toSet
            .map((x: EncryptionPrivateKeyWithName) => x.privateKey.toStored(x.name, None))
      }
    )

  override private[crypto] def deletePrivateKey(
      keyId: Fingerprint
  )(implicit traceContext: TraceContext): EitherT[Future, CryptoPrivateStoreError, Unit] = {
    storedSigningKeyMap.remove(keyId)
    storedDecryptionKeyMap.remove(keyId)
    EitherT.rightT(())
  }

  override def close(): Unit = ()
}
