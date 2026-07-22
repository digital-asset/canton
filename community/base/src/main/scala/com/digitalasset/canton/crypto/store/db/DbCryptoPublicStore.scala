// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.store.db

import cats.data.OptionT
import cats.implicits.toTraverseOps
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.store.*
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.resource.DbStorage.DbAction
import com.digitalasset.canton.resource.{DbStorage, DbStore, IdempotentInsert}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ReleaseProtocolVersion
import slick.dbio.DBIOAction
import slick.jdbc.GetResult

import scala.concurrent.ExecutionContext

class DbCryptoPublicStore(
    override protected val storage: DbStorage,
    protected val releaseProtocolVersion: ReleaseProtocolVersion,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(override implicit val ec: ExecutionContext)
    extends CryptoPublicStore
    with DbStore {

  import storage.api.*
  import storage.converters.*

  private def queryKeys[K: GetResult](purpose: KeyPurpose): DbAction.ReadOnly[Set[K]] =
    sql"select data, name from common_crypto_public_keys where purpose = $purpose"
      .as[K]
      .map(_.toSet)

  private def queryKeyO[K <: PublicKeyWithName: GetResult](
      keyId: Fingerprint,
      purpose: KeyPurpose,
  ): DbAction.ReadOnly[Option[K]] =
    sql"select data, name from common_crypto_public_keys where key_id = $keyId and purpose = $purpose"
      .as[K]
      .headOption

  private def queryKey[K <: PublicKeyWithName: GetResult](
      keyId: Fingerprint,
      purpose: KeyPurpose,
  ): DbAction.ReadOnly[K] =
    sql"select data, name from common_crypto_public_keys where key_id = $keyId and purpose = $purpose"
      .as[K]
      .head

  private def insertKey[K <: PublicKey, KN <: PublicKeyWithName: GetResult](
      key: K,
      name: Option[KeyName],
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] =
    // We cannot use a `SetParameter`, because serialization of the keys can fail
    key.toByteArrayE(releaseProtocolVersion.v) match {
      case Right(serializedKey) =>
        storage.queryAndUpdate(
          IdempotentInsert.insertVerifyingConflicts(
            sql"""insert into common_crypto_public_keys (key_id, purpose, data, name)
              values (${key.id}, ${key.purpose}, $serializedKey, $name)
              on conflict do nothing""".asUpdate,
            queryKey(key.id, key.purpose),
          )(
            existingKey => existingKey.publicKey == key && existingKey.name == name,
            _ => s"Existing public key for ${key.id} is different than inserted key",
          ),
          functionFullName,
        )

      case Left(err) =>
        FutureUnlessShutdown.failed(
          new IllegalStateException(
            s"Unable to serialize key for storage with protocol version ${releaseProtocolVersion.v}: $err"
          )
        )
    }

  override def readSigningKey(signingKeyId: Fingerprint)(implicit
      traceContext: TraceContext
  ): OptionT[FutureUnlessShutdown, SigningPublicKeyWithName] =
    storage
      .querySingle(
        queryKeyO[SigningPublicKeyWithName](signingKeyId, KeyPurpose.Signing),
        functionFullName,
      )

  override def readEncryptionKey(encryptionKeyId: Fingerprint)(implicit
      traceContext: TraceContext
  ): OptionT[FutureUnlessShutdown, EncryptionPublicKeyWithName] =
    storage
      .querySingle(
        queryKeyO[EncryptionPublicKeyWithName](encryptionKeyId, KeyPurpose.Encryption),
        functionFullName,
      )

  override protected def writeSigningKey(key: SigningPublicKey, name: Option[KeyName])(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] =
    insertKey[SigningPublicKey, SigningPublicKeyWithName](key, name)

  override protected def writeEncryptionKey(key: EncryptionPublicKey, name: Option[KeyName])(
      implicit traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] =
    insertKey[EncryptionPublicKey, EncryptionPublicKeyWithName](key, name)

  override private[store] def listSigningKeys(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Set[SigningPublicKeyWithName]] =
    storage.query(
      queryKeys[SigningPublicKeyWithName](KeyPurpose.Signing),
      functionFullName,
    )

  override private[store] def listEncryptionKeys(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Set[EncryptionPublicKeyWithName]] =
    storage
      .query(
        queryKeys[EncryptionPublicKeyWithName](KeyPurpose.Encryption),
        functionFullName,
      )

  override def deleteKey(
      keyId: Fingerprint
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    storage
      .update_(
        sqlu"delete from common_crypto_public_keys where key_id = $keyId",
        functionFullName,
      )

  override private[crypto] def replaceSigningPublicKeys(
      newKeys: Seq[SigningPublicKey]
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = replacePublicKeys(newKeys)

  override private[crypto] def replaceEncryptionPublicKeys(
      newKeys: Seq[EncryptionPublicKey]
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = replacePublicKeys(newKeys)

  private def replacePublicKeys[K <: PublicKey](
      newKeys: Seq[K]
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] =
    // We cannot use a `SetParameter`, because serialization of the keys can fail
    newKeys.traverse(key => key.toByteArrayE(releaseProtocolVersion.v).map((key, _))) match {
      case Right(newKeysWithSerialization) =>
        storage.update_(
          DBIOAction
            .sequence(newKeysWithSerialization.map { case (key, serializedKey) =>
              updateKey(key, serializedKey)
            })
            .transactionally,
          functionFullName,
        )

      case Left(err) =>
        FutureUnlessShutdown.failed(
          new IllegalStateException(
            s"Unable to serialize key for storage with protocol version ${releaseProtocolVersion.v}: $err"
          )
        )
    }

  // Update the contents of a key identified by its id; `purpose` and `name` remain unchanged.
  // Used for key format migrations.
  private def updateKey[K <: PublicKey](
      key: K,
      serializedKey: Array[Byte],
  ): DbAction.WriteOnly[Int] =
    sqlu"""update common_crypto_public_keys set data = $serializedKey where key_id = ${key.id}"""
}
