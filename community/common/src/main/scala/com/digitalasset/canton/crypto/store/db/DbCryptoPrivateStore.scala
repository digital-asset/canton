// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.store.db

import cats.data.EitherT
import cats.syntax.bifunctor._
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto._
import com.digitalasset.canton.crypto.store._
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.metrics.MetricHandle.GaugeM
import com.digitalasset.canton.metrics.TimedLoadGauge
import com.digitalasset.canton.resource.DbStorage.{DbAction, Profile}
import com.digitalasset.canton.resource.{DbStorage, DbStore}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{EitherTUtil, ErrorUtil}
import io.functionmeta.functionFullName
import slick.jdbc.{GetResult, SetParameter}

import scala.concurrent.{ExecutionContext, Future}

class DbCryptoPrivateStore(
    override protected val storage: DbStorage,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(override implicit val ec: ExecutionContext)
    extends CryptoPrivateStore
    with DbStore {

  import storage.api._
  import storage.converters._

  private val insertTime: GaugeM[TimedLoadGauge, Double] =
    storage.metrics.loadGaugeM("crypto-private-store-insert")
  private val queryTime: GaugeM[TimedLoadGauge, Double] =
    storage.metrics.loadGaugeM("crypto-private-store-query")

  private val queryHmacSecret =
    sql"select data from crypto_hmac_secret".as[HmacSecret].headOption

  private def insertHmacSecret(hmacSecret: HmacSecret): DbAction.WriteOnly[Int] =
    storage.profile match {
      case _: Profile.Oracle | _: Profile.H2 =>
        sqlu"""merge into crypto_hmac_secret using dual on (hmac_secret_id = 1)
               when matched then update set data = $hmacSecret
               when not matched then insert (data) values ($hmacSecret)"""

      case _: Profile.Postgres =>
        sqlu"""insert into crypto_hmac_secret(data)
             values ($hmacSecret)
             on conflict (hmac_secret_id)
             do update set data = $hmacSecret"""
    }

  private def queryKeys[K: GetResult](purpose: KeyPurpose): DbAction.ReadOnly[Set[K]] =
    sql"select data, name from crypto_private_keys where purpose = $purpose"
      .as[K]
      .map(_.toSet)

  private def queryKey[K <: PrivateKeyWithName: GetResult](
      keyId: Fingerprint,
      purpose: KeyPurpose,
  ): DbAction.ReadOnly[Option[K]] =
    sql"select data, name from crypto_private_keys where key_id = $keyId and purpose = $purpose"
      .as[K]
      .headOption

  private def insertKeyUpdate[K <: PrivateKey: SetParameter, KN <: PrivateKeyWithName: GetResult](
      key: K,
      name: Option[KeyName],
  ): DbAction.WriteOnly[Int] =
    storage.profile match {
      case _: DbStorage.Profile.Oracle =>
        sqlu"""insert 
               /*+  IGNORE_ROW_ON_DUPKEY_INDEX ( CRYPTO_PRIVATE_KEYS ( key_id ) ) */
               into crypto_private_keys (key_id, purpose, data, name)
           values (${key.id}, ${key.purpose}, $key, $name)"""
      case _ =>
        sqlu"""insert into crypto_private_keys (key_id, purpose, data, name)
           values (${key.id}, ${key.purpose}, $key, $name)
           on conflict do nothing"""
    }

  private def insertKey[K <: PrivateKey: SetParameter, KN <: PrivateKeyWithName: GetResult](
      key: K,
      name: Option[KeyName],
  )(implicit traceContext: TraceContext): EitherT[Future, CryptoPrivateStoreError, Unit] =
    insertTime.metric.eitherTEvent {
      for {
        inserted <- EitherT.right(storage.update(insertKeyUpdate(key, name), functionFullName))
        res <-
          if (inserted == 0) {
            // If no key was inserted by the insert query, check that the existing value matches
            storage
              .querySingle(queryKey(key.id, key.purpose), functionFullName)
              // If we don't find the duplicate key, it may have been concurrently deleted and we could retry to insert it.
              .toRight(
                CryptoPrivateStoreError
                  .FailedToInsertKey(key.id, "No key inserted and no key found")
              )
              .flatMap { existingKey =>
                EitherT
                  .cond[Future](
                    existingKey.privateKey == key && existingKey.name == name,
                    (),
                    CryptoPrivateStoreError.KeyAlreadyExists(key.id, existingKey.name.map(_.unwrap)),
                  )
                  .leftWiden[CryptoPrivateStoreError]
              }
          } else EitherT.rightT[Future, CryptoPrivateStoreError](())
      } yield res
    }

  override def readSigningKey(signingKeyId: Fingerprint)(implicit
      traceContext: TraceContext
  ): EitherT[Future, CryptoPrivateStoreError, Option[SigningPrivateKeyWithName]] =
    EitherTUtil.fromFuture(
      storage
        .querySingle(
          queryKey[SigningPrivateKeyWithName](signingKeyId, KeyPurpose.Signing),
          functionFullName,
        )
        .value,
      err => CryptoPrivateStoreError.FailedToReadKey(signingKeyId, err.toString),
    )

  override def readDecryptionKey(encryptionKeyId: Fingerprint)(implicit
      traceContext: TraceContext
  ): EitherT[Future, CryptoPrivateStoreError, Option[EncryptionPrivateKeyWithName]] =
    EitherTUtil.fromFuture(
      storage
        .querySingle(
          queryKey[EncryptionPrivateKeyWithName](encryptionKeyId, KeyPurpose.Encryption),
          functionFullName,
        )
        .value,
      err => CryptoPrivateStoreError.FailedToReadKey(encryptionKeyId, err.toString),
    )

  override protected def writeSigningKey(key: SigningPrivateKey, name: Option[KeyName])(implicit
      traceContext: TraceContext
  ): EitherT[Future, CryptoPrivateStoreError, Unit] =
    insertKey[SigningPrivateKey, SigningPrivateKeyWithName](key, name)

  override protected def writeDecryptionKey(key: EncryptionPrivateKey, name: Option[KeyName])(
      implicit traceContext: TraceContext
  ): EitherT[Future, CryptoPrivateStoreError, Unit] =
    insertKey[EncryptionPrivateKey, EncryptionPrivateKeyWithName](key, name)

  override protected def writeHmacSecret(
      hmacSecret: HmacSecret
  )(implicit traceContext: TraceContext): EitherT[Future, CryptoPrivateStoreError, Unit] = {

    for {
      nrRows <- EitherTUtil.fromFuture(
        insertTime.metric.event(storage.update(insertHmacSecret(hmacSecret), functionFullName)),
        err => CryptoPrivateStoreError.FailedToInsertHmacSecret(err.toString),
      )
      _ <- nrRows match {
        case 1 => EitherTUtil.unit[CryptoPrivateStoreError]
        case 0 =>
          for {
            existingHmacSecret <- loadHmacSecret().map(
              _.getOrElse(
                ErrorUtil.internalError(
                  new IllegalStateException("No existing HMAC secret found but failed to insert")
                )
              )
            )
            _ <- EitherTUtil
              .condUnitET[Future](
                existingHmacSecret == hmacSecret,
                CryptoPrivateStoreError.HmacSecretAlreadyExists,
              )
              .leftWiden[CryptoPrivateStoreError]
          } yield ()
        case _ =>
          ErrorUtil.internalError(
            new IllegalStateException(s"Updated more than 1 row for HMAC secrets: $nrRows")
          )
      }
    } yield ()
  }

  override private[store] def listSigningKeys(implicit
      traceContext: TraceContext
  ): EitherT[Future, CryptoPrivateStoreError, Set[SigningPrivateKeyWithName]] =
    EitherTUtil
      .fromFuture(
        queryTime.metric.event(
          storage.query(queryKeys[SigningPrivateKeyWithName](KeyPurpose.Signing), functionFullName)
        ),
        err => CryptoPrivateStoreError.FailedToListKeys(err.toString),
      )

  override private[store] def listDecryptionKeys(implicit
      traceContext: TraceContext
  ): EitherT[Future, CryptoPrivateStoreError, Set[EncryptionPrivateKeyWithName]] =
    EitherTUtil.fromFuture(
      queryTime.metric.event(
        storage
          .query(queryKeys[EncryptionPrivateKeyWithName](KeyPurpose.Encryption), functionFullName)
      ),
      err => CryptoPrivateStoreError.FailedToListKeys(err.toString),
    )

  override private[store] def loadHmacSecret()(implicit
      traceContext: TraceContext
  ): EitherT[Future, CryptoPrivateStoreError, Option[HmacSecret]] =
    EitherTUtil.fromFuture(
      queryTime.metric.event(storage.query(queryHmacSecret, functionFullName)),
      err => CryptoPrivateStoreError.FailedToLoadHmacSecret(err.toString),
    )

  override protected def deletePrivateKey(keyId: Fingerprint)(implicit
      traceContext: TraceContext
  ): EitherT[Future, CryptoPrivateStoreError, Unit] =
    EitherTUtil.fromFuture(
      insertTime.metric.event(
        storage
          .update_(sqlu"delete from crypto_private_keys where key_id = $keyId", functionFullName)
      ),
      err => CryptoPrivateStoreError.FailedToDeleteKey(keyId, err.toString),
    )
}
