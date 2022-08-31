// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.store.db

import cats.data.EitherT
import cats.syntax.bifunctor._
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.String300
import com.digitalasset.canton.crypto._
import com.digitalasset.canton.crypto.store._
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.metrics.MetricHandle.GaugeM
import com.digitalasset.canton.metrics.TimedLoadGauge
import com.digitalasset.canton.resource.DbStorage.DbAction
import com.digitalasset.canton.resource.DbStorage.Implicits._
import com.digitalasset.canton.resource.{DbStorage, DbStore}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.EitherTUtil
import com.digitalasset.canton.version.ProtocolVersion
import com.google.protobuf.ByteString
import io.functionmeta.functionFullName
import slick.jdbc.{GetResult, SetParameter}

import scala.concurrent.{ExecutionContext, Future}

/** Represents the data to be stored in the crypto_private_keys table.
  * If wrapperKeyId is set (Some(wrapperKeyId)) then the data field is encrypted
  * otherwise (None), then the data field is in plaintext.
  * @param id canton identifier for a private key
  * @param data a ByteString that stores either: (1) the serialized private key case class, which contains the private
  *             key plus metadata, or (2) the above proto serialization but encrypted with the wrapper key if present.
  * @param purpose to identify if the key is for signing or encryption
  * @param name an alias name for the private key
  * @param wrapperKeyId identifies what is the key being used to encrypt the data field. If empty, data is
  *                     unencrypted.
  */
final case class StoredPrivateKey(
    id: Fingerprint,
    data: ByteString,
    purpose: KeyPurpose,
    name: Option[KeyName],
    wrapperKeyId: Option[String300],
) extends Product
    with Serializable

object StoredPrivateKey {
  implicit def getResultStoredPrivateKey(implicit
      getResultByteString: GetResult[ByteString]
  ): GetResult[StoredPrivateKey] =
    GetResult { r =>
      StoredPrivateKey(r.<<, r.<<, r.<<, r.<<, r.<<)
    }
}

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

  private val protocolVersion = ProtocolVersion.v2_0_0_Todo_i8793
  private implicit val setParameterEncryptionPrivateKey: SetParameter[EncryptionPrivateKey] =
    EncryptionPrivateKey.getVersionedSetParameter(protocolVersion)
  private implicit val setParameterSigningPrivateKey: SetParameter[SigningPrivateKey] =
    SigningPrivateKey.getVersionedSetParameter(protocolVersion)

  private def queryKeys(purpose: KeyPurpose): DbAction.ReadOnly[Set[StoredPrivateKey]] =
    sql"select key_id, data, purpose, name, wrapper_key_id from crypto_private_keys where purpose = $purpose"
      .as[StoredPrivateKey]
      .map(_.toSet)

  private def queryKey(
      keyId: Fingerprint,
      purpose: KeyPurpose,
  ): DbAction.ReadOnly[Option[StoredPrivateKey]] =
    sql"select key_id, data, purpose, name, wrapper_key_id from crypto_private_keys where key_id = $keyId and purpose = $purpose"
      .as[StoredPrivateKey]
      .headOption

  private def insertKeyUpdate(
      key: StoredPrivateKey
  ): DbAction.WriteOnly[Int] = {
    storage.profile match {
      case _: DbStorage.Profile.Oracle =>
        sqlu"""insert
               /*+  IGNORE_ROW_ON_DUPKEY_INDEX ( CRYPTO_PRIVATE_KEYS ( key_id ) ) */
               into crypto_private_keys (key_id, purpose, data, name, wrapper_key_id)
           values (${key.id}, ${key.purpose}, ${key.data}, ${key.name}, ${key.wrapperKeyId})"""
      case _ =>
        sqlu"""insert into crypto_private_keys (key_id, purpose, data, name, wrapper_key_id)
           values (${key.id}, ${key.purpose}, ${key.data}, ${key.name}, ${key.wrapperKeyId})
           on conflict do nothing"""
    }
  }

  private def insertKey(key: StoredPrivateKey)(implicit
      traceContext: TraceContext
  ): EitherT[Future, CryptoPrivateStoreError, Unit] = {

    def equalKeys(existingKey: StoredPrivateKey, newKey: StoredPrivateKey): Boolean = {
      if (existingKey.wrapperKeyId.isEmpty) {
        existingKey.data == newKey.data &&
        existingKey.name == newKey.name &&
        existingKey.purpose == newKey.purpose
      } else {
        // in the encrypted case we cannot compare the contents of data directly, we simply do not allow
        // keys having the same name and purpose
        existingKey.name == newKey.name &&
        existingKey.purpose == newKey.purpose
      }
    }

    insertTime.metric.eitherTEvent {
      for {
        inserted <- EitherT.right(
          storage.update(insertKeyUpdate(key), functionFullName)
        )
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
                    equalKeys(existingKey, key),
                    (),
                    CryptoPrivateStoreError.KeyAlreadyExists(key.id, existingKey.name.map(_.unwrap)),
                  )
                  .leftWiden[CryptoPrivateStoreError]
              }
          } else EitherT.rightT[Future, CryptoPrivateStoreError](())
      } yield res
    }
  }

  override private[crypto] def readPrivateKey(
      keyId: Fingerprint,
      purpose: KeyPurpose,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, CryptoPrivateStoreError, Option[StoredPrivateKey]] =
    EitherTUtil.fromFuture(
      storage
        .querySingle(
          queryKey(keyId, purpose),
          functionFullName,
        )
        .value,
      err => CryptoPrivateStoreError.FailedToReadKey(keyId, err.toString),
    )

  override private[crypto] def writePrivateKey(
      key: StoredPrivateKey
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, CryptoPrivateStoreError, Unit] =
    insertKey(key)

  override private[store] def listPrivateKeys(purpose: KeyPurpose)(implicit
      traceContext: TraceContext
  ): EitherT[Future, CryptoPrivateStoreError, Set[StoredPrivateKey]] =
    EitherTUtil
      .fromFuture(
        queryTime.metric
          .event(
            storage.query(queryKeys(purpose), functionFullName)
          ),
        err => CryptoPrivateStoreError.FailedToListKeys(err.toString),
      )

  override private[crypto] def deletePrivateKey(keyId: Fingerprint)(implicit
      traceContext: TraceContext
  ): EitherT[Future, CryptoPrivateStoreError, Unit] =
    EitherTUtil.fromFuture(
      insertTime.metric.event(
        storage
          .update_(sqlu"delete from crypto_private_keys where key_id = $keyId", functionFullName)
      ),
      err => CryptoPrivateStoreError.FailedToDeleteKey(keyId, err.toString),
    )

  private[store] def getWrapperKeyId()(implicit
      traceContext: TraceContext
  ): EitherT[Future, CryptoPrivateStoreError, Option[String300]] =
    EitherTUtil
      .fromFuture(
        queryTime.metric
          .event(
            storage
              .query(
                {
                  sql"select distinct wrapper_key_id from crypto_private_keys"
                    .as[String300]
                    .map(_.toSeq)
                },
                functionFullName,
              )
          ),
        err => CryptoPrivateStoreError.FailedToGetWrapperKeyId(err.toString),
      )
      .transform {
        case Left(err) => Left(err)
        case Right(wrapper_keys) =>
          if (wrapper_keys.size > 1)
            Left(
              CryptoPrivateStoreError
                .FailedToGetWrapperKeyId("Found more than one distinct wrapper_key_id")
            )
          else
            Right(wrapper_keys.headOption)
      }

}
