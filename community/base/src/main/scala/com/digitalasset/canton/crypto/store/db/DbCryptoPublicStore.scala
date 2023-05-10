// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.store.db

import cats.data.{EitherT, OptionT}
import cats.syntax.bifunctor.*
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.store.*
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.metrics.TimedLoadGauge
import com.digitalasset.canton.resource.DbStorage.{DbAction, Profile}
import com.digitalasset.canton.resource.{DbStorage, DbStore}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{EitherTUtil, ErrorUtil}
import com.digitalasset.canton.version.ReleaseProtocolVersion
import slick.jdbc.{GetResult, SetParameter}

import scala.concurrent.{ExecutionContext, Future}

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

  private val insertTime: TimedLoadGauge =
    storage.metrics.loadGaugeM("crypto-public-store-insert")
  private val queryTime: TimedLoadGauge =
    storage.metrics.loadGaugeM("crypto-public-store-query")

  private implicit val setParameterEncryptionPublicKey: SetParameter[EncryptionPublicKey] =
    EncryptionPublicKey.getVersionedSetParameter(releaseProtocolVersion.v)
  private implicit val setParameterSigningPublicKey: SetParameter[SigningPublicKey] =
    SigningPublicKey.getVersionedSetParameter(releaseProtocolVersion.v)

  private def queryKeys[K: GetResult](purpose: KeyPurpose): DbAction.ReadOnly[Set[K]] =
    sql"select data, name from crypto_public_keys where purpose = $purpose"
      .as[K]
      .map(_.toSet)

  private def queryKey[K <: PublicKeyWithName: GetResult](
      keyId: Fingerprint,
      purpose: KeyPurpose,
  ): DbAction.ReadOnly[Option[K]] =
    sql"select data, name from crypto_public_keys where key_id = $keyId and purpose = $purpose"
      .as[K]
      .headOption

  private def insertKeyUpdate[K <: PublicKey: SetParameter, KN <: PublicKeyWithName: GetResult](
      key: K,
      name: Option[KeyName],
  ): DbAction.WriteOnly[Int] =
    storage.profile match {
      case _: DbStorage.Profile.Oracle =>
        sqlu"""insert
               /*+  IGNORE_ROW_ON_DUPKEY_INDEX ( crypto_public_keys ( key_id ) ) */
               into crypto_public_keys (key_id, purpose, data, name)
           values (${key.id}, ${key.purpose}, $key, $name)"""
      case _ =>
        sqlu"""insert into crypto_public_keys (key_id, purpose, data, name)
           values (${key.id}, ${key.purpose}, $key, $name)
           on conflict do nothing"""
    }

  private def insertKey[K <: PublicKey: SetParameter, KN <: PublicKeyWithName: GetResult](
      key: K,
      name: Option[KeyName],
  )(implicit traceContext: TraceContext): EitherT[Future, CryptoPublicStoreError, Unit] =
    insertTime.eitherTEvent {
      for {
        inserted <- EitherT.right(storage.update(insertKeyUpdate(key, name), functionFullName))
        res <-
          if (inserted == 0) {
            // If no key was inserted by the insert query, check that the existing value matches
            storage
              .querySingle(queryKey(key.id, key.purpose), functionFullName)
              .toRight(
                CryptoPublicStoreError.FailedToInsertKey(key.id, "No key inserted and no key found")
              )
              .flatMap { existingKey =>
                EitherT
                  .cond[Future](
                    existingKey.publicKey == key && existingKey.name == name,
                    (),
                    CryptoPublicStoreError.KeyAlreadyExists(key.id, existingKey.name.map(_.unwrap)),
                  )
                  .leftWiden[CryptoPublicStoreError]
              }
          } else EitherT.rightT[Future, CryptoPublicStoreError](())
      } yield res
    }

  override def readSigningKey(signingKeyId: Fingerprint)(implicit
      traceContext: TraceContext
  ): EitherT[Future, CryptoPublicStoreError, Option[SigningPublicKeyWithName]] =
    EitherTUtil.fromFuture(
      storage
        .querySingle(
          queryKey[SigningPublicKeyWithName](signingKeyId, KeyPurpose.Signing),
          functionFullName,
        )
        .value,
      err => CryptoPublicStoreError.FailedToReadKey(signingKeyId, err.toString),
    )

  override def readEncryptionKey(encryptionKeyId: Fingerprint)(implicit
      traceContext: TraceContext
  ): EitherT[Future, CryptoPublicStoreError, Option[EncryptionPublicKeyWithName]] =
    EitherTUtil.fromFuture(
      storage
        .querySingle(
          queryKey[EncryptionPublicKeyWithName](encryptionKeyId, KeyPurpose.Encryption),
          functionFullName,
        )
        .value,
      err => CryptoPublicStoreError.FailedToReadKey(encryptionKeyId, err.toString),
    )

  override protected def writeSigningKey(key: SigningPublicKey, name: Option[KeyName])(implicit
      traceContext: TraceContext
  ): EitherT[Future, CryptoPublicStoreError, Unit] =
    insertKey[SigningPublicKey, SigningPublicKeyWithName](key, name)

  override protected def writeEncryptionKey(key: EncryptionPublicKey, name: Option[KeyName])(
      implicit traceContext: TraceContext
  ): EitherT[Future, CryptoPublicStoreError, Unit] =
    insertKey[EncryptionPublicKey, EncryptionPublicKeyWithName](key, name)

  override private[store] def listSigningKeys(implicit
      traceContext: TraceContext
  ): EitherT[Future, CryptoPublicStoreError, Set[SigningPublicKeyWithName]] =
    EitherTUtil.fromFuture(
      queryTime.event(
        storage.query(queryKeys[SigningPublicKeyWithName](KeyPurpose.Signing), functionFullName)
      ),
      err => CryptoPublicStoreError.FailedToListKeys(err.toString),
    )

  override private[store] def listEncryptionKeys(implicit
      traceContext: TraceContext
  ): EitherT[Future, CryptoPublicStoreError, Set[EncryptionPublicKeyWithName]] =
    EitherTUtil
      .fromFuture(
        queryTime.event(
          storage
            .query(queryKeys[EncryptionPublicKeyWithName](KeyPurpose.Encryption), functionFullName)
        ),
        err => CryptoPublicStoreError.FailedToListKeys(err.toString),
      )

  override def storeCertificate(
      cert: X509Certificate
  )(implicit traceContext: TraceContext): EitherT[Future, CryptoPublicStoreError, Unit] = {

    val insertCertAction = storage.profile match {
      case _: Profile.Oracle =>
        sqlu"""insert
               /*+ IGNORE_ROW_ON_DUPKEY_INDEX ( crypto_certs ( cert_id ) ) */
               into crypto_certs (cert_id, data)
               values (${cert.id}, $cert)"""
      case _: Profile.Postgres | _: Profile.H2 =>
        sqlu"""insert into crypto_certs (cert_id, data)
               values (${cert.id}, $cert)
               on conflict do nothing"""
    }

    def getExistingCert: OptionT[Future, X509Certificate] =
      storage.querySingle(
        sql"select data from crypto_certs where cert_id = ${cert.id}"
          .as[X509Certificate]
          .headOption,
        functionFullName,
      )

    for {
      nrRows <- EitherTUtil.fromFuture(
        insertTime.event(storage.update(insertCertAction, functionFullName)),
        err => CryptoPublicStoreError.FailedToInsertCertificate(cert.id, err.toString),
      )
      _ <- nrRows match {
        case 1 => EitherTUtil.unit[CryptoPublicStoreError]
        case 0 =>
          for {
            existingCert <- EitherT.right(
              getExistingCert.getOrElse(
                ErrorUtil.internalError(
                  new IllegalStateException(
                    s"No existing cert found for ${cert.id} but failed to insert"
                  )
                )
              )
            )
            _ <- EitherTUtil
              .condUnitET[Future](
                existingCert == cert,
                CryptoPublicStoreError.CertificateAlreadyExists(cert.id),
              )
              .leftWiden[CryptoPublicStoreError]
          } yield ()
        case _ =>
          ErrorUtil.internalError(
            new IllegalStateException(s"Updated more than 1 row for certificates: $nrRows")
          )
      }
    } yield ()
  }

  override def listCertificates()(implicit
      traceContext: TraceContext
  ): EitherT[Future, CryptoPublicStoreError, Set[X509Certificate]] =
    EitherTUtil.fromFuture(
      queryTime.event(
        storage.query(
          sql"select data from crypto_certs".as[X509Certificate].map(_.toSet),
          functionFullName,
        )
      ),
      err => CryptoPublicStoreError.FailedToListCertificates(err.toString),
    )
}
