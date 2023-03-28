// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.parallel.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.Password
import com.digitalasset.canton.crypto.store.{CryptoPrivateStore, CryptoPublicStore}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureInstances.*
import org.bouncycastle.asn1.edec.EdECObjectIdentifiers
import org.bouncycastle.asn1.gm.GMObjectIdentifiers
import org.bouncycastle.asn1.sec.SECObjectIdentifiers
import org.bouncycastle.asn1.x509.AlgorithmIdentifier
import org.bouncycastle.asn1.x9.X9ObjectIdentifiers

import java.security.{
  KeyStore as JKeyStore,
  KeyStoreException,
  PrivateKey as JPrivateKey,
  PublicKey as JPublicKey,
}
import scala.concurrent.{ExecutionContext, Future}

trait JavaKeyConverter {

  import com.digitalasset.canton.util.ShowUtil.*

  /** Convert to Java private key */
  def toJava(privateKey: PrivateKey): Either[JavaKeyConversionError, JPrivateKey]

  /** Convert to Java public key */
  def toJava(
      publicKey: PublicKey
  ): Either[JavaKeyConversionError, (AlgorithmIdentifier, JPublicKey)]

  /** Export certificates and corresponding signing private keys to a Java key store */
  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  def toJava(
      cryptoPrivateStore: CryptoPrivateStore,
      cryptoPublicStore: CryptoPublicStore,
      keyStorePass: Password,
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[Future, JavaKeyConversionError, JKeyStore] = {

    def exportCertificate(
        keystore: JKeyStore,
        cert: X509Certificate,
    ): EitherT[Future, JavaKeyConversionError, Unit] =
      for {
        _ <- Either
          .catchOnly[KeyStoreException](keystore.setCertificateEntry(cert.id.unwrap, cert.unwrap))
          .leftMap(err =>
            JavaKeyConversionError.KeyStoreError(
              show"Failed to set certificate entry in keystore: $err"
            )
          )
          .toEitherT[Future]
        keyId <- cert
          .publicKey(this)
          .bimap(
            err =>
              JavaKeyConversionError.KeyStoreError(
                s"Failed to get public key id from certificate: $err"
              ),
            _.fingerprint,
          )
          .toEitherT[Future]
        privateKey <- cryptoPrivateStore
          .signingKey(keyId)
          .leftMap(storeError =>
            JavaKeyConversionError.KeyStoreError(
              show"Error retrieving private signing key $keyId: $storeError"
            )
          )
          .subflatMap(
            _.toRight(
              JavaKeyConversionError.KeyStoreError(show"Unknown private signing key $keyId")
            )
          )
        javaPrivateKey <- toJava(privateKey).toEitherT[Future]
        _ <- Either
          .catchOnly[KeyStoreException](
            keystore.setKeyEntry(
              cert.id.unwrap,
              javaPrivateKey,
              keyStorePass.toCharArray,
              Array(cert.unwrap),
            )
          )
          .leftMap[JavaKeyConversionError](err =>
            JavaKeyConversionError.KeyStoreError(show"Failed to set key in keystore: $err")
          )
          .toEitherT[Future]
      } yield ()

    for {
      keystore <- Either
        .catchOnly[KeyStoreException](JKeyStore.getInstance("PKCS12"))
        .leftMap(err =>
          JavaKeyConversionError.KeyStoreError(show"Failed to create keystore instance: $err")
        )
        .toEitherT[Future]

      _ <- Either
        .catchNonFatal(keystore.load(null, keyStorePass.toCharArray))
        .leftMap(err =>
          JavaKeyConversionError.KeyStoreError(show"Failed to initialize keystore: $err")
        )
        .toEitherT[Future]

      certs <- cryptoPublicStore
        .listCertificates()
        .leftMap(err =>
          JavaKeyConversionError.KeyStoreError(show"Failed to list certificates: $err")
        )

      _ <- certs.toList.parTraverse_(cert => exportCertificate(keystore, cert))
    } yield keystore
  }

  def fromJavaSigningKey(
      publicKey: JPublicKey,
      algorithmIdentifier: AlgorithmIdentifier,
  ): Either[JavaKeyConversionError, SigningPublicKey]
}

object JavaKeyConverter {

  def toSigningKeyScheme(
      algoId: AlgorithmIdentifier
  ): Either[JavaKeyConversionError, SigningKeyScheme] =
    algoId.getAlgorithm match {
      case X9ObjectIdentifiers.ecdsa_with_SHA256 | SECObjectIdentifiers.secp256r1 =>
        Right(SigningKeyScheme.EcDsaP256)
      case X9ObjectIdentifiers.ecdsa_with_SHA384 | SECObjectIdentifiers.secp384r1 =>
        Right(SigningKeyScheme.EcDsaP384)
      case EdECObjectIdentifiers.id_Ed25519 => Right(SigningKeyScheme.Ed25519)
      case GMObjectIdentifiers.sm2p256v1 => Right(SigningKeyScheme.Sm2)
      case unsupportedIdentifier =>
        Left(JavaKeyConversionError.UnsupportedAlgorithm(algoId))
    }

}

sealed trait JavaKeyConversionError extends Product with Serializable with PrettyPrinting

object JavaKeyConversionError {

  final case class GeneralError(error: Exception) extends JavaKeyConversionError {
    override def pretty: Pretty[GeneralError] =
      prettyOfClass(unnamedParam(_.error))
  }

  final case class UnsupportedAlgorithm(algorithmIdentifier: AlgorithmIdentifier)
      extends JavaKeyConversionError {
    override def pretty: Pretty[UnsupportedAlgorithm] =
      prettyOfClass(unnamedParam(_.algorithmIdentifier.toString.unquoted))
  }

  final case class UnsupportedKeyFormat(format: CryptoKeyFormat, expectedFormat: CryptoKeyFormat)
      extends JavaKeyConversionError {
    override def pretty: Pretty[UnsupportedKeyFormat] =
      prettyOfClass(param("format", _.format), param("expected format", _.expectedFormat))
  }

  final case class UnsupportedKeyScheme(
      scheme: SigningKeyScheme,
      supportedSchemes: NonEmpty[Set[SigningKeyScheme]],
  ) extends JavaKeyConversionError {
    override def pretty: Pretty[UnsupportedKeyScheme] =
      prettyOfClass(
        param("scheme", _.scheme),
        param("supported schemes", _.supportedSchemes),
      )
  }

  final case class KeyStoreError(error: String) extends JavaKeyConversionError {
    override def pretty: Pretty[KeyStoreError] =
      prettyOfClass(unnamedParam(_.error.unquoted))
  }

  final case class InvalidKey(error: String) extends JavaKeyConversionError {
    override def pretty: Pretty[InvalidKey] =
      prettyOfClass(unnamedParam(_.error.unquoted))
  }

}
