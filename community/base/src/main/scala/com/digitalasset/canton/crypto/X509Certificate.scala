// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import better.files.*
import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.functorFilter.*
import cats.syntax.traverse.*
import com.digitalasset.canton.config.CantonRequireTypes.{
  LengthLimitedStringWrapper,
  LengthLimitedStringWrapperCompanion,
  String255,
}
import com.digitalasset.canton.crypto
import com.digitalasset.canton.crypto.X509CertificateError.DecodingError
import com.digitalasset.canton.crypto.store.CryptoPublicStoreError
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.store.db.DbDeserializationException
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ShowUtil.*
import com.google.protobuf.ByteString
import org.bouncycastle.asn1.x500.X500Name
import org.bouncycastle.asn1.x500.style.{BCStyle, IETFUtils}
import org.bouncycastle.asn1.x509.*
import org.bouncycastle.asn1.{ASN1ObjectIdentifier, ASN1Sequence, DERIA5String}
import org.bouncycastle.cert.X509v3CertificateBuilder
import org.bouncycastle.cert.jcajce.{JcaX509CertificateConverter, JcaX509CertificateHolder}
import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.bouncycastle.openssl.jcajce.JcaPEMWriter
import org.bouncycastle.operator.ContentSigner
import slick.jdbc.{GetResult, SetParameter}

import java.io.{IOException, OutputStream, OutputStreamWriter}
import java.math.BigInteger
import java.security.cert.{
  CertificateEncodingException,
  CertificateException,
  CertificateFactory,
  CertificateParsingException,
  X509Certificate as JX509Certificate,
}
import java.time.{Duration, Instant}
import java.util.Date
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters.*

final case class CertificateId(override val str: String255)
    extends LengthLimitedStringWrapper
    with PrettyPrinting {
  override def pretty: Pretty[CertificateId] = prettyOfString(_.unwrap)
}

object CertificateId extends LengthLimitedStringWrapperCompanion[String255, CertificateId] {
  override def instanceName: String = "CertificateId"
  override protected def companion: String255.type = String255
  override protected def factoryMethodWrapper(str: String255): CertificateId = CertificateId(str)
}

/** A Scala wrapper for Java X509 Certificates */
final case class X509Certificate(private val cert: JX509Certificate) {

  def unwrap: JX509Certificate = cert

  // TODO(i11013): We may want to use a combination of public key fingerprint and serial as the id
  // We assume this `tryCreate` is always safe to do, as the certificate ID has a limited amount of data
  // https://datatracker.ietf.org/doc/html/rfc3280#page-17
  def id: CertificateId = CertificateId.tryCreate(cert.getSerialNumber.toString)

  def subjectIdentifier: Either[X509CertificateError, X500Name] =
    Either
      .catchOnly[CertificateEncodingException] {
        new JcaX509CertificateHolder(cert).getSubject
      }
      .leftMap(err => X509CertificateError.DecodingError(s"Failed to decode certificate: $err"))

  def subjectCommonName: Either[X509CertificateError, String] =
    for {
      sid <- subjectIdentifier
      cnRDN <- sid
        .getRDNs(BCStyle.CN)
        .headOption
        .toRight(X509CertificateError.DecodingError("No common name RDN found"))
      cn <- Option(cnRDN.getFirst).toRight(
        X509CertificateError.DecodingError("No common name found")
      )
    } yield IETFUtils.valueToString(cn.getValue)

  def subjectAlternativeNames: Either[X509CertificateError, Seq[String]] =
    for {
      rawNames <- Either
        .catchOnly[CertificateParsingException](cert.getSubjectAlternativeNames)
        .leftMap(err => DecodingError(s"Failed to extract subject alternative names: $err"))
        // The method may return null which we treat as no SANs by returning an empty list
        .map(res => Option(res).map(_.asScala.toList).getOrElse(List.empty))

      names = rawNames.mapFilter { namePair =>
        namePair.asScala.toList match {
          case List(GeneralName.dNSName, name: String) => Some(name)
          case _ => None
        }
      }
    } yield names

  def publicKey(
      javaKeyConverter: JavaKeyConverter
  ): Either[X509CertificateError, SigningPublicKey] =
    for {
      algo <- Option(cert.getSigAlgOID)
        .toRight(X509CertificateError.DecodingError("Signature algorithm ID is not set"))
      algoObjId <- Either
        .catchOnly[IllegalArgumentException](new ASN1ObjectIdentifier(algo))
        .leftMap(err =>
          X509CertificateError.DecodingError(s"Invalid object identifier $algo: $err")
        )
      algoId = new AlgorithmIdentifier(algoObjId)
      signingPubKey <- javaKeyConverter
        .fromJavaSigningKey(cert.getPublicKey, algoId)
        .leftMap(err =>
          X509CertificateError.DecodingError(s"Failed to extract public key from certificate: $err")
        )
    } yield signingPubKey

  override def toString: String = cert.toString

  /** Serialize certificate into PEM. */
  def toPem: Either[X509CertificateError, X509CertificatePem] = {
    val output = ByteString.newOutput()
    val writer = new OutputStreamWriter(output)
    val pemWriter = new JcaPEMWriter(writer)
    val result = Either
      .catchOnly[IOException] {
        pemWriter.writeObject(cert)
        pemWriter.flush()
        pemWriter.close()
      }
      .leftMap(ex =>
        X509CertificateError.EncodingError(s"Failed to write certificate to PEM string: $ex")
      )

    result.flatMap { _ =>
      X509CertificatePem
        .fromBytes(output.toByteString)
        .leftMap(err =>
          X509CertificateError.EncodingError(
            s"Failed to produce PEM encoding for certificate: $err"
          )
        )
    }
  }

  def tryToPem: X509CertificatePem =
    toPem.valueOr(err =>
      throw new IllegalStateException(s"Failed to serialize certificate to PEM: $err")
    )

  def toDer: Either[X509CertificateError, X509CertificateDer] =
    (for {
      derByteString <- Either
        .catchOnly[CertificateEncodingException](cert.getEncoded)
        .map(ByteString.copyFrom)
        .leftMap(_.toString)

      der <- X509CertificateDer.fromBytes(derByteString)

    } yield der).leftMap(ex =>
      X509CertificateError.EncodingError(s"Failed to get DER encoding for certificate: $ex")
    )

  def tryToDer: X509CertificateDer =
    toDer.valueOr(err =>
      throw new IllegalStateException(s"Failed to serialize certificate to DER: $err")
    )
}

class X509CertificateGenerator(
    crypto: Crypto,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends NamedLogging {

  /** Helper class to sign a x509 certificate using the crypto private api. */
  private class CantonContentSigner(algorithmIdentifier: AlgorithmIdentifier) {
    private val certConverter = new JcaX509CertificateConverter()
      .setProvider(new BouncyCastleProvider())

    // since ContentSigner has a synchronous interface, while our crypto api is Future-based we create one signer
    // that captures the input bytes and another one that outputs the signature
    private abstract class Signer(override val getSignature: Array[Byte]) extends ContentSigner {
      override def getAlgorithmIdentifier: AlgorithmIdentifier = algorithmIdentifier
    }
    private class BytesReceiver() extends Signer(Array[Byte]()) {
      private val stream = ByteString.newOutput()
      override def getOutputStream: OutputStream = stream
      def bytes: ByteString = stream.toByteString
    }
    private class SignatureProvider(
        signature: ByteString,
        override val getOutputStream: OutputStream = ByteString.newOutput(),
    ) extends Signer(signature.toByteArray)

    def signedCertificate(
        certificateBuilder: X509v3CertificateBuilder,
        signignKeyId: Fingerprint,
    ): EitherT[Future, X509CertificateError, X509Certificate] = {
      def getCertificate(signer: ContentSigner) =
        Either
          .catchOnly[CertificateException](
            certConverter.getCertificate(certificateBuilder.build(signer))
          )
          .leftMap[X509CertificateError](err =>
            X509CertificateError.EncodingError(show"Failed to generate certificate: $err")
          )
          .toEitherT[Future]
      val receiver = new BytesReceiver()
      for {
        _ <- getCertificate(receiver)
        signature <- crypto.privateCrypto
          .sign(receiver.bytes, signignKeyId)
          .leftMap(signingError => X509CertificateError.SigningError(signingError))
        certificate <- getCertificate(
          new SignatureProvider(signature.unwrap)
        )
      } yield X509Certificate(certificate)
    }
  }

  /** Generates a new self-signed X509 Certificate for the given subjectIdentifier and using the existing keypair
    * given by keyId.
    */
  def generate(
      subjectIdentifier: X500Name,
      signingKeyId: Fingerprint,
      subjectAlternativeNames: Seq[String],
      validityInDays: Long = 365,
  )(implicit traceContext: TraceContext): EitherT[Future, X509CertificateError, X509Certificate] = {

    val now = Instant.now()
    val notBefore = Date.from(now)
    val notAfter = Date.from(now.plus(Duration.ofDays(validityInDays)))

    // Use the timestamp as the serial number for the certificate
    val serial = BigInteger.valueOf(now.toEpochMilli)

    def certificate(publicKey: PublicKey): EitherT[Future, X509CertificateError, X509Certificate] =
      for {
        javaPublicKeyResult <- crypto.javaKeyConverter
          .toJava(publicKey)
          .leftMap(err => X509CertificateError.EncodingError(err.toString))
          .toEitherT[Future]
        (publicKeyAlgoId, javaPublicKey) = javaPublicKeyResult
        publicKeyInfo = SubjectPublicKeyInfo.getInstance(
          ASN1Sequence.getInstance(javaPublicKey.getEncoded)
        )
        certificateBuilder = new X509v3CertificateBuilder(
          subjectIdentifier,
          serial,
          notBefore,
          notAfter,
          subjectIdentifier,
          publicKeyInfo,
        )

        // Set subject alternative names if any are given
        _ <- (if (subjectAlternativeNames.nonEmpty) {
                for {
                  subjectAltNames <- subjectAlternativeNames.traverse { name =>
                    Either
                      .catchNonFatal(
                        // TODO(i11014): Consider to have the unique identifiers or member ids in a standard format
                        // Uses a DNS name format although we are not strictly setting a DNS name. However, CCF's mbedtls only supports DNS names right now.
                        new GeneralName(GeneralName.dNSName, new DERIA5String(name, true))
                      )
                      .leftMap(err =>
                        X509CertificateError.InvalidSubjectAlternativeNames(
                          subjectAlternativeNames,
                          s"Invalid subject alternative name $name: $err",
                        )
                      )
                  }
                  _ <- Either
                    .catchNonFatal(
                      certificateBuilder
                        .addExtension(
                          Extension.subjectAlternativeName,
                          false,
                          new GeneralNames(subjectAltNames.toArray),
                        )
                    )
                    .leftMap[X509CertificateError](err =>
                      X509CertificateError.InvalidSubjectAlternativeNames(
                        subjectAlternativeNames,
                        s"Failed to add subject alternative names extension to certificate: $err",
                      )
                    )
                } yield ()
              } else Either.right(())).toEitherT[Future]

        cert <- new CantonContentSigner(publicKeyAlgoId)
          .signedCertificate(certificateBuilder, publicKey.id)
      } yield cert

    for {
      publicKey <- crypto.cryptoPublicStore
        .signingKey(signingKeyId)
        .leftMap(err => X509CertificateError.KeyStoreError(signingKeyId, err))
        .subflatMap(
          _.toRight(
            X509CertificateError
              .InvalidPublicKey(signingKeyId, s"Failed to lookup public key: $signingKeyId")
          )
        )
      cert <- certificate(publicKey)
    } yield cert
  }

  def generate(commonName: String, signingKeyId: Fingerprint)(implicit
      traceContext: TraceContext
  ): EitherT[Future, X509CertificateError, X509Certificate] = {
    val x500Name = new X500Name("CN=" + commonName)
    generate(x500Name, signingKeyId, Seq.empty)
  }

  def generate(commonName: String, signingKeyId: Fingerprint, subjectAlternativeNames: Seq[String])(
      implicit traceContext: TraceContext
  ): EitherT[Future, X509CertificateError, X509Certificate] = {
    val x500Name = new X500Name("CN=" + commonName)
    generate(x500Name, signingKeyId, subjectAlternativeNames)
  }
}

object X509Certificate {
  implicit val getResultCertificate: GetResult[X509Certificate] = GetResult { r =>
    import com.digitalasset.canton.resource.DbStorage.Implicits.getResultByteString

    X509CertificatePem
      .fromBytes(r.<<)
      .flatMap(pem => X509Certificate.fromPem(pem).leftMap(_.toString))
      .valueOr(err => throw new DbDeserializationException(err))
  }

  implicit val setParameterCertificate: SetParameter[X509Certificate] = { (c, pp) =>
    import com.digitalasset.canton.resource.DbStorage.Implicits.setParameterByteString
    pp.>>(c.tryToPem.unwrap)
  }

  def fromPem(pem: X509CertificatePem): Either[X509CertificateError, X509Certificate] = {
    val cf = CertificateFactory.getInstance("X.509")
    Either
      .catchOnly[CertificateException](cf.generateCertificate(pem.unwrap.newInput()))
      .leftMap(err =>
        X509CertificateError.DecodingError(s"Failed to parse certificate: $pem: $err")
      )
      .flatMap {
        case jcert: JX509Certificate => X509Certificate(jcert).asRight[X509CertificateError]
        case invalidCert =>
          X509CertificateError.DecodingError(s"Invalid cert: $invalidCert").asLeft[X509Certificate]
      }
  }
}

sealed trait X509CertificateError extends Product with Serializable
object X509CertificateError {
  final case class DecodingError(reason: String) extends X509CertificateError
  final case class EncodingError(reason: String) extends X509CertificateError
  final case class SigningError(reason: crypto.SigningError) extends X509CertificateError
  final case class InvalidPublicKey(key: Fingerprint, reason: String) extends X509CertificateError
  final case class KeyStoreError(key: Fingerprint, reason: CryptoPublicStoreError)
      extends X509CertificateError
  final case class InvalidSubjectAlternativeNames(
      subjectAlternativeNames: Seq[String],
      reason: String,
  ) extends X509CertificateError

}

sealed trait X509CertificateEncoder[Encoding] {

  def fromBytes(encoded: ByteString): Either[String, Encoding]

  protected def unwrap(value: Either[String, Encoding]): Encoding =
    value.valueOr(err => throw new IllegalArgumentException(s"Failed to load certificate: $err"))

  def tryFromBytes(encoded: ByteString): Encoding = unwrap(fromBytes(encoded))
}

/** A X509 Certificate serialized in PEM format. */
final case class X509CertificatePem private (private val bytes: ByteString) {
  def unwrap: ByteString = bytes

  override def toString: String = bytes.toStringUtf8
}

object X509CertificatePem extends X509CertificateEncoder[X509CertificatePem] {
  def fromString(pem: String): Either[String, X509CertificatePem] =
    fromBytes(ByteString.copyFromUtf8(pem))

  def tryFromString(pem: String): X509CertificatePem = unwrap(fromString(pem))

  def fromFile(pemFile: File): Either[String, X509CertificatePem] = {
    Either
      .catchNonFatal(pemFile.loadBytes)
      .leftMap(err => s"Failed to load PEM file: $err")
      .map(ByteString.copyFrom)
      .flatMap(X509CertificatePem.fromBytes)
  }

  def tryFromFile(pemFile: File): X509CertificatePem = unwrap(fromFile(pemFile))

  override def fromBytes(encoded: ByteString): Either[String, X509CertificatePem] =
    Right(new X509CertificatePem(encoded))
}

/** A X509 Certificate serialized in DER format. */
final case class X509CertificateDer private (private val bytes: ByteString) {
  def unwrap: ByteString = bytes

  override def toString: String = bytes.toStringUtf8
}

object X509CertificateDer extends X509CertificateEncoder[X509CertificateDer] {
  override def fromBytes(der: ByteString): Either[String, X509CertificateDer] = Right(
    new X509CertificateDer(der)
  )
}
