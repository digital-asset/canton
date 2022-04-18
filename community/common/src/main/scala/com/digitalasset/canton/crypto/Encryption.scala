// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import cats.Order
import cats.data.EitherT
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.crypto.store.{
  CryptoPrivateStore,
  CryptoPrivateStoreError,
  CryptoPublicStoreError,
}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.serialization.{
  DeserializationError,
  MemoizedEvidence,
  ProtoConverter,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util._
import com.digitalasset.canton.version.{
  HasMemoizedVersionedMessageCompanion,
  HasProtoV0,
  HasVersionedMessageCompanion,
  HasVersionedToByteString,
  HasVersionedWrapper,
  ProtocolVersion,
  VersionedMessage,
}
import com.google.protobuf.ByteString
import slick.jdbc.GetResult

import scala.concurrent.{ExecutionContext, Future}

/** Encryption operations that do not require access to a private key store but operates with provided keys. */
trait EncryptionOps {

  def defaultSymmetricKeyScheme: SymmetricKeyScheme

  /** Generates and returns a random symmetric key using the specified scheme. */
  def generateSymmetricKey(
      scheme: SymmetricKeyScheme = defaultSymmetricKeyScheme
  ): Either[EncryptionKeyGenerationError, SymmetricKey]

  /** Encrypts the given bytes using the given public key */
  def encryptWith[M <: HasVersionedToByteString](
      message: M,
      publicKey: EncryptionPublicKey,
      version: ProtocolVersion,
  ): Either[EncryptionError, Encrypted[M]]

  /** Decrypts a message encrypted using `encryptWith` */
  def decryptWith[M](encrypted: Encrypted[M], privateKey: EncryptionPrivateKey)(
      deserialize: ByteString => Either[DeserializationError, M]
  ): Either[DecryptionError, M]

  /** Encrypts the given message with the given symmetric key */
  def encryptWith[M <: HasVersionedToByteString](
      message: M,
      symmetricKey: SymmetricKey,
      version: ProtocolVersion,
  ): Either[EncryptionError, Encrypted[M]]

  /** Decrypts a message encrypted using `encryptWith` */
  def decryptWith[M](encrypted: Encrypted[M], symmetricKey: SymmetricKey)(
      deserialize: ByteString => Either[DeserializationError, M]
  ): Either[DecryptionError, M]

  /** Encrypts the given message with the given randomness as the symmetric key for the given scheme.
    *
    * TODO(i4612): this is a bit of a kludge; a cleaner way would be to provide a conversion from SecureRandom to
    *             SymmetricKey, and use the encryptWith method, but that's annoying to do with Tink. Clean up once Tink
    *             is removed,
    */
  def encryptWith[M <: HasVersionedToByteString](
      message: M,
      symmetricKey: SecureRandomness,
      version: ProtocolVersion,
      scheme: SymmetricKeyScheme = defaultSymmetricKeyScheme,
  ): Either[EncryptionError, Encrypted[M]]

  /** Decrypts a message encrypted using `encryptWith` */
  def decryptWith[M](
      encrypted: Encrypted[M],
      symmetricKey: SecureRandomness,
      scheme: SymmetricKeyScheme = defaultSymmetricKeyScheme,
  )(deserialize: ByteString => Either[DeserializationError, M]): Either[DecryptionError, M]

}

/** Encryption operations that require access to stored private keys. */
trait EncryptionPrivateOps {

  def defaultEncryptionKeyScheme: EncryptionKeyScheme

  /** Decrypts an encrypted message using the referenced private encryption key */
  def decrypt[M](encrypted: Encrypted[M], encryptionKeyId: Fingerprint)(
      deserialize: ByteString => Either[DeserializationError, M]
  ): EitherT[Future, DecryptionError, M]

  /** Generates a new encryption key pair with the given scheme and optional name, stores the private key and returns the public key. */
  def generateEncryptionKey(
      scheme: EncryptionKeyScheme = defaultEncryptionKeyScheme,
      name: Option[KeyName] = None,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, EncryptionKeyGenerationError, EncryptionPublicKey]
}

/** A default implementation with a private key store */
trait EncryptionPrivateStoreOps extends EncryptionPrivateOps {

  implicit val ec: ExecutionContext

  protected def store: CryptoPrivateStore

  protected val encryptionOps: EncryptionOps

  /** Internal method to generate and return the entire encryption key pair */
  protected def generateEncryptionKeypair(scheme: EncryptionKeyScheme)(implicit
      traceContext: TraceContext
  ): EitherT[Future, EncryptionKeyGenerationError, EncryptionKeyPair]

  /** Decrypts an encrypted message using the referenced private encryption key */
  def decrypt[M](encryptedMessage: Encrypted[M], encryptionKeyId: Fingerprint)(
      deserialize: ByteString => Either[DeserializationError, M]
  ): EitherT[Future, DecryptionError, M] =
    store
      .decryptionKey(encryptionKeyId)(TraceContext.todo)
      .leftMap(DecryptionError.KeyStoreError)
      .subflatMap(_.toRight(DecryptionError.UnknownEncryptionKey(encryptionKeyId)))
      .subflatMap(encryptionKey =>
        encryptionOps.decryptWith(encryptedMessage, encryptionKey)(deserialize)
      )

  def generateEncryptionKey(
      scheme: EncryptionKeyScheme = defaultEncryptionKeyScheme,
      name: Option[KeyName] = None,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, EncryptionKeyGenerationError, EncryptionPublicKey] =
    for {
      keypair <- generateEncryptionKeypair(scheme)
      _ <- store
        .storeDecryptionKey(keypair.privateKey, name)
        .leftMap[EncryptionKeyGenerationError](
          EncryptionKeyGenerationError.EncryptionPrivateStoreError
        )
    } yield keypair.publicKey

}

/** A tag to denote encrypted data. */
case class Encrypted[+M] private[crypto] (ciphertext: ByteString) extends NoCopy

object Encrypted {
  private[this] def apply[M](ciphertext: ByteString): Encrypted[M] =
    throw new UnsupportedOperationException("Use encryption methods instead")

  def fromByteString[M](byteString: ByteString): Either[DeserializationError, Encrypted[M]] =
    Right(new Encrypted[M](byteString))
}

/** Key schemes for asymmetric/hybrid encryption. */
sealed trait EncryptionKeyScheme extends Product with Serializable with PrettyPrinting {
  def name: String
  def toProtoEnum: v0.EncryptionKeyScheme
  override val pretty: Pretty[this.type] = prettyOfString(_.name)
}

object EncryptionKeyScheme {

  implicit val encryptionKeySchemeOrder: Order[EncryptionKeyScheme] =
    Order.by[EncryptionKeyScheme, String](_.name)

  case object EciesP256HkdfHmacSha256Aes128Gcm extends EncryptionKeyScheme {
    override val name: String = "ECIES-P256_HMAC256_AES128-GCM"
    override def toProtoEnum: v0.EncryptionKeyScheme =
      v0.EncryptionKeyScheme.EciesP256HkdfHmacSha256Aes128Gcm
  }

  def fromProtoEnum(
      field: String,
      schemeP: v0.EncryptionKeyScheme,
  ): ParsingResult[EncryptionKeyScheme] =
    schemeP match {
      case v0.EncryptionKeyScheme.MissingEncryptionKeyScheme =>
        Left(ProtoDeserializationError.FieldNotSet(field))
      case v0.EncryptionKeyScheme.Unrecognized(value) =>
        Left(ProtoDeserializationError.UnrecognizedEnum(field, value))
      case v0.EncryptionKeyScheme.EciesP256HkdfHmacSha256Aes128Gcm =>
        Right(EncryptionKeyScheme.EciesP256HkdfHmacSha256Aes128Gcm)
    }
}

/** Key schemes for symmetric encryption. */
sealed trait SymmetricKeyScheme extends Product with Serializable with PrettyPrinting {
  def name: String
  def toProtoEnum: v0.SymmetricKeyScheme
  def keySizeInBytes: Int
  override def pretty: Pretty[this.type] = prettyOfString(_.name)
}

object SymmetricKeyScheme {

  implicit val symmetricKeySchemeOrder: Order[SymmetricKeyScheme] =
    Order.by[SymmetricKeyScheme, String](_.name)

  /** AES with 128bit key in GCM */
  case object Aes128Gcm extends SymmetricKeyScheme {
    override def name: String = "AES128-GCM"
    override def toProtoEnum: v0.SymmetricKeyScheme = v0.SymmetricKeyScheme.Aes128Gcm
    override def keySizeInBytes: Int = 16
  }

  def fromProtoEnum(
      field: String,
      schemeP: v0.SymmetricKeyScheme,
  ): ParsingResult[SymmetricKeyScheme] =
    schemeP match {
      case v0.SymmetricKeyScheme.MissingSymmetricKeyScheme =>
        Left(ProtoDeserializationError.FieldNotSet(field))
      case v0.SymmetricKeyScheme.Unrecognized(value) =>
        Left(ProtoDeserializationError.UnrecognizedEnum(field, value))
      case v0.SymmetricKeyScheme.Aes128Gcm => Right(SymmetricKeyScheme.Aes128Gcm)
    }
}

final case class SymmetricKey private (
    format: CryptoKeyFormat,
    protected[crypto] val key: ByteString,
    scheme: SymmetricKeyScheme,
)(override val deserializedFrom: Option[ByteString])
    extends CryptoKey
    with MemoizedEvidence
    with HasVersionedWrapper[VersionedMessage[SymmetricKey]]
    with NoCopy {

  protected def toProtoVersioned(version: ProtocolVersion): VersionedMessage[SymmetricKey] =
    VersionedMessage(toProtoV0.toByteString, 0)

  protected def toProtoV0: v0.SymmetricKey =
    v0.SymmetricKey(format = format.toProtoEnum, key = key, scheme = scheme.toProtoEnum)

  override protected def toByteStringUnmemoized(version: ProtocolVersion): ByteString =
    super[HasVersionedWrapper].toByteString(version)
}

object SymmetricKey extends HasMemoizedVersionedMessageCompanion[SymmetricKey] {
  override val name: String = "SymmetricKey"

  val supportedProtoVersions: Map[Int, Parser] = Map(
    0 -> supportedProtoVersionMemoized(v0.SymmetricKey)(fromProtoV0)
  )

  private[this] def apply(
      format: CryptoKeyFormat,
      key: ByteString,
      scheme: SymmetricKeyScheme,
  ): SymmetricKey =
    throw new UnsupportedOperationException("Use generate or deserialization methods")

  private[crypto] def create(
      format: CryptoKeyFormat,
      key: ByteString,
      scheme: SymmetricKeyScheme,
  ): SymmetricKey = {
    new SymmetricKey(format, key, scheme)(None)
  }

  private def fromProtoV0(keyP: v0.SymmetricKey)(bytes: ByteString): ParsingResult[SymmetricKey] =
    for {
      format <- CryptoKeyFormat.fromProtoEnum("format", keyP.format)
      scheme <- SymmetricKeyScheme.fromProtoEnum("scheme", keyP.scheme)
    } yield new SymmetricKey(format, keyP.key, scheme)(Some(bytes))
}

final case class EncryptionKeyPair(publicKey: EncryptionPublicKey, privateKey: EncryptionPrivateKey)
    extends CryptoKeyPair[EncryptionPublicKey, EncryptionPrivateKey]
    with HasProtoV0[v0.EncryptionKeyPair]
    with NoCopy {
  override def toProtoV0: v0.EncryptionKeyPair =
    v0.EncryptionKeyPair(Some(publicKey.toProtoV0), Some(privateKey.toProtoV0))

  override protected def toProtoCryptoKeyPairPairV0: v0.CryptoKeyPair.Pair =
    v0.CryptoKeyPair.Pair.EncryptionKeyPair(toProtoV0)
}

object EncryptionKeyPair {

  private[this] def apply(
      publicKey: EncryptionPublicKey,
      privateKey: EncryptionPrivateKey,
  ): EncryptionKeyPair =
    throw new UnsupportedOperationException("Use generate or deserialization methods")

  private[crypto] def create(
      id: Fingerprint,
      format: CryptoKeyFormat,
      publicKeyBytes: ByteString,
      privateKeyBytes: ByteString,
      scheme: EncryptionKeyScheme,
  ): EncryptionKeyPair = {
    val publicKey = new EncryptionPublicKey(id, format, publicKeyBytes, scheme)
    val privateKey = new EncryptionPrivateKey(publicKey.id, format, privateKeyBytes, scheme)
    new EncryptionKeyPair(publicKey, privateKey)
  }

  def fromProtoV0(
      encryptionKeyPairP: v0.EncryptionKeyPair
  ): ParsingResult[EncryptionKeyPair] =
    for {
      publicKey <- ProtoConverter.parseRequired(
        EncryptionPublicKey.fromProtoV0,
        "public_key",
        encryptionKeyPairP.publicKey,
      )
      privateKey <- ProtoConverter.parseRequired(
        EncryptionPrivateKey.fromProtoV0,
        "private_key",
        encryptionKeyPairP.privateKey,
      )
    } yield new EncryptionKeyPair(publicKey, privateKey)
}

case class EncryptionPublicKey private[crypto] (
    id: Fingerprint,
    format: CryptoKeyFormat,
    protected[crypto] val key: ByteString,
    scheme: EncryptionKeyScheme,
) extends PublicKey
    with HasVersionedWrapper[VersionedMessage[EncryptionPublicKey]]
    with PrettyPrinting
    with HasProtoV0[v0.EncryptionPublicKey]
    with NoCopy {
  val purpose: KeyPurpose = KeyPurpose.Encryption

  override def toProtoVersioned(version: ProtocolVersion): VersionedMessage[EncryptionPublicKey] =
    VersionedMessage(toProtoV0.toByteString, 0)

  override def toProtoV0: v0.EncryptionPublicKey =
    v0.EncryptionPublicKey(
      id = id.toProtoPrimitive,
      format = format.toProtoEnum,
      publicKey = key,
      scheme = scheme.toProtoEnum,
    )

  override protected def toProtoPublicKeyKeyV0: v0.PublicKey.Key =
    v0.PublicKey.Key.EncryptionPublicKey(toProtoV0)

  override val pretty: Pretty[EncryptionPublicKey] =
    prettyOfClass(param("id", _.id), param("format", _.format), param("scheme", _.scheme))
}

object EncryptionPublicKey extends HasVersionedMessageCompanion[EncryptionPublicKey] {
  val supportedProtoVersions: Map[Int, Parser] = Map(
    0 -> supportedProtoVersion(v0.EncryptionPublicKey)(fromProtoV0)
  )

  override protected def name: String = "encryption public key"

  private[this] def apply(
      id: Fingerprint,
      format: CryptoKeyFormat,
      key: ByteString,
      scheme: EncryptionKeyScheme,
  ): EncryptionPrivateKey =
    throw new UnsupportedOperationException("Use generate or deserialization methods")

  def fromProtoV0(
      publicKeyP: v0.EncryptionPublicKey
  ): ParsingResult[EncryptionPublicKey] =
    for {
      id <- Fingerprint.fromProtoPrimitive(publicKeyP.id)
      format <- CryptoKeyFormat.fromProtoEnum("format", publicKeyP.format)
      scheme <- EncryptionKeyScheme.fromProtoEnum("scheme", publicKeyP.scheme)
    } yield new EncryptionPublicKey(id, format, publicKeyP.publicKey, scheme)
}

case class EncryptionPublicKeyWithName(
    override val publicKey: EncryptionPublicKey,
    override val name: Option[KeyName],
) extends PublicKeyWithName {
  type K = EncryptionPublicKey
}

object EncryptionPublicKeyWithName {
  implicit def getResultEncryptionPublicKeyWithName(implicit
      getResultByteArray: GetResult[Array[Byte]]
  ): GetResult[EncryptionPublicKeyWithName] =
    GetResult { r =>
      EncryptionPublicKeyWithName(r.<<, r.<<)
    }
}

final case class EncryptionPrivateKey private[crypto] (
    id: Fingerprint,
    format: CryptoKeyFormat,
    protected[crypto] val key: ByteString,
    scheme: EncryptionKeyScheme,
) extends PrivateKey
    with HasVersionedWrapper[VersionedMessage[EncryptionPrivateKey]]
    with HasProtoV0[v0.EncryptionPrivateKey]
    with NoCopy {

  override def purpose: KeyPurpose = KeyPurpose.Encryption

  override def toProtoVersioned(version: ProtocolVersion): VersionedMessage[EncryptionPrivateKey] =
    VersionedMessage(toProtoV0.toByteString, 0)

  override def toProtoV0: v0.EncryptionPrivateKey =
    v0.EncryptionPrivateKey(
      id = id.toProtoPrimitive,
      format = format.toProtoEnum,
      privateKey = key,
      scheme = scheme.toProtoEnum,
    )

  override protected def toProtoPrivateKeyKeyV0: v0.PrivateKey.Key =
    v0.PrivateKey.Key.EncryptionPrivateKey(toProtoV0)
}

object EncryptionPrivateKey extends HasVersionedMessageCompanion[EncryptionPrivateKey] {
  val supportedProtoVersions: Map[Int, Parser] = Map(
    0 -> supportedProtoVersion(v0.EncryptionPrivateKey)(fromProtoV0)
  )

  override protected def name: String = "encryption private key"

  private[this] def apply(
      id: Fingerprint,
      format: CryptoKeyFormat,
      key: ByteString,
      scheme: EncryptionKeyScheme,
  ): EncryptionPrivateKey =
    throw new UnsupportedOperationException("Use generate or deserialization methods")

  def fromProtoV0(
      privateKeyP: v0.EncryptionPrivateKey
  ): ParsingResult[EncryptionPrivateKey] =
    for {
      id <- Fingerprint.fromProtoPrimitive(privateKeyP.id)
      format <- CryptoKeyFormat.fromProtoEnum("format", privateKeyP.format)
      scheme <- EncryptionKeyScheme.fromProtoEnum("scheme", privateKeyP.scheme)
    } yield new EncryptionPrivateKey(id, format, privateKeyP.privateKey, scheme)
}

sealed trait EncryptionError extends Product with Serializable with PrettyPrinting
object EncryptionError {
  case class FailedToEncrypt(error: String) extends EncryptionError {
    override def pretty: Pretty[FailedToEncrypt] = prettyOfClass(unnamedParam(_.error.unquoted))
  }
  case class InvalidSymmetricKey(error: String) extends EncryptionError {
    override def pretty: Pretty[InvalidSymmetricKey] = prettyOfClass(unnamedParam(_.error.unquoted))
  }
  case class InvalidEncryptionKey(error: String) extends EncryptionError {
    override def pretty: Pretty[InvalidEncryptionKey] = prettyOfClass(
      unnamedParam(_.error.unquoted)
    )
  }
}

sealed trait DecryptionError extends Product with Serializable with PrettyPrinting
object DecryptionError {
  case class FailedToDecrypt(error: String) extends DecryptionError {
    override def pretty: Pretty[FailedToDecrypt] = prettyOfClass(unnamedParam(_.error.unquoted))
  }
  case class InvalidSymmetricKey(error: String) extends DecryptionError {
    override def pretty: Pretty[InvalidSymmetricKey] = prettyOfClass(unnamedParam(_.error.unquoted))
  }
  case class InvalidEncryptionKey(error: String) extends DecryptionError {
    override def pretty: Pretty[InvalidEncryptionKey] = prettyOfClass(
      unnamedParam(_.error.unquoted)
    )
  }
  case class UnknownEncryptionKey(keyId: Fingerprint) extends DecryptionError {
    override def pretty: Pretty[UnknownEncryptionKey] = prettyOfClass(param("keyId", _.keyId))
  }
  case class DecryptionKeyError(error: CryptoPrivateStoreError) extends DecryptionError {
    override def pretty: Pretty[DecryptionKeyError] = prettyOfClass(unnamedParam(_.error))
  }
  case class FailedToDeserialize(error: DeserializationError) extends DecryptionError {
    override def pretty: Pretty[FailedToDeserialize] = prettyOfClass(unnamedParam(_.error))
  }
  case class KeyStoreError(error: CryptoPrivateStoreError) extends DecryptionError {
    override def pretty: Pretty[KeyStoreError] =
      prettyOfClass(unnamedParam(_.error))
  }
}

sealed trait EncryptionKeyGenerationError extends Product with Serializable with PrettyPrinting
object EncryptionKeyGenerationError {

  case class GeneralError(error: Exception) extends EncryptionKeyGenerationError {
    override def pretty: Pretty[GeneralError] = prettyOfClass(unnamedParam(_.error))
  }

  case class NameInvalidError(error: String) extends EncryptionKeyGenerationError {
    override def pretty: Pretty[NameInvalidError] = prettyOfClass(unnamedParam(_.error.unquoted))
  }

  case class FingerprintError(error: String) extends EncryptionKeyGenerationError {
    override def pretty: Pretty[FingerprintError] = prettyOfClass(unnamedParam(_.error.unquoted))
  }

  case class UnsupportedKeyScheme(scheme: EncryptionKeyScheme)
      extends EncryptionKeyGenerationError {
    override def pretty: Pretty[UnsupportedKeyScheme] = prettyOfClass(param("scheme", _.scheme))
  }

  case class EncryptionPrivateStoreError(error: CryptoPrivateStoreError)
      extends EncryptionKeyGenerationError {
    override def pretty: Pretty[EncryptionPrivateStoreError] = prettyOfClass(unnamedParam(_.error))
  }

  case class EncryptionPublicStoreError(error: CryptoPublicStoreError)
      extends EncryptionKeyGenerationError {
    override def pretty: Pretty[EncryptionPublicStoreError] = prettyOfClass(unnamedParam(_.error))
  }
}
