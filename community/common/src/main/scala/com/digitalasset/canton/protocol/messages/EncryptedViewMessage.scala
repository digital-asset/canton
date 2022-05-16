// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import cats.Functor
import cats.data.EitherT
import cats.syntax.either._
import cats.syntax.traverse._
import com.digitalasset.canton.ProtoDeserializationError.CryptoDeserializationError
import com.digitalasset.canton.crypto._
import com.digitalasset.canton.data.ViewType
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.messages.ProtocolMessage.ProtocolMessageContentCast
import com.digitalasset.canton.protocol.{ViewHash, v0}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.serialization.{DeserializationError, ProtoConverter}
import com.digitalasset.canton.topology.{DomainId, ParticipantId, UniqueIdentifier}
import com.digitalasset.canton.util._
import com.digitalasset.canton.version.{HasProtoV0, HasVersionedToByteString, ProtocolVersion}
import com.google.protobuf.ByteString

import scala.concurrent.{ExecutionContext, Future}

/** An encrypted [[com.digitalasset.canton.data.ViewTree]] together with its [[com.digitalasset.canton.data.ViewType]].
  * The correspondence is encoded via a path-dependent type.
  * The type parameter `VT` exposes a upper bound on the type of view types that may be contained.
  *
  * The view tree is compressed before encryption.
  */
// This is not a case class due to the type dependency between `viewType` and `viewTree`.
// We therefore implement the case class boilerplate stuff to the extent needed.
sealed trait EncryptedView[+VT <: ViewType] extends Product with Serializable {
  val viewType: VT
  val viewTree: Encrypted[EncryptedView.CompressedView[viewType.View]]

  override def productArity: Int = 1
  override def productElement(n: Int): Any = n match {
    case 0 => viewTree
    case _ => throw new IndexOutOfBoundsException(s"Index out of range: $n")
  }
  @SuppressWarnings(Array("org.wartremover.warts.IsInstanceOf"))
  override def canEqual(that: Any): Boolean = that.isInstanceOf[EncryptedView[_]]
  @SuppressWarnings(
    Array(
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.IsInstanceOf",
      "org.wartremover.warts.Null",
    )
  )
  override def equals(that: Any): Boolean = {
    if (this eq that.asInstanceOf[Object]) true
    else if (!that.isInstanceOf[EncryptedView[_]]) false
    else {
      val other = that.asInstanceOf[EncryptedView[ViewType]]
      val thisViewTree = this.viewTree
      if (thisViewTree eq null) other.viewTree eq null else thisViewTree == other.viewTree
    }
  }
  override def hashCode(): Int = scala.runtime.ScalaRunTime._hashCode(this)

  /** Cast the type parameter to the given argument's [[com.digitalasset.canton.data.ViewType]]
    * provided that the argument is the same as [[viewType]]
    * @return [[scala.None$]] if `desiredViewType` does not equal [[viewType]].
    */
  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  def select(desiredViewType: ViewType): Option[EncryptedView[desiredViewType.type]] =
    // Unfortunately, there doesn't seem to be a way to convince Scala's type checker that the two types must be equal.
    if (desiredViewType == viewType) Some(this.asInstanceOf[EncryptedView[desiredViewType.type]])
    else None
}
object EncryptedView {
  def apply[VT <: ViewType](
      aViewType: VT
  )(aViewTree: Encrypted[CompressedView[aViewType.View]]): EncryptedView[VT] =
    new EncryptedView[VT] {
      override val viewType: aViewType.type = aViewType
      override val viewTree = aViewTree
    }

  def compressed[VT <: ViewType](
      encryptionOps: EncryptionOps,
      secureRandomness: SecureRandomness,
      aViewType: VT,
      version: ProtocolVersion,
  )(aViewTree: aViewType.View): Either[EncryptionError, EncryptedView[VT]] =
    encryptionOps
      .encryptWith(CompressedView(aViewTree), secureRandomness, version)
      .map(apply(aViewType))

  def decrypt[VT <: ViewType](
      encryptionOps: EncryptionOps,
      symmetricKey: SecureRandomness,
      encrypted: EncryptedView[VT],
  )(
      deserialize: ByteString => Either[DeserializationError, encrypted.viewType.View]
  ): Either[DecryptionError, encrypted.viewType.View] =
    encryptionOps
      .decryptWith(encrypted.viewTree, symmetricKey)(
        CompressedView.fromByteString[encrypted.viewType.View](deserialize)(_)
      )
      .map(_.value)

  /** Wrapper class to compress the view before encrypting it.
    *
    * This class's methods are essentially private to [[EncryptedView]]
    * because compression is in theory non-deterministic (the gzip format can store a timestamp that is ignored by decryption)
    * and we want to avoid that this is applied to [[com.digitalasset.canton.serialization.HasCryptographicEvidence]]
    * instances.
    */
  case class CompressedView[+V <: HasVersionedToByteString] private (value: V)
      extends HasVersionedToByteString
      with NoCopy {
    override def toByteString(version: ProtocolVersion): ByteString =
      ByteStringUtil.compressGzip(value.toByteString(version))
  }

  object CompressedView {
    private[EncryptedView] def apply[V <: HasVersionedToByteString](value: V): CompressedView[V] =
      new CompressedView(value)

    private[EncryptedView] def fromByteString[V <: HasVersionedToByteString](
        deserialize: ByteString => Either[DeserializationError, V]
    )(bytes: ByteString): Either[DeserializationError, CompressedView[V]] =
      // TODO(M40) Make sure that this view does not explode into an arbitrarily large object
      ByteStringUtil.decompressGzip(bytes).flatMap(deserialize).map(CompressedView(_))
  }

}

/** See [[https://engineering.da-int.net/docs/platform-architecture-handbook/arch/canton/tx-data-structures.html#transaction-hashes-and-views]]
  *
  * @param submitterParticipantSignature An optional submitter participant's signature.
  * @param viewHash      Transaction view hash in plain text - included such that the recipient can prove to a 3rd party
  *                      that it has correctly decrypted the `viewTree`
  * @param randomSeed    The randomness used to derive the key used for encrypting this `viewTree`, itself encrypted using
  *                      the public key of the recipient
  */
case class EncryptedViewMessage[+VT <: ViewType] private (
    // We can't include it into the SubmitterMetadata, because that would create a cycle dependency:
    // - The signature depends on the transaction id.
    // - The transaction id depends on the submitter metadata.
    submitterParticipantSignature: Option[Signature],
    viewHash: ViewHash,
    randomSeed: Map[ParticipantId, Encrypted[SecureRandomness]],
    encryptedView: EncryptedView[VT],
    override val domainId: DomainId,
) extends ProtocolMessage
    with HasProtoV0[v0.EncryptedViewMessage] {

  def viewTree = encryptedView.viewTree
  def viewType = encryptedView.viewType

  override def toProtoV0: v0.EncryptedViewMessage =
    v0.EncryptedViewMessage(
      viewTree = viewTree.ciphertext,
      submitterParticipantSignature = submitterParticipantSignature.map(_.toProtoV0),
      viewHash = viewHash.toProtoPrimitive,
      randomness = randomSeed.map(EncryptedViewMessage.serializeRandomnessEntry).toSeq,
      domainId = domainId.toProtoPrimitive,
      viewType = viewType.toProtoEnum,
    )

  override def toProtoEnvelopeContentV0(version: ProtocolVersion): v0.EnvelopeContent =
    v0.EnvelopeContent(v0.EnvelopeContent.SomeEnvelopeContent.EncryptedViewMessage(toProtoV0))

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  def traverse[F[_], VT2 <: ViewType](
      f: EncryptedView[VT] => F[EncryptedView[VT2]]
  )(implicit F: Functor[F]): F[EncryptedViewMessage[VT2]] = {
    F.map(f(encryptedView)) { newEncryptedView =>
      if (newEncryptedView eq encryptedView) this.asInstanceOf[EncryptedViewMessage[VT2]]
      else this.copy(encryptedView = newEncryptedView)
    }
  }

  override def pretty: Pretty[EncryptedViewMessage.this.type] = prettyOfClass(
    param("view hash", _.viewHash),
    param("view type", _.viewType),
  )
}

object EncryptedViewMessage {

  private def serializeRandomnessEntry(
      entry: (ParticipantId, Encrypted[SecureRandomness])
  ): v0.ParticipantRandomnessLookup = {
    val (participant, encryptedRandomness) = entry
    v0.ParticipantRandomnessLookup(
      participant = participant.toProtoPrimitive,
      randomness = encryptedRandomness.ciphertext,
    )
  }

  def fromProtoV0(
      encryptedViewMessageP: v0.EncryptedViewMessage
  ): ParsingResult[EncryptedViewMessage[ViewType]] = {
    val v0.EncryptedViewMessage(
      viewTreeP,
      signatureP,
      viewHashP,
      randomnessMapP,
      domainIdP,
      viewTypeP,
    ) =
      encryptedViewMessageP
    for {
      viewType <- ViewType.fromProtoEnum(viewTypeP)
      signature <- signatureP.traverse(Signature.fromProtoV0)
      viewTree <- Encrypted
        .fromByteString[EncryptedView.CompressedView[viewType.View]](viewTreeP)
        .leftMap(CryptoDeserializationError)
      encryptedView = EncryptedView(viewType)(viewTree)
      viewHash <- ViewHash.fromProtoPrimitive(viewHashP)
      randomnessList <- randomnessMapP.traverse(deserializeRandomnessEntry)
      randomnessMap = randomnessList.toMap
      domainUid <- UniqueIdentifier.fromProtoPrimitive(domainIdP, "domainId")
    } yield new EncryptedViewMessage(
      signature,
      viewHash,
      randomnessMap,
      encryptedView,
      DomainId(domainUid),
    )
  }

  def fromByteString(
      bytes: ByteString
  ): ParsingResult[EncryptedViewMessage[ViewType]] =
    ProtoConverter.protoParser(v0.EncryptedViewMessage.parseFrom)(bytes).flatMap(fromProtoV0)

  private def deserializeRandomnessEntry(
      randomnessLookup: v0.ParticipantRandomnessLookup
  ): ParsingResult[(ParticipantId, Encrypted[SecureRandomness])] =
    for {
      participantId <- ParticipantId.fromProtoPrimitive(randomnessLookup.participant, "participant")
      encryptedKey <- Encrypted
        .fromByteString[SecureRandomness](randomnessLookup.randomness)
        .leftMap(CryptoDeserializationError)
    } yield (participantId, encryptedKey)

  private def eitherT[VT <: ViewType, B](value: Either[EncryptedViewMessageDecryptionError[VT], B])(
      implicit ec: ExecutionContext
  ): EitherT[Future, EncryptedViewMessageDecryptionError[VT], B] =
    EitherT.fromEither[Future](value)

  def computeRandomnessLength(snapshot: DomainSnapshotSyncCryptoApi): Int = {
    val pureCrypto = snapshot.pureCrypto
    pureCrypto.defaultHashAlgorithm.length.toInt
  }

  def computeKeyLength(snapshot: DomainSnapshotSyncCryptoApi): Int = {
    val pureCrypto = snapshot.pureCrypto
    pureCrypto.defaultSymmetricKeyScheme.keySizeInBytes
  }

  // This method is not defined as a member of EncryptedViewMessage because the covariant parameter VT conflicts
  // with the parameter deserialize.
  def decryptFor[VT <: ViewType](
      snapshot: DomainSnapshotSyncCryptoApi,
      encrypted: EncryptedViewMessage[VT],
      participantId: ParticipantId,
      protocolVersion: ProtocolVersion,
      optViewRandomness: Option[SecureRandomness] = None,
  )(deserialize: ByteString => Either[DeserializationError, encrypted.encryptedView.viewType.View])(
      implicit ec: ExecutionContext
  ): EitherT[Future, EncryptedViewMessageDecryptionError[VT], VT#View] = {

    val EncryptedViewMessage(signature, viewHash, symmetricKeys, encryptedView, domainId) =
      encrypted
    val pureCrypto = snapshot.pureCrypto
    val keyLength = computeKeyLength(snapshot)
    val randomnessLength = computeRandomnessLength(snapshot)

    for {
      _ <- EitherT.cond[Future](
        optViewRandomness.forall(_.unwrap.size == randomnessLength),
        (),
        WrongRandomnessLength(optViewRandomness.fold(0)(_.unwrap.size), randomnessLength, encrypted),
      )
      viewRandomness <- optViewRandomness.fold(
        decryptRandomness(snapshot, encrypted, participantId)
      )(r => EitherT.pure(r))
      viewKey <-
        eitherT(
          ProtocolCryptoApi
            .hkdf(pureCrypto, protocolVersion)(viewRandomness, keyLength, HkdfInfo.ViewKey)
            .leftMap(HkdfExpansionError(_, encrypted))
        )
      decrypted <- eitherT(
        EncryptedView
          .decrypt(pureCrypto, viewKey, encrypted.encryptedView)(deserialize)
          .leftMap(SymmetricDecryptError(_, encrypted))
      )
      _ <- eitherT(
        EitherUtil.condUnitE(
          decrypted.domainId == domainId,
          WrongDomainIdInEncryptedViewMessage(domainId, decrypted.domainId, encrypted),
        )
      )
    } yield decrypted
  }

  def decryptRandomness[VT <: ViewType](
      snapshot: DomainSnapshotSyncCryptoApi,
      encrypted: EncryptedViewMessage[VT],
      participantId: ParticipantId,
  )(implicit
      ec: ExecutionContext
  ): EitherT[Future, EncryptedViewMessageDecryptionError[VT], SecureRandomness] = {
    val randomnessLength = computeRandomnessLength(snapshot)
    for {
      encryptedRandomness <- eitherT(
        encrypted.randomSeed
          .get(participantId)
          .toRight(MissingParticipantKey(participantId, encrypted))
      )
      viewRandomness <- snapshot
        .decrypt(encryptedRandomness)(SecureRandomness.fromByteString(randomnessLength))
        .leftMap[EncryptedViewMessageDecryptionError[VT]](SyncCryptoDecryptError(_, encrypted))
    } yield viewRandomness
  }

  sealed trait EncryptedViewMessageDecryptionError[+VT <: ViewType]
      extends Product
      with Serializable
      with PrettyPrinting {
    def message: EncryptedViewMessage[VT]

    override def pretty: Pretty[EncryptedViewMessageDecryptionError.this.type] = adHocPrettyInstance
  }

  case class MissingParticipantKey[+VT <: ViewType](
      participantId: ParticipantId,
      override val message: EncryptedViewMessage[VT],
  ) extends EncryptedViewMessageDecryptionError[VT]
  case class SyncCryptoDecryptError[+VT <: ViewType](
      syncCryptoError: SyncCryptoError,
      override val message: EncryptedViewMessage[VT],
  ) extends EncryptedViewMessageDecryptionError[VT]
  case class SymmetricDecryptError[+VT <: ViewType](
      decryptError: DecryptionError,
      override val message: EncryptedViewMessage[VT],
  ) extends EncryptedViewMessageDecryptionError[VT]
  case class WrongDomainIdInEncryptedViewMessage[VT <: ViewType](
      declaredDomainId: DomainId,
      containedDomainId: DomainId,
      override val message: EncryptedViewMessage[VT],
  ) extends EncryptedViewMessageDecryptionError[VT]

  case class HkdfExpansionError[+VT <: ViewType](
      cause: HkdfError,
      override val message: EncryptedViewMessage[VT],
  ) extends EncryptedViewMessageDecryptionError[VT]

  case class WrongRandomnessLength[+VT <: ViewType](
      length: Int,
      expectedLength: Int,
      override val message: EncryptedViewMessage[VT],
  ) extends EncryptedViewMessageDecryptionError[VT]

  implicit val encryptedViewMessageCast
      : ProtocolMessageContentCast[EncryptedViewMessage[ViewType]] = {
    case evm: EncryptedViewMessage[_] => Some(evm)
    case _ => None
  }
}
