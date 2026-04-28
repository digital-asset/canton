// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.traverse.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.SyncCryptoError.SyncCryptoDecryptionError
import com.digitalasset.canton.crypto.store.CryptoPrivateStoreError
import com.digitalasset.canton.crypto.store.CryptoPrivateStoreError.FailedToReadKey
import com.digitalasset.canton.crypto.v30 as V30Crypto
import com.digitalasset.canton.data.{ViewTree, ViewType}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.messages.EncryptedViewMessageError.SyncCryptoDecryptError
import com.digitalasset.canton.protocol.messages.ProtocolMessage.ProtocolMessageContentCast
import com.digitalasset.canton.protocol.{v30, v31, *}
import com.digitalasset.canton.serialization.ProtoConverter.{ParsingResult, parseRequiredNonEmpty}
import com.digitalasset.canton.serialization.{
  DefaultDeserializationError,
  DeserializationError,
  ProtoConverter,
}
import com.digitalasset.canton.store.ConfirmationRequestSessionKeyStore
import com.digitalasset.canton.topology.{ParticipantId, PhysicalSynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.*
import com.digitalasset.canton.version.*
import com.google.protobuf.ByteString

import scala.concurrent.ExecutionContext

/** A wrapper over a view tree collection, a separate class is needed, so it can implement
  * [[com.digitalasset.canton.version.HasToByteString]] and can be wrapped in [[CompressedView]]
  */
final case class MultipleViewTrees[+VT <: ViewTree with HasToByteString](
    viewTrees: NonEmpty[Seq[VT]]
) extends HasToByteString {

  @SuppressWarnings(Array("com.digitalasset.canton.ProtobufToByteString"))
  override def toByteString: ByteString = {
    val proto = v31.EncryptedMultipleViewsMessage.UncompressedViewTrees(
      viewTrees.map(_.toByteString)
    )

    proto.toByteString
  }
}

/** An encrypted [[com.digitalasset.canton.data.ViewTree]] together with its
  * [[com.digitalasset.canton.data.ViewType]]. The correspondence is encoded via a path-dependent
  * type. The type parameter `VT` exposes a upper bound on the type of view types that may be
  * contained.
  *
  * The view tree is compressed before encryption.
  */
// This is not a case class due to the type dependency between `viewType` and `viewTree`.
// We therefore implement the case class boilerplate stuff to the extent needed.
sealed trait EncryptedView[+VT <: ViewType] extends Product with Serializable {
  val viewType: VT
  val viewTree: Encrypted[CompressedView[viewType.View]]

  override def productArity: Int = 1
  override def productElement(n: Int): Any = n match {
    case 0 => viewTree
    case _ => throw new IndexOutOfBoundsException(s"Index out of range: $n")
  }
  @SuppressWarnings(Array("org.wartremover.warts.IsInstanceOf"))
  override def canEqual(that: Any): Boolean = that.isInstanceOf[EncryptedView[?]]
  @SuppressWarnings(
    Array(
      "org.wartremover.warts.AsInstanceOf",
      "org.wartremover.warts.IsInstanceOf",
      "org.wartremover.warts.Null",
    )
  )
  override def equals(that: Any): Boolean =
    if (this eq that.asInstanceOf[Object]) true
    else if (!that.isInstanceOf[EncryptedView[?]]) false
    else {
      val other = that.asInstanceOf[EncryptedView[ViewType]]
      val thisViewTree = this.viewTree
      if (thisViewTree eq null) other.viewTree eq null else thisViewTree == other.viewTree
    }
  override def hashCode(): Int = scala.runtime.ScalaRunTime._hashCode(this)

  /** Indicative size for pretty printing */
  def sizeHint: Int

}

object EncryptedView {
  def apply[VT <: ViewType](
      aViewType: VT
  )(aViewTree: Encrypted[CompressedView[aViewType.View]]): EncryptedView[VT] =
    new EncryptedView[VT] {
      override val viewType: aViewType.type = aViewType
      override val viewTree = aViewTree
      override lazy val sizeHint: Int = aViewTree.ciphertext.size
    }

  def compressed[VT <: ViewType](
      encryptionOps: EncryptionOps,
      viewKey: SymmetricKey,
      aViewType: VT,
  )(
      aViewTree: aViewType.View,
      maxBytesToDecompress: MaxBytesToDecompress,
  ): Either[EncryptionError, EncryptedView[VT]] = {
    val viewSize = aViewTree.toByteString.size()
    for {
      _ <- Either.cond(
        maxBytesToDecompress.limit.value >= viewSize,
        (),
        EncryptionError.MaxViewSizeExceeded(viewSize, maxBytesToDecompress.limit),
      )
      encryptedView <- encryptionOps
        .encryptSymmetricWith(CompressedView(aViewTree), viewKey)
        .map(apply(aViewType))
    } yield encryptedView
  }

  def decrypt[View <: ViewTree with HasToByteString](
      encryptionOps: EncryptionOps,
      viewKey: SymmetricKey,
      encryptedViewTree: Encrypted[CompressedView[View]],
  )(
      deserialize: ByteString => Either[DeserializationError, View],
      maxBytesToDecompress: MaxBytesToDecompress,
  ): Either[DecryptionError, View] =
    encryptionOps
      .decryptWith(encryptedViewTree, viewKey)(
        CompressedView
          .fromByteString[View](deserialize)(_, maxBytesToDecompress)
      )
      .map(_.value)
}

/** A version of [[com.digitalasset.canton.protocol.messages.EncryptedView]], but for multiple
  * views.
  */
final case class EncryptedMultipleViews[+VT <: ViewType](
    viewType: VT,
    viewTrees: Encrypted[CompressedView[MultipleViewTrees[VT#View]]],
) {
  lazy val sizeHint: Int = viewTrees.ciphertext.size
}

object EncryptedMultipleViews {
  def compressed[VT <: ViewType](
      encryptionOps: EncryptionOps,
      viewKey: SymmetricKey,
      viewType: VT,
  )(
      viewTrees: NonEmpty[Seq[viewType.View]],
      maxBytesToDecompress: MaxBytesToDecompress,
  ): Either[EncryptionError, EncryptedMultipleViews[VT]] = {
    val multiView = MultipleViewTrees(viewTrees)
    val size = multiView.toByteString.size()

    for {
      _ <- Either.cond(
        maxBytesToDecompress.limit.value >= size,
        (),
        EncryptionError.MaxViewSizeExceeded(size, maxBytesToDecompress.limit),
      )
      encryptedViews <- encryptionOps.encryptSymmetricWith(CompressedView(multiView), viewKey)
    } yield EncryptedMultipleViews(viewType, encryptedViews)
  }

  def decrypt[View <: ViewTree with HasToByteString](
      encryptionOps: EncryptionOps,
      viewKey: SymmetricKey,
      encryptedViewTrees: Encrypted[CompressedView[MultipleViewTrees[View]]],
  )(
      deserialize: ByteString => Either[DeserializationError, View],
      maxBytesToDecompress: MaxBytesToDecompress,
  ): Either[DecryptionError, MultipleViewTrees[View]] = {

    def deserializeMultiView(
        bytestring: ByteString
    ): Either[DeserializationError, MultipleViewTrees[View]] =
      ProtoConverter
        .protoParser(v31.EncryptedMultipleViewsMessage.UncompressedViewTrees.parseFrom)(bytestring)
        .leftMap(err => DefaultDeserializationError(err.message))
        .flatMap(protoTrees => protoTrees.viewTrees.traverse(deserialize))
        .flatMap { viewTrees =>
          NonEmpty
            .from(viewTrees)
            .toRight(
              DefaultDeserializationError("An empty collection of view trees is not allowed")
            )
        }
        .map(MultipleViewTrees.apply)

    encryptionOps
      .decryptWith(encryptedViewTrees, viewKey)(
        CompressedView
          .fromByteString[MultipleViewTrees[View]](deserializeMultiView)(_, maxBytesToDecompress)
      )
      .map(_.value)
  }
}

/** Wrapper class to compress the view or the views before encrypting it.
  *
  * This class's methods should be used only from [[EncryptedView]] or [[EncryptedMultipleViews]]
  * because compression is in theory non-deterministic (the gzip format can store a timestamp that
  * is ignored by decryption) and we want to avoid that this is applied to
  * [[com.digitalasset.canton.serialization.HasCryptographicEvidence]] instances.
  */
final case class CompressedView[+V <: HasToByteString] private (value: V) extends HasToByteString {
  override def toByteString: ByteString =
    ByteStringUtil.compressGzip(value.toByteString)
}

object CompressedView {
  private[messages] def apply[V <: HasToByteString](value: V): CompressedView[V] =
    new CompressedView(value)

  private[messages] def fromByteString[V <: HasToByteString](
      deserialize: ByteString => Either[DeserializationError, V]
  )(
      bytes: ByteString,
      maxBytesToDecompress: MaxBytesToDecompress,
  ): Either[DeserializationError, CompressedView[V]] =
    ByteStringUtil
      .decompressGzip(bytes, maxBytesLimit = maxBytesToDecompress)
      .flatMap(deserialize)
      .map(CompressedView(_))
}

/** An encrypted view message. The view message is encrypted with a symmetric key derived from the
  * view's randomness.
  */
sealed trait EncryptedViewMessage[+VT <: ViewType] extends UnsignedProtocolMessage {
  def submittingParticipantSignature: Option[Signature]

  def viewHashes: NonEmpty[Seq[ViewHash]]

  /** Cast the type parameter to the given argument's [[com.digitalasset.canton.data.ViewType]]
    * provided that the argument is the same as [[viewType]]
    * @return
    *   [[scala.None$]] if `desiredViewType` does not equal [[viewType]].
    */
  def select(desiredViewType: ViewType): Option[EncryptedViewMessage[desiredViewType.type]]

  def viewEncryptionKeyRandomness: NonEmpty[Seq[AsymmetricEncrypted[SecureRandomness]]]

  def viewEncryptionScheme: SymmetricKeyScheme

  def viewType: VT

  def encryptedSizeHint: Int
}

/** @param viewHash
  *   Transaction view hash in plain text - included such that the recipient can prove to a 3rd
  *   party that it has correctly decrypted the `viewTree`
  * @param viewEncryptionKeyRandomness
  *   the view encryption key, i.e., the symmetric key used to encrypt the view Encoding:
  *   - For every informee participant of the view, the field should contain exactly one entry
  *     containing the view encryption key, asymmetrically encrypted with the participant's
  *     encryption key.
  *   - The view key is encoded as SecureRandomness to have a portable representation.
  *     [[com.digitalasset.canton.crypto.SynchronizerCryptoPureApi#createSymmetricKey]] is used to
  *     derive the symmetric key.
  */
final case class EncryptedSingleViewMessage[+VT <: ViewType](
    override val submittingParticipantSignature: Option[
      Signature
    ],
    viewHash: ViewHash,
    override val viewEncryptionKeyRandomness: NonEmpty[Seq[AsymmetricEncrypted[SecureRandomness]]],
    encryptedView: EncryptedView[VT],
    override val psid: PhysicalSynchronizerId,
    override val viewEncryptionScheme: SymmetricKeyScheme,
)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[
      EncryptedSingleViewMessage.type
    ]
) extends HasProtocolVersionedWrapper[EncryptedSingleViewMessage[ViewType]]
    with EncryptedViewMessage[VT] {

  override def viewHashes: NonEmpty[Seq[ViewHash]] = NonEmpty.mk(Seq, viewHash)

  @transient override protected lazy val companionObj: EncryptedSingleViewMessage.type =
    EncryptedSingleViewMessage

  override val viewType: VT = encryptedView.viewType

  override def encryptedSizeHint: Int = encryptedView.sizeHint

  def copy[A <: ViewType](
      submittingParticipantSignature: Option[Signature] = this.submittingParticipantSignature,
      viewHash: ViewHash = this.viewHash,
      viewEncryptionKeyRandomness: NonEmpty[Seq[AsymmetricEncrypted[SecureRandomness]]] =
        this.viewEncryptionKeyRandomness,
      encryptedView: EncryptedView[A] = this.encryptedView,
      synchronizerId: PhysicalSynchronizerId = this.psid,
      viewEncryptionScheme: SymmetricKeyScheme = this.viewEncryptionScheme,
  ): EncryptedSingleViewMessage[A] = new EncryptedSingleViewMessage(
    submittingParticipantSignature,
    viewHash,
    viewEncryptionKeyRandomness,
    encryptedView,
    synchronizerId,
    viewEncryptionScheme,
  )(representativeProtocolVersion)

  private def toProtoV30: v30.EncryptedViewMessage = v30.EncryptedViewMessage(
    viewTree = encryptedView.viewTree.ciphertext,
    encryptionScheme = viewEncryptionScheme.toProtoEnum,
    submittingParticipantSignature = submittingParticipantSignature.map(_.toProtoV30),
    viewHash = viewHash.toProtoPrimitive,
    sessionKeyLookup =
      viewEncryptionKeyRandomness.map(EncryptedViewMessage.serializeEncryptedRandomness),
    physicalSynchronizerId = psid.toProtoPrimitive,
    viewType = viewType.toProtoEnum,
  )

  override def toProtoSomeEnvelopeContentV30: v30.EnvelopeContent.SomeEnvelopeContent =
    v30.EnvelopeContent.SomeEnvelopeContent.EncryptedViewMessage(toProtoV30)

  // Not implemented on purpose, this type does not exist for v31+
  override def toProtoSomeEnvelopeContentV31: v31.EnvelopeContent.SomeEnvelopeContent =
    throw new UnsupportedOperationException(
      "Cannot serialize an EncryptedSingleViewMessage to proto version 31"
    )

  /** Cast the type parameter to the given argument's [[com.digitalasset.canton.data.ViewType]]
    * provided that the argument is the same as [[viewType]]
    * @return
    *   [[scala.None$]] if `desiredViewType` does not equal [[viewType]].
    */
  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  override def select(
      desiredViewType: ViewType
  ): Option[EncryptedSingleViewMessage[desiredViewType.type]] =
    if (desiredViewType == viewType)
      Some(this.asInstanceOf[EncryptedSingleViewMessage[desiredViewType.type]])
    else None

  override def pretty: Pretty[EncryptedSingleViewMessage.this.type] = prettyOfClass(
    param("view hash", _.viewHash),
    param("view type", _.viewType),
    param("size", _.encryptedView.sizeHint),
    param("psid", _.psid),
    param("number of view keys", _.viewEncryptionKeyRandomness.size),
    param("view encryption scheme", _.viewEncryptionScheme),
  )
}

object EncryptedViewMessage {
  def decryptRandomness[VT <: ViewType](
      snapshot: SynchronizerSnapshotSyncCryptoApi,
      sessionKeyStore: ConfirmationRequestSessionKeyStore,
      encrypted: EncryptedViewMessage[VT],
      participantId: ParticipantId,
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[FutureUnlessShutdown, EncryptedViewMessageError, SecureRandomness] =
    for {
      /* We first need to check whether the target private encryption key exists and is active in the store; otherwise,
       * we cannot decrypt and should abort. This situation can occur
       * if an encryption key has been added to this participant's topology by another entity with the
       * correct rights to do so, but this participant does not have the corresponding private key in the store.
       */
      encryptionKeys <- EitherT
        .right(
          snapshot.ipsSnapshot.encryptionKeys(participantId)
        )
        .map(_.map(_.id).toSet)

      encryptedRandomnessForParticipant <- encrypted.viewEncryptionKeyRandomness
        .find(e => encryptionKeys.contains(e.encryptedFor))
        .toRight(
          EncryptedViewMessageError.MissingParticipantKey(participantId)
        )
        .toEitherT[FutureUnlessShutdown]
      // TODO(#12911): throw an exception instead of a left for a missing private key in the store
      _ <- snapshot.crypto.cryptoPrivateStore
        .existsDecryptionKey(encryptedRandomnessForParticipant.encryptedFor)
        .leftMap(err => EncryptedViewMessageError.PrivateKeyStoreVerificationError(err))
        .subflatMap {
          Either.cond(
            _,
            (),
            EncryptedViewMessageError.PrivateKeyStoreVerificationError(
              FailedToReadKey(
                encryptedRandomnessForParticipant.encryptedFor,
                "matching private key does not exist",
              )
            ),
          )
        }
      // we get the randomness for the session key from the message or by searching the cache. If this encrypted
      // randomness is in the cache this means that a previous view with the same recipients tree has been
      // received before.
      viewRandomness <-
        // we try to search for the cached session key randomness. If it does not exist
        // (or is disabled) we decrypt and store it
        // the result in the cache. There is no need to sync on this read-write operation because
        // there is no problem if the value gets re-written.
        sessionKeyStore
          .getSessionKeyRandomness(
            snapshot.crypto.privateCrypto,
            encrypted.viewEncryptionScheme.keySizeInBytes,
            encryptedRandomnessForParticipant,
          )
          .leftMap[EncryptedViewMessageError](err =>
            SyncCryptoDecryptError(
              SyncCryptoDecryptionError(err)
            )
          )
    } yield viewRandomness

  private def eitherT[B](value: Either[EncryptedViewMessageError, B])(implicit
      ec: ExecutionContext
  ): EitherT[FutureUnlessShutdown, EncryptedViewMessageError, B] =
    EitherT.fromEither[FutureUnlessShutdown](value)

  def computeRandomnessLength(pureCrypto: CryptoPureApi): Int =
    pureCrypto.defaultSymmetricKeyScheme.keySizeInBytes

  // This method is not defined as a member of EncryptedViewMessage because the covariant parameter VT conflicts
  // with the parameter deserialize.
  private def decryptWithRandomness[VT <: ViewType](
      snapshot: SynchronizerSnapshotSyncCryptoApi,
      encrypted: EncryptedViewMessage[VT],
      viewRandomness: SecureRandomness,
  )(
      deserialize: ByteString => Either[
        DeserializationError,
        VT#View,
      ]
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, EncryptedViewMessageError, MultipleViewTrees[VT#View]] = {

    val pureCrypto = snapshot.pureCrypto
    val randomnessLength = encrypted.viewEncryptionScheme.keySizeInBytes

    for {
      _ <- EitherT.cond[FutureUnlessShutdown](
        viewRandomness.unwrap.size == randomnessLength,
        (),
        EncryptedViewMessageError.WrongRandomnessLength(
          viewRandomness.unwrap.size,
          randomnessLength,
        ),
      )
      viewKey <- eitherT(
        pureCrypto
          .createSymmetricKey(viewRandomness, encrypted.viewEncryptionScheme)
          .leftMap(err =>
            EncryptedViewMessageError
              .SessionKeyCreationError(err)
          )
      )

      maxBytesToDecompress <- EitherT(snapshot.ipsSnapshot.findDynamicSynchronizerParameters())
        .leftMap(error =>
          EncryptedViewMessageError.UnableToGetDynamicSynchronizerParameters(error, snapshot.psid)
        )
        .map(_.parameters.maxRequestSize.value)
        .map(MaxBytesToDecompress(_))

      result <- encrypted match {
        case singleViewMessage: EncryptedSingleViewMessage[VT] =>
          decryptSingleView(
            pureCrypto,
            singleViewMessage,
            viewKey,
            maxBytesToDecompress,
          )(deserialize).map(view => MultipleViewTrees(NonEmpty.mk(Seq, view)))
        case multipleViewsMessage: EncryptedMultipleViewsMessage[VT] =>
          decryptMultipleViews(
            pureCrypto,
            multipleViewsMessage,
            viewKey,
            maxBytesToDecompress,
          )(deserialize)
      }
    } yield result
  }

  private def decryptSingleView[VT <: ViewType](
      pureCrypto: SynchronizerCryptoPureApi,
      encrypted: EncryptedSingleViewMessage[VT],
      viewKey: SymmetricKey,
      maxBytesToDecompress: MaxBytesToDecompress,
  )(
      deserialize: ByteString => Either[
        DeserializationError,
        VT#View,
      ]
  )(implicit
      ec: ExecutionContext
  ): EitherT[FutureUnlessShutdown, EncryptedViewMessageError, VT#View] =
    for {
      decryptedView <- eitherT(
        EncryptedView
          .decrypt[VT#View](pureCrypto, viewKey, encrypted.encryptedView.viewTree)(
            deserialize,
            maxBytesToDecompress = maxBytesToDecompress,
          )
          .leftMap(EncryptedViewMessageError.SymmetricDecryptError.apply)
      )
      _ <- eitherT(
        Either.cond(
          decryptedView.psid == encrypted.psid,
          (),
          EncryptedViewMessageError.WrongSynchronizerIdsInEncryptedViewMessage(
            encrypted.psid,
            Set(decryptedView.psid),
          ),
        )
      )
    } yield decryptedView

  private def decryptMultipleViews[VT <: ViewType](
      pureCrypto: SynchronizerCryptoPureApi,
      encrypted: EncryptedMultipleViewsMessage[VT],
      viewKey: SymmetricKey,
      maxBytesToDecompress: MaxBytesToDecompress,
  )(
      deserialize: ByteString => Either[
        DeserializationError,
        VT#View,
      ]
  )(implicit
      ec: ExecutionContext
  ): EitherT[FutureUnlessShutdown, EncryptedViewMessageError, MultipleViewTrees[VT#View]] =
    for {
      decryptedMultiView <- eitherT(
        EncryptedMultipleViews
          .decrypt(pureCrypto, viewKey, encrypted.encryptedViews.viewTrees)(
            deserialize,
            maxBytesToDecompress = maxBytesToDecompress,
          )
          .leftMap(EncryptedViewMessageError.SymmetricDecryptError.apply)
      )
      _ <- eitherT(
        Either.cond(
          decryptedMultiView.viewTrees.forall(_.psid == encrypted.psid),
          (),
          EncryptedViewMessageError.WrongSynchronizerIdsInEncryptedViewMessage(
            encrypted.psid,
            decryptedMultiView.viewTrees.map(_.psid).toSet,
          ),
        )
      )
    } yield decryptedMultiView

  def decryptFor[VT <: ViewType](
      snapshot: SynchronizerSnapshotSyncCryptoApi,
      sessionKeyStore: ConfirmationRequestSessionKeyStore,
      encrypted: EncryptedViewMessage[VT],
      participantId: ParticipantId,
      optViewRandomness: Option[SecureRandomness] = None,
  )(
      deserialize: ByteString => Either[
        DeserializationError,
        VT#View,
      ]
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[FutureUnlessShutdown, EncryptedViewMessageError, MultipleViewTrees[VT#View]] =
    for {
      viewRandomness <- optViewRandomness.fold(
        decryptRandomness(
          snapshot,
          sessionKeyStore,
          encrypted,
          participantId,
        )
      )(r => EitherT.pure(r))
      decrypted <- decryptWithRandomness(
        snapshot,
        encrypted,
        viewRandomness,
      )(
        deserialize
      )
    } yield decrypted

  def serializeEncryptedRandomness(
      encryptedRandomness: AsymmetricEncrypted[SecureRandomness]
  ): V30Crypto.AsymmetricEncrypted =
    AsymmetricEncrypted(
      encryptedRandomness.ciphertext,
      encryptedRandomness.encryptionAlgorithmSpec,
      encryptedRandomness.encryptedFor,
    ).toProtoV30

  def deserializeEncryptedRandomness(
      encryptedRandomnessP: V30Crypto.AsymmetricEncrypted
  ): ParsingResult[AsymmetricEncrypted[SecureRandomness]] =
    AsymmetricEncrypted.fromProtoV30(encryptedRandomnessP)

  implicit val encryptedViewMessageCast
      : ProtocolMessageContentCast[EncryptedViewMessage[ViewType]] =
    ProtocolMessageContentCast.create[EncryptedViewMessage[ViewType]](
      "EncryptedViewMessage"
    ) {
      case evm: EncryptedViewMessage[?] => Some(evm)
      case _ => None
    }
}

object EncryptedSingleViewMessage
    extends VersioningCompanion[EncryptedSingleViewMessage[ViewType]] {

  val versioningTable: VersioningTable = VersioningTable(
    ProtoVersion(30) -> VersionedProtoCodec(ProtocolVersion.v34)(v30.EncryptedViewMessage)(
      supportedProtoVersion(_)(EncryptedSingleViewMessage.fromProto),
      _.toProtoV30,
    ),
    ProtoVersion(31) -> UnsupportedProtoCodec(ProtocolVersion.v35),
  )

  def apply[VT <: ViewType](
      submittingParticipantSignature: Option[Signature],
      viewHash: ViewHash,
      viewEncryptionKeyRandomness: NonEmpty[Seq[AsymmetricEncrypted[SecureRandomness]]],
      encryptedView: EncryptedView[VT],
      synchronizerId: PhysicalSynchronizerId,
      viewEncryptionScheme: SymmetricKeyScheme,
      protocolVersion: ProtocolVersion,
  ): EncryptedSingleViewMessage[VT] = EncryptedSingleViewMessage(
    submittingParticipantSignature,
    viewHash,
    viewEncryptionKeyRandomness,
    encryptedView,
    synchronizerId,
    viewEncryptionScheme,
  )(protocolVersionRepresentativeFor(protocolVersion))

  def fromProto(
      encryptedViewMessageP: v30.EncryptedViewMessage
  ): ParsingResult[EncryptedSingleViewMessage[ViewType]] = {
    val v30.EncryptedViewMessage(
      viewTreeP,
      encryptionSchemeP,
      signatureP,
      viewHashP,
      sessionKeyLookupP,
      synchronizerIdP,
      viewTypeP,
    ) =
      encryptedViewMessageP
    for {
      viewType <- ViewType.fromProtoEnum(viewTypeP)
      viewEncryptionScheme <- SymmetricKeyScheme.fromProtoEnum(
        "encryptionScheme",
        encryptionSchemeP,
      )
      signature <- signatureP.traverse(Signature.fromProtoV30)
      viewTree = Encrypted.fromByteString[CompressedView[viewType.View]](viewTreeP)
      encryptedView = EncryptedView(viewType)(viewTree)
      viewHash <- ViewHash.fromProtoPrimitive(viewHashP)
      viewEncryptionKeyRandomness <- parseRequiredNonEmpty(
        EncryptedViewMessage.deserializeEncryptedRandomness,
        "session key",
        sessionKeyLookupP,
      )
      synchronizerId <- PhysicalSynchronizerId.fromProtoPrimitive(
        synchronizerIdP,
        "physical_synchronizer_id",
      )
      rpv <- protocolVersionRepresentativeFor(ProtoVersion(30))
    } yield new EncryptedSingleViewMessage(
      signature,
      viewHash,
      viewEncryptionKeyRandomness,
      encryptedView,
      synchronizerId,
      viewEncryptionScheme,
    )(rpv)
  }

  override def name: String = "EncryptedSingleViewMessage"
}

sealed trait EncryptedViewMessageError extends Product with Serializable with PrettyPrinting {

  override protected def pretty: Pretty[EncryptedViewMessageError.this.type] = adHocPrettyInstance
}

object EncryptedViewMessageError {

  final case class SessionKeyCreationError(
      err: EncryptionKeyCreationError
  ) extends EncryptedViewMessageError

  final case class MissingParticipantKey(
      participantId: ParticipantId
  ) extends EncryptedViewMessageError

  final case class SyncCryptoDecryptError(
      syncCryptoError: SyncCryptoError
  ) extends EncryptedViewMessageError

  final case class SymmetricDecryptError(
      decryptError: DecryptionError
  ) extends EncryptedViewMessageError

  final case class WrongSynchronizerIdsInEncryptedViewMessage(
      declaredSynchronizerId: PhysicalSynchronizerId,
      containedSynchronizerId: Set[PhysicalSynchronizerId],
  ) extends EncryptedViewMessageError

  final case class WrongRandomnessLength(
      length: Int,
      expectedLength: Int,
  ) extends EncryptedViewMessageError

  final case class PrivateKeyStoreVerificationError(
      privatekeyStoreError: CryptoPrivateStoreError
  ) extends EncryptedViewMessageError

  final case class UnableToGetDynamicSynchronizerParameters(
      error: String,
      synchronizerId: PhysicalSynchronizerId,
  ) extends EncryptedViewMessageError

  final case class InvalidContractIdInView(error: String) extends EncryptedViewMessageError

  final case class TooManyViews(error: String) extends EncryptedViewMessageError
}

final case class EncryptedMultipleViewsMessage[+VT <: ViewType](
    encryptedViews: EncryptedMultipleViews[VT],
    override val viewHashes: NonEmpty[Seq[ViewHash]],
    override val viewEncryptionKeyRandomness: NonEmpty[Seq[AsymmetricEncrypted[SecureRandomness]]],
    override val psid: PhysicalSynchronizerId,
    override val viewEncryptionScheme: SymmetricKeyScheme,
    override val submittingParticipantSignature: Option[Signature],
)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[
      EncryptedMultipleViewsMessage.type
    ]
) extends HasProtocolVersionedWrapper[EncryptedMultipleViewsMessage[ViewType]]
    with EncryptedViewMessage[VT] {

  @transient override protected lazy val companionObj: EncryptedMultipleViewsMessage.type =
    EncryptedMultipleViewsMessage

  override val viewType: VT = encryptedViews.viewType

  override def encryptedSizeHint: Int = encryptedViews.sizeHint

  private def toProtoV31: v31.EncryptedMultipleViewsMessage =
    v31.EncryptedMultipleViewsMessage(
      compressedViewTrees = encryptedViews.viewTrees.ciphertext,
      viewHashes = viewHashes.map(_.toProtoPrimitive),
      encryptionScheme = viewEncryptionScheme.toProtoEnum,
      submittingParticipantSignature = submittingParticipantSignature.map(_.toProtoV30),
      sessionKeyLookup =
        viewEncryptionKeyRandomness.map(EncryptedViewMessage.serializeEncryptedRandomness),
      physicalSynchronizerId = psid.toProtoPrimitive,
      viewType = viewType.toProtoEnum,
    )

  // Not implemented on purpose, this type exists for 31+ only
  override def toProtoSomeEnvelopeContentV30: v30.EnvelopeContent.SomeEnvelopeContent =
    throw new UnsupportedOperationException(
      "Cannot serialize an EncryptedMultipleViewsMessage to proto version 30"
    )

  override def toProtoSomeEnvelopeContentV31: v31.EnvelopeContent.SomeEnvelopeContent =
    v31.EnvelopeContent.SomeEnvelopeContent.EncryptedMultipleViewsMessage(toProtoV31)

  /** Cast the type parameter to the given argument's [[com.digitalasset.canton.data.ViewType]]
    * provided that the argument is the same as [[viewType]]
    * @return
    *   [[scala.None$]] if `desiredViewType` does not equal [[viewType]].
    */
  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  override def select(
      desiredViewType: ViewType
  ): Option[EncryptedMultipleViewsMessage[desiredViewType.type]] =
    if (desiredViewType == viewType)
      Some(this.asInstanceOf[EncryptedMultipleViewsMessage[desiredViewType.type]])
    else None

  override def pretty: Pretty[EncryptedMultipleViewsMessage.this.type] = prettyOfClass(
    param("view hashes", _.viewHashes),
    param("view type", _.viewType),
    param("size", _.encryptedViews.sizeHint),
    param("psid", _.psid),
    param("number of view keys", _.viewEncryptionKeyRandomness.size),
    param("view encryption scheme", _.viewEncryptionScheme),
  )

  def copy[A <: ViewType](
      encryptedViews: EncryptedMultipleViews[A] = this.encryptedViews,
      viewHashes: NonEmpty[Seq[ViewHash]] = this.viewHashes,
      viewEncryptionKeyRandomness: NonEmpty[Seq[AsymmetricEncrypted[SecureRandomness]]] =
        this.viewEncryptionKeyRandomness,
      psid: PhysicalSynchronizerId = this.psid,
      viewEncryptionScheme: SymmetricKeyScheme = this.viewEncryptionScheme,
      submittingParticipantSignature: Option[Signature] = this.submittingParticipantSignature,
  ): EncryptedMultipleViewsMessage[A] = new EncryptedMultipleViewsMessage(
    encryptedViews,
    viewHashes,
    viewEncryptionKeyRandomness,
    psid,
    viewEncryptionScheme,
    submittingParticipantSignature,
  )(representativeProtocolVersion)
}

object EncryptedMultipleViewsMessage
    extends VersioningCompanion[EncryptedMultipleViewsMessage[ViewType]] {

  val versioningTable: VersioningTable = VersioningTable(
    ProtoVersion(30) -> UnsupportedProtoCodec(ProtocolVersion.v34),
    ProtoVersion(31) -> VersionedProtoCodec(ProtocolVersion.v35)(v31.EncryptedMultipleViewsMessage)(
      supportedProtoVersion(_)(EncryptedMultipleViewsMessage.fromProto),
      _.toProtoV31,
    ),
  )

  def apply[VT <: ViewType](
      encryptedViews: EncryptedMultipleViews[VT],
      viewHashes: NonEmpty[Seq[ViewHash]],
      viewEncryptionKeyRandomness: NonEmpty[Seq[AsymmetricEncrypted[SecureRandomness]]],
      synchronizerId: PhysicalSynchronizerId,
      viewEncryptionScheme: SymmetricKeyScheme,
      submittingParticipantSignature: Option[Signature],
      protocolVersion: ProtocolVersion,
  ): EncryptedMultipleViewsMessage[VT] = EncryptedMultipleViewsMessage(
    encryptedViews,
    viewHashes,
    viewEncryptionKeyRandomness,
    synchronizerId,
    viewEncryptionScheme,
    submittingParticipantSignature,
  )(protocolVersionRepresentativeFor(protocolVersion))

  def fromProto(
      encryptedViewMessageP: v31.EncryptedMultipleViewsMessage
  ): ParsingResult[EncryptedMultipleViewsMessage[ViewType]] = {
    val v31.EncryptedMultipleViewsMessage(
      compressedViewTreesP,
      viewHashesP,
      encryptionSchemeP,
      signatureP,
      sessionKeyLookupP,
      synchronizerIdP,
      viewTypeP,
    ) =
      encryptedViewMessageP
    for {
      viewType <- ViewType.fromProtoEnum(viewTypeP)
      viewEncryptionScheme <- SymmetricKeyScheme.fromProtoEnum(
        "encryptionScheme",
        encryptionSchemeP,
      )

      signature <- signatureP.traverse(Signature.fromProtoV30)
      viewHashes <- parseRequiredNonEmpty(
        ViewHash.fromProtoPrimitive,
        "view_hashes",
        viewHashesP,
      )

      viewEncryptionKeyRandomness <- parseRequiredNonEmpty(
        EncryptedViewMessage.deserializeEncryptedRandomness,
        "session_key_lookup",
        sessionKeyLookupP,
      )
      synchronizerId <- PhysicalSynchronizerId.fromProtoPrimitive(
        synchronizerIdP,
        "physical_synchronizer_id",
      )
      rpv <- protocolVersionRepresentativeFor(ProtoVersion(31))
    } yield new EncryptedMultipleViewsMessage(
      EncryptedMultipleViews(viewType, Encrypted.fromByteString(compressedViewTreesP)),
      viewHashes,
      viewEncryptionKeyRandomness,
      synchronizerId,
      viewEncryptionScheme,
      signature,
    )(rpv)
  }

  override def name: String = "EncryptedMultipleViewsMessage"
}
