// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import cats.syntax.either.*
import cats.syntax.traverse.*
import com.digitalasset.canton.*
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.data.ViewPosition.MerklePathElement
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.{v30, *}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.{ParsingResult, parsePositiveInt}
import com.digitalasset.canton.util.RoseTree
import com.digitalasset.canton.version.*
import monocle.PLens

import scala.annotation.{tailrec, unused}
import scala.collection.mutable

/** Wraps a `GenTransactionTree` where exactly one view is unblinded. The direct subviews of the
  * unblinded view are blinded - this is why the class name is prefixed "Light".
  *
  * The `commonMetadata` and `participantMetadata` are also unblinded. The `submitterMetadata` is
  * unblinded if and only if the unblinded view is a root view.
  *
  * @param subviewReferencesAndKeys
  *   contains the hashes of direct subviews together with their view encryption keys. The view
  *   encryption keys are already sent as part of
  *   [[com.digitalasset.canton.protocol.messages.EncryptedViewMessage]]. They need to be sent again
  *   as part of this class, because a participant that receives this view but does not host an
  *   informee of a subview will not receive the view encryption key as part of
  *   [[com.digitalasset.canton.protocol.messages.EncryptedViewMessage]].
  *
  * @throws LightTransactionViewTree$.InvalidLightTransactionViewTree
  *   if [[tree]] is not a light transaction view tree (i.e. the wrong set of nodes is blinded)
  */
sealed abstract case class LightTransactionViewTree private[data] (
    tree: GenTransactionTree,
    subviewReferencesAndKeys: Seq[SubviewReferenceAndKey],
)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[
      LightTransactionViewTree.type
    ]
) extends TransactionViewTree
    with HasProtocolVersionedWrapper[LightTransactionViewTree]
    with PrettyPrinting {

  // TODO(#32952): change the type of subviewHashes Seq[SubviewReference]
  override val subviewHashes: Seq[ViewHash] = subviewReferencesAndKeys.map {
    case SubviewReferenceAndKey(ByViewHash(viewHash), _) => viewHash
    case SubviewReferenceAndKey(ByCiphertextId(_, _), _) =>
      // TODO(#32393): enable after fully implementing CiphertextId-based subview references in LightTransactionViewTree
      throw new NotImplementedError(
        "CiphertextId-based subview references are not supported in LightTransactionViewTree yet."
      )
  }

  @tailrec
  private[data] override def findTheView(
      viewsWithIndex: Seq[(TransactionView, MerklePathElement)],
      viewPosition: ViewPosition = ViewPosition.root,
  ): Either[String, (TransactionView, ViewPosition)] =
    viewsWithIndex match {
      case Seq() =>
        Left("A light transaction view tree must contain an unblinded view.")
      case Seq((singleView, index)) if singleView.hasAllLeavesBlinded =>
        findTheView(singleView.subviews.unblindedElementsWithIndex, index +: viewPosition)
      case Seq((singleView, index))
          if singleView.viewCommonData.isFullyUnblinded && singleView.viewParticipantData.isFullyUnblinded && singleView.subviews.areFullyBlinded =>
        Right((singleView, index +: viewPosition))
      case Seq((singleView, _)) =>
        Left(s"Invalid blinding in a light transaction view tree: $singleView")
      case multipleViews =>
        Left(
          s"A transaction view tree must not contain several (partially) unblinded views: " +
            s"${multipleViews.map(_._1)}"
        )
    }

  override def validated: Either[String, this.type] = for {
    _ <- super[TransactionViewTree].validated
    // Check that the subview hashes are consistent with the tree
    _ <- Either.cond(
      view.subviewHashesConsistentWith(subviewHashes),
      (),
      s"The provided subview hashes are inconsistent with the provided view (view: ${view.viewHash} " +
        s"at position: $viewPosition, subview hashes: $subviewHashes)",
    )
  } yield this

  @transient override protected lazy val companionObj: LightTransactionViewTree.type =
    LightTransactionViewTree

  def toProtoV30: v30.LightTransactionViewTree =
    v30.LightTransactionViewTree(
      tree = Some(tree.toProtoV30),
      subviewHashesAndKeys = subviewReferencesAndKeys.map {
        case SubviewReferenceAndKey(ByViewHash(viewHash), key) =>
          v30.ViewHashAndKey(viewHash.toProtoPrimitive, key.getCryptographicEvidence)
        case SubviewReferenceAndKey(_: ByCiphertextId, _) =>
          throw new IllegalStateException(
            "CiphertextId-based subview references cannot be serialized to proto V30."
          )
      },
    )

  @unused
  def toProtoV31: v31.LightTransactionViewTree =
    v31.LightTransactionViewTree(
      tree = Some(tree.toProtoV30),
      subviewKeysByCiphertextId = subviewReferencesAndKeys.map {
        case SubviewReferenceAndKey(ByCiphertextId(ciphertextId, index), key) =>
          v31.CiphertextIdAndKey(
            ciphertextId.getCryptographicEvidence,
            index.unwrap,
            key.getCryptographicEvidence,
          )
        case SubviewReferenceAndKey(_: ByViewHash, _) =>
          throw new IllegalStateException(
            "ViewHash-based subview references cannot be serialized to proto V31."
          )
      },
    )

  override lazy val pretty: Pretty[LightTransactionViewTree] = prettyOfClass(unnamedParam(_.tree))
}

object LightTransactionViewTree
    extends VersioningCompanionContextPVValidation2[
      LightTransactionViewTree,
      (HashOps, Int),
    ] {
  override val name: String = "LightTransactionViewTree"

  val versioningTable: VersioningTable = VersioningTable(
    ProtoVersion(30) -> VersionedProtoCodec(ProtocolVersion.v34)(v30.LightTransactionViewTree)(
      supportedProtoVersion(_)((context, proto) => fromProtoV30(context)(proto)),
      _.toProtoV30,
    )
    // TODO(#32393): enable after fully implementing CiphertextId-based subview references in LightTransactionViewTree
    /*ProtoVersion(31) -> VersionedProtoCodec(ProtocolVersion.v36)(v31.LightTransactionViewTree)(
      supportedProtoVersion(_)((context, proto) => fromProtoV31(context)(proto)),
      _.toProtoV31,
    ),*/
  )

  final case class InvalidLightTransactionViewTree(message: String)
      extends RuntimeException(message)

  /** @throws InvalidLightTransactionViewTree
    *   if the tree is not a legal lightweight transaction view tree
    */
  def tryCreate(
      tree: GenTransactionTree,
      subviewReferencesAndKeys: Seq[SubviewReferenceAndKey],
      protocolVersion: ProtocolVersion,
  ): LightTransactionViewTree =
    create(tree, subviewReferencesAndKeys, protocolVersionRepresentativeFor(protocolVersion))
      .valueOr(err => throw InvalidLightTransactionViewTree(err))

  def create(
      tree: GenTransactionTree,
      subviewReferencesAndKeys: Seq[SubviewReferenceAndKey],
      representativeProtocolVersion: RepresentativeProtocolVersion[LightTransactionViewTree.type],
  ): Either[String, LightTransactionViewTree] =
    new LightTransactionViewTree(tree, subviewReferencesAndKeys)(
      representativeProtocolVersion
    ) {}.validated

  private def fromProtoV30(context: ((HashOps, Int), ProtocolVersion))(
      protoT: v30.LightTransactionViewTree
  ): ParsingResult[LightTransactionViewTree] =
    for {
      protoTree <- ProtoConverter.required("tree", protoT.tree)
      ((hashOps, expectedLength), protocolVersion) = context
      tree <- GenTransactionTree.fromProtoV30((hashOps, protocolVersion), protoTree)
      subviewReferencesAndKeys <- protoT.subviewHashesAndKeys.traverse {
        case v30.ViewHashAndKey(viewHashT, keyT) =>
          for {
            viewHash <- ByViewHash.fromProtoPrimitive(viewHashT)
            key <- SecureRandomness
              .fromByteString(expectedLength)(keyT)
              .leftMap[ProtoDeserializationError](
                ProtoDeserializationError.CryptoDeserializationError.apply
              )
          } yield SubviewReferenceAndKey(viewHash, key)
      }
      rpv <- protocolVersionRepresentativeFor(ProtoVersion(30))
      result <- LightTransactionViewTree
        .create(tree, subviewReferencesAndKeys, rpv)
        .leftMap(e =>
          ProtoDeserializationError
            .InvariantViolation("tree", s"Unable to create transaction tree: $e")
        )
    } yield result

  @unused
  private def fromProtoV31(context: ((HashOps, Int), ProtocolVersion))(
      protoT: v31.LightTransactionViewTree
  ): ParsingResult[LightTransactionViewTree] =
    for {
      protoTree <- ProtoConverter.required("tree", protoT.tree)
      ((hashOps, expectedLength), protocolVersion) = context
      tree <- GenTransactionTree.fromProtoV30((hashOps, protocolVersion), protoTree)
      subviewReferencesAndKeys <- protoT.subviewKeysByCiphertextId.traverse {
        case v31.CiphertextIdAndKey(ciphertextIdT, indexT, keyT) =>
          for {
            ciphertextId <- Hash.fromProtoPrimitive(ciphertextIdT)
            index <- parsePositiveInt("index", indexT)
            key <- SecureRandomness
              .fromByteString(expectedLength)(keyT)
              .leftMap[ProtoDeserializationError](
                ProtoDeserializationError.CryptoDeserializationError.apply
              )
          } yield SubviewReferenceAndKey(ByCiphertextId(ciphertextId, index), key)
      }
      rpv <- protocolVersionRepresentativeFor(ProtoVersion(31))
      result <- LightTransactionViewTree
        .create(tree, subviewReferencesAndKeys, rpv)
        .leftMap(e =>
          ProtoDeserializationError
            .InvariantViolation("tree", s"Unable to create transaction tree: $e")
        )
    } yield result

  /** Converts a sequence of light transaction view trees to the corresponding full view trees. A
    * light transaction view tree can be converted to its corresponding full view tree if and only
    * if all descendants can be converted.
    *
    * To make the method more generic, light view trees are represented as `A` and full view trees
    * as `B` and the `lens` parameter is used to convert between these types, as needed.
    *
    * @tparam C
    *   Additional data associated with [[LightTransactionViewTree]]s. Each
    *   [[FullTransactionViewTree]] in the result aggregates the data from all aggregated
    *   [[LightTransactionViewTree]]s in preorder.
    * @param topLevelOnly
    *   whether to return only top-level full view trees
    * @param lightViewTrees
    *   the light transaction view trees to convert
    */
  def toFullViewTrees[A, B, C](
      lens: PLens[A, B, (LightTransactionViewTree, C), (FullTransactionViewTree, RoseTree[C])],
      protocolVersion: ProtocolVersion,
      hashOps: HashOps,
      // TODO(#23971) we don't need this parameter any more, only the true case is used.
      topLevelOnly: Boolean,
      lightViewTrees: Seq[A],
  ): ToFullViewTreesResult[A, B] = {

    val lightViewTreesBoxedInPostOrder = lightViewTrees
      .sortBy(lens.get(_)._1.viewPosition)(ViewPosition.orderViewPosition.toOrdering)
      .reverse

    // All reconstructed full views
    val fullViewByHash = mutable.Map.empty[ViewHash, (TransactionView, RoseTree[C])]
    // All reconstructed full view trees, boxed, paired with their view hashes.
    val allFullViewTreesInPreorderB = mutable.ListBuffer.empty[(ViewHash, B)]
    // All light view trees, boxed, that could not be reconstructed to full view trees, due to missing descendants
    val invalidLightViewTreesB = Seq.newBuilder[A]
    // All duplicate light view trees, boxed.
    val duplicateLightViewTreesB = Seq.newBuilder[A]
    // All hashes of non-toplevel full view trees that could be reconstructed
    val subviewHashesB = Set.newBuilder[ViewHash]

    for (lightViewTreeBoxed <- lightViewTreesBoxedInPostOrder) {
      val (lightViewTree, c) = lens.get(lightViewTreeBoxed)
      val subviewHashes = lightViewTree.subviewHashes.toSet
      val missingSubviews = subviewHashes -- fullViewByHash.keys

      if (missingSubviews.isEmpty) {
        val (fullSubviewsSeq, subviewCs) = lightViewTree.subviewHashes.map(fullViewByHash).unzip
        val fullSubviews = TransactionSubviews(fullSubviewsSeq)(protocolVersion, hashOps)
        val fullView = lightViewTree.view.tryCopy(subviews = fullSubviews)
        val fullViewTree = FullTransactionViewTree.tryCreate(
          lightViewTree.tree.mapUnblindedRootViews(_.replace(fullView.viewHash, fullView))
        )
        val cs = RoseTree(c, subviewCs*)
        val fullViewTreeBoxed = lens.replace(fullViewTree -> cs)(lightViewTreeBoxed)

        if (topLevelOnly)
          subviewHashesB ++= subviewHashes
        if (fullViewByHash.contains(fullViewTree.viewHash)) {
          // Deduplicate views
          duplicateLightViewTreesB += lightViewTreeBoxed
        } else {
          (fullViewTree.viewHash -> fullViewTreeBoxed) +=: allFullViewTreesInPreorderB
          fullViewByHash += fullView.viewHash -> (fullView -> cs)
        }
      } else {
        invalidLightViewTreesB += lightViewTreeBoxed
      }
    }

    val allSubviewHashes = subviewHashesB.result()
    val allFullViewTreesInPreorder =
      allFullViewTreesInPreorderB
        .result()
        .collect {
          case (viewHash, fullViewTreeBoxed)
              if !topLevelOnly || !allSubviewHashes.contains(viewHash) =>
            fullViewTreeBoxed
        }

    ToFullViewTreesResult(
      allFullViewTreesInPreorder,
      invalidLightViewTreesB.result().reverse,
      duplicateLightViewTreesB.result().reverse,
    )
  }

  /** The result of the conversion from [[LightTransactionViewTree]]s to
    * [[FullTransactionViewTree]]s. The view trees in the output are sorted by view position,
    * i.e., in pre-order. If the input contains the same view several times, then
    * [[ToFullViewTreesResult.convertedFullViews]] contains one occurrence and
    * [[ToFullViewTreesResult.duplicateLightViews]] every other occurrence of the view.
    *
    * @param convertedFullViews
    *   the full view trees that could be converted
    * @param lightViewsWithMissingDescendants
    *   the light view trees that could not be converted due to missing descendants
    * @param duplicateLightViews
    *   duplicate light view trees in the input.
    */
  final case class ToFullViewTreesResult[+A, +B](
      convertedFullViews: Seq[B],
      lightViewsWithMissingDescendants: Seq[A],
      duplicateLightViews: Seq[A],
  )

  /** Turns a full transaction view tree into a lightweight one. Not stack-safe. */
  private def fromTransactionViewTree(
      tvt: FullTransactionViewTree,
      subviewKeys: Seq[SecureRandomness],
      byCiphertextIdMapO: Option[Map[ViewHash, ByCiphertextId]],
      protocolVersion: ProtocolVersion,
  ): Either[String, LightTransactionViewTree] = {
    val withBlindedSubviews = tvt.view.tryCopy(subviews = tvt.view.subviews.blindFully)
    val genTransactionTree =
      tvt.tree.mapUnblindedRootViews(_.replace(tvt.viewHash, withBlindedSubviews))
    // you must have one key for each subview (to be able to decrypt them)
    for {
      _ <- Either.cond(
        subviewKeys.sizeCompare(tvt.subviewHashes) == 0,
        (),
        s"Expected ${tvt.subviewHashes.size} subview keys, but got ${subviewKeys.size}",
      )
      subviewReferencesAndKeys <-
        // By definition, the view in a TransactionViewTree has all subviews unblinded
        tvt.subviewHashes
          .lazyZip(subviewKeys)
          .toList
          .traverse { case (viewHash, key) =>
            byCiphertextIdMapO match {
              case Some(byCiphertextIdMap) =>
                // For PV36+, we use the ciphertext ID as the reference for subviews. The ciphertext ID is a hash of the
                // ciphertext (generated by encrypting all views with the same recipient tree) combined with its
                // relative "position" before encryption. We no longer use the view hash as a reference in PV36+,
                // since the view hash is not guaranteed to be unique during decryption.
                byCiphertextIdMap.get(viewHash) match {
                  case Some(ref) => Right(SubviewReferenceAndKey(ref, key))
                  case None =>
                    Left(
                      s"Expected to find a ciphertext ID for view hash ${viewHash.unwrap} in the provided " +
                        s"map, but it was not found."
                    )
                }
              case _ => Right(SubviewReferenceAndKey(ByViewHash(viewHash), key))
            }
          }
    } yield LightTransactionViewTree.tryCreate(
      genTransactionTree,
      subviewReferencesAndKeys,
      protocolVersion,
    )
  }

  /** Builds a LightTransactionViewTree using ViewHash-based references (legacy mode).
    *
    * Subviews are identified using their ViewHash, and assumes stability and uniqueness during
    * encryption/decryption.
    *
    * This mode is used for protocol versions <= v35.
    */
  def fromTransactionViewTreeUsingViewHashReference(
      tvt: FullTransactionViewTree,
      subviewKeys: Seq[SecureRandomness],
      protocolVersion: ProtocolVersion,
  ): Either[String, LightTransactionViewTree] =
    fromTransactionViewTree(tvt, subviewKeys, None, protocolVersion)

  /** Builds a LightTransactionViewTree using Ciphertext ID-based references (PV36+ mode).
    *
    * Subviews are identified using deterministic ciphertext IDs derived from encryption output,
    * ensuring correctness even when ViewHash is not unique during decryption.
    *
    * This mode is required for protocol versions > v35.
    */
  def fromTransactionViewTreeUsingCiphertextIdReference(
      tvt: FullTransactionViewTree,
      subviewKeys: Seq[SecureRandomness],
      byCiphertextIdMap: Map[ViewHash, ByCiphertextId],
      protocolVersion: ProtocolVersion,
  ): Either[String, LightTransactionViewTree] =
    fromTransactionViewTree(tvt, subviewKeys, Some(byCiphertextIdMap), protocolVersion)

}

/** A view hash and its corresponding encryption key.
  *
  * @param subviewReference
  *   identifies the subview this key applies to. It can either be a view hash or a ciphertext ID,
  *   depending on how the view was referenced during encryption/decryption.
  * @param viewEncryptionKeyRandomness
  *   the view key is encoded as SecureRandomness to have a portable representation.
  *   [[com.digitalasset.canton.crypto.SynchronizerCryptoPureApi.createSymmetricKey]] is used to
  *   derive the symmetric key.
  */
final case class SubviewReferenceAndKey(
    subviewReference: SubviewReference,
    viewEncryptionKeyRandomness: SecureRandomness,
)
