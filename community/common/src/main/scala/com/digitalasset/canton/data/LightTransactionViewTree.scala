// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import cats.syntax.either.*
import cats.syntax.foldable.*
import cats.syntax.functorFilter.*
import cats.syntax.traverse.*
import com.daml.nonempty.{NonEmpty, NonEmptyUtil}
import com.digitalasset.canton.*
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.data.LightTransactionViewTree.InvalidLightTransactionViewTree
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.{v0, *}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.version.*

import java.util.UUID
import scala.annotation.tailrec

/** Wraps a `GenTransactionTree` where exactly one view (not including subviews) is unblinded.
  * The `commonMetadata` and `participantMetadata` are also unblinded.
  * The `submitterMetadata` is unblinded if and only if the unblinded view is a root view.
  *
  * @throws LightTransactionViewTree$.InvalidLightTransactionViewTree if [[tree]] is not a light transaction view tree
  *                                    (i.e. the wrong set of nodes is blinded)
  */
abstract class LightTransactionViewTree private[data] (val tree: GenTransactionTree)
    extends ViewTree
    with HasVersionedWrapper[LightTransactionViewTree]
    with PrettyPrinting {
  val subviewHashes: Seq[ViewHash]

  override protected def companionObj = LightTransactionViewTree

  private def findTheView(
      views: Seq[MerkleTree[TransactionView]]
  ): TransactionView = {
    val lightUnblindedViews = views.mapFilter { view =>
      for {
        v <- view.unwrap.toOption
        _cmd <- v.viewCommonData.unwrap.toOption
        _pmd <- v.viewParticipantData.unwrap.toOption
        _ <- if (v.subviews.areFullyBlinded) Some(()) else None
      } yield v
    }
    lightUnblindedViews match {
      case Seq(v) => v
      case Seq() => throw InvalidLightTransactionViewTree(s"No light transaction views in tree")
      case l =>
        throw InvalidLightTransactionViewTree(
          s"Found too many light transaction views: ${l.length}"
        )
    }
  }

  lazy val transactionId: TransactionId = TransactionId.fromRootHash(tree.rootHash)

  lazy val transactionUuid: UUID = checked(tree.commonMetadata.tryUnwrap).uuid

  /** The unblinded view. */
  val view: TransactionView = findTheView(
    tree.rootViews.unblindedElements.flatMap(_.flatten)
  )

  override val viewHash: ViewHash = ViewHash.fromRootHash(view.rootHash)

  private[this] val commonMetadata: CommonMetadata =
    tree.commonMetadata.unwrap
      .getOrElse(
        throw InvalidLightTransactionViewTree(
          s"The common metadata of a light transaction view tree must be unblinded."
        )
      )

  override def domainId: DomainId = commonMetadata.domainId

  override def mediatorId: MediatorId = commonMetadata.mediatorId

  lazy val confirmationPolicy: ConfirmationPolicy = commonMetadata.confirmationPolicy

  private val unwrapParticipantMetadata: ParticipantMetadata =
    tree.participantMetadata.unwrap
      .getOrElse(
        throw InvalidLightTransactionViewTree(
          s"The participant metadata of a light transaction view tree must be unblinded."
        )
      )

  val ledgerTime: CantonTimestamp = unwrapParticipantMetadata.ledgerTime

  val submissionTime: CantonTimestamp = unwrapParticipantMetadata.submissionTime

  val workflowId: Option[WorkflowId] = unwrapParticipantMetadata.workflowId

  override lazy val informees: Set[Informee] = view.viewCommonData.tryUnwrap.informees

  lazy val viewParticipantData: ViewParticipantData = view.viewParticipantData.tryUnwrap

  def toProtoV0: v0.LightTransactionViewTree =
    v0.LightTransactionViewTree(tree = Some(tree.toProtoV0))

  def toProtoV1: v1.LightTransactionViewTree =
    v1.LightTransactionViewTree(
      tree = Some(tree.toProtoV1),
      subviewHashes = subviewHashes.map(_.toProtoPrimitive),
    )

  override lazy val toBeSigned: Option[RootHash] =
    tree.rootViews.unblindedElements
      .find(_.rootHash == view.rootHash)
      .map(_ => transactionId.toRootHash)

  override def rootHash: RootHash = tree.rootHash

  override lazy val pretty: Pretty[LightTransactionViewTree] = prettyOfClass(unnamedParam(_.tree))
}

/** Specialization of `LightTransactionViewTree` when subviews are a sequence of `TransactionViews`.
  * The hashes of all the direct subviews are computed directly from the sequence of subviews.
  */
case class LightTransactionViewTreeV0 private[data] (override val tree: GenTransactionTree)
    extends LightTransactionViewTree(tree) {
  override val subviewHashes: Seq[ViewHash] = view.subviews.trySubviewHashes
}

/** Specialization of `LightTransactionViewTree` when subviews are a MerkleSeq of `TransactionViews`.
  * In this case, the subview hashes need to be provided because the extra blinding provided by MerkleSeqs
  * could hide some of the subviews.
  *
  * @param subviewHashes: view hashes of all direct subviews
  */
case class LightTransactionViewTreeV1 private[data] (
    override val tree: GenTransactionTree,
    override val subviewHashes: Seq[ViewHash],
) extends LightTransactionViewTree(tree)
// TODO(i10962): Validate on construction that the `subviewHashes` are consistent with `tree`.

object LightTransactionViewTree
    extends HasVersionedMessageWithContextCompanion[LightTransactionViewTree, HashOps] {
  override val name: String = "LightTransactionViewTree"

  val supportedProtoVersions: SupportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(0) -> ProtoCodec(
      ProtocolVersion.v3,
      supportedProtoVersion(v0.LightTransactionViewTree)(fromProtoV0),
      _.toProtoV0.toByteString,
    ),
    ProtoVersion(1) -> ProtoCodec(
      ProtocolVersion.v4,
      supportedProtoVersion(v1.LightTransactionViewTree)(fromProtoV1),
      _.toProtoV1.toByteString,
    ),
  )

  case class InvalidLightTransactionViewTree(message: String) extends RuntimeException(message)
  case class InvalidLightTransactionViewTreeSequence(message: String)
      extends RuntimeException(message)

  /** @throws InvalidLightTransactionViewTree if the tree is not a legal lightweight transaction view tree
    */
  def tryCreate(
      tree: GenTransactionTree,
      subviewHashes: Seq[ViewHash],
      protocolVersion: ProtocolVersion,
  ): LightTransactionViewTree = {
    if (protocolVersion >= ProtocolVersion.v4)
      LightTransactionViewTreeV1(tree, subviewHashes)
    else LightTransactionViewTreeV0(tree)
  }

  private def createV0(tree: GenTransactionTree): Either[String, LightTransactionViewTree] =
    Either
      .catchOnly[InvalidLightTransactionViewTree](LightTransactionViewTreeV0(tree))
      .leftMap(_.message)

  private def createV1(
      tree: GenTransactionTree,
      subviewHashes: Seq[ViewHash],
  ): Either[String, LightTransactionViewTree] =
    Either
      .catchOnly[InvalidLightTransactionViewTree](LightTransactionViewTreeV1(tree, subviewHashes))
      .leftMap(_.message)

  def fromProtoV0(
      hashOps: HashOps,
      protoT: v0.LightTransactionViewTree,
  ): ParsingResult[LightTransactionViewTree] =
    for {
      protoTree <- ProtoConverter.required("tree", protoT.tree)
      tree <- GenTransactionTree.fromProtoV0(hashOps, protoTree)
      result <- LightTransactionViewTree
        .createV0(tree)
        .leftMap(e =>
          ProtoDeserializationError.OtherError(s"Unable to create transaction tree: $e")
        )
    } yield result

  def fromProtoV1(
      hashOps: HashOps,
      protoT: v1.LightTransactionViewTree,
  ): ParsingResult[LightTransactionViewTree] =
    for {
      protoTree <- ProtoConverter.required("tree", protoT.tree)
      tree <- GenTransactionTree.fromProtoV1(hashOps, protoTree)
      subviewHashes <- protoT.subviewHashes.traverse(ViewHash.fromProtoPrimitive)
      result <- LightTransactionViewTree
        .createV1(tree, subviewHashes)
        .leftMap(e =>
          ProtoDeserializationError.OtherError(s"Unable to create transaction tree: $e")
        )
    } yield result

  /** Converts the prefix of a sequence of lightweight transaction view trees into a full tree, returning the unused suffix.
    *
    * The unused suffix normally describes other full transaction view trees, and the function may be called again on the
    * suffix.
    * Errors on an empty sequence, or a sequence whose prefix doesn't describe a full transaction view tree.
    * A valid full transaction view tree results from traversing a forest of views in preorder.
    */
  private def toFullViewTree(protocolVersion: ProtocolVersion, hashOps: HashOps)(
      trees: NonEmpty[Seq[LightTransactionViewTree]]
  ): Either[
    InvalidLightTransactionViewTreeSequence,
    (TransactionViewTree, Seq[LightTransactionViewTree]),
  ] = {
    def toFullView(trees: NonEmpty[Seq[LightTransactionViewTree]]): Either[
      InvalidLightTransactionViewTreeSequence,
      (TransactionView, Seq[LightTransactionViewTree]),
    ] = {
      val headView = trees.head1.view
      // We cannot get the number of subviews from the `subviews` member, because in case it is a MerkleSeq,
      // it can be partly blinded and won't know the actual number
      val nbOfSubViews = trees.head1.subviewHashes.length
      if (nbOfSubViews == 0)
        Right(headView -> trees.tail1)
      else {
        for {
          subViewsAndRemaining <- ((1 to nbOfSubViews): Seq[Int])
            .foldM(Seq.empty[TransactionView] -> trees.tail1) {
              case ((leftSiblings, remaining), _) =>
                NonEmpty.from(remaining) match {
                  case None =>
                    Left(
                      InvalidLightTransactionViewTreeSequence(
                        "Can't extract a transaction view from an empty sequence of lightweight transaction view trees"
                      )
                    )
                  case Some(remainingNE) =>
                    // TODO(M40): the recursion may blow the stack here
                    toFullView(remainingNE).map { case (fv, newRemaining) =>
                      (fv +: leftSiblings) -> newRemaining
                    }
                }
            }
          (subViewsReversed, remaining) = subViewsAndRemaining
          subViews = subViewsReversed.reverse

          newSubViews = TransactionSubviews(subViews)(protocolVersion, hashOps)
          newView = headView.copy(subviews = newSubViews)

          // Check that the new view still has the same hash
          // (which implicitly validates the ordering of the reconstructed subviews)
          _ <- Either.cond(
            newView.viewHash == headView.viewHash,
            (),
            InvalidLightTransactionViewTreeSequence(
              s"Mismatch in expected hashes of old and new view"
            ),
          )
        } yield (newView, remaining)
      }
    }
    val t = trees.head1
    toFullView(trees).flatMap { case (tv, remaining) =>
      val wrappedEnrichedTree = t.tree.mapUnblindedRootViews(_.replace(tv.viewHash, tv))
      TransactionViewTree
        .create(checked(wrappedEnrichedTree.tryUnwrap))
        .bimap(InvalidLightTransactionViewTreeSequence, tvt => tvt -> remaining)
    }
  }

  /** Get all the full transaction view trees described by a sequence of lightweight trees.
    *
    * Note that this repeats subtrees. More precisely, the following holds:
    * [[toAllFullViewTrees]]([[GenTransactionTree.allLightTransactionViewTrees]]) ==
    * ([[GenTransactionTree.allTransactionViewTrees]])
    */
  def toAllFullViewTrees(protocolVersion: ProtocolVersion, hashOps: HashOps)(
      trees: NonEmpty[Seq[LightTransactionViewTree]]
  ): Either[InvalidLightTransactionViewTreeSequence, NonEmpty[Seq[TransactionViewTree]]] =
    sequenceConsumer(protocolVersion, hashOps)(trees, repeats = true)

  /** Returns the top-level full transaction view trees described by a sequence of lightweight ones */
  def toToplevelFullViewTrees(protocolVersion: ProtocolVersion, hashOps: HashOps)(
      trees: NonEmpty[Seq[LightTransactionViewTree]]
  ): Either[InvalidLightTransactionViewTreeSequence, NonEmpty[Seq[TransactionViewTree]]] =
    sequenceConsumer(protocolVersion, hashOps)(trees, repeats = false)

  // Extracts the common logic behind extracting all full trees and just top-level ones
  private def sequenceConsumer(
      protocolVersion: ProtocolVersion,
      hashOps: HashOps,
  )(
      trees: NonEmpty[Seq[LightTransactionViewTree]],
      repeats: Boolean,
  ): Either[InvalidLightTransactionViewTreeSequence, NonEmpty[Seq[TransactionViewTree]]] = {
    val resBuilder = List.newBuilder[TransactionViewTree]
    @tailrec
    def go(
        ts: NonEmpty[Seq[LightTransactionViewTree]]
    ): Option[InvalidLightTransactionViewTreeSequence] = {
      toFullViewTree(protocolVersion, hashOps)(ts) match {
        case Right((fvt, rest)) =>
          resBuilder += fvt
          NonEmpty.from(if (repeats) ts.tail1 else rest) match {
            case None => None
            case Some(next) => go(next)
          }
        case Left(err) =>
          Some(err)
      }
    }
    go(trees) match {
      case None => Right(NonEmptyUtil.fromUnsafe(resBuilder.result()))
      case Some(err) => Left(err)
    }
  }

  /** Turns a full transaction view tree into a lightweight one. Not stack-safe. */
  def fromTransactionViewTree(
      tvt: TransactionViewTree,
      protocolVersion: ProtocolVersion,
  ): LightTransactionViewTree = {
    val withBlindedSubviews = tvt.view.copy(subviews = tvt.view.subviews.blindFully)
    val genTransactionTree =
      tvt.tree.mapUnblindedRootViews(_.replace(tvt.viewHash, withBlindedSubviews))
    // By definition, the view in a TransactionViewTree has all subviews unblinded
    LightTransactionViewTree.tryCreate(genTransactionTree, tvt.subviewHashes, protocolVersion)
  }

}
