// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import cats.syntax.either.*
import cats.syntax.foldable.*
import com.digitalasset.canton.*
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.data.MerkleTree.*
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.protocol.{v0, *}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.util.{EitherUtil, MonadUtil}
import com.digitalasset.canton.version.*
import com.google.common.annotations.VisibleForTesting
import monocle.Lens
import monocle.macros.GenLens

import scala.collection.mutable

/** Partially blinded version of a transaction tree.
  * This class is also used to represent transaction view trees and informee trees.
  */
// private constructor, because object invariants are checked by factory methods
case class GenTransactionTree private (
    submitterMetadata: MerkleTree[SubmitterMetadata],
    commonMetadata: MerkleTree[CommonMetadata],
    participantMetadata: MerkleTree[ParticipantMetadata],
    rootViews: MerkleSeq[TransactionView],
)(hashOps: HashOps)
    extends MerkleTreeInnerNode[GenTransactionTree](hashOps) {

  def validated: Either[String, this.type] = {
    // Check that every subtree has a unique root hash
    val usedHashes = mutable.Set[RootHash]()

    def go(tree: MerkleTree[_]): Either[String, this.type] = {
      val rootHash = tree.rootHash

      for {
        _ <- EitherUtil.condUnitE(
          !usedHashes.contains(rootHash),
          "A transaction tree must contain a hash at most once. " +
            s"Found the hash ${rootHash.toString} twice.",
        )
        _ = usedHashes.add(rootHash).discard
        _ <- MonadUtil.sequentialTraverse(tree.subtrees)(go)
      } yield this
    }

    go(this)
  }

  @VisibleForTesting
  // Private, because it does not check object invariants and is therefore unsafe.
  private[data] def copy(
      submitterMetadata: MerkleTree[SubmitterMetadata] = this.submitterMetadata,
      commonMetadata: MerkleTree[CommonMetadata] = this.commonMetadata,
      participantMetadata: MerkleTree[ParticipantMetadata] = this.participantMetadata,
      rootViews: MerkleSeq[TransactionView] = this.rootViews,
  ): GenTransactionTree =
    GenTransactionTree(submitterMetadata, commonMetadata, participantMetadata, rootViews)(hashOps)

  override def subtrees: Seq[MerkleTree[_]] =
    Seq[MerkleTree[_]](
      submitterMetadata,
      commonMetadata,
      participantMetadata,
    ) ++ rootViews.rootOrEmpty.toList

  override private[data] def withBlindedSubtrees(
      blindingCommandPerNode: PartialFunction[RootHash, BlindingCommand]
  ): MerkleTree[GenTransactionTree] =
    GenTransactionTree(
      submitterMetadata.doBlind(blindingCommandPerNode),
      commonMetadata.doBlind(blindingCommandPerNode),
      participantMetadata.doBlind(blindingCommandPerNode),
      rootViews.doBlind(blindingCommandPerNode),
    )(hashOps)

  lazy val transactionId: TransactionId = TransactionId.fromRootHash(rootHash)

  /** Yields the full informee tree corresponding to this transaction tree.
    * The resulting informee tree is full, only if every view common data is unblinded.
    */
  def tryFullInformeeTree(protocolVersion: ProtocolVersion): FullInformeeTree = {
    val tree = blind({
      case _: GenTransactionTree => RevealIfNeedBe
      case _: SubmitterMetadata => BlindSubtree
      case _: CommonMetadata => RevealSubtree
      case _: ParticipantMetadata => BlindSubtree
      case _: TransactionView => RevealIfNeedBe
      case _: ViewCommonData => RevealSubtree
      case _: ViewParticipantData => BlindSubtree
    }).tryUnwrap
    FullInformeeTree.tryCreate(tree, protocolVersion)
  }

  /** Yields the transaction view tree corresponding to a given view.
    * If some subtrees have already been blinded, they will remain blinded.
    */
  def transactionViewTree(viewHash: RootHash, isTopLevelView: Boolean): TransactionViewTree = {
    val genTransactionTree = blind({
      case _: GenTransactionTree => RevealIfNeedBe
      case _: SubmitterMetadata =>
        if (isTopLevelView) RevealSubtree
        else BlindSubtree
      case _: CommonMetadata => RevealSubtree
      case _: ParticipantMetadata => RevealSubtree
      case v: TransactionView =>
        if (v.rootHash == viewHash) RevealSubtree
        else RevealIfNeedBe
      case _: ViewCommonData => BlindSubtree
      case _: ViewParticipantData => BlindSubtree
    }).tryUnwrap
    TransactionViewTree(genTransactionTree)
  }

  lazy val allTransactionViewTrees: Seq[TransactionViewTree] = for {
    rootView <- rootViews.unblindedElements
    rootViewTree = transactionViewTree(rootView.rootHash, isTopLevelView = true)
    nonRootViewTrees = for (subView <- rootView.flatten.drop(1))
      yield rootViewTree.tree.transactionViewTree(subView.rootHash, isTopLevelView = false)
    viewTree <- rootViewTree +: nonRootViewTrees
  } yield viewTree

  def allLightTransactionViewTrees(
      protocolVersion: ProtocolVersion
  ): Seq[LightTransactionViewTree] =
    allTransactionViewTrees.map(
      LightTransactionViewTree.fromTransactionViewTree(_, protocolVersion)
    )

  /** All lightweight transaction trees in this [[GenTransactionTree]], accompanied by their witnesses and randomness
    * suitable for deriving encryption keys for encrypted view messages.
    *
    * The witnesses are useful for constructing the BCC-style recipient trees for the view messages.
    * The lightweight transaction + BCC scheme requires that, for every non top-level view,
    * the encryption key used to encrypt that view's lightweight transaction tree can be (deterministically) derived
    * from the randomness used for the parent view. This function returns suitable randomness that is derived using
    * a HKDF. For top-level views, the randomness is derived from the provided initial seed.
    *
    * All the returned random values have the same length as the provided initial seed. The caller should ensure that
    * the provided randomness is long enough to be used for the default HMAC implementation.
    */
  def allLightTransactionViewTreesWithWitnessesAndSeeds(
      initSeed: SecureRandomness,
      hkdfOps: HkdfOps,
      protocolVersion: ProtocolVersion,
  ): Either[HkdfError, Seq[(LightTransactionViewTree, Witnesses, SecureRandomness)]] = {
    val randomnessLength = initSeed.unwrap.size
    val witnessAndSeedMapE =
      allTransactionViewTrees.toList.foldLeftM(
        Map.empty[ViewPosition, (Witnesses, SecureRandomness)]
      ) { case (ws, tvt) =>
        val parentPosition = ViewPosition(tvt.viewPosition.position.drop(1))
        val (parentWitnesses, parentSeed) = ws.getOrElse(
          parentPosition,
          if (parentPosition.position.isEmpty) (Witnesses.empty -> initSeed)
          else
            throw new IllegalStateException(
              s"Can't find the parent witnesses for position ${tvt.viewPosition}"
            ),
        )
        val witnesses = parentWitnesses.prepend(tvt.informees)
        val viewIndex =
          tvt.viewPosition.position.headOption
            .getOrElse(throw new IllegalStateException("View with no position"))
        val seedE = hkdfOps.computeHkdf(
          parentSeed.unwrap,
          randomnessLength,
          HkdfInfo.subview(viewIndex),
        )
        seedE.map(seed => ws.updated(tvt.viewPosition, witnesses -> seed))
      }
    witnessAndSeedMapE.map { witnessAndSeedMap =>
      allTransactionViewTrees.map { tvt =>
        val (witnesses, seed) = witnessAndSeedMap(tvt.viewPosition)
        (LightTransactionViewTree.fromTransactionViewTree(tvt, protocolVersion), witnesses, seed)
      }
    }
  }

  def toProtoV0: v0.GenTransactionTree =
    v0.GenTransactionTree(
      submitterMetadata = Some(MerkleTree.toBlindableNodeV0(submitterMetadata)),
      commonMetadata = Some(MerkleTree.toBlindableNodeV0(commonMetadata)),
      participantMetadata = Some(MerkleTree.toBlindableNodeV0(participantMetadata)),
      rootViews = Some(rootViews.toProtoV0),
    )

  def toProtoV1: v1.GenTransactionTree =
    v1.GenTransactionTree(
      submitterMetadata = Some(MerkleTree.toBlindableNodeV1(submitterMetadata)),
      commonMetadata = Some(MerkleTree.toBlindableNodeV1(commonMetadata)),
      participantMetadata = Some(MerkleTree.toBlindableNodeV1(participantMetadata)),
      rootViews = Some(rootViews.toProtoV1),
    )

  def mapUnblindedRootViews(f: TransactionView => TransactionView): GenTransactionTree =
    this.copy(rootViews = rootViews.mapM(f))

  override def pretty: Pretty[GenTransactionTree] = prettyOfClass(
    param("submitter metadata", _.submitterMetadata),
    param("common metadata", _.commonMetadata),
    param("participant metadata", _.participantMetadata),
    param("roots", _.rootViews),
  )
}

object GenTransactionTree {

  /** @throws GenTransactionTree$.InvalidGenTransactionTree if two subtrees have the same root hash
    */
  def tryCreate(hashOps: HashOps)(
      submitterMetadata: MerkleTree[SubmitterMetadata],
      commonMetadata: MerkleTree[CommonMetadata],
      participantMetadata: MerkleTree[ParticipantMetadata],
      rootViews: MerkleSeq[TransactionView],
  ): GenTransactionTree =
    create(hashOps)(submitterMetadata, commonMetadata, participantMetadata, rootViews).valueOr(
      err => throw InvalidGenTransactionTree(err)
    )

  /** Creates a [[GenTransactionTree]].
    * Yields `Left(...)` if two subtrees have the same root hash.
    */
  def create(hashOps: HashOps)(
      submitterMetadata: MerkleTree[SubmitterMetadata],
      commonMetadata: MerkleTree[CommonMetadata],
      participantMetadata: MerkleTree[ParticipantMetadata],
      rootViews: MerkleSeq[TransactionView],
  ): Either[String, GenTransactionTree] =
    GenTransactionTree(submitterMetadata, commonMetadata, participantMetadata, rootViews)(
      hashOps
    ).validated

  /** Indicates an attempt to create an invalid [[GenTransactionTree]]. */
  case class InvalidGenTransactionTree(message: String) extends RuntimeException(message) {}

  @VisibleForTesting
  val submitterMetadataUnsafe: Lens[GenTransactionTree, MerkleTree[SubmitterMetadata]] =
    GenLens[GenTransactionTree](_.submitterMetadata)

  @VisibleForTesting
  val commonMetadataUnsafe: Lens[GenTransactionTree, MerkleTree[CommonMetadata]] =
    GenLens[GenTransactionTree](_.commonMetadata)

  @VisibleForTesting
  val participantMetadataUnsafe: Lens[GenTransactionTree, MerkleTree[ParticipantMetadata]] =
    GenLens[GenTransactionTree](_.participantMetadata)

  @VisibleForTesting
  val rootViewsUnsafe: Lens[GenTransactionTree, MerkleSeq[TransactionView]] =
    GenLens[GenTransactionTree](_.rootViews)

  def fromProtoV0(
      hashOps: HashOps,
      protoTransactionTree: v0.GenTransactionTree,
  ): ParsingResult[GenTransactionTree] =
    for {
      submitterMetadata <- MerkleTree
        .fromProtoOptionV0(
          protoTransactionTree.submitterMetadata,
          SubmitterMetadata.fromByteString(hashOps),
        )
      commonMetadata <- MerkleTree
        .fromProtoOptionV0(
          protoTransactionTree.commonMetadata,
          CommonMetadata.fromByteString(hashOps),
        )
      participantMetadata <- MerkleTree
        .fromProtoOptionV0(
          protoTransactionTree.participantMetadata,
          ParticipantMetadata.fromByteString(hashOps),
        )
      rootViewsP <- ProtoConverter
        .required("GenTransactionTree.rootViews", protoTransactionTree.rootViews)
      rootViews <- MerkleSeq.fromProtoV0(
        (hashOps, TransactionView.fromByteString(ProtoVersion(0))(hashOps)),
        rootViewsP,
      )
      genTransactionTree <- createGenTransactionTreeV0V1(
        hashOps,
        submitterMetadata,
        commonMetadata,
        participantMetadata,
        rootViews,
      )
    } yield genTransactionTree

  def fromProtoV1(
      hashOps: HashOps,
      protoTransactionTree: v1.GenTransactionTree,
  ): ParsingResult[GenTransactionTree] =
    for {
      submitterMetadata <- MerkleTree
        .fromProtoOptionV1(
          protoTransactionTree.submitterMetadata,
          SubmitterMetadata.fromByteString(hashOps),
        )
      commonMetadata <- MerkleTree
        .fromProtoOptionV1(
          protoTransactionTree.commonMetadata,
          CommonMetadata.fromByteString(hashOps),
        )
      participantMetadata <- MerkleTree
        .fromProtoOptionV1(
          protoTransactionTree.participantMetadata,
          ParticipantMetadata.fromByteString(hashOps),
        )
      rootViewsP <- ProtoConverter
        .required("GenTransactionTree.rootViews", protoTransactionTree.rootViews)
      rootViews <- MerkleSeq.fromProtoV1(
        (
          hashOps,
          TransactionView.fromByteString(ProtoVersion(1))(hashOps),
        ),
        rootViewsP,
      )
      genTransactionTree <- createGenTransactionTreeV0V1(
        hashOps,
        submitterMetadata,
        commonMetadata,
        participantMetadata,
        rootViews,
      )
    } yield genTransactionTree

  def createGenTransactionTreeV0V1(
      hashOps: HashOps,
      submitterMetadata: MerkleTree[SubmitterMetadata],
      commonMetadata: MerkleTree[CommonMetadata],
      participantMetadata: MerkleTree[ParticipantMetadata],
      rootViews: MerkleSeq[TransactionView],
  ): ParsingResult[GenTransactionTree] =
    GenTransactionTree
      .create(hashOps)(
        submitterMetadata,
        commonMetadata,
        participantMetadata,
        rootViews,
      )
      .leftMap(e => ProtoDeserializationError.OtherError(s"Unable to create transaction tree: $e"))
}
