// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.foldable.*
import cats.syntax.functorFilter.*
import cats.syntax.traverse.*
import com.daml.ledger.api.DeduplicationPeriod
import com.daml.ledger.participant.state.v2.SubmitterInfo
import com.daml.nonempty.{NonEmpty, NonEmptyUtil}
import com.digitalasset.canton.*
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.data.InformeeTree.InvalidInformeeTree
import com.digitalasset.canton.data.LightTransactionViewTree.InvalidLightTransactionViewTree
import com.digitalasset.canton.data.MerkleTree.*
import com.digitalasset.canton.data.TransactionViewTree.InvalidTransactionViewTree
import com.digitalasset.canton.data.ViewPosition.MerklePathElement
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.{v0, *}
import com.digitalasset.canton.sequencing.protocol.{Recipients, RecipientsTree}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.serialization.{ProtoConverter, ProtocolVersionedMemoizedEvidence}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.PartyTopologySnapshotClient
import com.digitalasset.canton.util.{EitherUtil, MonadUtil}
import com.digitalasset.canton.version.*
import com.google.common.annotations.VisibleForTesting
import com.google.protobuf.ByteString
import monocle.Lens
import monocle.macros.GenLens

import java.util.UUID
import scala.annotation.tailrec
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

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
        val seedE = ProtocolCryptoApi.hkdf(hkdfOps, protocolVersion)(
          parentSeed,
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

/** Wraps a `GenTransactionTree` where exactly one view (including subviews) is unblinded.
  * The `commonMetadata` and `participantMetadata` are also unblinded.
  * The `submitterMetadata` is unblinded if and only if the unblinded view is a root view.
  *
  * @throws TransactionViewTree$.InvalidTransactionViewTree if [[tree]] is not a transaction view tree
  *                                    (i.e. the wrong set of nodes is blinded)
  */
case class TransactionViewTree(tree: GenTransactionTree) extends ViewTree with PrettyPrinting {

  @tailrec
  private def findTheView(
      viewsWithIndex: Seq[(TransactionView, MerklePathElement)],
      viewPosition: ViewPosition = ViewPosition.root,
  ): (TransactionView, ViewPosition) = {
    viewsWithIndex match {
      case Seq() =>
        throw InvalidTransactionViewTree("A transaction view tree must contain an unblinded view.")
      case Seq((singleView, index)) if singleView.hasAllLeavesBlinded =>
        findTheView(singleView.subviews.unblindedElementsWithIndex, index +: viewPosition)
      case Seq((singleView, index)) if singleView.isFullyUnblinded =>
        (singleView, index +: viewPosition)
      case Seq((singleView, _index)) =>
        throw InvalidTransactionViewTree(
          s"A transaction view tree must contain a fully unblinded view: $singleView"
        )
      case multipleViews =>
        throw InvalidTransactionViewTree(
          s"A transaction view tree must not contain several unblinded views: ${multipleViews.map(_._1)}"
        )
    }
  }

  lazy val transactionId: TransactionId = TransactionId.fromRootHash(tree.rootHash)

  lazy val transactionUuid: UUID = checked(tree.commonMetadata.tryUnwrap).uuid

  /** The (top-most) unblinded view. */
  val (view: TransactionView, viewPosition: ViewPosition) = findTheView(
    tree.rootViews.unblindedElementsWithIndex
  )

  /** Returns the hashes of the direct subviews of the view represented by this tree.
    * By definition, all subviews are unblinded, therefore this will also work when the subviews
    * are stored in a MerkleSeq.
    */
  def subviewHashes: Seq[ViewHash] = view.subviews.trySubviewHashes

  lazy val flatten: Seq[ParticipantTransactionView] = view.flatten.map(sv =>
    ParticipantTransactionView
      .create(sv)
      .valueOr(e =>
        throw new IllegalStateException(
          s"Transaction view tree has a descendant view (hash ${sv.viewHash}) that doesn't contain a participant view: $e"
        )
      )
  )

  override val viewHash: ViewHash = ViewHash.fromRootHash(view.rootHash)

  /** Determines whether `view` is top-level. */
  val isTopLevel: Boolean = viewPosition.position.sizeCompare(1) == 0

  val submitterMetadata: Option[SubmitterMetadata] = {
    val result = tree.submitterMetadata.unwrap.toOption

    if (result.isEmpty == isTopLevel) {
      throw InvalidTransactionViewTree(
        "The submitter metadata must be unblinded if and only if the represented view is top-level. " +
          s"Submitter metadata: ${tree.submitterMetadata.unwrap
              .fold(_ => "blinded", _ => "unblinded")}, isTopLevel: $isTopLevel"
      )
    }

    result
  }

  private[this] val commonMetadata: CommonMetadata =
    tree.commonMetadata.unwrap
      .getOrElse(
        throw InvalidTransactionViewTree(
          s"The common metadata of a transaction view tree must be unblinded."
        )
      )

  override def domainId: DomainId = commonMetadata.domainId

  override def mediatorId: MediatorId = commonMetadata.mediatorId

  def confirmationPolicy: ConfirmationPolicy = commonMetadata.confirmationPolicy

  private def unwrapParticipantMetadata: ParticipantMetadata =
    tree.participantMetadata.unwrap
      .getOrElse(
        throw InvalidTransactionViewTree(
          s"The participant metadata of a transaction view tree must be unblinded."
        )
      )

  val ledgerTime: CantonTimestamp = unwrapParticipantMetadata.ledgerTime

  val submissionTime: CantonTimestamp = unwrapParticipantMetadata.submissionTime

  val workflowId: Option[WorkflowId] = unwrapParticipantMetadata.workflowId

  override lazy val informees: Set[Informee] = view.viewCommonData.tryUnwrap.informees

  lazy val viewParticipantData: ViewParticipantData = view.viewParticipantData.tryUnwrap

  override def toBeSigned: Option[RootHash] =
    if (isTopLevel) Some(transactionId.toRootHash) else None

  override def rootHash: RootHash = tree.rootHash

  override def pretty: Pretty[TransactionViewTree] = prettyOfClass(unnamedParam(_.tree))
}

object TransactionViewTree {
  def create(tree: GenTransactionTree): Either[String, TransactionViewTree] =
    Either.catchOnly[InvalidTransactionViewTree](TransactionViewTree(tree)).leftMap(_.message)

  /** Indicates an attempt to create an invalid [[TransactionViewTree]]. */
  case class InvalidTransactionViewTree(message: String) extends RuntimeException(message) {}
}

/** Encapsulates a [[GenTransactionTree]] that is also an informee tree.
  */
// private constructor, because object invariants are checked by factory methods
case class InformeeTree private (tree: GenTransactionTree)(
    val representativeProtocolVersion: RepresentativeProtocolVersion[InformeeTree]
) extends HasProtocolVersionedWrapper[InformeeTree] {

  def validated: Either[String, this.type] = for {
    _ <- InformeeTree.checkGlobalMetadata(tree)
    _ <- InformeeTree.checkViews(tree.rootViews, assertFull = false)
  } yield this

  override protected def companionObj = InformeeTree

  lazy val transactionId: TransactionId = TransactionId.fromRootHash(tree.rootHash)

  lazy val informeesByView: Map[ViewHash, Set[Informee]] =
    InformeeTree.viewCommonDataByView(tree).map { case (hash, viewCommonData) =>
      hash -> viewCommonData.informees
    }

  private lazy val commonMetadata = checked(tree.commonMetadata.tryUnwrap)

  def domainId: DomainId = commonMetadata.domainId

  def mediatorId: MediatorId = commonMetadata.mediatorId

  def toProtoV0: v0.InformeeTree = v0.InformeeTree(tree = Some(tree.toProtoV0))

  def toProtoV1: v1.InformeeTree = v1.InformeeTree(tree = Some(tree.toProtoV1))
}

object InformeeTree extends HasProtocolVersionedWithContextCompanion[InformeeTree, HashOps] {
  override val name: String = "InformeeTree"

  val supportedProtoVersions: SupportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(0) -> VersionedProtoConverter(
      ProtocolVersion.v2,
      supportedProtoVersion(v0.InformeeTree)(fromProtoV0),
      _.toProtoV0.toByteString,
    ),
    ProtoVersion(1) -> VersionedProtoConverter(
      ProtocolVersion.v4,
      supportedProtoVersion(v1.InformeeTree)(fromProtoV1),
      _.toProtoV1.toByteString,
    ),
  )

  /** Creates an informee tree from a [[GenTransactionTree]].
    * @throws InformeeTree$.InvalidInformeeTree if `tree` is not a valid informee tree (i.e. the wrong nodes are blinded)
    */
  def tryCreate(
      tree: GenTransactionTree,
      protocolVersion: ProtocolVersion,
  ): InformeeTree = create(tree, protocolVersionRepresentativeFor(protocolVersion)).valueOr(err =>
    throw InvalidInformeeTree(err)
  )

  /** Creates an [[InformeeTree]] from a [[GenTransactionTree]].
    * Yields `Left(...)` if `tree` is not a valid informee tree (i.e. the wrong nodes are blinded)
    */
  private[data] def create(
      tree: GenTransactionTree,
      representativeProtocolVersion: RepresentativeProtocolVersion[InformeeTree],
  ): Either[String, InformeeTree] = InformeeTree(tree)(representativeProtocolVersion).validated

  /** Creates an [[InformeeTree]] from a [[GenTransactionTree]].
    * Yields `Left(...)` if `tree` is not a valid informee tree (i.e. the wrong nodes are blinded)
    */
  private[data] def create(
      tree: GenTransactionTree,
      protocolVersion: ProtocolVersion,
  ): Either[String, InformeeTree] = create(tree, protocolVersionRepresentativeFor(protocolVersion))

  private[data] def checkGlobalMetadata(tree: GenTransactionTree): Either[String, Unit] = {
    val errors = Seq.newBuilder[String]

    if (tree.submitterMetadata.unwrap.isRight)
      errors += "The submitter metadata of an informee tree must be blinded."
    if (tree.commonMetadata.unwrap.isLeft)
      errors += "The common metadata of an informee tree must be unblinded."
    if (tree.participantMetadata.unwrap.isRight)
      errors += "The participant metadata of an informee tree must be blinded."

    val message = errors.result().mkString(" ")
    EitherUtil.condUnitE(message.isEmpty, message)
  }

  private[data] def checkViews(
      rootViews: MerkleSeq[TransactionView],
      assertFull: Boolean,
  ): Either[String, Unit] = {

    val errors = Seq.newBuilder[String]

    def checkIsEmpty(blinded: Seq[RootHash]): Unit =
      if (assertFull && blinded.nonEmpty) {
        val hashes = blinded.map(_.toString).mkString(", ")
        errors += s"All views in a full informee tree must be unblinded. Found $hashes"
      }

    checkIsEmpty(rootViews.blindedElements)

    def go(wrappedViews: Seq[TransactionView]): Unit =
      wrappedViews.foreach { view =>
        checkIsEmpty(view.subviews.blindedElements)

        if (assertFull && view.viewCommonData.unwrap.isLeft)
          errors += s"The view common data in a full informee tree must be unblinded. Found ${view.viewCommonData}."

        if (view.viewParticipantData.unwrap.isRight)
          errors += s"The view participant data in an informee tree must be blinded. Found ${view.viewParticipantData}."

        go(view.subviews.unblindedElements)
      }

    go(rootViews.unblindedElements)

    val message = errors.result().mkString("\n")
    EitherUtil.condUnitE(message.isEmpty, message)
  }

  /** Lens for modifying the [[GenTransactionTree]] inside of an informee tree.
    * It does not check if the new `tree` actually constitutes a valid informee tree, therefore:
    * DO NOT USE IN PRODUCTION.
    */
  @VisibleForTesting
  def genTransactionTreeUnsafe: Lens[InformeeTree, GenTransactionTree] =
    Lens[InformeeTree, GenTransactionTree](_.tree)(newTree =>
      oldInformeeTree => InformeeTree(newTree)(oldInformeeTree.representativeProtocolVersion)
    )

  private[data] def viewCommonDataByView(tree: GenTransactionTree): Map[ViewHash, ViewCommonData] =
    tree.rootViews.unblindedElements
      .flatMap(_.flatten)
      .map(view => view.viewHash -> view.viewCommonData.unwrap)
      .collect { case (hash, Right(viewCommonData)) => hash -> viewCommonData }
      .toMap

  /** Indicates an attempt to create an invalid [[InformeeTree]] or [[FullInformeeTree]]. */
  case class InvalidInformeeTree(message: String) extends RuntimeException(message) {}

  def fromProtoV0(
      hashOps: HashOps,
      protoInformeeTree: v0.InformeeTree,
  ): ParsingResult[InformeeTree] =
    for {
      protoTree <- ProtoConverter.required("tree", protoInformeeTree.tree)
      tree <- GenTransactionTree.fromProtoV0(hashOps, protoTree)
      informeeTree <- InformeeTree
        .create(tree, protocolVersionRepresentativeFor(ProtoVersion(0)))
        .leftMap(e => ProtoDeserializationError.OtherError(s"Unable to create informee tree: $e"))
    } yield informeeTree

  def fromProtoV1(
      hashOps: HashOps,
      protoInformeeTree: v1.InformeeTree,
  ): ParsingResult[InformeeTree] =
    for {
      protoTree <- ProtoConverter.required("tree", protoInformeeTree.tree)
      tree <- GenTransactionTree.fromProtoV1(hashOps, protoTree)
      informeeTree <- InformeeTree
        .create(tree, protocolVersionRepresentativeFor(ProtoVersion(1)))
        .leftMap(e => ProtoDeserializationError.OtherError(s"Unable to create informee tree: $e"))
    } yield informeeTree
}

/** Wraps a [[GenTransactionTree]] that is also a full informee tree.
  */
// private constructor, because object invariants are checked by factory methods
case class FullInformeeTree private (tree: GenTransactionTree)(
    val representativeProtocolVersion: RepresentativeProtocolVersion[FullInformeeTree]
) extends HasProtocolVersionedWrapper[FullInformeeTree]
    with PrettyPrinting {

  def validated: Either[String, this.type] = for {
    _ <- InformeeTree.checkGlobalMetadata(tree)
    _ <- InformeeTree.checkViews(tree.rootViews, assertFull = true)
  } yield this

  override protected def companionObj = FullInformeeTree

  lazy val transactionId: TransactionId = TransactionId.fromRootHash(tree.rootHash)

  private lazy val commonMetadata: CommonMetadata = checked(tree.commonMetadata.tryUnwrap)
  lazy val domainId: DomainId = commonMetadata.domainId
  lazy val mediatorId: MediatorId = commonMetadata.mediatorId

  /** Yields the informee tree unblinded for a defined set of parties.
    * If a view common data is already blinded, then it remains blinded even if one of the given parties is a stakeholder.
    */
  def informeeTreeUnblindedFor(
      parties: collection.Set[LfPartyId],
      protocolVersion: ProtocolVersion,
  ): InformeeTree = {
    val rawResult = tree
      .blind({
        case _: GenTransactionTree => RevealIfNeedBe
        case _: SubmitterMetadata => BlindSubtree
        case _: CommonMetadata => RevealSubtree
        case _: ParticipantMetadata => BlindSubtree
        case _: TransactionView => RevealIfNeedBe
        case v: ViewCommonData =>
          if (v.informees.map(_.party).intersect(parties).nonEmpty)
            RevealSubtree
          else
            BlindSubtree
        case _: ViewParticipantData => BlindSubtree
      })
      .tryUnwrap
    InformeeTree.tryCreate(rawResult, protocolVersion)
  }

  lazy val informeesAndThresholdByView: Map[ViewHash, (Set[Informee], NonNegativeInt)] =
    InformeeTree.viewCommonDataByView(tree).map { case (hash, viewCommonData) =>
      hash -> ((viewCommonData.informees, viewCommonData.threshold))
    }

  lazy val allInformees: Set[LfPartyId] = InformeeTree
    .viewCommonDataByView(tree)
    .flatMap { case (_, viewCommonData) => viewCommonData.informees }
    .map(_.party)
    .toSet

  lazy val transactionUuid: UUID = checked(tree.commonMetadata.tryUnwrap).uuid

  lazy val confirmationPolicy: ConfirmationPolicy = checked(
    tree.commonMetadata.tryUnwrap
  ).confirmationPolicy

  def toProtoV0: v0.FullInformeeTree =
    v0.FullInformeeTree(tree = Some(tree.toProtoV0))

  def toProtoV1: v1.FullInformeeTree =
    v1.FullInformeeTree(tree = Some(tree.toProtoV1))

  override def pretty: Pretty[FullInformeeTree] = prettyOfParam(_.tree)
}

object FullInformeeTree
    extends HasProtocolVersionedWithContextCompanion[FullInformeeTree, HashOps] {
  override val name: String = "FullInformeeTree"

  val supportedProtoVersions: SupportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(0) -> VersionedProtoConverter(
      ProtocolVersion.v2,
      supportedProtoVersion(v0.FullInformeeTree)(fromProtoV0),
      _.toProtoV0.toByteString,
    ),
    ProtoVersion(1) -> VersionedProtoConverter(
      ProtocolVersion.v4,
      supportedProtoVersion(v1.FullInformeeTree)(fromProtoV1),
      _.toProtoV1.toByteString,
    ),
  )

  /** Creates a full informee tree from a [[GenTransactionTree]].
    * @throws InformeeTree$.InvalidInformeeTree if `tree` is not a valid informee tree (i.e. the wrong nodes are blinded)
    */
  def tryCreate(tree: GenTransactionTree, protocolVersion: ProtocolVersion): FullInformeeTree =
    create(tree, protocolVersionRepresentativeFor(protocolVersion)).valueOr(err =>
      throw InvalidInformeeTree(err)
    )

  private[data] def create(
      tree: GenTransactionTree,
      representativeProtocolVersion: RepresentativeProtocolVersion[FullInformeeTree],
  ): Either[String, FullInformeeTree] =
    FullInformeeTree(tree)(representativeProtocolVersion).validated

  private[data] def create(
      tree: GenTransactionTree,
      protocolVersion: ProtocolVersion,
  ): Either[String, FullInformeeTree] =
    create(tree, protocolVersionRepresentativeFor(protocolVersion))

  /** Lens for modifying the [[GenTransactionTree]] inside of a full informee tree.
    * It does not check if the new `tree` actually constitutes a valid full informee tree, therefore:
    * DO NOT USE IN PRODUCTION.
    */
  @VisibleForTesting
  lazy val genTransactionTreeUnsafe: Lens[FullInformeeTree, GenTransactionTree] =
    Lens[FullInformeeTree, GenTransactionTree](_.tree)(newTree =>
      fullInformeeTree => FullInformeeTree(newTree)(fullInformeeTree.representativeProtocolVersion)
    )

  def fromProtoV0(
      hashOps: HashOps,
      protoInformeeTree: v0.FullInformeeTree,
  ): ParsingResult[FullInformeeTree] =
    for {
      protoTree <- ProtoConverter.required("tree", protoInformeeTree.tree)
      tree <- GenTransactionTree.fromProtoV0(hashOps, protoTree)
      fullInformeeTree <- FullInformeeTree
        .create(tree, protocolVersionRepresentativeFor(ProtoVersion(0)))
        .leftMap(e =>
          ProtoDeserializationError.OtherError(s"Unable to create full informee tree: $e")
        )
    } yield fullInformeeTree

  def fromProtoV1(
      hashOps: HashOps,
      protoInformeeTree: v1.FullInformeeTree,
  ): ParsingResult[FullInformeeTree] =
    for {
      protoTree <- ProtoConverter.required("tree", protoInformeeTree.tree)
      tree <- GenTransactionTree.fromProtoV1(hashOps, protoTree)
      fullInformeeTree <- FullInformeeTree
        .create(tree, protocolVersionRepresentativeFor(ProtoVersion(1)))
        .leftMap(e =>
          ProtoDeserializationError.OtherError(s"Unable to create full informee tree: $e")
        )
    } yield fullInformeeTree
}

/** Information about the submitters of the transaction
  */
final case class SubmitterMetadata private (
    actAs: NonEmpty[Set[LfPartyId]],
    applicationId: ApplicationId,
    commandId: CommandId,
    submitterParticipant: ParticipantId,
    salt: Salt,
    submissionId: Option[LedgerSubmissionId],
    dedupPeriod: DeduplicationPeriod,
)(
    hashOps: HashOps,
    val representativeProtocolVersion: RepresentativeProtocolVersion[SubmitterMetadata],
    override val deserializedFrom: Option[ByteString],
) extends MerkleTreeLeaf[SubmitterMetadata](hashOps)
    with HasProtocolVersionedWrapper[SubmitterMetadata]
    with ProtocolVersionedMemoizedEvidence {

  override protected[this] def toByteStringUnmemoized: ByteString =
    super[HasProtocolVersionedWrapper].toByteString

  override val hashPurpose: HashPurpose = HashPurpose.SubmitterMetadata

  override def pretty: Pretty[SubmitterMetadata] = prettyOfClass(
    param("act as", _.actAs),
    param("application id", _.applicationId),
    param("command id", _.commandId),
    param("submitter participant", _.submitterParticipant),
    param("salt", _.salt),
    paramIfDefined("submission id", _.submissionId),
    param("deduplication period", _.dedupPeriod),
  )

  override def companionObj = SubmitterMetadata

  protected def toProtoV0: v0.SubmitterMetadata = v0.SubmitterMetadata(
    actAs = actAs.toSeq,
    applicationId = applicationId.toProtoPrimitive,
    commandId = commandId.toProtoPrimitive,
    submitterParticipant = submitterParticipant.toProtoPrimitive,
    salt = Some(salt.toProtoV0),
    submissionId = submissionId.getOrElse(""),
    dedupPeriod = Some(SerializableDeduplicationPeriod(dedupPeriod).toProtoV0),
  )

}

object SubmitterMetadata
    extends HasMemoizedProtocolVersionedWithContextCompanion[
      SubmitterMetadata,
      HashOps,
    ] {
  override val name: String = "SubmitterMetadata"

  val supportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(0) -> VersionedProtoConverter(
      ProtocolVersion.v2,
      supportedProtoVersionMemoized(v0.SubmitterMetadata)(fromProtoV0),
      _.toProtoV0.toByteString,
    )
  )

  def apply(
      actAs: NonEmpty[Set[LfPartyId]],
      applicationId: ApplicationId,
      commandId: CommandId,
      submitterParticipant: ParticipantId,
      salt: Salt,
      submissionId: Option[LedgerSubmissionId],
      dedupPeriod: DeduplicationPeriod,
      hashOps: HashOps,
      protocolVersion: ProtocolVersion,
  ): SubmitterMetadata = SubmitterMetadata(
    actAs, // Canton ignores SubmitterInfo.readAs per https://github.com/digital-asset/daml/pull/12136
    applicationId,
    commandId,
    submitterParticipant,
    salt,
    submissionId,
    dedupPeriod,
  )(hashOps, protocolVersionRepresentativeFor(protocolVersion), None)

  def fromSubmitterInfo(hashOps: HashOps)(
      submitterInfo: SubmitterInfo,
      submitterParticipant: ParticipantId,
      salt: Salt,
      protocolVersion: ProtocolVersion,
  ): Either[String, SubmitterMetadata] = {
    NonEmpty.from(submitterInfo.actAs.toSet).toRight("The actAs set must not be empty.").map {
      actAsNes =>
        SubmitterMetadata(
          actAsNes, // Canton ignores SubmitterInfo.readAs per https://github.com/digital-asset/daml/pull/12136
          ApplicationId(submitterInfo.applicationId),
          CommandId(submitterInfo.commandId),
          submitterParticipant,
          salt,
          submitterInfo.submissionId,
          submitterInfo.deduplicationPeriod,
        )(hashOps, protocolVersionRepresentativeFor(protocolVersion), None)
    }
  }

  private def fromProtoV0(hashOps: HashOps, metaDataP: v0.SubmitterMetadata)(
      bytes: ByteString
  ): ParsingResult[SubmitterMetadata] = {
    val v0.SubmitterMetadata(
      saltOP,
      actAsP,
      applicationIdP,
      commandIdP,
      submitterParticipantP,
      submissionIdP,
      dedupPeriodOP,
    ) = metaDataP
    for {
      submitterParticipant <- ParticipantId
        .fromProtoPrimitive(submitterParticipantP, "SubmitterMetadata.submitter_participant")
      actAs <- actAsP.traverse(
        ProtoConverter
          .parseLfPartyId(_)
          .leftMap(e => ProtoDeserializationError.ValueConversionError("actAs", e.message))
      )
      applicationId <- ApplicationId
        .fromProtoPrimitive(applicationIdP)
        .leftMap(ProtoDeserializationError.ValueConversionError("applicationId", _))
      commandId <- CommandId
        .fromProtoPrimitive(commandIdP)
        .leftMap(ProtoDeserializationError.ValueConversionError("commandId", _))
      salt <- ProtoConverter
        .parseRequired(Salt.fromProtoV0, "salt", saltOP)
        .leftMap(e => ProtoDeserializationError.ValueConversionError("salt", e.message))
      submissionId <-
        if (submissionIdP.nonEmpty)
          LedgerSubmissionId
            .fromString(submissionIdP)
            .bimap(ProtoDeserializationError.ValueConversionError("submissionId", _), Some(_))
        else Right(None)
      dedupPeriod <- ProtoConverter
        .parseRequired(
          SerializableDeduplicationPeriod.fromProtoV0,
          "SubmitterMetadata.deduplication_period",
          dedupPeriodOP,
        )
        .leftMap(e =>
          ProtoDeserializationError.ValueConversionError("deduplicationPeriod", e.message)
        )

      actAsNes <- NonEmpty
        .from(actAs.toSet)
        .toRight(
          ProtoDeserializationError.ValueConversionError("acsAs", "actAs set must not be empty.")
        )
    } yield SubmitterMetadata(
      actAsNes,
      applicationId,
      commandId,
      submitterParticipant,
      salt,
      submissionId,
      dedupPeriod,
    )(hashOps, protocolVersionRepresentativeFor(ProtoVersion(0)), Some(bytes))
  }
}

/** Information concerning every '''member''' involved in the underlying transaction.
  *
  * @param confirmationPolicy determines who must confirm the request
  */
final case class CommonMetadata private (
    confirmationPolicy: ConfirmationPolicy,
    domainId: DomainId,
    mediatorId: MediatorId,
    salt: Salt,
    uuid: UUID,
)(
    hashOps: HashOps,
    val representativeProtocolVersion: RepresentativeProtocolVersion[CommonMetadata],
    override val deserializedFrom: Option[ByteString],
) extends MerkleTreeLeaf[CommonMetadata](hashOps)
    with HasProtocolVersionedWrapper[CommonMetadata]
    with ProtocolVersionedMemoizedEvidence {

  override protected[this] def toByteStringUnmemoized: ByteString =
    super[HasProtocolVersionedWrapper].toByteString

  override val hashPurpose: HashPurpose = HashPurpose.CommonMetadata

  override def pretty: Pretty[CommonMetadata] = prettyOfClass(
    param("confirmation policy", _.confirmationPolicy),
    param("domain id", _.domainId),
    param("mediator id", _.mediatorId),
    param("uuid", _.uuid),
    param("salt", _.salt),
  )

  override def companionObj = CommonMetadata

  private[CommonMetadata] def toProtoV0: v0.CommonMetadata = v0.CommonMetadata(
    confirmationPolicy = confirmationPolicy.toProtoPrimitive,
    domainId = domainId.toProtoPrimitive,
    salt = Some(salt.toProtoV0),
    uuid = ProtoConverter.UuidConverter.toProtoPrimitive(uuid),
    mediatorId = mediatorId.toProtoPrimitive,
  )
}

object CommonMetadata
    extends HasMemoizedProtocolVersionedWithContextCompanion[
      CommonMetadata,
      HashOps,
    ] {
  override val name: String = "CommonMetadata"

  val supportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(0) -> VersionedProtoConverter(
      ProtocolVersion.v2,
      supportedProtoVersionMemoized(v0.CommonMetadata)(fromProtoV0),
      _.toProtoV0.toByteString,
    )
  )

  def apply(
      hashOps: HashOps
  )(
      confirmationPolicy: ConfirmationPolicy,
      domain: DomainId,
      mediatorId: MediatorId,
      salt: Salt,
      uuid: UUID,
      protocolVersion: ProtocolVersion,
  ): CommonMetadata = CommonMetadata(confirmationPolicy, domain, mediatorId, salt, uuid)(
    hashOps,
    protocolVersionRepresentativeFor(protocolVersion),
    None,
  )

  private def fromProtoV0(hashOps: HashOps, metaDataP: v0.CommonMetadata)(
      bytes: ByteString
  ): ParsingResult[CommonMetadata] =
    for {
      confirmationPolicy <- ConfirmationPolicy
        .fromProtoPrimitive(metaDataP.confirmationPolicy)
        .leftMap(e =>
          ProtoDeserializationError.ValueDeserializationError("confirmationPolicy", e.show)
        )
      v0.CommonMetadata(saltP, _confirmationPolicyP, domainIdP, uuidP, mediatorIdP) = metaDataP
      domainUid <- UniqueIdentifier
        .fromProtoPrimitive_(domainIdP)
        .leftMap(ProtoDeserializationError.ValueDeserializationError("domainId", _))
      mediatorId <- MediatorId
        .fromProtoPrimitive(mediatorIdP, "CommonMetadata.mediator_id")
      salt <- ProtoConverter
        .parseRequired(Salt.fromProtoV0, "salt", saltP)
        .leftMap(_.inField("salt"))
      uuid <- ProtoConverter.UuidConverter.fromProtoPrimitive(uuidP).leftMap(_.inField("uuid"))
    } yield CommonMetadata(confirmationPolicy, DomainId(domainUid), mediatorId, salt, uuid)(
      hashOps,
      protocolVersionRepresentativeFor(ProtoVersion(0)),
      Some(bytes),
    )
}

/** Information concerning every '''participant''' involved in the underlying transaction.
  *
  * @param ledgerTime     The ledger time of the transaction
  * @param submissionTime The submission time of the transaction
  * @param workflowId     optional workflow id associated with the ledger api provided workflow instance
  */
final case class ParticipantMetadata private (
    ledgerTime: CantonTimestamp,
    submissionTime: CantonTimestamp,
    workflowId: Option[WorkflowId],
    salt: Salt,
)(
    hashOps: HashOps,
    val representativeProtocolVersion: RepresentativeProtocolVersion[ParticipantMetadata],
    override val deserializedFrom: Option[ByteString],
) extends MerkleTreeLeaf[ParticipantMetadata](hashOps)
    with HasProtocolVersionedWrapper[ParticipantMetadata]
    with ProtocolVersionedMemoizedEvidence {

  override protected[this] def toByteStringUnmemoized: ByteString =
    super[HasProtocolVersionedWrapper].toByteString

  override val hashPurpose: HashPurpose = HashPurpose.ParticipantMetadata

  override def pretty: Pretty[ParticipantMetadata] = prettyOfClass(
    param("ledger time", _.ledgerTime),
    param("submission time", _.submissionTime),
    paramIfDefined("workflow id", _.workflowId),
    param("salt", _.salt),
  )

  override def companionObj = ParticipantMetadata

  protected def toProtoV0: v0.ParticipantMetadata = v0.ParticipantMetadata(
    ledgerTime = Some(ledgerTime.toProtoPrimitive),
    submissionTime = Some(submissionTime.toProtoPrimitive),
    workflowId = workflowId.fold("")(_.toProtoPrimitive),
    salt = Some(salt.toProtoV0),
  )
}

object ParticipantMetadata
    extends HasMemoizedProtocolVersionedWithContextCompanion[ParticipantMetadata, HashOps] {
  override val name: String = "ParticipantMetadata"

  val supportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(0) -> VersionedProtoConverter(
      ProtocolVersion.v2,
      supportedProtoVersionMemoized(v0.ParticipantMetadata)(fromProtoV0),
      _.toProtoV0.toByteString,
    )
  )

  def apply(hashOps: HashOps)(
      ledgerTime: CantonTimestamp,
      submissionTime: CantonTimestamp,
      workflowId: Option[WorkflowId],
      salt: Salt,
      protocolVersion: ProtocolVersion,
  ): ParticipantMetadata =
    ParticipantMetadata(ledgerTime, submissionTime, workflowId, salt)(
      hashOps,
      protocolVersionRepresentativeFor(protocolVersion),
      None,
    )

  private def fromProtoV0(hashOps: HashOps, metadataP: v0.ParticipantMetadata)(
      bytes: ByteString
  ): ParsingResult[ParticipantMetadata] =
    for {
      let <- ProtoConverter
        .parseRequired(CantonTimestamp.fromProtoPrimitive, "ledgerTime", metadataP.ledgerTime)
      v0.ParticipantMetadata(saltP, _ledgerTimeP, submissionTimeP, workflowIdP) = metadataP
      submissionTime <- ProtoConverter
        .parseRequired(CantonTimestamp.fromProtoPrimitive, "submissionTime", submissionTimeP)
      workflowId <- workflowIdP match {
        case "" => Right(None)
        case wf =>
          WorkflowId
            .fromProtoPrimitive(wf)
            .map(Some(_))
            .leftMap(ProtoDeserializationError.ValueDeserializationError("workflowId", _))
      }
      salt <- ProtoConverter
        .parseRequired(Salt.fromProtoV0, "salt", saltP)
        .leftMap(_.inField("salt"))
    } yield ParticipantMetadata(let, submissionTime, workflowId, salt)(
      hashOps,
      protocolVersionRepresentativeFor(ProtoVersion(0)),
      Some(bytes),
    )
}

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
      ProtocolVersion.v2,
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

/** Encodes the hierarchy of the witnesses of a view.
  *
  * By convention, the order is: the view's informees are at the head of the list, then the parent's views informees,
  * then the grandparent's, etc.
  */
case class Witnesses(unwrap: Seq[Set[Informee]]) {
  import Witnesses.*

  def prepend(informees: Set[Informee]) = Witnesses(informees +: unwrap)

  /** Derive a recipient tree that mirrors the given hierarchy of witnesses. */
  def toRecipients(
      topology: PartyTopologySnapshotClient
  )(implicit ec: ExecutionContext): EitherT[Future, InvalidWitnesses, Recipients] =
    for {
      recipientsList <- unwrap.foldLeftM(Seq.empty[RecipientsTree]) { (children, informees) =>
        for {
          informeeParticipants <- topology
            .activeParticipantsOfAll(informees.map(_.party).toList)
            .leftMap(missing =>
              InvalidWitnesses(s"Found no active participants for informees: $missing")
            )
          informeeParticipantSet <- EitherT.fromOption[Future](
            NonEmpty.from(informeeParticipants.toSet[Member]),
            InvalidWitnesses(s"Empty set of witnesses given"),
          )
        } yield Seq(RecipientsTree(informeeParticipantSet, children))
      }
      // TODO(error handling) Why is it safe to assume that the recipient list is non-empty?
      //  It will be empty if `unwrap` is empty.
      recipients = Recipients(NonEmptyUtil.fromUnsafe(recipientsList))
    } yield recipients

  def flatten: Set[Informee] = unwrap.foldLeft(Set.empty[Informee])(_ union _)

}

case object Witnesses {
  lazy val empty: Witnesses = Witnesses(Seq.empty)

  case class InvalidWitnesses(message: String) extends PrettyPrinting {
    override def pretty: Pretty[InvalidWitnesses] = prettyOfClass(unnamedParam(_.message.unquoted))
  }
}
