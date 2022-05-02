// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import java.util.UUID
import cats.data.EitherT
import cats.syntax.either._
import cats.syntax.foldable._
import cats.syntax.functorFilter._
import cats.syntax.traverse._
import com.daml.ledger.api.DeduplicationPeriod
import com.daml.ledger.participant.state.v2.SubmitterInfo
import com.daml.nonempty.NonEmptyUtil
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton._
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.crypto._
import com.digitalasset.canton.data.GenTransactionTree.InvalidGenTransactionTree
import com.digitalasset.canton.data.InformeeTree.InvalidInformeeTree
import com.digitalasset.canton.data.LightTransactionViewTree.InvalidLightTransactionViewTree
import com.digitalasset.canton.data.MerkleTree._
import com.digitalasset.canton.data.TransactionViewTree.InvalidTransactionViewTree
import com.digitalasset.canton.data.ViewPosition.MerklePathElement
import com.digitalasset.canton.topology.client.PartyTopologySnapshotClient
import com.digitalasset.canton.topology.{
  DomainId,
  MediatorId,
  Member,
  ParticipantId,
  UniqueIdentifier,
}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.{
  ConfirmationPolicy,
  RootHash,
  SerializableDeduplicationPeriod,
  TransactionId,
  ViewHash,
  v0,
}
import com.digitalasset.canton.sequencing.protocol.{Recipients, RecipientsTree}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.serialization.{MemoizedEvidence, ProtoConverter}
import com.digitalasset.canton.util.NoCopy
import com.digitalasset.canton.version.{
  HasMemoizedVersionedMessageWithContextCompanion,
  HasProtoV0,
  HasVersionedMessageWithContextCompanion,
  HasVersionedWrapper,
  ProtocolVersion,
  VersionedMessage,
}
import com.google.protobuf.ByteString

import scala.annotation.tailrec
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

/** Partially blinded version of a transaction tree.
  * This class is also used to represent transaction view trees and informee trees.
  *
  * @throws GenTransactionTree$.InvalidGenTransactionTree if two subtrees have the same root hash
  */
case class GenTransactionTree(
    submitterMetadata: MerkleTree[SubmitterMetadata],
    commonMetadata: MerkleTree[CommonMetadata],
    participantMetadata: MerkleTree[ParticipantMetadata],
    rootViews: MerkleSeq[TransactionView],
)(hashOps: HashOps)
    extends MerkleTreeInnerNode[GenTransactionTree](hashOps)
    with HasProtoV0[v0.GenTransactionTree] {

  {
    // Check that every subtree has a unique root hash
    val usedHashes = mutable.Set[RootHash]()

    def checkUniqueness(tree: MerkleTree[_]): Unit = {
      val rootHash = tree.rootHash
      if (usedHashes.contains(rootHash)) {
        throw InvalidGenTransactionTree(
          "A transaction tree must contain a hash at most once. " +
            s"Found the hash ${rootHash.toString} twice."
        )
      }

      usedHashes.add(rootHash)
      tree.subtrees.foreach(checkUniqueness)
    }

    checkUniqueness(this)
  }

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
  def fullInformeeTree: FullInformeeTree = {
    val tree = blind({
      case _: GenTransactionTree => RevealIfNeedBe
      case _: SubmitterMetadata => BlindSubtree
      case _: CommonMetadata => RevealSubtree
      case _: ParticipantMetadata => BlindSubtree
      case _: TransactionView => RevealIfNeedBe
      case _: ViewCommonData => RevealSubtree
      case _: ViewParticipantData => BlindSubtree
    }).tryUnwrap
    FullInformeeTree(tree)
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

  lazy val allLightTransactionViewTrees: Seq[LightTransactionViewTree] =
    allTransactionViewTrees.map(LightTransactionViewTree.fromTransactionViewTree)

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
        val seedE = hkdfOps.hkdfExpand(parentSeed, randomnessLength, HkdfInfo.subview(viewIndex))
        seedE.map(seed => ws.updated(tvt.viewPosition, witnesses -> seed))
      }
    witnessAndSeedMapE.map { witnessAndSeedMap =>
      allTransactionViewTrees.map { tvt =>
        val (witnesses, seed) = witnessAndSeedMap(tvt.viewPosition)
        (LightTransactionViewTree.fromTransactionViewTree(tvt), witnesses, seed)
      }
    }
  }

  override def toProtoV0: v0.GenTransactionTree =
    v0.GenTransactionTree(
      submitterMetadata = Some(MerkleTree.toBlindableNode(submitterMetadata)),
      commonMetadata = Some(MerkleTree.toBlindableNode(commonMetadata)),
      participantMetadata = Some(MerkleTree.toBlindableNode(participantMetadata)),
      rootViews = Some(rootViews.toProtoV0),
    )

  def topLevelViewMap(f: TransactionView => TransactionView): GenTransactionTree = {
    this.copy(rootViews = rootViews.mapM(f))(hashOps)
  }

  override def pretty: Pretty[GenTransactionTree] = prettyOfClass(
    param("submitter metadata", _.submitterMetadata),
    param("common metadata", _.commonMetadata),
    param("participant metadata", _.participantMetadata),
    param("roots", _.rootViews),
  )
}

object GenTransactionTree {

  /** @throws InvalidGenTransactionTree if two subtrees have the same root hash
    */
  def apply(hashOps: HashOps)(
      submitterMetadata: MerkleTree[SubmitterMetadata],
      commonMetadata: MerkleTree[CommonMetadata],
      participantMetadata: MerkleTree[ParticipantMetadata],
      rootViews: MerkleSeq[TransactionView],
  ): GenTransactionTree =
    new GenTransactionTree(submitterMetadata, commonMetadata, participantMetadata, rootViews)(
      hashOps
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
    Either
      .catchOnly[InvalidGenTransactionTree](
        new GenTransactionTree(submitterMetadata, commonMetadata, participantMetadata, rootViews)(
          hashOps
        )
      )
      .leftMap(_.message)

  /** Indicates an attempt to create an invalid [[GenTransactionTree]]. */
  case class InvalidGenTransactionTree(message: String) extends RuntimeException(message) {}

  def fromProtoV0(
      hashOps: HashOps,
      protoTransactionTree: v0.GenTransactionTree,
  ): ParsingResult[GenTransactionTree] =
    for {
      submitterMetadata <- MerkleTree
        .fromProtoOption(
          protoTransactionTree.submitterMetadata,
          SubmitterMetadata.fromByteString(hashOps),
        )
      commonMetadata <- MerkleTree
        .fromProtoOption(
          protoTransactionTree.commonMetadata,
          CommonMetadata.fromByteString(hashOps),
        )
      participantMetadata <- MerkleTree
        .fromProtoOption(
          protoTransactionTree.participantMetadata,
          ParticipantMetadata.fromByteString(hashOps),
        )
      rootViewsP <- ProtoConverter
        .required("GenTransactionTree.rootViews", protoTransactionTree.rootViews)
      rootViews <- MerkleSeq.fromProtoV0(hashOps, TransactionView.fromByteString(hashOps))(
        rootViewsP
      )
      genTransactionTree <- GenTransactionTree
        .create(hashOps)(
          submitterMetadata,
          commonMetadata,
          participantMetadata,
          rootViews,
        )
        .leftMap(e =>
          ProtoDeserializationError.OtherError(s"Unable to create transaction tree: $e")
        )
    } yield genTransactionTree

  def fromByteString(hashOps: HashOps, bytes: ByteString): ParsingResult[GenTransactionTree] =
    for {
      protoTransactionTree <- ProtoConverter
        .protoParser(v0.GenTransactionTree.parseFrom)(bytes)
      transactionTree <- fromProtoV0(hashOps, protoTransactionTree)
    } yield transactionTree
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
      viewsWithIndex: Seq[(MerkleTree[TransactionView], MerklePathElement)],
      viewPosition: ViewPosition = ViewPosition.root,
  ): (TransactionView, ViewPosition) = {
    val partiallyUnblindedViewsWithIndex = viewsWithIndex.mapFilter { case (view, index) =>
      view.unwrap.toOption.map(_ -> index)
    }
    partiallyUnblindedViewsWithIndex match {
      case Seq() =>
        throw InvalidTransactionViewTree("A transaction view tree must contain an unblinded view.")
      case Seq((singleView, index)) if singleView.hasAllLeavesBlinded =>
        findTheView(singleView.subviewsWithIndex, index +: viewPosition)
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

  lazy val participantView: ParticipantTransactionView = ParticipantTransactionView
    .create(view)
    .valueOr(e =>
      throw new IllegalStateException(
        s"Transaction view tree doesn't contain a participant view: $e"
      )
    )

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

  /** The direct proper subviews of [[view]] */
  val subviewsOfView: Seq[TransactionView] = view.subviews.map(_.tryUnwrap)

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
  *
  * @throws InformeeTree$.InvalidInformeeTree if `tree` is not a valid informee tree (i.e. the wrong nodes are blinded)
  */
case class InformeeTree(tree: GenTransactionTree)
    extends HasVersionedWrapper[VersionedMessage[InformeeTree]]
    with HasProtoV0[v0.InformeeTree] {

  InformeeTree.checkGlobalMetadata(tree)

  InformeeTree.checkViews(tree.rootViews, assertFull = false)

  lazy val transactionId: TransactionId = TransactionId.fromRootHash(tree.rootHash)

  lazy val informeesByView: Map[ViewHash, Set[Informee]] =
    InformeeTree.viewCommonDataByView(tree).map { case (hash, viewCommonData) =>
      hash -> viewCommonData.informees
    }

  private val commonMetadata = checked(tree.commonMetadata.tryUnwrap)

  def domainId: DomainId = commonMetadata.domainId

  def mediatorId: MediatorId = commonMetadata.mediatorId

  /** @throws InformeeTree$.InvalidInformeeTree if this is not a full informee tree (i.e. the wrong nodes are blinded)
    */
  lazy val tryToFullInformeeTree: FullInformeeTree = FullInformeeTree(tree)

  override def toProtoVersioned(version: ProtocolVersion): VersionedMessage[InformeeTree] =
    VersionedMessage(toProtoV0.toByteString, 0)

  override def toProtoV0: v0.InformeeTree =
    v0.InformeeTree(tree = Some(tree.toProtoV0))
}

object InformeeTree extends HasVersionedMessageWithContextCompanion[InformeeTree, HashOps] {
  override val name: String = "InformeeTree"

  val supportedProtoVersions: Map[Int, Parser] = Map(
    0 -> supportedProtoVersion(v0.InformeeTree)(fromProtoV0)
  )

  /** Creates an [[InformeeTree]] from a [[GenTransactionTree]].
    * Yields `Left(...)` if `tree` is not a valid informee tree (i.e. the wrong nodes are blinded)
    */
  def create(tree: GenTransactionTree): Either[String, InformeeTree] =
    Either.catchOnly[InvalidInformeeTree](InformeeTree(tree)).leftMap(_.message)

  private[data] def checkGlobalMetadata(tree: GenTransactionTree): Unit = {
    val errors = Seq.newBuilder[String]

    if (tree.submitterMetadata.unwrap.isRight)
      errors += "The submitter metadata of an informee tree must be blinded."
    if (tree.commonMetadata.unwrap.isLeft)
      errors += "The common metadata of an informee tree must be unblinded."
    if (tree.participantMetadata.unwrap.isRight)
      errors += "The participant metadata of an informee tree must be blinded."

    val message = errors.result().mkString(" ")
    if (message.nonEmpty)
      throw InvalidInformeeTree(message)
  }

  private[data] def checkViews(rootViews: MerkleSeq[TransactionView], assertFull: Boolean): Unit = {

    val errors = Seq.newBuilder[String]

    if (assertFull) {
      rootViews.blindedElements.foreach(hash =>
        errors += s"All views in a full informee tree must be unblinded. Found $hash."
      )
    }

    def go(wrappedViews: Seq[MerkleTree[TransactionView]]): Unit =
      wrappedViews.map(_.unwrap).foreach {
        case Left(hash) =>
          if (assertFull)
            errors += s"All views in a full informee tree must be unblinded. Found $hash."

        case Right(view) =>
          if (assertFull && view.viewCommonData.unwrap.isLeft)
            errors += s"The view common data in a full informee tree must be unblinded. Found ${view.viewCommonData}."

          if (view.viewParticipantData.unwrap.isRight)
            errors += s"The view participant data in an informee tree must be blinded. Found ${view.viewParticipantData}."

          go(view.subviews)
      }

    go(rootViews.unblindedElements)

    val message = errors.result().mkString("\n")
    if (message.nonEmpty)
      throw InvalidInformeeTree(message)
  }

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
        .create(tree)
        .leftMap(e => ProtoDeserializationError.OtherError(s"Unable to create informee tree: $e"))
    } yield informeeTree
}

/** Wraps a [[GenTransactionTree]] that is also a full informee tree.
  *
  * @throws InformeeTree$.InvalidInformeeTree if `tree` is not a valid informee tree (i.e. the wrong nodes are blinded)
  */
case class FullInformeeTree(tree: GenTransactionTree)
    extends HasVersionedWrapper[VersionedMessage[FullInformeeTree]]
    with HasProtoV0[v0.FullInformeeTree]
    with PrettyPrinting {

  InformeeTree.checkGlobalMetadata(tree)

  InformeeTree.checkViews(tree.rootViews, assertFull = true)

  lazy val transactionId: TransactionId = TransactionId.fromRootHash(tree.rootHash)

  def domainId: DomainId = checked(tree.commonMetadata.tryUnwrap).domainId

  def mediatorId: MediatorId = checked(tree.commonMetadata.tryUnwrap).mediatorId

  /** Yields the informee tree unblinded for a defined set of parties.
    * If a view common data is already blinded, then it remains blinded even if one of the given parties is a stakeholder.
    */
  def informeeTreeUnblindedFor(parties: collection.Set[LfPartyId]): InformeeTree = {
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
    InformeeTree(rawResult)
  }

  lazy val toInformeeTree: InformeeTree = InformeeTree(tree)

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

  override def toProtoVersioned(version: ProtocolVersion): VersionedMessage[FullInformeeTree] =
    VersionedMessage(toProtoV0.toByteString, 0)

  override def toProtoV0: v0.FullInformeeTree =
    v0.FullInformeeTree(tree = Some(tree.toProtoV0))

  override def pretty: Pretty[FullInformeeTree] = prettyOfParam(_.tree)
}

object FullInformeeTree extends HasVersionedMessageWithContextCompanion[FullInformeeTree, HashOps] {
  override val name: String = "FullInformeeTree"

  val supportedProtoVersions: Map[Int, Parser] = Map(
    0 -> supportedProtoVersion(v0.FullInformeeTree)(fromProtoV0)
  )

  def create(tree: GenTransactionTree): Either[String, FullInformeeTree] =
    Either.catchOnly[InvalidInformeeTree](FullInformeeTree(tree)).leftMap(_.message)

  def fromProtoV0(
      hashOps: HashOps,
      protoInformeeTree: v0.FullInformeeTree,
  ): ParsingResult[FullInformeeTree] =
    for {
      protoTree <- ProtoConverter.required("tree", protoInformeeTree.tree)
      tree <- GenTransactionTree.fromProtoV0(hashOps, protoTree)
      fullInformeeTree <- FullInformeeTree
        .create(tree)
        .leftMap(e =>
          ProtoDeserializationError.OtherError(s"Unable to create full informee tree: $e")
        )
    } yield fullInformeeTree
}

/** Information about the submitters of the transaction
  */
case class SubmitterMetadata private (
    actAs: NonEmpty[Set[LfPartyId]],
    applicationId: ApplicationId,
    commandId: CommandId,
    submitterParticipant: ParticipantId,
    salt: Salt,
    submissionId: Option[LedgerSubmissionId],
    dedupPeriod: DeduplicationPeriod,
)(hashOps: HashOps, override val deserializedFrom: Option[ByteString])
    extends MerkleTreeLeaf[SubmitterMetadata](hashOps)
    with HasVersionedWrapper[VersionedMessage[SubmitterMetadata]]
    with MemoizedEvidence
    with NoCopy {

  override protected[this] def toByteStringUnmemoized(version: ProtocolVersion): ByteString =
    super[HasVersionedWrapper].toByteString(version)

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

  override protected def toProtoVersioned(
      version: ProtocolVersion
  ): VersionedMessage[SubmitterMetadata] =
    VersionedMessage(toProtoV0.toByteString, 0)

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
    extends HasMemoizedVersionedMessageWithContextCompanion[
      MerkleTree[SubmitterMetadata],
      HashOps,
    ] {
  override val name: String = "SubmitterMetadata"

  val supportedProtoVersions: Map[Int, Parser] = Map(
    0 -> supportedProtoVersionMemoized(v0.SubmitterMetadata)(fromProtoV0)
  )

  def fromSubmitterInfo(hashOps: HashOps)(
      submitterInfo: SubmitterInfo,
      submitterParticipant: ParticipantId,
      salt: Salt,
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
        )(hashOps, None)
    }
  }

  private def fromProtoV0(hashOps: HashOps, metaDataP: v0.SubmitterMetadata)(
      bytes: ByteString
  ): ParsingResult[MerkleTree[SubmitterMetadata]] = {
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
    )(hashOps, Some(bytes))
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
)(hashOps: HashOps, override val deserializedFrom: Option[ByteString])
    extends MerkleTreeLeaf[CommonMetadata](hashOps)
    with HasVersionedWrapper[VersionedMessage[CommonMetadata]]
    with MemoizedEvidence
    with NoCopy {

  override protected[this] def toByteStringUnmemoized(version: ProtocolVersion): ByteString =
    super[HasVersionedWrapper].toByteString(version)

  override val hashPurpose: HashPurpose = HashPurpose.CommonMetadata

  override def pretty: Pretty[CommonMetadata] = prettyOfClass(
    param("confirmation policy", _.confirmationPolicy),
    param("domain id", _.domainId),
    param("mediator id", _.mediatorId),
    param("uuid", _.uuid),
    param("salt", _.salt),
  )

  override protected def toProtoVersioned(
      version: ProtocolVersion
  ): VersionedMessage[CommonMetadata] =
    VersionedMessage(toProtoV0.toByteString, 0)

  protected def toProtoV0: v0.CommonMetadata = v0.CommonMetadata(
    confirmationPolicy = confirmationPolicy.toProtoPrimitive,
    domainId = domainId.toProtoPrimitive,
    salt = Some(salt.toProtoV0),
    uuid = ProtoConverter.UuidConverter.toProtoPrimitive(uuid),
    mediatorId = mediatorId.toProtoPrimitive,
  )
}

object CommonMetadata
    extends HasMemoizedVersionedMessageWithContextCompanion[
      MerkleTree[CommonMetadata],
      HashOps,
    ] {
  override val name: String = "CommonMetadata"

  val supportedProtoVersions: Map[Int, Parser] = Map(
    0 -> supportedProtoVersionMemoized(v0.CommonMetadata)(fromProtoV0)
  )

  private[this] def apply(
      confirmationPolicy: ConfirmationPolicy,
      domain: DomainId,
      mediatorId: MediatorId,
      salt: Salt,
      uuid: UUID,
  )(hashOps: HashOps, deserializedFrom: Option[ByteString]): CommonMetadata =
    throw new UnsupportedOperationException("Use the public apply method instead")

  def apply(
      hashOps: HashOps
  )(
      confirmationPolicy: ConfirmationPolicy,
      domain: DomainId,
      mediatorId: MediatorId,
      salt: Salt,
      uuid: UUID,
  ) =
    new CommonMetadata(confirmationPolicy, domain, mediatorId, salt, uuid)(hashOps, None)

  private def fromProtoV0(hashOps: HashOps, metaDataP: v0.CommonMetadata)(
      bytes: ByteString
  ): ParsingResult[MerkleTree[CommonMetadata]] =
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
    } yield new CommonMetadata(confirmationPolicy, DomainId(domainUid), mediatorId, salt, uuid)(
      hashOps,
      Some(bytes),
    )
}

/** Information concerning every '''participant''' involved in the underlying transaction.
  *
  * @param ledgerTime     The ledger time of the transaction
  * @param submissionTime The submission time of the transaction
  * @param workflowId     optional workflow id associated with the ledger api provided workflow instance
  */
case class ParticipantMetadata private (
    ledgerTime: CantonTimestamp,
    submissionTime: CantonTimestamp,
    workflowId: Option[WorkflowId],
    salt: Salt,
)(hashOps: HashOps, override val deserializedFrom: Option[ByteString])
    extends MerkleTreeLeaf[ParticipantMetadata](hashOps)
    with HasVersionedWrapper[VersionedMessage[ParticipantMetadata]]
    with MemoizedEvidence
    with NoCopy {

  override protected[this] def toByteStringUnmemoized(version: ProtocolVersion): ByteString =
    super[HasVersionedWrapper].toByteString(version)

  override val hashPurpose: HashPurpose = HashPurpose.ParticipantMetadata

  override def pretty: Pretty[ParticipantMetadata] = prettyOfClass(
    param("ledger time", _.ledgerTime),
    param("submission time", _.submissionTime),
    paramIfDefined("workflow id", _.workflowId),
    param("salt", _.salt),
  )

  override protected def toProtoVersioned(
      version: ProtocolVersion
  ): VersionedMessage[ParticipantMetadata] = VersionedMessage(toProtoV0.toByteString, 0)

  protected def toProtoV0: v0.ParticipantMetadata = v0.ParticipantMetadata(
    ledgerTime = Some(ledgerTime.toProtoPrimitive),
    submissionTime = Some(submissionTime.toProtoPrimitive),
    workflowId = workflowId.fold("")(_.toProtoPrimitive),
    salt = Some(salt.toProtoV0),
  )
}

object ParticipantMetadata
    extends HasMemoizedVersionedMessageWithContextCompanion[MerkleTree[
      ParticipantMetadata
    ], HashOps] {
  override val name: String = "ParticipantMetadata"

  val supportedProtoVersions: Map[Int, Parser] = Map(
    0 -> supportedProtoVersionMemoized(v0.ParticipantMetadata)(fromProtoV0)
  )

  private[this] def apply(
      ledgerTime: CantonTimestamp,
      submissionTime: CantonTimestamp,
      workflowId: Option[WorkflowId],
      salt: Salt,
  )(hashOps: HashOps, deserializedFrom: Option[ByteString]): ParticipantMetadata =
    throw new UnsupportedOperationException("Use the other apply method for external use")

  def apply(hashOps: HashOps)(
      ledgerTime: CantonTimestamp,
      submissionTime: CantonTimestamp,
      workflowId: Option[WorkflowId],
      salt: Salt,
  ): ParticipantMetadata =
    new ParticipantMetadata(ledgerTime, submissionTime, workflowId, salt)(hashOps, None)

  private def fromProtoV0(hashOps: HashOps, metadataP: v0.ParticipantMetadata)(
      bytes: ByteString
  ): ParsingResult[MerkleTree[ParticipantMetadata]] =
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
    } yield new ParticipantMetadata(let, submissionTime, workflowId, salt)(hashOps, Some(bytes))
}

/** Wraps a `GenTransactionTree` where exactly one view (not including subviews) is unblinded.
  * The `commonMetadata` and `participantMetadata` are also unblinded.
  * The `submitterMetadata` is unblinded if and only if the unblinded view is a root view.
  *
  * @throws LightTransactionViewTree$.InvalidLightTransactionViewTree if [[tree]] is not a light transaction view tree
  *                                    (i.e. the wrong set of nodes is blinded)
  */
case class LightTransactionViewTree(tree: GenTransactionTree)
    extends ViewTree
    with HasProtoV0[v0.LightTransactionViewTree]
    with HasVersionedWrapper[VersionedMessage[LightTransactionViewTree]]
    with PrettyPrinting {

  private def findTheView(views: Seq[MerkleTree[TransactionView]]): TransactionView = {
    val lightUnblindedViews = views.mapFilter { view =>
      for {
        v <- view.unwrap.toOption
        _cmd <- v.viewCommonData.unwrap.toOption
        _pmd <- v.viewParticipantData.unwrap.toOption
        _ <- if (v.subviews.forall(_.isBlinded)) Some(()) else None
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
  val view: TransactionView = findTheView(tree.rootViews.unblindedElements.flatMap(_.flatten))

  override val viewHash: ViewHash = ViewHash.fromRootHash(view.rootHash)

  val subviewHashes: Seq[ViewHash] = view.subviews.map(sv => ViewHash.fromRootHash(sv.rootHash))

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

  override def toProtoVersioned(
      version: ProtocolVersion
  ): VersionedMessage[LightTransactionViewTree] = VersionedMessage(toProtoV0.toByteString, 0)

  override def toProtoV0: v0.LightTransactionViewTree =
    v0.LightTransactionViewTree(tree = Some(tree.toProtoV0))

  override lazy val toBeSigned: Option[RootHash] =
    tree.rootViews.unblindedElements
      .find(_.rootHash == view.rootHash)
      .map(_ => transactionId.toRootHash)

  override def rootHash: RootHash = tree.rootHash

  override lazy val pretty: Pretty[LightTransactionViewTree] = prettyOfClass(unnamedParam(_.tree))
}

object LightTransactionViewTree
    extends HasVersionedMessageWithContextCompanion[LightTransactionViewTree, HashOps] {
  override val name: String = "LightTransactionViewTree"

  val supportedProtoVersions: Map[Int, Parser] = Map(
    0 -> supportedProtoVersion(v0.LightTransactionViewTree)(fromProtoV0)
  )

  case class InvalidLightTransactionViewTree(message: String) extends RuntimeException(message)
  case class InvalidLightTransactionViewTreeSequence(message: String)
      extends RuntimeException(message)

  /** @throws InvalidLightTransactionViewTree if the tree is not a legal lightweight transaction view tree
    */
  def apply(tree: GenTransactionTree): LightTransactionViewTree =
    new LightTransactionViewTree(tree)

  def create(tree: GenTransactionTree): Either[String, LightTransactionViewTree] = {
    Either
      .catchOnly[InvalidLightTransactionViewTree](new LightTransactionViewTree(tree))
      .leftMap(_.message)
  }

  def fromProtoV0(
      hashOps: HashOps,
      protoT: v0.LightTransactionViewTree,
  ): ParsingResult[LightTransactionViewTree] =
    for {
      protoTree <- ProtoConverter.required("tree", protoT.tree)
      tree <- GenTransactionTree.fromProtoV0(hashOps, protoTree)
      result <- LightTransactionViewTree
        .create(tree)
        .leftMap(e =>
          ProtoDeserializationError.OtherError(s"Unable to create transaction tree: $e")
        )
    } yield result

  /** Converts the prefix of a sequence of lightweight transaction view trees into a full tree, returning the unused suffix.
    *
    * The unused suffix normally describes other full transaction view trees, and the function may be called again on the
    * suffix.
    * Errors on an empty sequence, or a sequence whose prefix doesn't describe a full transaction view tree.
    */
  private def toFullViewTree(trees: NonEmpty[Seq[LightTransactionViewTree]]): Either[
    InvalidLightTransactionViewTreeSequence,
    (TransactionViewTree, Seq[LightTransactionViewTree]),
  ] = {

    def toFullView(trees: NonEmpty[Seq[LightTransactionViewTree]]): Either[
      InvalidLightTransactionViewTreeSequence,
      (TransactionView, Seq[LightTransactionViewTree]),
    ] = {
      val headView = trees.head1.view
      if (headView.subviews.isEmpty)
        Right(headView -> trees.tail1)
      else {
        for {
          subViewsAndRemaining <- ((1 to headView.subviews.length): Seq[Int])
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
          // Check that the ordering of the reconstructed transaction view trees matches the expected one
          _ <- Either.cond(
            subViews.map(_.rootHash) == headView.subviews.map(_.rootHash),
            (),
            InvalidLightTransactionViewTreeSequence(
              s"Mismatch in expected hashes of subtrees and what was found"
            ),
          )
          newView = headView.copy(subviews = subViews)
        } yield (newView, remaining)
      }
    }
    val t = trees.head1
    toFullView(trees).flatMap { case (tv, remaining) =>
      val wrappedEnrichedTree = t.tree.topLevelViewMap(_.replace(tv.viewHash, tv))
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
  def toAllFullViewTrees(
      trees: NonEmpty[Seq[LightTransactionViewTree]]
  ): Either[InvalidLightTransactionViewTreeSequence, NonEmpty[Seq[TransactionViewTree]]] =
    sequenceConsumer(trees, true)

  /** Returns the top-level full transaction view trees described by a sequence of lightweight ones */
  def toToplevelFullViewTrees(
      trees: NonEmpty[Seq[LightTransactionViewTree]]
  ): Either[InvalidLightTransactionViewTreeSequence, NonEmpty[Seq[TransactionViewTree]]] =
    sequenceConsumer(trees, false)

  // Extracts the common logic behind extracting all full trees and just top-level ones
  private def sequenceConsumer(
      trees: NonEmpty[Seq[LightTransactionViewTree]],
      repeats: Boolean,
  ): Either[InvalidLightTransactionViewTreeSequence, NonEmpty[Seq[TransactionViewTree]]] = {
    val resBuilder = List.newBuilder[TransactionViewTree]
    @tailrec
    def go(
        ts: NonEmpty[Seq[LightTransactionViewTree]]
    ): Option[InvalidLightTransactionViewTreeSequence] = {
      toFullViewTree(ts) match {
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
  def fromTransactionViewTree(tvt: TransactionViewTree): LightTransactionViewTree = {
    val withBlindedSubviews = tvt.view.copy(subviews = tvt.view.subviews.map(_.blindFully))
    val genTransactionTree = tvt.tree.topLevelViewMap(_.replace(tvt.viewHash, withBlindedSubviews))
    LightTransactionViewTree(genTransactionTree)
  }

}

/** Encodes the hierarchy of the witnesses of a view.
  *
  * By convention, the order is: the view's informees are at the head of the list, then the parent's views informees,
  * then the grandparent's, etc.
  */
case class Witnesses(unwrap: Seq[Set[Informee]]) {
  import Witnesses._

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
