// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import cats.syntax.either.*
import cats.syntax.functor.*
import cats.syntax.traverse.*
import com.daml.lf.transaction.ContractStateMachine.{ActiveLedgerState, KeyMapping}
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.data.ActionDescription.{
  CreateActionDescription,
  ExerciseActionDescription,
  FetchActionDescription,
  LookupByKeyActionDescription,
}
import com.digitalasset.canton.data.TransactionView.InvalidView
import com.digitalasset.canton.data.ViewParticipantData.{InvalidViewParticipantData, RootAction}
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.logging.{HasLoggerName, NamedLoggingContext}
import com.digitalasset.canton.protocol.ContractIdSyntax.*
import com.digitalasset.canton.protocol.{v0, v1, v2, *}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.serialization.{
  ProtoConverter,
  ProtocolVersionedMemoizedEvidence,
  SerializationCheckFailed,
}
import com.digitalasset.canton.util.{ErrorUtil, MapsUtil, NamedLoggingLazyVal}
import com.digitalasset.canton.version.*
import com.digitalasset.canton.{
  LfCommand,
  LfCreateCommand,
  LfExerciseByKeyCommand,
  LfExerciseCommand,
  LfFetchByKeyCommand,
  LfFetchCommand,
  LfLookupByKeyCommand,
  LfPartyId,
  ProtoDeserializationError,
  checked,
}
import com.google.common.annotations.VisibleForTesting
import com.google.protobuf.ByteString
import monocle.Lens
import monocle.macros.GenLens

/** Encapsulates a subaction of the underlying transaction.
  *
  * @param subviews the top-most subviews of this view
  * @throws TransactionView$.InvalidView if the `viewCommonData` is unblinded and equals the `viewCommonData` of a direct subview
  */
case class TransactionView private (
    viewCommonData: MerkleTree[ViewCommonData],
    viewParticipantData: MerkleTree[ViewParticipantData],
    subviews: TransactionSubviews,
)(
    hashOps: HashOps,
    override val representativeProtocolVersion: RepresentativeProtocolVersion[TransactionView],
) extends MerkleTreeInnerNode[TransactionView](hashOps)
    with HasProtocolVersionedWrapper[TransactionView]
    with HasLoggerName {

  override val companionObj: HasProtocolVersionedWrapperCompanion[TransactionView] = TransactionView

  if (viewCommonData.unwrap.isRight) {
    subviews.unblindedElementsWithIndex
      .find { case (view, _path) => view.viewCommonData == viewCommonData }
      .foreach { case (_view, path) =>
        throw InvalidView(
          s"The subview with index $path has an equal viewCommonData."
        )
      }
  }

  override def subtrees: Seq[MerkleTree[_]] =
    Seq[MerkleTree[_]](viewCommonData, viewParticipantData) ++ subviews.trees

  def tryUnblindViewParticipantData(
      fieldName: String
  )(implicit loggingContext: NamedLoggingContext): ViewParticipantData =
    viewParticipantData.unwrap.getOrElse(
      ErrorUtil.internalError(
        new IllegalStateException(
          s"$fieldName of view $viewHash can be computed only if the view participant data is unblinded"
        )
      )
    )

  private def tryUnblindSubview(subview: MerkleTree[TransactionView], fieldName: String)(implicit
      loggingContext: NamedLoggingContext
  ): TransactionView =
    subview.unwrap.getOrElse(
      ErrorUtil.internalError(
        new IllegalStateException(
          s"$fieldName of view $viewHash can be computed only if all subviews are unblinded, but ${subview.rootHash} is blinded"
        )
      )
    )

  override private[data] def withBlindedSubtrees(
      blindingCommandPerNode: PartialFunction[RootHash, MerkleTree.BlindingCommand]
  ): MerkleTree[TransactionView] =
    TransactionView.tryCreate(
      viewCommonData.doBlind(blindingCommandPerNode), // O(1)
      viewParticipantData.doBlind(blindingCommandPerNode), // O(1)
      subviews.doBlind(blindingCommandPerNode), // O(#subviews)
      representativeProtocolVersion,
    )(hashOps)

  val viewHash: ViewHash = ViewHash.fromRootHash(rootHash)

  /** Traverses all unblinded subviews `v1, v2, v3, ...` in pre-order and yields
    * `f(...f(f(z, v1), v2)..., vn)`
    */
  def foldLeft[A](z: A)(f: (A, TransactionView) => A): A =
    subviews.unblindedElements
      .to(LazyList)
      .foldLeft(f(z, this))((acc, subView) => subView.foldLeft(acc)(f))

  /** Yields all (direct and indirect) subviews of this view in pre-order.
    * The first element is this view.
    */
  def flatten: Seq[TransactionView] =
    foldLeft(Seq.newBuilder[TransactionView])((acc, v) => acc += v).result()

  override def pretty: Pretty[TransactionView] = prettyOfClass(
    param("root hash", _.rootHash),
    param("view common data", _.viewCommonData),
    param("view participant data", _.viewParticipantData),
    param("subviews", _.subviews),
  )

  @VisibleForTesting
  private[data] def copy(
      viewCommonData: MerkleTree[ViewCommonData] = this.viewCommonData,
      viewParticipantData: MerkleTree[ViewParticipantData] = this.viewParticipantData,
      subviews: TransactionSubviews = this.subviews,
  ) =
    new TransactionView(viewCommonData, viewParticipantData, subviews)(
      hashOps,
      representativeProtocolVersion,
    )

  /** If the view with the given hash appears either as this view or one of its unblinded descendants,
    * replace it by the given view.
    * TODO(M40): not stack safe unless we have limits on the depths of views.
    */
  def replace(h: ViewHash, v: TransactionView): TransactionView =
    if (viewHash == h) v
    else this.copy(subviews = subviews.mapUnblinded(_.replace(h, v)))

  protected def toProtoV0: v0.ViewNode = v0.ViewNode(
    viewCommonData = Some(MerkleTree.toBlindableNodeV0(viewCommonData)),
    viewParticipantData = Some(MerkleTree.toBlindableNodeV0(viewParticipantData)),
    subviews = subviews.toProtoV0,
  )

  protected def toProtoV1: v1.ViewNode = v1.ViewNode(
    viewCommonData = Some(MerkleTree.toBlindableNodeV1(viewCommonData)),
    viewParticipantData = Some(MerkleTree.toBlindableNodeV1(viewParticipantData)),
    subviews = Some(subviews.toProtoV1),
  )

  /** The global key inputs that the [[com.daml.lf.transaction.ContractStateMachine]] computes
    * while interpreting the root action of the view, enriched with the maintainers of the key and the
    * [[com.digitalasset.canton.protocol.LfTransactionVersion]] to be used for serializing the key.
    *
    * @throws java.lang.UnsupportedOperationException
    *   if the protocol version is below [[com.digitalasset.canton.version.ProtocolVersion.v3]]
    * @throws java.lang.IllegalStateException if the [[ViewParticipantData]] of this view or any subview is blinded
    */
  def globalKeyInputs(implicit
      loggingContext: NamedLoggingContext
  ): Map[LfGlobalKey, KeyResolutionWithMaintainers] =
    _globalKeyInputs.get

  private[this] val _globalKeyInputs
      : NamedLoggingLazyVal[Map[LfGlobalKey, KeyResolutionWithMaintainers]] =
    NamedLoggingLazyVal[Map[LfGlobalKey, KeyResolutionWithMaintainers]] { implicit loggingContext =>
      val viewParticipantData = tryUnblindViewParticipantData("Global key inputs")

      if (viewParticipantData.isEquivalentTo(ProtocolVersion.v2)) {
        ErrorUtil.internalError(
          new UnsupportedOperationException(
            s"Global key inputs can be computed only for protocol version ${ProtocolVersion.v3} and higher"
          )
        )
      }

      subviews.assertAllUnblinded(hash =>
        s"Global key inputs of view $viewHash can be computed only if all subviews are unblinded, but ${hash} is blinded"
      )

      subviews.unblindedElements.foldLeft(viewParticipantData.resolvedKeysWithMaintainers) {
        (acc, subview) =>
          val subviewGki = subview.globalKeyInputs
          MapsUtil.mergeWith(acc, subviewGki) { (accRes, _subviewRes) => accRes }
      }
    }

  /** The input contracts of the view (including subviews).
    *
    * @throws java.lang.IllegalStateException if the [[ViewParticipantData]] of this view or any subview is blinded
    */
  def inputContracts(implicit
      loggingContext: NamedLoggingContext
  ): Map[LfContractId, InputContract] = _inputsAndCreated.get._1

  /** The contracts appearing in create nodes in the view (including subviews).
    *
    * @throws java.lang.IllegalStateException if the [[ViewParticipantData]] of this view or any subview is blinded
    */
  def createdContracts(implicit
      loggingContext: NamedLoggingContext
  ): Map[LfContractId, CreatedContractInView] = _inputsAndCreated.get._2

  private[this] val _inputsAndCreated: NamedLoggingLazyVal[
    (Map[LfContractId, InputContract], Map[LfContractId, CreatedContractInView])
  ] = NamedLoggingLazyVal[
    (Map[LfContractId, InputContract], Map[LfContractId, CreatedContractInView])
  ] { implicit loggingContext =>
    val vpd = viewParticipantData.unwrap.getOrElse(
      ErrorUtil.internalError(
        new IllegalStateException(
          s"Inputs and created contracts of view $viewHash can be computed only if the view participant data is unblinded"
        )
      )
    )
    val currentRollbackScope = vpd.rollbackContext.rollbackScope
    subviews.assertAllUnblinded(hash =>
      s"Inputs and created contracts of view $viewHash can be computed only if all subviews are unblinded, but ${hash} is blinded"
    )
    val subviewInputsAndCreated = subviews.unblindedElements.map { subview =>
      val subviewVpd =
        subview.tryUnblindViewParticipantData("Inputs and created contracts")
      val created = subview.createdContracts
      val inputs = subview.inputContracts
      val subviewRollbackScope = subviewVpd.rollbackContext.rollbackScope
      // If the subview sits under a Rollback node in the view's core,
      // then the created contracts of the subview are all rolled back,
      // and all consuming inputs become non-consuming inputs.
      if (subviewRollbackScope != currentRollbackScope) {
        (
          inputs.fmap(_.copy(consumed = false)),
          created.fmap(_.copy(rolledBack = true)),
        )
      } else (inputs, created)
    }

    val createdCore = vpd.createdCore.map { contract =>
      contract.contract.contractId -> CreatedContractInView.fromCreatedContract(contract)
    }.toMap
    subviewInputsAndCreated.foldLeft((vpd.coreInputs, createdCore)) {
      case ((accInputs, accCreated), (subviewInputs, subviewCreated)) =>
        val subviewCreatedUpdated = subviewCreated.fmap { contract =>
          if (vpd.createdInSubviewArchivedInCore.contains(contract.contract.contractId))
            contract.copy(consumedInView = true)
          else contract
        }
        val accCreatedUpdated = accCreated.fmap { contract =>
          if (subviewInputs.get(contract.contract.contractId).exists(_.consumed))
            contract.copy(consumedInView = true)
          else contract
        }
        val nextCreated = MapsUtil.mergeWith(accCreatedUpdated, subviewCreatedUpdated) {
          (fromAcc, _) =>
            // By the contract ID allocation scheme, the contract IDs in the subviews are pairwise distinct
            // and distinct from `createdCore`
            // TODO(M40) Check this invariant somewhere
            ErrorUtil.internalError(
              new IllegalStateException(
                s"Contract ${fromAcc.contract.contractId} is created multiple times in view $viewHash"
              )
            )
        }

        val subviewNontransientInputs = subviewInputs.filter { case (cid, _) =>
          !accCreated.contains(cid)
        }
        val nextInputs = MapsUtil.mergeWith(accInputs, subviewNontransientInputs) {
          (fromAcc, fromSubview) =>
            // TODO(M40) Check that `fromAcc.contract == fromSubview.contract`
            // TODO(M40) Check that the contract is consumed either in `fromAcc` or in `fromSubview`, but not both
            fromAcc.copy(consumed = fromAcc.consumed || fromSubview.consumed)
        }
        (nextInputs, nextCreated)
    }
  }

  /** The [[com.daml.lf.transaction.ContractStateMachine.ActiveLedgerState]]
    * the [[com.daml.lf.transaction.ContractStateMachine]] reaches after interpreting the root action of the view.
    *
    * Must only be used in mode [[com.daml.lf.transaction.ContractKeyUniquenessMode.Strict]]
    *
    * @throws java.lang.UnsupportedOperationException
    *   if the protocol version is below [[com.digitalasset.canton.version.ProtocolVersion.v3]]
    * @throws java.lang.IllegalStateException if the [[ViewParticipantData]] of this view or any subview is blinded.
    */
  def activeLedgerState(implicit
      loggingContext: NamedLoggingContext
  ): ActiveLedgerState[Unit] =
    _activeLedgerStateAndUpdatedKeys.get._1

  /** The keys that this view updates (including reassigning the key), along with the maintainers of the key.
    *
    * Must only be used in mode [[com.daml.lf.transaction.ContractKeyUniquenessMode.Strict]]
    *
    * @throws java.lang.UnsupportedOperationException
    *   if the protocol version is below [[com.digitalasset.canton.version.ProtocolVersion.v3]]
    * @throws java.lang.IllegalStateException if the [[ViewParticipantData]] of this view or any subview is blinded.
    */
  def updatedKeys(implicit loggingContext: NamedLoggingContext): Map[LfGlobalKey, Set[LfPartyId]] =
    _activeLedgerStateAndUpdatedKeys.get._2

  /** The keys that this view updates (including reassigning the key), along with the assignment of that key at the end of the transaction.
    *
    * Must only be used in mode [[com.daml.lf.transaction.ContractKeyUniquenessMode.Strict]]
    *
    * @throws java.lang.UnsupportedOperationException
    *   if the protocol version is below [[com.digitalasset.canton.version.ProtocolVersion.v3]]
    * @throws java.lang.IllegalStateException if the [[ViewParticipantData]] of this view or any subview is blinded.
    */
  def updatedKeyValues(implicit
      loggingContext: NamedLoggingContext
  ): Map[LfGlobalKey, KeyMapping] = {
    val localActiveKeys = activeLedgerState.localActiveKeys
    def resolveKey(key: LfGlobalKey): KeyMapping =
      localActiveKeys.get(key) match {
        case None =>
          globalKeyInputs.get(key).map(_.resolution).flatten.filterNot(consumed.contains(_))
        case Some(mapping) => mapping
      }
    (localActiveKeys.keys ++ globalKeyInputs.keys).map(k => k -> resolveKey(k)).toMap
  }

  private[this] val _activeLedgerStateAndUpdatedKeys
      : NamedLoggingLazyVal[(ActiveLedgerState[Unit], Map[LfGlobalKey, Set[LfPartyId]])] =
    NamedLoggingLazyVal[(ActiveLedgerState[Unit], Map[LfGlobalKey, Set[LfPartyId]])] {
      implicit loggingContext =>
        val updatedKeysB = Map.newBuilder[LfGlobalKey, Set[LfPartyId]]
        @SuppressWarnings(Array("org.wartremover.warts.Var"))
        var localKeys: Map[LfGlobalKey, LfContractId] = Map.empty

        inputContracts.foreach { case (cid, inputContract) =>
          // Consuming exercises under a rollback node are rewritten to non-consuming exercises in the view inputs.
          // So here we are looking only at key usages that are outside of rollback nodes (inside the view).
          if (inputContract.consumed) {
            inputContract.contract.metadata.maybeKeyWithMaintainers.foreach { kWithM =>
              val key = kWithM.globalKey
              updatedKeysB += (key -> kWithM.maintainers)
            }
          }
        }
        createdContracts.foreach { case (cid, createdContract) =>
          if (!createdContract.rolledBack) {
            createdContract.contract.metadata.maybeKeyWithMaintainers.foreach { kWithM =>
              val key = kWithM.globalKey
              updatedKeysB += (key -> kWithM.maintainers)
              if (!createdContract.consumedInView) {
                // If we have an active contract, we use that mapping.
                localKeys += key -> cid
              } else {
                if (!localKeys.contains(key)) {
                  // If all contracts are inactive, we arbitrarily use the first in createdContracts
                  // (createdContracts is not ordered)
                  localKeys += key -> cid
                }
              }
            }
          }
        }

        val locallyCreatedThisTimeline = createdContracts.collect {
          case (contractId, createdContract) if !createdContract.rolledBack => contractId
        }.toSet

        ActiveLedgerState(
          locallyCreatedThisTimeline = locallyCreatedThisTimeline,
          consumedBy = consumed,
          localKeys = localKeys,
        ) ->
          updatedKeysB.result()
    }

  def consumed(implicit loggingContext: NamedLoggingContext): Map[LfContractId, Unit] = {
    // In strict mode, every node involving a key updates the active ledger state
    // unless it is under a rollback node.
    // So it suffices to look at the created and input contracts
    // Contract consumption under a rollback is ignored.

    val consumedInputs = inputContracts.collect {
      // No need to check for contract.rolledBack because consumption under a rollback does not set the consumed flag
      case (cid, contract) if contract.consumed => cid -> ()
    }
    val consumedCreates = createdContracts.collect {
      // If the creation is rolled back, then so are all archivals
      // because a rolled-back create can only be used in the same or deeper rollback scopes,
      // as ensured by `WellformedTransaction.checkCreatedContracts`.
      case (cid, contract) if !contract.rolledBack && contract.consumedInView => cid -> ()
    }
    consumedInputs ++ consumedCreates
  }
}

object TransactionView extends HasProtocolVersionedWithContextCompanion[TransactionView, HashOps] {
  override protected def name: String = "TransactionView"
  override def supportedProtoVersions: SupportedProtoVersions =
    SupportedProtoVersions(
      ProtoVersion(0) -> LegacyProtoConverter(
        ProtocolVersion.v2,
        supportedProtoVersion(v0.ViewNode)(fromProtoV0),
        _.toProtoV0.toByteString,
      ),
      ProtoVersion(1) -> VersionedProtoConverter(
        ProtocolVersion.v4,
        supportedProtoVersion(v1.ViewNode)(fromProtoV1),
        _.toProtoV1.toByteString,
      ),
    )

  private def tryCreate(
      viewCommonData: MerkleTree[ViewCommonData],
      viewParticipantData: MerkleTree[ViewParticipantData],
      subviews: TransactionSubviews,
      representativeProtocolVersion: RepresentativeProtocolVersion[TransactionView],
  )(hashOps: HashOps): TransactionView =
    new TransactionView(viewCommonData, viewParticipantData, subviews)(
      hashOps,
      representativeProtocolVersion,
    )

  /** Creates a view.
    *
    * @throws InvalidView if the `viewCommonData` is unblinded and equals the `viewCommonData` of a direct subview
    */
  def tryCreate(hashOps: HashOps)(
      viewCommonData: MerkleTree[ViewCommonData],
      viewParticipantData: MerkleTree[ViewParticipantData],
      subviews: TransactionSubviews,
      protocolVersion: ProtocolVersion,
  ): TransactionView =
    tryCreate(
      viewCommonData,
      viewParticipantData,
      subviews,
      protocolVersionRepresentativeFor(protocolVersion),
    )(hashOps)

  private def createFromRepresentativePV(hashOps: HashOps)(
      viewCommonData: MerkleTree[ViewCommonData],
      viewParticipantData: MerkleTree[ViewParticipantData],
      subviews: TransactionSubviews,
      representativeProtocolVersion: RepresentativeProtocolVersion[TransactionView],
  ): Either[String, TransactionView] =
    Either
      .catchOnly[InvalidView](
        TransactionView.tryCreate(
          viewCommonData,
          viewParticipantData,
          subviews,
          representativeProtocolVersion,
        )(hashOps)
      )
      .leftMap(_.message)

  /** Creates a view.
    *
    * Yields `Left(...)` if the `viewCommonData` is unblinded and equals the `viewCommonData` of a direct subview
    */
  def create(hashOps: HashOps)(
      viewCommonData: MerkleTree[ViewCommonData],
      viewParticipantData: MerkleTree[ViewParticipantData],
      subviews: TransactionSubviews,
      protocolVersion: ProtocolVersion,
  ): Either[String, TransactionView] =
    Either
      .catchOnly[InvalidView](
        TransactionView.tryCreate(hashOps)(
          viewCommonData,
          viewParticipantData,
          subviews,
          protocolVersion,
        )
      )
      .leftMap(_.message)

  /** DO NOT USE IN PRODUCTION, as it does not necessarily check object invariants. */
  @VisibleForTesting
  val viewCommonDataUnsafe: Lens[TransactionView, MerkleTree[ViewCommonData]] =
    GenLens[TransactionView](_.viewCommonData)

  /** DO NOT USE IN PRODUCTION, as it does not necessarily check object invariants. */
  @VisibleForTesting
  val viewParticipantDataUnsafe: Lens[TransactionView, MerkleTree[ViewParticipantData]] =
    GenLens[TransactionView](_.viewParticipantData)

  /** DO NOT USE IN PRODUCTION, as it does not necessarily check object invariants. */
  @VisibleForTesting
  val subviewsUnsafe: Lens[TransactionView, TransactionSubviews] =
    GenLens[TransactionView](_.subviews)

  private def fromProtoV0(
      hashOps: HashOps,
      protoView: v0.ViewNode,
  ): ParsingResult[TransactionView] = {
    for {
      commonData <- MerkleTree.fromProtoOptionV0(
        protoView.viewCommonData,
        ViewCommonData.fromByteString(hashOps),
      )
      participantData <- MerkleTree.fromProtoOptionV0(
        protoView.viewParticipantData,
        ViewParticipantData.fromByteString(hashOps),
      )
      subViews <- TransactionSubviews.fromProtoV0(hashOps, protoView.subviews)
      view <- createFromRepresentativePV(hashOps)(
        commonData,
        participantData,
        subViews,
        protocolVersionRepresentativeFor(ProtoVersion(0)),
      ).leftMap(e =>
        ProtoDeserializationError.OtherError(s"Unable to create transaction views: $e")
      )
    } yield view
  }

  private def fromProtoV1(
      hashOps: HashOps,
      protoView: v1.ViewNode,
  ): ParsingResult[TransactionView] = {
    for {
      commonData <- MerkleTree.fromProtoOptionV1(
        protoView.viewCommonData,
        ViewCommonData.fromByteString(hashOps),
      )
      participantData <- MerkleTree.fromProtoOptionV1(
        protoView.viewParticipantData,
        ViewParticipantData.fromByteString(hashOps),
      )
      subViews <- TransactionSubviews.fromProtoV1(hashOps, protoView.subviews)
      view <- createFromRepresentativePV(hashOps)(
        commonData,
        participantData,
        subViews,
        protocolVersionRepresentativeFor(ProtoVersion(1)),
      ).leftMap(e =>
        ProtoDeserializationError.OtherError(s"Unable to create transaction views: $e")
      )
    } yield view
  }

  /** Indicates an attempt to create an invalid view. */
  case class InvalidView(message: String) extends RuntimeException(message)

  private sealed trait AffectedKey extends Product with Serializable {
    def contractIdO: Option[LfContractId]
  }
}

/** Tags transaction views where all the view metadata are visible (such as in the views sent to participants).
  *
  * Note that the subviews and their metadata are not guaranteed to be visible.
  */
case class ParticipantTransactionView private (view: TransactionView) {
  def unwrap: TransactionView = view
  def viewCommonData: ViewCommonData = view.viewCommonData.tryUnwrap
  def viewParticipantData: ViewParticipantData = view.viewParticipantData.tryUnwrap
}

object ParticipantTransactionView {
  def create(view: TransactionView): Either[String, ParticipantTransactionView] = {
    val validated = view.viewCommonData.unwrap
      .leftMap(rh => s"Common data blinded (hash $rh)")
      .toValidatedNec
      .product(
        view.viewParticipantData.unwrap
          .leftMap(rh => s"Participant data blinded (hash $rh)")
          .toValidatedNec
      )
    validated
      .map(_ => new ParticipantTransactionView(view))
      .toEither
      .leftMap(_.toString)
  }
}

/** Information concerning every '''member''' involved in processing the underlying view.
  *
  * @param threshold If the sum of the weights of the parties approving the view attains the threshold,
  *                  the view is considered approved.
  */
// This class is a reference example of serialization best practices, demonstrating:
// - memoized serialization, which is required if we need to compute a signature or cryptographic hash of a class
// - use of an UntypedVersionedMessage wrapper when serializing to an anonymous binary format
// Please consult the team if you intend to change the design of serialization.
//
// The constructor and `fromProto...` methods are private to ensure that clients cannot create instances with an incorrect `deserializedFrom` field.
//
// Optional parameters are strongly discouraged, as each parameter needs to be consciously set in a production context.
case class ViewCommonData private (informees: Set[Informee], threshold: NonNegativeInt, salt: Salt)(
    hashOps: HashOps,
    val representativeProtocolVersion: RepresentativeProtocolVersion[ViewCommonData],
    override val deserializedFrom: Option[ByteString],
) extends MerkleTreeLeaf[ViewCommonData](hashOps)
    // The class needs to implement ProtocolVersionedMemoizedEvidence, because we want that serialize always yields the same ByteString.
    // This is to ensure that different participants compute the same hash after receiving a ViewCommonData over the network.
    // (Recall that serialization is in general not guaranteed to be deterministic.)
    with ProtocolVersionedMemoizedEvidence
    // The class implements `HasProtocolVersionedWrapper` because we serialize it to an anonymous binary format and need to encode
    // the version of the serialized Protobuf message
    with HasProtocolVersionedWrapper[ViewCommonData] {

  // The toProto... methods are deliberately protected, as they could otherwise be abused to bypass memoization.
  //
  // If another serializable class contains a ViewCommonData, it has to include it as a ByteString
  // (and not as "message ViewCommonData") in its ProtoBuf representation.

  override def companionObj: ViewCommonData.type = ViewCommonData

  // We use named parameters, because then the code remains correct even when the ProtoBuf code generator
  // changes the order of parameters.
  protected def toProtoV0: v0.ViewCommonData =
    v0.ViewCommonData(
      informees = informees.map(_.toProtoV0).toSeq,
      threshold = threshold.unwrap,
      salt = Some(salt.toProtoV0),
    )

  // When serializing the class to an anonymous binary format, we serialize it to an UntypedVersionedMessage version of the
  // corresponding Protobuf message
  override protected[this] def toByteStringUnmemoized: ByteString = toByteString

  override val hashPurpose: HashPurpose = HashPurpose.ViewCommonData

  override def pretty: Pretty[ViewCommonData] = prettyOfClass(
    param("informees", _.informees),
    param("threshold", _.threshold),
    param("salt", _.salt),
  )

  @VisibleForTesting
  def copy(
      informees: Set[Informee] = this.informees,
      threshold: NonNegativeInt = this.threshold,
      salt: Salt = this.salt,
  ): ViewCommonData =
    ViewCommonData(informees, threshold, salt)(hashOps, representativeProtocolVersion, None)
}

object ViewCommonData
    extends HasMemoizedProtocolVersionedWithContextCompanion[ViewCommonData, HashOps] {
  override val name: String = "ViewCommonData"

  val supportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(0) -> VersionedProtoConverter(
      ProtocolVersion.v2,
      supportedProtoVersionMemoized(v0.ViewCommonData)(fromProtoV0),
      _.toProtoV0.toByteString,
    )
  )

  /** Creates a fresh [[ViewCommonData]]. */
  // The "create" method has the following advantages over the auto-generated "apply" method:
  // - The parameter lists have been flipped to facilitate curried usages.
  // - The deserializedFrom field cannot be set; so it cannot be set incorrectly.
  //
  // The method is called "create" instead of "apply"
  // to not confuse the Idea compiler by overloading "apply".
  // (This is not a problem with this particular class, but it has been a problem with other classes.)
  def create(hashOps: HashOps)(
      informees: Set[Informee],
      threshold: NonNegativeInt,
      salt: Salt,
      protocolVersion: ProtocolVersion,
  ): ViewCommonData =
    // The deserializedFrom field is set to "None" as this is for creating "fresh" instances.
    new ViewCommonData(informees, threshold, salt)(
      hashOps,
      protocolVersionRepresentativeFor(protocolVersion),
      None,
    )

  private def fromProtoV0(
      hashOps: HashOps,
      viewCommonDataP: v0.ViewCommonData,
  )(bytes: ByteString): ParsingResult[ViewCommonData] =
    for {
      informees <- viewCommonDataP.informees.traverse(Informee.fromProtoV0)

      salt <- ProtoConverter
        .parseRequired(Salt.fromProtoV0, "salt", viewCommonDataP.salt)
        .leftMap(_.inField("salt"))

      threshold <- NonNegativeInt.create(viewCommonDataP.threshold).leftMap(_.inField("threshold"))
    } yield new ViewCommonData(informees.toSet, threshold, salt)(
      hashOps,
      protocolVersionRepresentativeFor(ProtoVersion(0)),
      Some(bytes),
    )
}

/** Information concerning every '''participant''' involved in processing the underlying view.
  *
  * @param coreInputs  [[LfContractId]] used by the core of the view and not assigned by a Create node in the view or its subviews,
  *                    independently of whether the creation is rolled back.
  *                    Every contract id is mapped to its contract instances and their meta-information.
  *                    Contracts are marked as being [[InputContract.consumed]] iff
  *                    they are consumed in the core of the view.
  * @param createdCore associates contract ids of Create nodes in the core of the view to the corresponding contract
  *                instance. The elements are ordered in execution order.
  * @param createdInSubviewArchivedInCore
  *   The contracts that are created in subviews and archived in the core.
  *   The archival has the same rollback scope as the view.
  *   For [[com.digitalasset.canton.protocol.WellFormedTransaction]]s, the creation therefore is not rolled
  *   back either as the archival can only refer to non-rolled back creates.
  * @param resolvedKeys
  * Specifies how to resolve [[com.daml.lf.engine.ResultNeedKey]] requests from DAMLe (resulting from e.g., fetchByKey,
  * lookupByKey) when interpreting the view. The resolved contract IDs must be in the [[coreInputs]].
  * * Up to protocol version [[com.digitalasset.canton.version.ProtocolVersion.v2]]:
  * [[com.digitalasset.canton.data.FreeKey]] is used only for lookup-by-key nodes.
  * * From protocol version [[com.digitalasset.canton.version.ProtocolVersion.v3]] on:
  * Stores only the resolution difference between this view's global key inputs
  * [[com.digitalasset.canton.data.TransactionView.globalKeyInputs]]
  * and the aggregated global key inputs from the subviews
  * (see [[com.digitalasset.canton.data.TransactionView.globalKeyInputs]] for the aggregation algorithm).
  * In [[com.daml.lf.transaction.ContractKeyUniquenessMode.Strict]],
  * the [[com.digitalasset.canton.data.FreeKey]] resolutions must be checked during conflict detection.
  * @param actionDescription The description of the root action of the view
  * @param rollbackContext The rollback context of the root action of the view.
  * @throws ViewParticipantData$.InvalidViewParticipantData
  * if [[createdCore]] contains two elements with the same contract id,
  * if [[coreInputs]]`(id).contractId != id`
  * if [[createdInSubviewArchivedInCore]] overlaps with [[createdCore]]'s ids or [[coreInputs]]
  * if [[coreInputs]] does not contain the resolved contract ids of [[resolvedKeys]]
  * if the [[actionDescription]] is a [[com.digitalasset.canton.data.ActionDescription.CreateActionDescription]]
  * and the created id is not the first contract ID in [[createdCore]]
  * if the [[actionDescription]] is a [[com.digitalasset.canton.data.ActionDescription.ExerciseActionDescription]]
  * or [[com.digitalasset.canton.data.ActionDescription.FetchActionDescription]] and the input contract is not in [[coreInputs]]
  * if the [[actionDescription]] is a [[com.digitalasset.canton.data.ActionDescription.LookupByKeyActionDescription]]
  * and the key is not in [[resolvedKeys]].
  * @throws com.digitalasset.canton.serialization.SerializationCheckFailed if this instance cannot be serialized
  */
final case class ViewParticipantData private (
    coreInputs: Map[LfContractId, InputContract],
    createdCore: Seq[CreatedContract],
    createdInSubviewArchivedInCore: Set[LfContractId],
    resolvedKeys: Map[LfGlobalKey, SerializableKeyResolution],
    actionDescription: ActionDescription,
    rollbackContext: RollbackContext,
    salt: Salt,
)(
    hashOps: HashOps,
    val representativeProtocolVersion: RepresentativeProtocolVersion[ViewParticipantData],
    override val deserializedFrom: Option[ByteString],
) extends MerkleTreeLeaf[ViewParticipantData](hashOps)
    with HasProtocolVersionedWrapper[ViewParticipantData]
    with ProtocolVersionedMemoizedEvidence {
  {
    def requireDistinct[A](vals: Seq[A])(message: A => String): Unit = {
      val set = scala.collection.mutable.Set[A]()
      vals.foreach { v =>
        if (set(v)) throw InvalidViewParticipantData(message(v))
        else set += v
      }
    }

    val createdIds = createdCore.map(_.contract.contractId)
    requireDistinct(createdIds) { id =>
      val indices = createdIds.zipWithIndex.filter(_._1 == id).map(_._2)
      s"createdCore contains the contract id $id multiple times at indices ${indices.mkString(", ")}"
    }

    coreInputs.foreach { case (id, usedContract) =>
      if (id != usedContract.contractId)
        throw InvalidViewParticipantData(
          s"Inconsistent ids for used contract: $id and ${usedContract.contractId}"
        )

      if (createdInSubviewArchivedInCore.contains(id))
        throw InvalidViewParticipantData(
          s"Contracts created in a subview overlap with core inputs: $id"
        )
    }

    val transientOverlap = createdInSubviewArchivedInCore intersect createdIds.toSet
    if (transientOverlap.nonEmpty)
      throw InvalidViewParticipantData(
        s"Contract created in a subview are also created in the core: $transientOverlap"
      )

    def inconsistentAssignedKey(
        keyWithResolution: (LfGlobalKey, SerializableKeyResolution)
    ): Boolean = {
      val (key, resolution) = keyWithResolution
      resolution.resolution.fold(false) { cid =>
        val inconsistent = for {
          inputContract <- coreInputs.get(cid)
          declaredKey <- inputContract.contract.metadata.maybeKey
        } yield declaredKey != key
        inconsistent.getOrElse(true)
      }
    }
    val keyInconsistencies = resolvedKeys.filter(inconsistentAssignedKey)
    if (keyInconsistencies.nonEmpty) {
      println(show"View participant data: ${resolvedKeys}\n\n${coreInputs}")
      throw InvalidViewParticipantData(show"Inconsistencies for resolved keys: $keyInconsistencies")
    }
  }

  val rootAction: RootAction =
    actionDescription match {
      case CreateActionDescription(contractId, _seed, _version) =>
        val createdContract = createdCore.headOption.getOrElse(
          throw InvalidViewParticipantData(
            show"No created core contracts declared for a view that creates contract $contractId at the root"
          )
        )
        if (createdContract.contract.contractId != contractId)
          throw InvalidViewParticipantData(
            show"View with root action Create $contractId declares ${createdContract.contract.contractId} as first created core contract."
          )
        val metadata = createdContract.contract.metadata
        val contractInst = createdContract.contract.rawContractInstance.contractInstance

        RootAction(
          LfCreateCommand(
            templateId = contractInst.unversioned.template,
            argument = contractInst.unversioned.arg,
          ),
          metadata.signatories,
          failed = false,
        )

      case ExerciseActionDescription(
            inputContractId,
            choice,
            interfaceId,
            chosenValue,
            actors,
            byKey,
            _seed,
            _version,
            failed,
          ) =>
        val inputContract = coreInputs.getOrElse(
          inputContractId,
          throw InvalidViewParticipantData(
            show"Input contract $inputContractId of the Exercise root action is not declared as core input."
          ),
        )
        val templateId = inputContract.contract.contractInstance.unversioned.template
        val cmd = if (byKey) {
          val key = inputContract.contract.metadata.maybeKey
            .map(_.key)
            .getOrElse(
              throw InvalidViewParticipantData(
                "Flag byKey set on an exercise of a contract without key."
              )
            )
          LfExerciseByKeyCommand(
            templateId = templateId,
            contractKey = key,
            choiceId = choice,
            argument = chosenValue,
          )
        } else {
          LfExerciseCommand(
            templateId = templateId,
            interfaceId = interfaceId,
            contractId = inputContractId,
            choiceId = choice,
            argument = chosenValue,
          )
        }
        RootAction(cmd, actors, failed)

      case FetchActionDescription(inputContractId, actors, byKey, _version) =>
        val inputContract = coreInputs.getOrElse(
          inputContractId,
          throw InvalidViewParticipantData(
            show"Input contract $inputContractId of the Fetch root action is not declared as core input."
          ),
        )
        val templateId = inputContract.contract.contractInstance.unversioned.template
        val cmd = if (byKey) {
          val key = inputContract.contract.metadata.maybeKey
            .map(_.key)
            .getOrElse(
              throw InvalidViewParticipantData(
                "Flag byKey set on a fetch of a contract without key."
              )
            )
          LfFetchByKeyCommand(templateId = templateId, key = key)
        } else {
          LfFetchCommand(templateId = templateId, coid = inputContractId)
        }
        RootAction(cmd, actors, failed = false)

      case LookupByKeyActionDescription(key, _version) =>
        val keyResolution = resolvedKeys.getOrElse(
          key,
          throw InvalidViewParticipantData(
            show"Key $key of LookupByKey root action is not resolved."
          ),
        )
        val maintainers = keyResolution match {
          case AssignedKey(contractId) => checked(coreInputs(contractId)).maintainers
          case FreeKey(maintainers) => maintainers
        }

        RootAction(
          LfLookupByKeyCommand(templateId = key.templateId, contractKey = key.key),
          maintainers,
          failed = false,
        )
    }

  override protected def companionObj: ViewParticipantData.type = ViewParticipantData

  private[ViewParticipantData] def toProtoV0: v0.ViewParticipantData =
    v0.ViewParticipantData(
      coreInputs = coreInputs.values.map(_.toProtoV0).toSeq,
      createdCore = createdCore.map(_.toProtoV0),
      createdInSubviewArchivedInCore = createdInSubviewArchivedInCore.toSeq.map(_.toProtoPrimitive),
      resolvedKeys = resolvedKeys.toList.map { case (k, res) => ResolvedKey(k, res).toProtoV0 },
      actionDescription = Some(actionDescription.toProtoV0),
      rollbackContext = if (rollbackContext.isEmpty) None else Some(rollbackContext.toProtoV0),
      salt = Some(salt.toProtoV0),
    )

  private[ViewParticipantData] def toProtoV1: v0.ViewParticipantData = toProtoV0

  private[ViewParticipantData] def toProtoV2: v2.ViewParticipantData = v2.ViewParticipantData(
    coreInputs = coreInputs.values.map(_.toProtoV1).toSeq,
    createdCore = createdCore.map(_.toProtoV1),
    createdInSubviewArchivedInCore = createdInSubviewArchivedInCore.toSeq.map(_.toProtoPrimitive),
    resolvedKeys = resolvedKeys.toList.map { case (k, res) => ResolvedKey(k, res).toProtoV0 },
    actionDescription = Some(actionDescription.toProtoV1),
    rollbackContext = if (rollbackContext.isEmpty) None else Some(rollbackContext.toProtoV0),
    salt = Some(salt.toProtoV0),
  )

  override protected[this] def toByteStringUnmemoized: ByteString =
    super[HasProtocolVersionedWrapper].toByteString

  override def hashPurpose: HashPurpose = HashPurpose.ViewParticipantData

  override def pretty: Pretty[ViewParticipantData] = prettyOfClass(
    paramIfNonEmpty("core inputs", _.coreInputs),
    paramIfNonEmpty("created core", _.createdCore),
    paramIfNonEmpty("created in subview, archived in core", _.createdInSubviewArchivedInCore),
    paramIfNonEmpty("resolved keys", _.resolvedKeys),
    param("action description", _.actionDescription),
    param("rollback context", _.rollbackContext),
    param("salt", _.salt),
  )

  /** Extends [[resolvedKeys]] with the maintainers of assigned keys */
  val resolvedKeysWithMaintainers: Map[LfGlobalKey, KeyResolutionWithMaintainers] =
    resolvedKeys.fmap {
      case assigned @ AssignedKey(contractId) =>
        val maintainers =
          // checked by `inconsistentAssignedKey` above
          checked(
            coreInputs.getOrElse(
              contractId,
              throw InvalidViewParticipantData(
                s"No input contract $contractId for a resolved key found"
              ),
            )
          ).maintainers
        AssignedKeyWithMaintainers(contractId, maintainers)(assigned.version)
      case free @ FreeKey(_) => free
    }
}

object ViewParticipantData
    extends HasMemoizedProtocolVersionedWithContextCompanion[ViewParticipantData, HashOps] {
  override val name: String = "ViewParticipantData"

  val supportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(0) -> VersionedProtoConverter(
      ProtocolVersion.v2,
      supportedProtoVersionMemoized(v0.ViewParticipantData)(fromProtoV0),
      _.toProtoV0.toByteString,
    ),
    // Proto version 1 uses the same message format as version 0,
    // but interprets resolvedKeys differently. See ViewParticipantData's scaladoc for details
    ProtoVersion(1) -> VersionedProtoConverter(
      ProtocolVersion.v3,
      supportedProtoVersionMemoized(v0.ViewParticipantData)(fromProtoV1),
      _.toProtoV1.toByteString,
    ),
    ProtoVersion(2) -> VersionedProtoConverter(
      ProtocolVersion.v4,
      supportedProtoVersionMemoized(v2.ViewParticipantData)(fromProtoV2),
      _.toProtoV2.toByteString,
    ),
  )

  /** Creates a view participant data.
    *
    * @throws InvalidViewParticipantData
    * if [[ViewParticipantData.createdCore]] contains two elements with the same contract id,
    * if [[ViewParticipantData.coreInputs]]`(id).contractId != id`
    * if [[ViewParticipantData.createdInSubviewArchivedInCore]] overlaps with [[ViewParticipantData.createdCore]]'s ids or [[ViewParticipantData.coreInputs]]
    * if [[ViewParticipantData.coreInputs]] does not contain the resolved contract ids in [[ViewParticipantData.resolvedKeys]]
    * if [[ViewParticipantData.createdCore]] creates a contract with a key that is not in [[ViewParticipantData.resolvedKeys]]
    * if the [[ViewParticipantData.actionDescription]] is a [[com.digitalasset.canton.data.ActionDescription.CreateActionDescription]]
    * and the created id is not the first contract ID in [[ViewParticipantData.createdCore]]
    * if the [[ViewParticipantData.actionDescription]] is a [[com.digitalasset.canton.data.ActionDescription.ExerciseActionDescription]]
    * or [[com.digitalasset.canton.data.ActionDescription.FetchActionDescription]] and the input contract is not in [[ViewParticipantData.coreInputs]]
    * if the [[ViewParticipantData.actionDescription]] is a [[com.digitalasset.canton.data.ActionDescription.LookupByKeyActionDescription]]
    * and the key is not in [[ViewParticipantData.resolvedKeys]].
    * @throws com.digitalasset.canton.serialization.SerializationCheckFailed if this instance cannot be serialized
    */
  @throws[SerializationCheckFailed[com.daml.lf.value.ValueCoder.EncodeError]]
  def apply(hashOps: HashOps)(
      coreInputs: Map[LfContractId, InputContract],
      createdCore: Seq[CreatedContract],
      createdInSubviewArchivedInCore: Set[LfContractId],
      resolvedKeys: Map[LfGlobalKey, SerializableKeyResolution],
      actionDescription: ActionDescription,
      rollbackContext: RollbackContext,
      salt: Salt,
      protocolVersion: ProtocolVersion,
  ): ViewParticipantData =
    ViewParticipantData(
      coreInputs,
      createdCore,
      createdInSubviewArchivedInCore,
      resolvedKeys,
      actionDescription,
      rollbackContext,
      salt,
    )(hashOps, protocolVersionRepresentativeFor(protocolVersion), None)

  /** Creates a view participant data.
    *
    * Yields `Left(...)`
    * if [[ViewParticipantData.createdCore]] contains two elements with the same contract id,
    * if [[ViewParticipantData.coreInputs]]`(id).contractId != id`
    * if [[ViewParticipantData.createdInSubviewArchivedInCore]] overlaps with [[ViewParticipantData.createdCore]]'s ids or [[ViewParticipantData.coreInputs]]
    * if [[ViewParticipantData.coreInputs]] does not contain the resolved contract ids in [[ViewParticipantData.resolvedKeys]]
    * if [[ViewParticipantData.createdCore]] creates a contract with a key that is not in [[ViewParticipantData.resolvedKeys]]
    * if the [[ViewParticipantData.actionDescription]] is a [[com.digitalasset.canton.data.ActionDescription.CreateActionDescription]]
    *   and the created id is not the first contract ID in [[ViewParticipantData.createdCore]]
    * if the [[ViewParticipantData.actionDescription]] is a [[com.digitalasset.canton.data.ActionDescription.ExerciseActionDescription]]
    *   or [[com.digitalasset.canton.data.ActionDescription.FetchActionDescription]] and the input contract is not in [[ViewParticipantData.coreInputs]]
    * if the [[ViewParticipantData.actionDescription]] is a [[com.digitalasset.canton.data.ActionDescription.LookupByKeyActionDescription]]
    *   and the key is not in [[ViewParticipantData.resolvedKeys]].
    * if this instance cannot be serialized.
    */
  def create(hashOps: HashOps)(
      coreInputs: Map[LfContractId, InputContract],
      createdCore: Seq[CreatedContract],
      createdInSubviewArchivedInCore: Set[LfContractId],
      resolvedKeys: Map[LfGlobalKey, SerializableKeyResolution],
      actionDescription: ActionDescription,
      rollbackContext: RollbackContext,
      salt: Salt,
      protocolVersion: ProtocolVersion,
  ): Either[String, ViewParticipantData] =
    returnLeftWhenInitializationFails(
      ViewParticipantData(hashOps)(
        coreInputs,
        createdCore,
        createdInSubviewArchivedInCore,
        resolvedKeys,
        actionDescription,
        rollbackContext,
        salt,
        protocolVersion,
      )
    )

  private[this] def returnLeftWhenInitializationFails[A](initialization: => A): Either[String, A] =
    try {
      Right(initialization)
    } catch {
      case InvalidViewParticipantData(message) => Left(message)
      case SerializationCheckFailed(err) => Left(err.toString)
    }

  private def fromProtoV0(hashOps: HashOps, dataP: v0.ViewParticipantData)(
      bytes: ByteString
  ): ParsingResult[ViewParticipantData] =
    fromProtoV0V1(hashOps, dataP, ProtoVersion(0))(bytes)

  private def fromProtoV1(hashOps: HashOps, dataP: v0.ViewParticipantData)(
      bytes: ByteString
  ): ParsingResult[ViewParticipantData] =
    fromProtoV0V1(hashOps, dataP, ProtoVersion(1))(bytes)

  private def fromProtoV2(hashOps: HashOps, dataP: v2.ViewParticipantData)(
      bytes: ByteString
  ): ParsingResult[ViewParticipantData] = {
    val v2.ViewParticipantData(
      saltP,
      coreInputsP,
      createdCoreP,
      createdInSubviewArchivedInCoreP,
      resolvedKeysP,
      actionDescriptionP,
      rbContextP,
    ) = dataP

    fromProtoV0V1V2(hashOps, ProtoVersion(2))(
      saltP,
      coreInputsP,
      createdCoreP,
      createdInSubviewArchivedInCoreP,
      resolvedKeysP,
      actionDescriptionP,
      ActionDescription.fromProtoV1,
      CreatedContract.fromProtoV1,
      InputContract.fromProtoV1,
      rbContextP,
    )(bytes)
  }

  private def fromProtoV0V1V2[ActionDescriptionProto, CreatedContractProto, InputContractProto](
      hashOps: HashOps,
      protoVersion: ProtoVersion,
  )(
      saltP: Option[com.digitalasset.canton.crypto.v0.Salt],
      coreInputsP: Seq[InputContractProto],
      createdCoreP: Seq[CreatedContractProto],
      createdInSubviewArchivedInCoreP: Seq[String],
      resolvedKeysP: Seq[v0.ViewParticipantData.ResolvedKey],
      actionDescriptionP: Option[ActionDescriptionProto],
      actionDescriptionDeserializer: ActionDescriptionProto => ParsingResult[ActionDescription],
      createdContractDeserializer: CreatedContractProto => ParsingResult[CreatedContract],
      inputContractDeserializer: InputContractProto => ParsingResult[InputContract],
      rbContextP: Option[v0.ViewParticipantData.RollbackContext],
  )(bytes: ByteString): ParsingResult[ViewParticipantData] = for {
    coreInputsSeq <- coreInputsP.traverse(inputContractDeserializer)
    coreInputs = coreInputsSeq.view
      .map(inputContract => inputContract.contract.contractId -> inputContract)
      .toMap
    createdCore <- createdCoreP.traverse(createdContractDeserializer)
    createdInSubviewArchivedInCore <- createdInSubviewArchivedInCoreP
      .traverse(LfContractId.fromProtoPrimitive)
    resolvedKeys <- resolvedKeysP.traverse(
      ResolvedKey.fromProtoV0(_).map(rk => rk.key -> rk.resolution)
    )
    resolvedKeysMap = resolvedKeys.toMap
    actionDescription <- ProtoConverter
      .required("action_description", actionDescriptionP)
      .flatMap(actionDescriptionDeserializer)

    salt <- ProtoConverter
      .parseRequired(Salt.fromProtoV0, "salt", saltP)
      .leftMap(_.inField("salt"))

    rollbackContext <- RollbackContext
      .fromProtoV0(rbContextP)
      .leftMap(_.inField("rollbackContext"))

    viewParticipantData <- returnLeftWhenInitializationFails(
      ViewParticipantData(
        coreInputs = coreInputs,
        createdCore = createdCore,
        createdInSubviewArchivedInCore = createdInSubviewArchivedInCore.toSet,
        resolvedKeys = resolvedKeysMap,
        actionDescription = actionDescription,
        rollbackContext = rollbackContext,
        salt = salt,
      )(hashOps, protocolVersionRepresentativeFor(protoVersion), Some(bytes))
    ).leftMap(ProtoDeserializationError.OtherError)
  } yield viewParticipantData

  private def fromProtoV0V1(
      hashOps: HashOps,
      dataP: v0.ViewParticipantData,
      protoVersion: ProtoVersion,
  )(bytes: ByteString): ParsingResult[ViewParticipantData] = {
    val v0.ViewParticipantData(
      saltP,
      coreInputsP,
      createdCoreP,
      createdInSubviewArchivedInCoreP,
      resolvedKeysP,
      actionDescriptionP,
      rbContextP,
    ) = dataP

    fromProtoV0V1V2(hashOps, protoVersion)(
      saltP,
      coreInputsP,
      createdCoreP,
      createdInSubviewArchivedInCoreP,
      resolvedKeysP,
      actionDescriptionP,
      ActionDescription.fromProtoV0,
      CreatedContract.fromProtoV0,
      InputContract.fromProtoV0,
      rbContextP,
    )(bytes)
  }

  case class RootAction(command: LfCommand, authorizers: Set[LfPartyId], failed: Boolean)

  /** Indicates an attempt to create an invalid [[ViewParticipantData]]. */
  case class InvalidViewParticipantData(message: String) extends RuntimeException(message)
}
