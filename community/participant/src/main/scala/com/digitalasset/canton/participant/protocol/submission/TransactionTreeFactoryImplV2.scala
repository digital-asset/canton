// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission

import cats.data.EitherT
import cats.syntax.bifunctor._
import com.daml.lf.value.Value
import com.digitalasset.canton.crypto.{HashOps, HmacOps, Salt, SaltSeed}
import com.digitalasset.canton.data.ViewPosition.ListIndex
import com.digitalasset.canton.data._
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.protocol.submission.TransactionTreeFactory.{
  DivergingKeyResolutionError,
  SerializableContractOfId,
  TransactionTreeConversionError,
}
import com.digitalasset.canton.protocol.RollbackContext.RollbackScope
import com.digitalasset.canton.protocol._
import com.digitalasset.canton.topology.{DomainId, MediatorId, ParticipantId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{LfTransactionUtil, MonadUtil}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{LfKeyResolver, checked}

import java.util.UUID
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

/** Generate transaction trees as used up to protocol version [[com.digitalasset.canton.version.ProtocolVersion.v2]]
  */
final class TransactionTreeFactoryImplV2(
    submitterParticipant: ParticipantId,
    domainId: DomainId,
    protocolVersion: ProtocolVersion,
    contractSerializer: LfContractInst => SerializableRawContractInstance,
    packageInfoService: PackageInfoService,
    cryptoOps: HashOps with HmacOps,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends TransactionTreeFactoryImpl(
      submitterParticipant,
      domainId,
      protocolVersion,
      contractSerializer,
      packageInfoService,
      cryptoOps,
      loggerFactory,
    ) {
  require(
    protocolVersion <= ProtocolVersion.v2,
    s"${this.getClass.getSimpleName} can only be used up to protocol version ${ProtocolVersion.v2}, but not for $protocolVersion",
  )

  protected[submission] class State private (
      override val mediatorId: MediatorId,
      override val transactionUUID: UUID,
      override val ledgerTime: CantonTimestamp,
      override protected val salts: Iterator[Salt],
  ) extends TransactionTreeFactoryImpl.State {

    /** Out parameter for contracts consumed in the view (including subviews). */
    @SuppressWarnings(Array("org.wartremover.warts.Var"))
    var consumedContractsInView: collection.Set[LfContractId] = Set.empty

    /** Out parameter for resolved keys in the view (including subviews).
      * Propagates the key resolution info from subviews to the parent view.
      */
    @SuppressWarnings(Array("org.wartremover.warts.Var"))
    var resolvedKeysInView: collection.Map[LfGlobalKey, Option[LfContractId]] = Map.empty
  }

  private[submission] object State {
    private[submission] def submission(
        transactionSeed: SaltSeed,
        mediatorId: MediatorId,
        transactionUUID: UUID,
        ledgerTime: CantonTimestamp,
        nextSaltIndex: Int,
    ): State = {
      val salts = LazyList
        .from(nextSaltIndex)
        .map(index => Salt.tryDeriveSalt(transactionSeed, index, cryptoOps))
      new State(mediatorId, transactionUUID, ledgerTime, salts.iterator)
    }

    private[submission] def validation(
        mediatorId: MediatorId,
        transactionUUID: UUID,
        ledgerTime: CantonTimestamp,
        salts: Iterable[Salt],
    ): State = new State(mediatorId, transactionUUID, ledgerTime, salts.iterator)
  }

  override protected def stateForSubmission(
      transactionSeed: SaltSeed,
      mediatorId: MediatorId,
      transactionUUID: UUID,
      ledgerTime: CantonTimestamp,
      nextSaltIndex: Int,
      keyResolver: LfKeyResolver,
  ): State =
    State.submission(transactionSeed, mediatorId, transactionUUID, ledgerTime, nextSaltIndex)

  override protected def stateForValidation(
      mediatorId: MediatorId,
      transactionUUID: UUID,
      ledgerTime: CantonTimestamp,
      salts: Iterable[Salt],
      keyResolver: LfKeyResolver,
  ): State = State.validation(mediatorId, transactionUUID, ledgerTime, salts)

  override protected def createView(
      view: TransactionViewDecomposition.NewView,
      viewPosition: ViewPosition,
      state: State,
      contractOfId: SerializableContractOfId,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, TransactionTreeConversionError, TransactionView] = {

    // Process core nodes and subviews
    val coreCreatedBuilder =
      List.newBuilder[(LfNodeCreate, RollbackScope)] // contract IDs have already been suffixed
    val coreOtherBuilder =
      List.newBuilder[
        ((LfNodeId, LfActionNode), RollbackScope)
      ] // contract IDs have not yet been suffixed
    val childViewsBuilder = Seq.newBuilder[TransactionView]

    @SuppressWarnings(Array("org.wartremover.warts.Var"))
    var createIndex = 0
    @SuppressWarnings(Array("org.wartremover.warts.Var"))
    var subviewCount = 0
    val createdInView = mutable.Set.empty[LfContractId]
    val consumedInView = mutable.Set.empty[LfContractId]
    val resolvedKeysInCore = mutable.Map.empty[LfGlobalKey, SerializableKeyResolution]
    // Resolves keys to contracts as needed for interpreting the view.
    // A key being mapped to `None` means that the key must resolve to no contract
    // whereas a key that is not in the map should not need to be resolved.
    // In the ledger model terminology, the former corresponds to `free` whereas the latter models `unknown`.
    val resolvedKeysInView = mutable.Map.empty[LfGlobalKey, Option[LfContractId]]

    // We assume that a correctly functioning DAMLe produces action where every pair of byKeyNodes
    // for the same key satisfies one of the following properties:
    // 1. The key lookup results are the same (same contract IDs or both are`None`)
    // 2. The key lookup results are two contract IDs and at least one of them is created in the action.
    // 3. The key lookup results are None and a contract ID, and if the contract ID is not created in the action
    //    then the contract is archived before the LookupByKey node that resolves to None.
    // If we nevertheless encounter such diverging keys (e.g., when recreating a view sent by a malicious submitter),
    // we collect them here to be able to report the error.
    val divergingKeys = mutable.Map.empty[LfGlobalKey, Set[Option[LfContractId]]]

    def fromEither[A <: TransactionTreeConversionError, B](
        either: Either[A, B]
    ): EitherT[Future, TransactionTreeConversionError, B] =
      EitherT.fromEither(either.leftWiden[TransactionTreeConversionError])

    def globalKey(lfNode: LfActionNode, key: Value) = {
      // The key comes from a `WellformedTransaction` and thus cannot contain a contract ID
      val safeKey = checked(LfTransactionUtil.assertNoContractIdInKey(key))
      LfGlobalKey(LfTransactionUtil.nodeTemplate(lfNode), safeKey)
    }

    def addToDivergingKeys(key: LfGlobalKey, divergence: Set[Option[LfContractId]]): Unit =
      divergingKeys.update(key, divergingKeys.getOrElse(key, Set.empty) union divergence)

    for {
      // Compute salts
      viewCommonDataSalt <- fromEither(state.nextSalt())
      viewParticipantDataSalt <- fromEither(state.nextSalt())

      // Ensure that nodes are processed sequentially. Note the state is built up in the mutable variables, not in the
      // fold here.
      _ <- MonadUtil.sequentialTraverse_(view.allNodes.toList) {
        case childView: TransactionViewDecomposition.NewView =>
          // Compute subviews, recursively
          createView(childView, ListIndex(subviewCount) +: viewPosition, state, contractOfId).map {
            v =>
              childViewsBuilder += v
              subviewCount += 1
              val createdInSubview = state.createdContractsInView
              createdInView ++= createdInSubview
              val consumedInSubview = state.consumedContractsInView
              consumedInView ++= consumedInSubview
              // Add new key resolutions, but keep the old ones.
              val resolvedKeysInSubview = state.resolvedKeysInView
              resolvedKeysInSubview.foreach { case (key, contractIdO) =>
                // TODO(M40): Check that there are no diverging key resolutions
                val _ = resolvedKeysInView.getOrElseUpdate(key, contractIdO)
              }
          }

        case TransactionViewDecomposition.SameView(lfActionNode, nodeId, rbContext) =>
          lfActionNode match {
            case createNode: LfNodeCreate =>
              val suffixedNode = updateStateWithContractCreation(
                nodeId,
                createNode,
                viewParticipantDataSalt,
                viewPosition,
                createIndex,
                state,
              )
              coreCreatedBuilder += ((suffixedNode, rbContext.rollbackScope))
              createdInView += suffixedNode.coid
              createIndex += 1
            case lfNode: LfActionNode =>
              LfTransactionUtil.consumedContractId(lfNode).foreach(consumedInView += _)
              if (lfNode.byKey) {
                val LfKeyWithMaintainers(key, maintainers) = LfTransactionUtil
                  .keyWithMaintainers(lfNode)
                  .getOrElse(
                    throw new IllegalArgumentException(
                      s"Node $nodeId of a well-formed transaction marked as byKeyNode, but has no contract key"
                    )
                  )
                val gk = globalKey(lfNode, key)

                LfTransactionUtil.usedContractIdWithMetadata(
                  checked(trySuffixNode(state)(nodeId -> lfNode))
                ) match {
                  case None => // LookupByKey node
                    resolvedKeysInCore
                      .getOrElseUpdate(gk, FreeKey(maintainers)(lfNode.version))
                      .discard
                    val previous = resolvedKeysInView.getOrElseUpdate(gk, None)
                    previous.foreach { coid =>
                      // TODO(M40) This check does not detect when an earlier create node has assigned the key
                      if (!consumedInView.contains(coid))
                        addToDivergingKeys(gk, Set(previous, None))
                    }
                  case Some(used) =>
                    val cid = used.unwrap
                    val someCid = Some(cid)
                    // Could be a contract created in the same view, so DAMLe will resolve that itself
                    if (!createdInView.contains(cid)) {
                      val _ = resolvedKeysInCore.getOrElseUpdate(
                        gk,
                        AssignedKey(cid)(lfNode.version),
                      )
                      val previous = resolvedKeysInView.getOrElseUpdate(gk, someCid)
                      if (!previous.contains(cid)) {
                        addToDivergingKeys(gk, Set(previous, someCid))
                      }
                    }
                }
              }
              coreOtherBuilder += ((nodeId -> lfNode, rbContext.rollbackScope))
          }
          EitherT.rightT[Future, TransactionTreeConversionError](())
      }

      _noDuplicates <- EitherT
        .cond[Future](divergingKeys.isEmpty, (), DivergingKeyResolutionError(divergingKeys.toMap))

      coreCreatedNodes = coreCreatedBuilder.result()
      // Translate contract ids in untranslated core nodes
      // This must be done only after visiting the whole action (rather than just the node)
      // because an Exercise result may contain an unsuffixed contract ID of a contract
      // that was created in the consequences of the exercise, i.e., we know the suffix only
      // after we have visited the create node.
      coreOtherNodes = coreOtherBuilder.result().map { case (nodeInfo, rbc) =>
        (checked(trySuffixNode(state)(nodeInfo)), rbc)
      }
      childViews = childViewsBuilder.result()

      suffixedRootNode = coreOtherNodes.headOption
        .orElse(coreCreatedNodes.headOption)
        .map { case (node, _) => node }
        .getOrElse(
          throw new IllegalArgumentException(s"The received view has no core nodes. $view")
        )

      // Compute the parameters of the view
      seed = view.rootSeed
      rbContext = view.rbContext
      actionDescription = createActionDescription(suffixedRootNode, seed)
      viewCommonData = createViewCommonData(view, viewCommonDataSalt)

      viewParticipantData <- createViewParticipantData(
        coreCreatedNodes,
        coreOtherNodes,
        childViews,
        state.createdContractInfo,
        resolvedKeysInCore,
        actionDescription,
        viewParticipantDataSalt,
        contractOfId,
        rbContext,
      )

    } yield {
      // Update the out parameters in the `State`
      state.createdContractsInView = createdInView
      state.consumedContractsInView = consumedInView
      state.resolvedKeysInView = resolvedKeysInView

      // Compute the result
      TransactionView.tryCreate(cryptoOps)(viewCommonData, viewParticipantData, childViews)
    }
  }
}
