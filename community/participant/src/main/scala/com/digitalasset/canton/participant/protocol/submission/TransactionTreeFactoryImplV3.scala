// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission

import cats.data.EitherT
import cats.syntax.bifunctor._
import cats.syntax.functor._
import cats.syntax.functorFilter._
import cats.syntax.traverse._
import com.daml.lf.transaction.Transaction.{KeyActive, KeyCreate, KeyInput, NegativeKeyLookup}
import com.daml.lf.transaction.{ContractKeyUniquenessMode, ContractStateMachine}
import com.daml.lf.value.Value
import com.digitalasset.canton.crypto.{HashOps, HmacOps, Salt, SaltSeed}
import com.digitalasset.canton.data.ViewPosition.ListIndex
import com.digitalasset.canton.data._
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory}
import com.digitalasset.canton.participant.protocol.submission.TransactionTreeFactory.{
  ContractKeyResolutionError,
  MissingContractKeyLookupError,
  SerializableContractOfId,
  TransactionTreeConversionError,
}
import com.digitalasset.canton.protocol.RollbackContext.RollbackScope
import com.digitalasset.canton.protocol._
import com.digitalasset.canton.topology.{DomainId, MediatorId, ParticipantId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ShowUtil._
import com.digitalasset.canton.util.{ErrorUtil, LfTransactionUtil, MapsUtil, MonadUtil}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{LfKeyResolver, LfPartyId, checked}

import java.util.UUID
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

/** Generate transaction trees as used from protocol version [[com.digitalasset.canton.version.ProtocolVersion.v3_0_0]] on
  */
class TransactionTreeFactoryImplV3(
    submitterParticipant: ParticipantId,
    domainId: DomainId,
    protocolVersion: ProtocolVersion,
    contractSerializer: LfContractInst => SerializableRawContractInstance,
    packageInfoService: PackageInfoService,
    cryptoOps: HashOps with HmacOps,
    uniqueContractKeys: Boolean,
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
    protocolVersion >= ProtocolVersion.v3_0_0,
    s"${this.getClass.getSimpleName} can only be used with protocol version ${ProtocolVersion.v3_0_0} or higher, but not for $protocolVersion",
  )

  private[TransactionTreeFactoryImplV3] val csm: ContractStateMachine[Unit] =
    new ContractStateMachine[Unit](
      if (uniqueContractKeys) ContractKeyUniquenessMode.Strict else ContractKeyUniquenessMode.Off
    )

  protected[submission] class State private (
      override val mediatorId: MediatorId,
      override val transactionUUID: UUID,
      override val ledgerTime: CantonTimestamp,
      override protected val salts: Iterator[Salt],
      initialResolver: LfKeyResolver,
  ) extends TransactionTreeFactoryImpl.State {

    /** An [[com.digitalasset.canton.protocol.LfGlobalKey]] stores neither the
      * [[com.digitalasset.canton.protocol.LfTransactionVersion]] to be used during serialization
      * nor the maintainers, which we need to cache in case no contract is found.
      *
      * Out parameter that stores version and maintainers for all keys
      * that have been referenced by an already-processed node.
      */
    val keyVersionAndMaintainers: mutable.Map[LfGlobalKey, (LfTransactionVersion, Set[LfPartyId])] =
      mutable.Map.empty

    /** Out parameter for the [[com.daml.lf.transaction.ContractStateMachine.State]]
      *
      * The state of the [[com.daml.lf.transaction.ContractStateMachine]]
      * after iterating over the following nodes in execution order:
      * 1. The iteration starts at the root node of the current view.
      * 2. The iteration includes all processed nodes of the view. This includes the nodes of fully processed subviews.
      */
    @SuppressWarnings(Array("org.wartremover.warts.Var"))
    var csmState: csm.State = csm.initial

    /** This resolver is used to feed [[com.daml.lf.transaction.ContractStateMachine.State.handleLookupWith]]
      * if `uniqueContractKeys` is false.
      */
    @SuppressWarnings(Array("org.wartremover.warts.Var"))
    var currentResolver: LfKeyResolver = initialResolver

    @SuppressWarnings(Array("org.wartremover.warts.Var"))
    private var rollbackScope: RollbackScope = RollbackScope.empty

    def signalRollbackScope(target: RollbackScope): Unit = {
      val (pops, pushes) = RollbackScope.popsAndPushes(rollbackScope, target)
      for (_ <- 1 to pops) { csmState = csmState.endRollback() }
      for (_ <- 1 to pushes) { csmState = csmState.beginRollback() }
      rollbackScope = target
    }
  }

  private[submission] object State {
    private[submission] def submission(
        transactionSeed: SaltSeed,
        mediatorId: MediatorId,
        transactionUUID: UUID,
        ledgerTime: CantonTimestamp,
        nextSaltIndex: Int,
        keyResolver: LfKeyResolver,
    ): State = {
      val salts = LazyList
        .from(nextSaltIndex)
        .map(index => Salt.tryDeriveSalt(transactionSeed, index, cryptoOps))
      new State(mediatorId, transactionUUID, ledgerTime, salts.iterator, keyResolver)
    }

    private[submission] def validation(
        mediatorId: MediatorId,
        transactionUUID: UUID,
        ledgerTime: CantonTimestamp,
        salts: Iterable[Salt],
        keyResolver: LfKeyResolver,
    ): State = new State(mediatorId, transactionUUID, ledgerTime, salts.iterator, keyResolver)
  }

  override protected def stateForSubmission(
      transactionSeed: SaltSeed,
      mediatorId: MediatorId,
      transactionUUID: UUID,
      ledgerTime: CantonTimestamp,
      nextSaltIndex: Int,
      keyResolver: LfKeyResolver,
  ): State =
    State.submission(
      transactionSeed,
      mediatorId,
      transactionUUID,
      ledgerTime,
      nextSaltIndex,
      keyResolver,
    )

  override protected def stateForValidation(
      mediatorId: MediatorId,
      transactionUUID: UUID,
      ledgerTime: CantonTimestamp,
      salts: Iterable[Salt],
      keyResolver: LfKeyResolver,
  ): State = State.validation(mediatorId, transactionUUID, ledgerTime, salts, keyResolver)

  override protected def createView(
      view: TransactionViewDecomposition.NewView,
      viewPosition: ViewPosition,
      state: State,
      contractOfId: SerializableContractOfId,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, TransactionTreeConversionError, TransactionView] = {
    state.signalRollbackScope(view.rbContext.rollbackScope)

    // reset to a fresh state with projected resolver before visiting the subtree
    val previousCsmState = state.csmState
    val previousResolver = state.currentResolver
    state.currentResolver = state.csmState.projectKeyResolver(previousResolver)
    state.csmState = csm.initial

    // Process core nodes and subviews
    val coreCreatedBuilder =
      List.newBuilder[(LfNodeCreate, RollbackScope)] // contract IDs have already been suffixed
    val coreOtherBuilder = // contract IDs have not yet been suffixed
      List.newBuilder[((LfNodeId, LfActionNode), RollbackScope)]
    val childViewsBuilder = Seq.newBuilder[TransactionView]
    val subViewKeyResolutions = mutable.Map.empty[LfGlobalKey, SerializableKeyResolution]

    @SuppressWarnings(Array("org.wartremover.warts.Var"))
    var createIndex = 0
    @SuppressWarnings(Array("org.wartremover.warts.Var"))
    var subviewCount = 0
    val createdInView = mutable.Set.empty[LfContractId]

    def fromEither[A <: TransactionTreeConversionError, B](
        either: Either[A, B]
    ): EitherT[Future, TransactionTreeConversionError, B] =
      EitherT.fromEither(either.leftWiden[TransactionTreeConversionError])

    def globalKey(lfNode: LfActionNode, key: Value) = {
      // The key comes from a `WellformedTransaction` and thus cannot contain a contract ID
      checked(LfGlobalKey.assertBuild(LfTransactionUtil.nodeTemplate(lfNode), key))
    }

    for {
      // Compute salts
      viewCommonDataSalt <- fromEither(state.nextSalt())
      viewParticipantDataSalt <- fromEither(state.nextSalt())
      _ <- MonadUtil.sequentialTraverse_(view.allNodes) {
        case childView: TransactionViewDecomposition.NewView =>
          // Compute subviews, recursively
          createView(childView, ListIndex(subviewCount) +: viewPosition, state, contractOfId)
            .map { v =>
              childViewsBuilder += v
              subviewCount += 1
              val createdInSubview = state.createdContractsInView
              createdInView ++= createdInSubview

              val subviewRolledBack =
                childView.rbContext.rollbackScope != view.rbContext.rollbackScope
              val keyResolutionsInSubview = v.globalKeyInputs.fmap { resolution =>
                val serRes = resolution.asSerializable
                if (subviewRolledBack) serRes.withRolledBack(true) else serRes
              }
              MapsUtil.extendMapWith(subViewKeyResolutions, keyResolutionsInSubview) {
                (accRes, newRes) =>
                  accRes.withRolledBack(accRes.rolledBack && newRes.rolledBack)
              }
            }

        case TransactionViewDecomposition.SameView(lfActionNode, nodeId, rbContext) =>
          val rbScope = rbContext.rollbackScope
          val suffixedNode = lfActionNode match {
            case createNode: LfNodeCreate =>
              val suffixedNode = updateStateWithContractCreation(
                nodeId,
                createNode,
                viewParticipantDataSalt,
                viewPosition,
                createIndex,
                state,
              )
              coreCreatedBuilder += (suffixedNode -> rbScope)
              createdInView += suffixedNode.coid
              createIndex += 1
              suffixedNode
            case lfNode: LfActionNode =>
              val suffixedNode = trySuffixNode(state)(nodeId -> lfNode)
              coreOtherBuilder += ((nodeId, lfNode) -> rbScope)
              suffixedNode
          }

          LfTransactionUtil.keyWithMaintainers(suffixedNode).foreach {
            case LfKeyWithMaintainers(key, maintainers) =>
              val gkey = globalKey(suffixedNode, key)
              state.keyVersionAndMaintainers += (gkey -> (suffixedNode.version -> maintainers))
          }
          state.signalRollbackScope(rbScope)
          val nextStateE = suffixedNode match { // TODO(i9720): call state.handleNode instead
            case create: LfNodeCreate =>
              state.csmState.handleCreate(create).leftMap(ContractKeyResolutionError)
            case exercise: LfNodeExercises =>
              state.csmState.handleExercise((), exercise).leftMap(ContractKeyResolutionError)
            case fetch: LfNodeFetch =>
              state.csmState.handleFetch(fetch).leftMap(ContractKeyResolutionError)
            case lookupByKey: LfNodeLookupByKey =>
              if (state.csmState.mode == ContractKeyUniquenessMode.Off) {
                val gkey = globalKey(lookupByKey, lookupByKey.key.key)
                for {
                  resolution <- state.currentResolver
                    .get(gkey)
                    .toRight(MissingContractKeyLookupError(gkey))
                  nextState <- state.csmState
                    .handleLookupWith(lookupByKey, resolution)
                    .leftMap(ContractKeyResolutionError)
                } yield nextState
              } else {
                state.csmState.handleLookup(lookupByKey).leftMap(ContractKeyResolutionError)
              }
          }
          EitherT.fromEither[Future](nextStateE.map(nextState => state.csmState = nextState))
      }
      _ = state.signalRollbackScope(view.rbContext.rollbackScope)

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
      actionDescription = createActionDescription(suffixedRootNode, seed)
      viewCommonData = createViewCommonData(view, viewCommonDataSalt)
      viewKeyInputs = state.csmState.globalKeyInputs
      coreUsedKeysOutsideRollback = coreOtherNodes.mapFilter { case (an, rbScopeOther) =>
        LfTransactionUtil.keyWithMaintainers(an).flatMap { kWithM =>
          if (rbScopeOther == view.rbContext.rollbackScope)
            Some(LfGlobalKey.assertBuild(an.templateId, kWithM.key))
          else None
        }
      }.toSet
      resolvedK <- EitherT.fromEither[Future](
        resolvedKeys(
          viewKeyInputs,
          coreUsedKeysOutsideRollback,
          state.keyVersionAndMaintainers,
          subViewKeyResolutions,
        )
      )
      viewParticipantData <- createViewParticipantData(
        coreCreatedNodes,
        coreOtherNodes,
        childViews,
        state.createdContractInfo,
        resolvedK,
        actionDescription,
        viewParticipantDataSalt,
        contractOfId,
        view.rbContext,
      )

      // fast-forward the former state over the subtree
      nextCsmState <- EitherT.fromEither[Future](
        previousCsmState
          .advance(
            // advance ignores the resolver in mode Strict
            if (state.csmState.mode == ContractKeyUniquenessMode.Strict) Map.empty
            else previousResolver,
            state.csmState,
          )
          .leftMap(ContractKeyResolutionError(_): TransactionTreeConversionError)
      )
    } yield {
      // Compute the result
      val transactionView =
        TransactionView(cryptoOps)(viewCommonData, viewParticipantData, childViews)

      checkCsmStateMatchesView(state.csmState, transactionView, viewPosition)

      // Update the out parameters in the `State`
      state.createdContractsInView = createdInView
      state.csmState = nextCsmState
      state.currentResolver = previousResolver

      transactionView
    }
  }

  /** Check that we correctly reconstruct the csm state machine
    * Canton does not distinguish between the different com.daml.lf.transaction.Transaction.KeyInactive forms right now
    */
  private def checkCsmStateMatchesView(
      csmState: ContractStateMachine[Unit]#State,
      transactionView: TransactionView,
      viewPosition: ViewPosition,
  )(implicit loggingContext: ErrorLoggingContext): Unit = {
    val viewGki = transactionView.globalKeyInputs.fmap(_.resolution)
    val stateGki = csmState.globalKeyInputs.fmap(_.toKeyMapping)
    ErrorUtil.requireState(
      viewGki == stateGki,
      show"""Failed to reconstruct the global key inputs for the view at position $viewPosition.
            |  Reconstructed: $viewGki
            |  Expected: $stateGki""".stripMargin,
    )
    val viewLocallyCreated = transactionView.createdContracts.keySet
    val stateLocallyCreated = csmState.locallyCreated
    ErrorUtil.requireState(
      viewLocallyCreated == stateLocallyCreated,
      show"Failed to reconstruct created contracts for the view at position $viewPosition.\n  Reconstructed: $viewLocallyCreated\n  Expected: $stateLocallyCreated",
    )
    // Reconstruction of the active ledger state works currently only in UCK mode
    if (uniqueContractKeys) {
      // The locally created contracts should also be computable in non-UCK mode from the view data.
      // However, `activeLedgerState` as a whole can be reconstructed only in UCK mode,
      // and therefore we check this only in UCK mode.
      val viewLocallyCreatedThisTimeline =
        transactionView.activeLedgerState.locallyCreatedThisTimeline
      val stateLocallyCreatedThisTimeline = csmState.activeState.locallyCreatedThisTimeline
      ErrorUtil.requireState(
        viewLocallyCreatedThisTimeline == stateLocallyCreatedThisTimeline,
        show"Failed to reconstruct non-rolled back created contracts for the view at position $viewPosition.\n  Reconstructed: $viewLocallyCreatedThisTimeline\n  Expected: $stateLocallyCreatedThisTimeline",
      )
      val viewConsumed = transactionView.activeLedgerState.consumedBy.keySet
      val stateConsumed = csmState.activeState.consumedBy.keySet
      ErrorUtil.requireState(
        viewConsumed == stateConsumed,
        show"Failed to reconstruct the consumed contracts for the view at position $viewPosition.\n  Reconstructed: $viewConsumed\n  Expected: $stateConsumed",
      )
      ErrorUtil.requireState(
        transactionView.activeLedgerState.keys == csmState.activeState.keys,
        show"Failed to reconstruct active key state for the view at position $viewPosition.\n  Reconstructed: ${transactionView.activeLedgerState.keys}\n  Expected: ${csmState.activeState.keys}",
      )
    }
  }

  /** The difference between `viewKeyInputs: Map[LfGlobalKey, KeyInput]` and
    * `subviewKeyResolutions: Map[LfGlobalKey, SerializableKeyResolution]`, computed as follows:
    * <ul>
    * <li>First, `keyVersionAndMaintainers` and `coreUsedOutsideRollback` are used to compute
    *     `viewKeyResolutions: Map[LfGlobalKey, SerializableKeyResolution]` from `viewKeyInputs`.
    *     A key `k` is considered rolled back, if it is rolled back everywhere, i.e.,
    *     `subviewKeyResolutions.get(k).forall(_.rolledBack)` and `k` is not contained in `coreUsedOutsideRollback`.</li>
    * <li>Second, the result consists of all key-resolution pairs that are in `viewKeyResolutions`,
    *     but not in `subviewKeyResolutions`.</li>
    * </ul>
    *
    * Note: The following argument depends on how this method is used.
    * It just sits here because we can then use scaladoc referencing.
    *
    * All resolved contract IDs in the map difference are core input contracts by the following argument:
    * Suppose that the map difference resolves a key `k` to a contract ID `cid`.
    * - In mode [[com.daml.lf.transaction.ContractKeyUniquenessMode.Strict]],
    *   the first node (in execution order) involving the key `k` determines the key's resolution for the view.
    *   So the first node `n` in execution order involving `k` is an Exercise, Fetch, or positive LookupByKey node.
    * - In mode [[com.daml.lf.transaction.ContractKeyUniquenessMode.Off]],
    *   the first by-key node (in execution order, including Creates) determines the global key input of the view.
    *   So the first by-key node `n` is an ExerciseByKey, FetchByKey, or positive LookupByKey node.
    * In particular, `n` cannot be a Create node because then the resolution for the view
    * would be [[com.daml.lf.transaction.ContractStateMachine.KeyInactive]].
    * If this node `n` is in the core of the view, then `cid` is a core input and we are done.
    * If this node `n` is in a proper subview, then the aggregated global key inputs
    * [[com.digitalasset.canton.data.TransactionView.globalKeyInputs]]
    * of the subviews resolve `k` to `cid` (as resolutions from earlier subviews are preferred)
    * and therefore the map difference does not resolve `k` at all.
    *
    * @param coreUsedOutsideRollback The set of keys that are used by core nodes that are not under a rollback within the view.
    * @return `Left(...)` if `viewKeyInputs` contains a key not in the `keyVersionAndMaintainers.keySet`
    * @throws java.lang.IllegalArgumentException if `subviewKeyResolutions.keySet` is not a subset of `viewKeyInputs.keySet`
    */
  private def resolvedKeys(
      viewKeyInputs: Map[LfGlobalKey, KeyInput],
      coreUsedOutsideRollback: Set[LfGlobalKey],
      keyVersionAndMaintainers: collection.Map[LfGlobalKey, (LfTransactionVersion, Set[LfPartyId])],
      subviewKeyResolutions: collection.Map[LfGlobalKey, SerializableKeyResolution],
  )(implicit
      traceContext: TraceContext
  ): Either[TransactionTreeConversionError, Map[LfGlobalKey, SerializableKeyResolution]] = {
    ErrorUtil.requireArgument(
      subviewKeyResolutions.keySet.subsetOf(viewKeyInputs.keySet),
      s"Global key inputs of subview not part of the global key inputs of the parent view. Missing keys: ${subviewKeyResolutions.keySet
        .diff(viewKeyInputs.keySet)}",
    )

    def resolutionFor(
        key: LfGlobalKey,
        rolledBack: Boolean,
        keyInput: KeyInput,
    ): Either[MissingContractKeyLookupError, SerializableKeyResolution] = {
      keyVersionAndMaintainers.get(key).toRight(MissingContractKeyLookupError(key)).map {
        case (lfVersion, maintainers) =>
          val resolution = keyInput match {
            case KeyActive(cid) => AssignedKey(cid, rolledBack)(lfVersion)
            case KeyCreate | NegativeKeyLookup => FreeKey(maintainers, rolledBack)(lfVersion)
          }
          resolution
      }
    }

    for {
      viewKeyResolutionSeq <- viewKeyInputs.toSeq
        .traverse { case (gkey, keyInput) =>
          val rolledBack = !coreUsedOutsideRollback.contains(gkey) &&
            subviewKeyResolutions.get(gkey).forall(_.rolledBack)
          resolutionFor(gkey, rolledBack, keyInput).map(gkey -> _)
        }
    } yield {
      MapsUtil.mapDiff(viewKeyResolutionSeq.toMap, subviewKeyResolutions)
    }
  }
}
