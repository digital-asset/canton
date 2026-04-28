// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission

import cats.data.EitherT
import cats.syntax.bifunctor.*
import cats.syntax.either.*
import cats.syntax.parallel.*
import com.digitalasset.canton.*
import com.digitalasset.canton.crypto.{HashOps, HmacOps, Salt, SaltSeed}
import com.digitalasset.canton.data.*
import com.digitalasset.canton.data.TransactionViewDecomposition.{NewView, SameView}
import com.digitalasset.canton.data.ViewConfirmationParameters.InvalidViewConfirmationParameters
import com.digitalasset.canton.ledger.participant.state.SubmitterInfo
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.protocol.submission.NextGenTransactionTreeFactory.*
import com.digitalasset.canton.participant.protocol.submission.TransactionTreeFactory.*
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.RollbackContext.RollbackScope
import com.digitalasset.canton.protocol.WellFormedTransaction.{
  WithAbsoluteSuffixes,
  WithoutSuffixes,
}
import com.digitalasset.canton.sequencing.protocol.MediatorGroupRecipient
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.{ParticipantId, PhysicalSynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.PackageConsumer.PackageResolver
import com.digitalasset.canton.util.collection.MapsUtil
import com.digitalasset.canton.util.{ContractHasher, ErrorUtil, LfTransactionUtil, MonadUtil}
import com.digitalasset.daml.lf.data.Ref.PackageId
import com.digitalasset.daml.lf.transaction.CreationTime
import io.scalaland.chimney.dsl.*

import java.util.UUID
import scala.annotation.{nowarn, tailrec}
import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.concurrent.ExecutionContext

class NextGenTransactionTreeFactory(
    participantId: ParticipantId,
    psid: PhysicalSynchronizerId,
    override val cantonContractIdVersion: CantonContractIdVersion,
    cryptoOps: HashOps & HmacOps,
    hasher: ContractHasher,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends TransactionTreeFactory
    with NamedLogging {

  private val protocolVersion = psid.protocolVersion
  private val contractIdSuffixer: ContractIdSuffixer =
    new ContractIdSuffixer(cryptoOps, cantonContractIdVersion)
  private val transactionViewDecompositionFactory = TransactionViewDecompositionFactory

  override def createTransactionTree(
      transaction: WellFormedTransaction[WithoutSuffixes],
      submitterInfo: SubmitterInfo,
      workflowId: Option[WorkflowId],
      mediator: MediatorGroupRecipient,
      transactionSeed: SaltSeed,
      transactionUuid: UUID,
      topologySnapshot: TopologySnapshot,
      contractOfId: ContractInstanceOfId,
      legacyKeyResolver: LfGlobalKeyMapping,
      maxSequencingTime: CantonTimestamp,
      validatePackageVettings: Boolean,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TransactionTreeConversionError, GenTransactionTree] = {
    val metadata = transaction.metadata
    val state = stateForSubmission(
      transactionSeed,
      mediator,
      transactionUuid,
      metadata.ledgerTime,
    )

    // Create salts
    val submitterMetadataSalt = checked(state.tryNextSalt())
    val commonMetadataSalt = checked(state.tryNextSalt())
    val participantMetadataSalt = checked(state.tryNextSalt())

    // Create fields
    val participantMetadata = ParticipantMetadata(cryptoOps)(
      metadata.ledgerTime,
      metadata.preparationTime,
      workflowId,
      participantMetadataSalt,
      protocolVersion,
    )

    val rootViewDecompositionsF =
      transactionViewDecompositionFactory.fromTransaction(
        topologySnapshot,
        transaction,
        RollbackContext.empty,
        Some(participantId.adminParty.toLf),
      )

    val commonMetadata = CommonMetadata
      .create(cryptoOps)(
        psid,
        mediator,
        commonMetadataSalt,
        transactionUuid,
      )

    for {
      submitterMetadata <- SubmitterMetadata
        .fromSubmitterInfo(cryptoOps)(
          submitterActAs = submitterInfo.actAs,
          submitterUserId = submitterInfo.userId,
          submitterCommandId = submitterInfo.commandId,
          submitterSubmissionId = submitterInfo.submissionId,
          submitterDeduplicationPeriod = submitterInfo.deduplicationPeriod,
          submittingParticipant = participantId,
          salt = submitterMetadataSalt,
          maxSequencingTime,
          externalAuthorization = submitterInfo.externallySignedSubmission.map(s =>
            ExternalAuthorization.create(
              s.signatures,
              s.version,
              s.maxRecordTime.map(CantonTimestamp(_)),
              protocolVersion,
            )
          ),
          protocolVersion = protocolVersion,
        )
        .leftMap(SubmitterMetadataError.apply)
        .toEitherT[FutureUnlessShutdown]

      rootViewDecompositions <- EitherT
        .liftF(rootViewDecompositionsF)

      _ = if (logger.underlying.isDebugEnabled) {
        val numRootViews = rootViewDecompositions.length
        val numViews = TransactionViewDecomposition.countNestedViews(rootViewDecompositions)
        logger.debug(
          s"Computed transaction tree with total=$numViews views for #root-nodes=$numRootViews"
        )
      }

      rootViews <- createRootViews(
        rootViewDecompositions,
        state,
        contractOfId,
        topologySnapshot,
      )

      _ <-
        if (validatePackageVettings) {
          val requiredPackageByParty = requiredPackagesByParty(rootViewDecompositions)
          UsableSynchronizers
            .checkPackagesVetted(
              synchronizerId = psid,
              snapshot = topologySnapshot,
              requiredPackagesByParty = requiredPackageByParty,
              ledgerTime = metadata.ledgerTime,
            )
            .leftMap[TransactionTreeConversionError](_.transformInto[UnknownPackageError])
        } else EitherT.rightT[FutureUnlessShutdown, TransactionTreeConversionError](())

    } yield {
      GenTransactionTree.tryCreate(cryptoOps)(
        submitterMetadata,
        commonMetadata,
        participantMetadata,
        MerkleSeq.fromSeq(cryptoOps, protocolVersion)(rootViews),
      )
    }
  }

  private def stateForSubmission(
      transactionSeed: SaltSeed,
      mediator: MediatorGroupRecipient,
      transactionUUID: UUID,
      ledgerTime: CantonTimestamp,
  ): State = {
    val salts = LazyList
      .from(0)
      .map(index => Salt.tryDeriveSalt(transactionSeed, index, cryptoOps))
    new State(mediator, transactionUUID, ledgerTime, salts.iterator)
  }

  /** @return set of packages required for command execution, by party */
  private def requiredPackagesByParty(
      rootViewDecompositions: Seq[TransactionViewDecomposition.NewView]
  ): Map[LfPartyId, Set[PackageId]] = {
    def requiredPackagesByParty(
        rootViewDecomposition: TransactionViewDecomposition.NewView,
        parentInformees: Set[LfPartyId],
    ): Map[LfPartyId, Set[PackageId]] = {
      val allInformees =
        parentInformees ++ rootViewDecomposition.viewConfirmationParameters.informees
      val childRequirements =
        rootViewDecomposition.tailNodes.foldLeft(Map.empty[LfPartyId, Set[PackageId]]) {
          case (acc, newView: TransactionViewDecomposition.NewView) =>
            MapsUtil.mergeMapsOfSets(acc, requiredPackagesByParty(newView, allInformees))
          case (acc, _) => acc
        }
      val rootPackages = rootViewDecomposition.allNodes
        .collect { case sameView: TransactionViewDecomposition.SameView =>
          sameView.lfNode.packageIds.toSet
        }
        .flatten
        .toSet

      allInformees.foldLeft(childRequirements) { case (acc, party) =>
        acc.updated(party, acc.getOrElse(party, Set()).union(rootPackages))
      }
    }

    rootViewDecompositions.foldLeft(Map.empty[LfPartyId, Set[PackageId]]) { case (acc, view) =>
      MapsUtil.mergeMapsOfSets(acc, requiredPackagesByParty(view, Set.empty))
    }
  }

  private def createRootViews(
      decompositions: Seq[TransactionViewDecomposition.NewView],
      state: State,
      contractOfId: ContractInstanceOfId,
      topologySnapshot: TopologySnapshot,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TransactionTreeConversionError, Seq[TransactionView]] = {

    // collect all contract ids referenced
    val preloadCids = Set.newBuilder[LfContractId]
    def go(view: TransactionViewDecomposition.NewView): Unit = {
      preloadCids.addAll(
        LfTransactionUtil.usedContractId(view.rootNode)
      )
      preloadCids.addAll(
        view.tailNodes
          .flatMap(x => LfTransactionUtil.usedContractId(x.lfNode))
      )
      view.childViews.foreach(go)
    }
    decompositions.foreach(go)

    // prefetch contracts using resolver. while we execute this on each contract individually,
    // we know that the contract loader will batch these requests together at the level of the contract
    // store
    EitherT
      .right(
        preloadCids
          .result()
          .toList
          .parTraverse(cid => contractOfId(cid).value.map((cid, _)))
          .map(_.toMap)
      )
      .flatMap { preloaded =>
        def fromPreloaded(
            cid: LfContractId
        ): EitherT[FutureUnlessShutdown, ContractLookupError, GenContractInstance] =
          preloaded.get(cid) match {
            case Some(value) => EitherT.fromEither[FutureUnlessShutdown](value)
            case None =>
              // if we ever missed a contract during prefetching due to mistake, then we can
              // fallback to the original loader
              logger.warn(s"Prefetch missed $cid")
              contractOfId(cid)
          }
        // Creating the views sequentially
        MonadUtil.sequentialTraverse(
          decompositions.zip(MerkleSeq.indicesFromSeq(decompositions.size))
        ) { case (rootView, index) =>
          createTransactionView(
            rootView,
            index +: ViewPosition.root,
            state,
            fromPreloaded,
            topologySnapshot,
          )
        }
      }
  }

  private def createTransactionView(
      view: TransactionViewDecomposition.NewView,
      viewPosition: ViewPosition,
      state: State,
      contractOfId: ContractInstanceOfId,
      topologySnapshot: TopologySnapshot,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TransactionTreeConversionError, TransactionView] = {

    // Process core nodes and subviews
    val coreCreatedBuilder =
      List.newBuilder[LfNodeCreate] // contract IDs have already been suffixed

    // contract IDs have not yet been suffixed
    val coreOtherBuilder = List.newBuilder[((LfNodeId, LfActionNode), RollbackScope)]

    val childViewsBuilder = Seq.newBuilder[TransactionView]

    @SuppressWarnings(Array("org.wartremover.warts.Var"))
    var viewKeyMaintainers = Map.empty[LfGlobalKey, LfVersioned[Set[LfPartyId]]]

    // TODO(#31527): SPM can this be made a mutable.Map
    val observedKeyContractIds = TrieMap.empty[LfGlobalKey, mutable.LinkedHashSet[LfContractId]]

    def buildResolvedKeys(
        createdContractIds: Set[LfContractId]
    ): Map[LfGlobalKey, LfVersioned[KeyResolutionWithMaintainers]] =
      // TODO(#31527): SPM will need to consider whether this is sufficient for NUCK
      viewKeyMaintainers.transform { (key, versioned) =>
        versioned.map { maintainers =>
          val cids = observedKeyContractIds
            .get(key)
            .map(observed => observed.iterator.filterNot(createdContractIds.contains).toSeq)
            .getOrElse(Seq.empty)
          KeyResolutionWithMaintainers(cids, maintainers)
        }
      }

    @SuppressWarnings(Array("org.wartremover.warts.Var"))
    var createIndex = 0

    val nbSubViews = view.allNodes.count {
      case _: TransactionViewDecomposition.NewView => true
      case _ => false
    }
    val subviewIndex = TransactionSubviews.indices(nbSubViews).iterator

    for {
      // Compute salts
      viewCommonDataSalt <- EitherT.fromEither[FutureUnlessShutdown](state.nextSalt())
      viewParticipantDataSalt <- EitherT.fromEither[FutureUnlessShutdown](state.nextSalt())

      _ <- MonadUtil.sequentialTraverse_(view.allNodes) {
        case childView: TransactionViewDecomposition.NewView =>
          // Compute subviews, recursively
          createTransactionView(
            childView,
            subviewIndex.next() +: viewPosition,
            state,
            contractOfId,
            topologySnapshot,
          )
            .map { v =>
              childViewsBuilder += v
              // TODO(#31527): SPM consider what to do when different nodes have different serialization versions
              v.viewParticipantData.tryUnwrap.keyResolution.foreach { case (key, resolution) =>
                viewKeyMaintainers = viewKeyMaintainers + (key -> resolution.map(_.maintainers))
                val observed =
                  observedKeyContractIds.getOrElseUpdate(key, mutable.LinkedHashSet.empty)
                observed ++= resolution.unversioned.contracts
              }
            }

        case TransactionViewDecomposition.SameView(lfActionNode, nodeId, rbContext) =>
          val rbScope = rbContext.rollbackScope

          for {

            suffixedNode <- lfActionNode match {
              case createNode: LfNodeCreate =>
                updateStateWithContractCreation(
                  nodeId,
                  createNode,
                  viewParticipantDataSalt,
                  viewPosition,
                  createIndex,
                  state,
                  topologySnapshot,
                ).map { suffixedNode =>
                  coreCreatedBuilder += suffixedNode
                  createIndex += 1
                  suffixedNode
                }
              case lfNode: LfActionNode =>
                val suffixedNode = trySuffixNode(state)(nodeId -> lfNode)
                coreOtherBuilder += ((nodeId, lfNode) -> rbScope)
                EitherT.pure[FutureUnlessShutdown, TransactionTreeConversionError](suffixedNode)
            }

            _ = suffixedNode.keyOpt.foreach { case LfGlobalKeyWithMaintainers(key, maintainers) =>
              viewKeyMaintainers += (key -> LfVersioned(
                suffixedNode.version,
                maintainers,
              ))
              val observed =
                observedKeyContractIds.getOrElseUpdate(key, mutable.LinkedHashSet.empty)
              // The returned order is most recently observed first
              observed ++= LfTransactionUtil.queriedContractIds(suffixedNode)
            }

          } yield ()
      }

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

      suffixedRootNode: LfActionNode = coreOtherNodes.headOption
        .map(_._1)
        .orElse(coreCreatedNodes.headOption)
        .getOrElse(
          throw new IllegalArgumentException(s"The received view has no core nodes. $view")
        )

      createdContractIds: Set[LfContractId] = childViews.foldLeft(
        coreCreatedNodes.map(_.coid).toSet
      )((acc, tv) => acc ++ tv.createdContracts.keySet)

      // Compute the parameters of the view
      seed = view.rootSeed
      packagePreference <- EitherT.fromEither[FutureUnlessShutdown](buildPackagePreference(view))
      actionDescription = createActionDescription(suffixedRootNode, seed, packagePreference)
      viewCommonData = createViewCommonData(view, viewCommonDataSalt).fold(
        ErrorUtil.internalError,
        identity,
      )

      viewParticipantData <- createViewParticipantData(
        coreCreatedNodes,
        coreOtherNodes,
        childViews,
        state.createdContractInfo,
        buildResolvedKeys(createdContractIds),
        actionDescription,
        viewParticipantDataSalt,
        contractOfId,
        view.rbContext,
      )

    } yield {
      // Compute the result
      val subviews = TransactionSubviews(childViews)(protocolVersion, cryptoOps)
      val transactionView =
        TransactionView.tryCreate(cryptoOps)(
          viewCommonData,
          viewParticipantData,
          subviews,
          protocolVersion,
        )
      transactionView
    }
  }

  private def updateStateWithContractCreation(
      nodeId: LfNodeId,
      createNode: LfNodeCreate,
      viewParticipantDataSalt: Salt,
      viewPosition: ViewPosition,
      createIndex: Int,
      state: State,
      topologySnapshot: TopologySnapshot,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, TransactionTreeConversionError, LfNodeCreate] = {

    val cantonContractInst = checked(
      LfTransactionUtil
        .suffixContractInst(state.suffixOfCreatedContract)(createNode.versionedCoinst)
        .valueOr(cid =>
          throw new IllegalStateException(
            s"Invalid contract id $cid found in contract instance of create node"
          )
        )
    ).unversioned
    val createNodeWithSuffixedArg = createNode.copy(arg = cantonContractInst.arg)

    val contractSalt = cantonContractIdVersion match {
      case _: CantonContractIdV1Version =>
        ContractSalt.createV1(cryptoOps)(
          state.transactionUUID,
          psid,
          state.mediator,
          viewParticipantDataSalt,
          createIndex,
          viewPosition,
        )
      case _: CantonContractIdV2Version =>
        ContractSalt.createV2(cryptoOps)(
          viewParticipantDataSalt,
          createIndex,
          viewPosition,
        )
    }
    val creationTime = cantonContractIdVersion match {
      case _: CantonContractIdV1Version =>
        CreationTime.CreatedAt(state.ledgerTime.toLf)
      case _: CantonContractIdV2Version =>
        CreationTime.Now
    }
    hasher
      .hash(
        createNodeWithSuffixedArg,
        contractIdSuffixer.contractHashingMethod,
        PackageResolver.crashOnMissingPackage(topologySnapshot, participantId, state.ledgerTime),
      )
      .map { contractHash =>
        val ContractIdSuffixer.RelativeSuffixResult(
          suffixedCreateNode,
          localContractId,
          relativeSuffix,
          authenticationData,
        ) = contractIdSuffixer
          .relativeSuffixForLocalContract(
            contractSalt,
            creationTime,
            createNodeWithSuffixedArg,
            contractHash,
          )
          .valueOr(err =>
            throw new IllegalArgumentException(s"Failed to compute contract ID: $err")
          )

        state.setSuffixFor(localContractId, relativeSuffix)

        val inst = LfFatContractInst.fromCreateNode(
          create = suffixedCreateNode,
          createTime = creationTime,
          authenticationData = authenticationData.toLfBytes,
        )
        val createdInfo = ContractInstance
          .create(inst)
          .valueOr(err =>
            throw new IllegalArgumentException(
              s"Failed to create contract instance well formed instrument: $err"
            )
          )

        state.setCreatedContractInfo(suffixedCreateNode.coid, createdInfo)
        state.addSuffixedNode(nodeId, suffixedCreateNode)
        suffixedCreateNode
      }
      .leftMap(error => FailedToHashContact(error))
  }

  private def trySuffixNode(
      state: State
  )(idAndNode: (LfNodeId, LfActionNode)): LfActionNode = {
    val (nodeId, node) = idAndNode
    val suffixedNode = LfTransactionUtil
      .suffixNode(state.suffixOfCreatedContract)(node)
      .valueOr(cid => throw new IllegalArgumentException(s"Invalid contract id $cid found"))
    state.addSuffixedNode(nodeId, suffixedNode)
    suffixedNode
  }

  private[submission] def buildPackagePreference(
      decomposition: TransactionViewDecomposition
  ): Either[ConflictingPackagePreferenceError, Set[LfPackageId]] = {

    def nodePref(n: LfActionNode): Set[(LfPackageName, LfPackageId)] = n match {
      case ex: LfNodeExercises if ex.interfaceId.isDefined =>
        Set(ex.packageName -> ex.templateId.packageId)
      case ex: LfNodeFetch if ex.interfaceId.isDefined =>
        Set(ex.packageName -> ex.templateId.packageId)
      case _ => Set.empty
    }

    @tailrec
    def go(
        decompositions: List[TransactionViewDecomposition],
        resolved: Set[(LfPackageName, LfPackageId)],
    ): Set[(LfPackageName, LfPackageId)] =
      decompositions match {
        case Nil =>
          resolved
        case (v: SameView) :: others =>
          go(others, resolved ++ nodePref(v.lfNode))
        case (v: NewView) :: others =>
          go(v.tailNodes.toList ::: others, resolved ++ nodePref(v.lfNode))
      }

    val preferences = go(List(decomposition), Set.empty)
    MapsUtil
      .toNonConflictingMap(preferences)
      .bimap(
        conflicts => ConflictingPackagePreferenceError(conflicts),
        map => map.values.toSet,
      )
  }

  private def createActionDescription(
      actionNode: LfActionNode,
      seed: Option[LfHash],
      packagePreference: Set[LfPackageId],
  ): ActionDescription =
    checked(
      ActionDescription.tryFromLfActionNode(actionNode, seed, packagePreference)
    )

  private def createViewCommonData(
      rootView: TransactionViewDecomposition.NewView,
      salt: Salt,
  ): Either[InvalidViewConfirmationParameters, ViewCommonData] =
    ViewCommonData.create(cryptoOps)(
      rootView.viewConfirmationParameters,
      salt,
      protocolVersion,
    )

  private def createViewParticipantData(
      coreCreatedNodes: List[LfNodeCreate],
      coreOtherNodes: List[(LfActionNode, RollbackScope)],
      childViews: Seq[TransactionView],
      createdContractInfo: collection.Map[LfContractId, NewContractInstance],
      resolvedKeys: Map[LfGlobalKey, LfVersioned[KeyResolutionWithMaintainers]],
      actionDescription: ActionDescription,
      salt: Salt,
      contractOfId: ContractInstanceOfId,
      rbContextCore: RollbackContext,
  ): EitherT[FutureUnlessShutdown, TransactionTreeConversionError, ViewParticipantData] = {

    val consumedInCore =
      coreOtherNodes.flatMap { case (an, _) =>
        LfTransactionUtil.consumedContractId(an)
      }.toSet
    val created = coreCreatedNodes.map { n =>
      val cid = n.coid
      // The preconditions of tryCreate are met as we have created all contract IDs of created contracts in this class.
      checked(
        CreatedContract
          .tryCreate(
            createdContractInfo(cid),
            consumedInCore = consumedInCore.contains(cid),
            rolledBack = false,
          )
      )
    }

    val createdInSubviewsSeq = for {
      childView <- childViews
      subView <- childView.flatten
      createdContract <- subView.viewParticipantData.tryUnwrap.createdCore
    } yield createdContract.contract.contractId

    val createdInSubviews = createdInSubviewsSeq.toSet
    val createdInSameViewOrSubviews = createdInSubviews ++ created.map(_.contract.contractId)

    val coreInputs = coreOtherNodes.view
      .flatMap { case (node, _) =>
        LfTransactionUtil.usedContractId(node)
      }
      .filterNot(createdInSameViewOrSubviews.contains)
    val createdInSubviewArchivedInCore = consumedInCore intersect createdInSubviews

    def withInstance(
        contractId: LfContractId
    ): EitherT[FutureUnlessShutdown, ContractLookupError, InputContract] = {
      val cons = consumedInCore.contains(contractId)
      createdContractInfo.get(contractId) match {
        case Some(info) =>
          EitherT.pure(InputContract(info, cons))
        case None =>
          contractOfId(contractId).map(c => InputContract(c, cons))
      }
    }

    for {
      coreInputsWithInstances <- coreInputs.toSeq
        .parTraverse(cid => withInstance(cid).map(cid -> _))
        .leftWiden[TransactionTreeConversionError]
        .map(_.toMap)
      viewParticipantData <- EitherT
        .fromEither[FutureUnlessShutdown](
          ViewParticipantData.create(cryptoOps)(
            coreInputs = coreInputsWithInstances,
            createdCore = created,
            createdInSubviewArchivedInCore = createdInSubviewArchivedInCore,
            resolvedKeys = resolvedKeys,
            actionDescription = actionDescription,
            rollbackContext = rbContextCore,
            salt = salt,
            protocolVersion = protocolVersion,
          )
        )
        .leftMap[TransactionTreeConversionError](ViewParticipantDataError.apply)
    } yield viewParticipantData
  }

  @SuppressWarnings(Array("org.wartremover.warts.IterableOps"))
  override def tryReconstruct(
      transaction: WellFormedTransaction[WithoutSuffixes],
      rootPosition: ViewPosition,
      mediator: MediatorGroupRecipient,
      submittingParticipantO: Option[ParticipantId],
      viewSalts: Iterable[Salt],
      transactionUuid: UUID,
      topologySnapshot: TopologySnapshot,
      contractOfId: ContractInstanceOfId,
      rbContext: RollbackContext,
      legacyKeyResolver: LfGlobalKeyMapping,
      absolutizer: ContractIdAbsolutizer,
  )(implicit traceContext: TraceContext): EitherT[
    FutureUnlessShutdown,
    TransactionTreeConversionError,
    (TransactionView, WellFormedTransaction[WithAbsoluteSuffixes]),
  ] = {
    /* We ship the root node of the view with suffixed contract IDs.
     * If this is a create node, reinterpretation will allocate an unsuffixed contract id instead of the one given in the node.
     * If this is an exercise node, the exercise result may contain unsuffixed contract ids created in the body of the exercise.
     * Accordingly, the reinterpreted transaction must first be suffixed before we can compare it against
     * the shipped views.
     */

    ErrorUtil.requireArgument(
      transaction.unwrap.roots.length == 1,
      s"Sub-action must have a single root node, but has ${transaction.unwrap.roots.iterator.mkString(", ")}",
    )

    val metadata = transaction.metadata
    val state = stateForValidation(
      mediator,
      transactionUuid,
      metadata.ledgerTime,
      viewSalts,
    )

    val decompositionsF =
      transactionViewDecompositionFactory.fromTransaction(
        topologySnapshot,
        transaction,
        rbContext,
        submittingParticipantO.map(_.adminParty.toLf),
      )

    val rolledBackEffect = rbContext.inRollback && transactionEffectful(transaction.unwrap)

    for {
      _ <- EitherT.cond[FutureUnlessShutdown](
        !rolledBackEffect,
        (),
        RolledBackEffect(rbContext, rootPosition),
      )
      decompositions <- EitherT.right(decompositionsF)
      decomposition = checked(decompositions.head)
      view <- createTransactionView(
        decomposition,
        rootPosition,
        state,
        contractOfId,
        topologySnapshot,
      )
      suffixedNodes = state.suffixedNodes() transform {
        // Recover the children
        case (nodeId, ne: LfNodeExercises) =>
          checked(transaction.unwrap.nodes(nodeId)) match {
            case ne2: LfNodeExercises =>
              ne.copy(children = ne2.children)
            case _: LfNode =>
              throw new IllegalStateException(
                "Node type changed while constructing the transaction tree"
              )
          }
        case (_, nl: LfLeafOnlyActionNode) => nl
      }

      // keep around the rollback nodes (not suffixed as they don't have a contract id), so that we don't orphan suffixed nodes.
      rollbackNodes = transaction.unwrap.nodes.collect { case tuple @ (_, _: LfNodeRollback) =>
        tuple
      }

      suffixedTx = LfVersionedTransaction(
        transaction.unwrap.version,
        suffixedNodes ++ rollbackNodes,
        transaction.unwrap.roots,
      )
      absolutizedTx <- EitherT
        .fromEither[FutureUnlessShutdown](absolutizer.absolutizeTransaction(suffixedTx))
        .leftMap(ContractIdAbsolutizationError(_): TransactionTreeConversionError)
    } yield {
      view -> checked(
        WellFormedTransaction.checkOrThrow(absolutizedTx, metadata, WithAbsoluteSuffixes)
      )
    }
  }

  private def stateForValidation(
      mediator: MediatorGroupRecipient,
      transactionUUID: UUID,
      ledgerTime: CantonTimestamp,
      salts: Iterable[Salt],
  ): State = new State(mediator, transactionUUID, ledgerTime, salts.iterator)

  override def saltsFromView(view: TransactionView): Iterable[Salt] = {
    val salts = Iterable.newBuilder[Salt]

    def addSaltsFrom(subview: TransactionView): Unit = {
      // Salts must be added in the same order as they are requested by checkView
      salts += checked(subview.viewCommonData.tryUnwrap).salt
      salts += checked(subview.viewParticipantData.tryUnwrap).salt
    }

    @tailrec
    @nowarn("msg=match may not be exhaustive")
    def go(stack: Seq[TransactionView]): Unit = stack match {
      case Seq() =>
      case subview +: toVisit =>
        addSaltsFrom(subview)
        subview.subviews.assertAllUnblinded(hash =>
          s"View ${subview.viewHash} contains an unexpected blinded subview $hash"
        )
        go(subview.subviews.unblindedElements ++ toVisit)
    }

    go(Seq(view))

    salts.result()
  }
}

object NextGenTransactionTreeFactory {

  // TODO(#31527): SPM add test for RolledBackEffect
  private def transactionEffectful(tx: LfVersionedTransaction): Boolean =
    tx.nodes.values.exists {
      case n: LfActionNode => LfTransactionUtil.isEffectful(n)
      case _: LfNodeRollback => false
    }

  private class State(
      val mediator: MediatorGroupRecipient,
      val transactionUUID: UUID,
      val ledgerTime: CantonTimestamp,
      val salts: Iterator[Salt],
  ) {

    private val suffixedNodesBuilder
        : mutable.Builder[(LfNodeId, LfActionNode), Map[LfNodeId, LfActionNode]] =
      Map.newBuilder[LfNodeId, LfActionNode]

    val createdContractInfo: mutable.Map[LfContractId, NewContractInstance] = mutable.Map.empty

    def nextSalt(): Either[TransactionTreeFactory.TooFewSalts, Salt] =
      Either.cond(salts.hasNext, salts.next(), TooFewSalts)

    def tryNextSalt()(implicit loggingContext: ErrorLoggingContext): Salt =
      nextSalt().valueOr { case TooFewSalts =>
        ErrorUtil.internalError(new IllegalStateException("No more salts available"))
      }

    def suffixedNodes(): Map[LfNodeId, LfActionNode] = suffixedNodesBuilder.result()

    def addSuffixedNode(nodeId: LfNodeId, suffixedNode: LfActionNode): Unit =
      suffixedNodesBuilder += nodeId -> suffixedNode

    private val suffixOfCreatedContractMap: mutable.Map[LocalContractId, RelativeContractIdSuffix] =
      mutable.Map.empty

    def suffixOfCreatedContract: LocalContractId => Option[RelativeContractIdSuffix] =
      suffixOfCreatedContractMap.get

    def setSuffixFor(prefix: LocalContractId, suffix: RelativeContractIdSuffix)(implicit
        loggingContext: ErrorLoggingContext
    ): Unit =
      suffixOfCreatedContractMap.put(prefix, suffix).foreach { _ =>
        ErrorUtil.internalError(
          new IllegalStateException(s"Two contracts have the same prefix: $prefix")
        )
      }

    def setCreatedContractInfo(
        contractId: LfContractId,
        createdInfo: NewContractInstance,
    )(implicit loggingContext: ErrorLoggingContext): Unit =
      createdContractInfo.put(contractId, createdInfo).foreach { _ =>
        ErrorUtil.internalError(
          new IllegalStateException(
            s"Two created contracts have the same contract id: $contractId"
          )
        )
      }

  }
}
