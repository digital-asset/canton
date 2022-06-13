// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission

import cats.data.EitherT
import cats.syntax.bifunctor._
import cats.syntax.either._
import cats.syntax.foldable._
import cats.syntax.functorFilter._
import cats.syntax.traverse._
import com.daml.ledger.participant.state.v2.SubmitterInfo
import com.daml.lf.CantonOnly
import com.daml.lf.data.Ref.PackageId
import com.daml.lf.value.{Value, ValueCoder}
import com.digitalasset.canton._
import com.digitalasset.canton.crypto.{HashOps, HmacOps, Salt, SaltSeed}
import com.digitalasset.canton.data.ViewPosition.ListIndex
import com.digitalasset.canton.data._
import com.digitalasset.canton.topology.{DomainId, MediatorId, ParticipantId}
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.admin.PackageService
import com.digitalasset.canton.participant.protocol.submission.TransactionTreeFactory._
import com.digitalasset.canton.protocol.ContractIdSyntax._
import com.digitalasset.canton.protocol.RollbackContext.RollbackScope
import com.digitalasset.canton.protocol.WellFormedTransaction.{WithSuffixes, WithoutSuffixes}
import com.digitalasset.canton.protocol._
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{ErrorUtil, LfTransactionUtil, MapsUtil, MonadUtil}
import com.digitalasset.canton.version.ProtocolVersion

import java.util.UUID
import scala.annotation.{nowarn, tailrec}
import scala.collection.immutable.SortedSet
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

/** Factory class that can create the [[com.digitalasset.canton.data.GenTransactionTree]]s from a
  * [[com.digitalasset.canton.protocol.WellFormedTransaction]].
  *
  * @param contractSerializer used to serialize contract instances for contract ids
  *                           Will only be used to serialize contract instances contained in well-formed transactions.
  * @param cryptoOps is used to derive Merkle hashes and contract ids [[com.digitalasset.canton.crypto.HashOps]]
  *                  as well as salts and contract ids [[com.digitalasset.canton.crypto.HmacOps]]
  */
class TransactionTreeFactoryImpl(
    submitterParticipant: ParticipantId,
    domainId: DomainId,
    protocolVersion: ProtocolVersion,
    contractSerializer: LfContractInst => SerializableRawContractInstance,
    packageInfoService: PackageInfoService,
    cryptoOps: HashOps with HmacOps,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends TransactionTreeFactory
    with NamedLogging {

  private val unicumGenerator = new UnicumGenerator(cryptoOps)

  private[this] class State private (
      val mediatorId: MediatorId,
      val transactionUUID: UUID,
      val ledgerTime: CantonTimestamp,
      private val salts: Iterator[Salt],
  ) {

    val unicumOfCreatedContract: mutable.Map[LfHash, Unicum] = mutable.Map.empty

    val createdContractInfo: mutable.Map[LfContractId, SerializableContract] = mutable.Map.empty

    /** Out parameter for contracts created in the view (including subviews). */
    @SuppressWarnings(Array("org.wartremover.warts.Var"))
    var createdContractsInView: collection.Set[LfContractId] = Set.empty

    /** Out parameter for contracts consumed in the view (including subviews). */
    @SuppressWarnings(Array("org.wartremover.warts.Var"))
    var consumedContractsInView: collection.Set[LfContractId] = Set.empty

    /** Out parameter for resolved keys in the view (including subviews).
      * Propagates the key resolution info from subviews to the parent view.
      */
    @SuppressWarnings(Array("org.wartremover.warts.Var"))
    var resolvedKeysInView: collection.Map[LfGlobalKey, Option[LfContractId]] = Map.empty

    private val suffixedNodesBuilder
        : mutable.Builder[(LfNodeId, LfActionNode), Map[LfNodeId, LfActionNode]] =
      Map.newBuilder[LfNodeId, LfActionNode]

    def tryNextSalt()(implicit traceContext: TraceContext): Salt = {
      ErrorUtil.requireState(salts.hasNext, "No more salts available")
      salts.next()
    }

    def nextSalt(): Either[TransactionTreeFactory.TooFewSalts, Salt] = {
      Either.cond(salts.hasNext, salts.next(), TooFewSalts)
    }

    def addSuffixedNode(nodeId: LfNodeId, suffixedNode: LfActionNode): Unit = {
      suffixedNodesBuilder += nodeId -> suffixedNode
    }

    def suffixedNodes(): Map[LfNodeId, LfActionNode] = suffixedNodesBuilder.result()
  }

  private[this] object State {
    def submission(
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

    def validation(
        mediatorId: MediatorId,
        transactionUUID: UUID,
        ledgerTime: CantonTimestamp,
        salts: Iterable[Salt],
    ): State = {
      new State(mediatorId, transactionUUID, ledgerTime, salts.iterator)
    }
  }

  override def createTransactionTree(
      transaction: WellFormedTransaction[WithoutSuffixes],
      submitterInfo: SubmitterInfo,
      confirmationPolicy: ConfirmationPolicy,
      workflowId: Option[WorkflowId],
      mediatorId: MediatorId,
      transactionSeed: SaltSeed,
      transactionUuid: UUID,
      topologySnapshot: TopologySnapshot,
      contractOfId: SerializableContractOfId,
      keyResolver: LfKeyResolver, // TODO(#9386) use this parameter
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, TransactionTreeConversionError, GenTransactionTree] = {
    val metadata = transaction.metadata
    val state =
      State.submission(transactionSeed, mediatorId, transactionUuid, metadata.ledgerTime, 0)

    // Create salts
    val submitterMetadataSalt = checked(state.tryNextSalt())
    val commonMetadataSalt = checked(state.tryNextSalt())
    val participantMetadataSalt = checked(state.tryNextSalt())

    // Create fields
    val commonMetadata = CommonMetadata(cryptoOps)(
      confirmationPolicy,
      domainId,
      mediatorId,
      commonMetadataSalt,
      transactionUuid,
      protocolVersion,
    )

    val participantMetadata = ParticipantMetadata(cryptoOps)(
      metadata.ledgerTime,
      metadata.submissionTime,
      workflowId,
      participantMetadataSalt,
      protocolVersion,
    )

    val rootViewDecompositionsF =
      TransactionViewDecomposition.fromTransaction(
        confirmationPolicy,
        topologySnapshot,
        transaction,
        RollbackContext.empty,
      )

    for {
      submitterMetadata <- EitherT.fromEither[Future](
        SubmitterMetadata
          .fromSubmitterInfo(cryptoOps)(
            submitterInfo,
            submitterParticipant,
            submitterMetadataSalt,
            protocolVersion,
          )
          .leftMap(SubmitterMetadataError)
      )
      rootViewDecompositions <- EitherT.liftF(rootViewDecompositionsF)
      _ <- checkPackagesAvailable(rootViewDecompositions, topologySnapshot)
      rootViews <- createRootViews(rootViewDecompositions, state, contractOfId)
        .map(rootViews => {
          GenTransactionTree(cryptoOps)(
            submitterMetadata,
            commonMetadata,
            participantMetadata,
            MerkleSeq.fromSeq(cryptoOps)(rootViews),
          )
        })
    } yield rootViews
  }

  @SuppressWarnings(Array("org.wartremover.warts.TraversableOps"))
  override def tryReconstruct(
      subaction: WellFormedTransaction[WithoutSuffixes],
      rootPosition: ViewPosition,
      confirmationPolicy: ConfirmationPolicy,
      mediatorId: MediatorId,
      viewSalts: Iterable[Salt],
      transactionUuid: UUID,
      topologySnapshot: TopologySnapshot,
      contractOfId: SerializableContractOfId,
      rbContext: RollbackContext,
      keyResolver: LfKeyResolver, // TODO(#9386) use this parameter
  )(implicit traceContext: TraceContext): EitherT[
    Future,
    TransactionTreeConversionError,
    (TransactionView, WellFormedTransaction[WithSuffixes]),
  ] = {
    /* We ship the root node of the view with suffixed contract IDs.
     * If this is a create node, reinterpretation will allocate an unsuffixed contract id instead of the one given in the node.
     * If this is an exercise node, the exercise result may contain unsuffixed contract ids created in the body of the exercise.
     * Accordingly, the reinterpreted transaction must first be suffixed before we can compare it against
     * the shipped views.
     * Ideally we'd ship only the inputs needed to reconstruct the transaction rather than computed data
     * such as exercise results and created contract IDs.
     */

    ErrorUtil.requireArgument(
      subaction.unwrap.roots.length == 1,
      s"Subaction must have a single root node, but has ${subaction.unwrap.roots.iterator.mkString(", ")}",
    )

    val metadata = subaction.metadata
    val state = State.validation(mediatorId, transactionUuid, metadata.ledgerTime, viewSalts)

    val decompositionsF =
      TransactionViewDecomposition.fromTransaction(
        confirmationPolicy,
        topologySnapshot,
        subaction,
        rbContext,
      )
    for {
      decompositions <- EitherT.liftF(decompositionsF)
      decomposition = checked(decompositions.head)
      view <- createView(decomposition, rootPosition, state, contractOfId)
    } yield {
      val suffixedNodes = state.suffixedNodes() transform {
        // Recover the children
        case (nodeId, ne: LfNodeExercises) =>
          checked(subaction.unwrap.nodes(nodeId)) match {
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
      val rollbackNodes = subaction.unwrap.nodes.collect { case tuple @ (_, _rn: LfNodeRollback) =>
        tuple
      }

      val suffixedTx =
        CantonOnly.setTransactionNodes(subaction.unwrap, suffixedNodes ++ rollbackNodes)
      view -> checked(WellFormedTransaction.normalizeAndAssert(suffixedTx, metadata, WithSuffixes))
    }
  }

  override def saltsFromView(view: TransactionViewTree): Iterable[Salt] = {
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
        val subviews = subview.subviews.map(tv => checked(tv.tryUnwrap))
        go(subviews ++ toVisit)
    }

    go(Seq(view.view))

    salts.result()
  }

  private def checkPackagesAvailable(
      rootViewDecompositions: Seq[TransactionViewDecomposition.NewView],
      topologySnapshot: TopologySnapshot,
  )(implicit traceContext: TraceContext): EitherT[Future, TransactionTreeConversionError, Unit] = {
    val requiredPerParty = rootViewDecompositions.foldLeft(Map.empty[LfPartyId, Set[PackageId]]) {
      case (acc, view) =>
        MapsUtil.mergeMapsOfSets(acc, requiredPackagesByParty(view, Set.empty))
    }

    def unknownPackages(
        participantId: ParticipantId,
        required: Set[PackageId],
    ): Future[List[PackageUnknownTo]] = {
      topologySnapshot.findUnvettedPackagesOrDependencies(participantId, required).value.flatMap {
        case Right(notVetted) =>
          notVetted.toList.traverse(packageId =>
            packageInfoService.getDescription(packageId).map { description =>
              PackageUnknownTo(
                packageId,
                description
                  .map(_.sourceDescription.unwrap)
                  .getOrElse("package does not exist on local node"),
                participantId,
              )
            }
          )
        case Left(missingPackageId) =>
          Future.successful(
            List(
              PackageUnknownTo(
                missingPackageId,
                "package missing on submitting participant!",
                submitterParticipant,
              )
            )
          )
      }
    }

    EitherT(for {
      requiredPerParticipant <- requiredPackagesByParticipant(requiredPerParty, topologySnapshot)
      unknownPackages <- requiredPerParticipant.toList
        .flatTraverse { case (participant, packages) =>
          unknownPackages(participant, packages)
        }
    } yield Either.cond(unknownPackages.isEmpty, (), UnknownPackageError(unknownPackages)))
  }

  /** compute set of required packages for each party */
  private def requiredPackagesByParty(
      rootViewDecomposition: TransactionViewDecomposition.NewView,
      parentInformeeParticipants: Set[LfPartyId],
  ): Map[LfPartyId, Set[PackageId]] = {
    val rootInformees = rootViewDecomposition.informees.map(_.party)
    val allInformees = parentInformeeParticipants ++ rootInformees
    val childRequirements =
      rootViewDecomposition.tailNodes.foldLeft(Map.empty[LfPartyId, Set[PackageId]]) {
        case (acc, newView: TransactionViewDecomposition.NewView) =>
          MapsUtil.mergeMapsOfSets(acc, requiredPackagesByParty(newView, allInformees))
        case (acc, _) => acc
      }
    val rootPackages = rootViewDecomposition.allNodes.collect {
      case sameView: TransactionViewDecomposition.SameView =>
        LfTransactionUtil.nodeTemplate(sameView.lfNode).packageId
    }.toSet

    allInformees.foldLeft(childRequirements) { case (acc, party) =>
      acc.updated(party, acc.getOrElse(party, Set()).union(rootPackages))
    }
  }

  private def requiredPackagesByParticipant(
      requiredPackages: Map[LfPartyId, Set[PackageId]],
      snapshot: TopologySnapshot,
  ): Future[Map[ParticipantId, Set[PackageId]]] = {
    requiredPackages.toList.foldM(Map.empty[ParticipantId, Set[PackageId]]) {
      case (acc, (party, packages)) =>
        for {
          // fetch all participants of this party
          participants <- snapshot.activeParticipantsOf(party)
        } yield {
          // add the required packages for this party to the set of required packages of this participant
          participants.foldLeft(acc) { case (res, (participantId, _)) =>
            res.updated(participantId, res.getOrElse(participantId, Set()).union(packages))
          }
        }
    }
  }

  private def createRootViews(
      decompositions: Seq[TransactionViewDecomposition.NewView],
      state: State,
      contractOfId: SerializableContractOfId,
  ): EitherT[Future, TransactionTreeConversionError, Seq[TransactionView]] = {
    // Creating the views sequentially
    MonadUtil.sequentialTraverse(
      decompositions.zip(MerkleSeq.indicesFromSeq(decompositions.size))
    ) { case (rootView, index) =>
      createView(rootView, index +: ViewPosition.root, state, contractOfId)
    }
  }

  private[this] def createView(
      view: TransactionViewDecomposition.NewView,
      viewPosition: ViewPosition,
      state: State,
      contractOfId: SerializableContractOfId,
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
    val resolvedKeysInCore = mutable.Map.empty[LfGlobalKey, KeyResolution]
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
                    val _ =
                      resolvedKeysInCore.getOrElseUpdate(gk, FreeKey(maintainers)(lfNode.version))
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
                      val _ =
                        resolvedKeysInCore.getOrElseUpdate(gk, AssignedKey(cid)(lfNode.version))
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
      coreOtherNodes = coreOtherBuilder.result().map { case (nodeInfo, rbc) =>
        (checked(trySuffixNode(state)(nodeInfo)), rbc)
      }
      childViews = childViewsBuilder.result()

      rootNode = coreOtherNodes.headOption
        .orElse(coreCreatedNodes.headOption)
        .map { case (node, _) => node }
        .getOrElse(
          throw new IllegalArgumentException(s"The received view has no core nodes. $view")
        )

      // Compute the parameters of the view
      seed = view.rootSeed
      rbContext = view.rbContext
      actionDescription = createActionDescription(rootNode, seed)
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
      TransactionView(cryptoOps)(viewCommonData, viewParticipantData, childViews)
    }

  }

  private[this] def updateStateWithContractCreation(
      nodeId: LfNodeId,
      createNode: LfNodeCreate,
      viewParticipantDataSalt: Salt,
      viewPosition: ViewPosition,
      createIndex: Int,
      state: State,
  ): LfNodeCreate = {
    val cantonContractInst = checked(
      LfTransactionUtil
        .suffixContractInst(state.unicumOfCreatedContract.get)(createNode.versionedCoinst)
        .fold(
          cid =>
            throw new IllegalStateException(
              s"Invalid contract id $cid found in contract instance of create node"
            ),
          Predef.identity,
        )
    )
    val serializedCantonContractInst = contractSerializer(cantonContractInst)

    val discriminator = createNode.coid match {
      case LfContractId.V1(discriminator, suffix) if suffix.isEmpty =>
        discriminator
      case _: LfContractId =>
        throw new IllegalStateException(
          s"Invalid contract id for created contract ${createNode.coid}"
        )
    }
    val unicum = unicumGenerator.generateUnicum(
      domainId,
      state.mediatorId,
      state.transactionUUID,
      viewPosition,
      viewParticipantDataSalt,
      createIndex,
      state.ledgerTime,
      serializedCantonContractInst,
    )
    val contractId = ContractId.fromDiscriminator(discriminator, unicum)

    state.unicumOfCreatedContract.put(discriminator, unicum).foreach { _ =>
      throw new IllegalStateException(s"Two contracts have the same discriminator: $discriminator")
    }

    val createdMetadata = LfTransactionUtil
      .createdContractIdWithMetadata(createNode)
      .getOrElse(throw new RuntimeException("Created metadata be defined for create node"))
      .metadata
    val createdInfo = SerializableContract(
      contractId,
      serializedCantonContractInst,
      createdMetadata,
      state.ledgerTime,
    )
    state.createdContractInfo.put(contractId, createdInfo).foreach { _ =>
      throw new IllegalStateException(
        s"Two created contracts have the same contract id: $contractId"
      )
    }

    // No need to update the key because the key does not contain contract ids
    val suffixedNode =
      createNode.copy(coid = contractId, arg = cantonContractInst.unversioned.arg)
    state.addSuffixedNode(nodeId, suffixedNode)
    suffixedNode
  }

  private[this] def trySuffixNode(
      state: State
  )(idAndNode: (LfNodeId, LfActionNode)): LfActionNode = {
    val (nodeId, node) = idAndNode
    val suffixedNode = LfTransactionUtil
      .suffixNode(state.unicumOfCreatedContract.get)(node)
      .fold(
        cid => throw new IllegalArgumentException(s"Invalid contract id $cid found"),
        Predef.identity,
      )
    state.addSuffixedNode(nodeId, suffixedNode)
    suffixedNode
  }

  private[this] def createActionDescription(
      actionNode: LfActionNode,
      seed: Option[LfHash],
  ): ActionDescription =
    checked(ActionDescription.tryFromLfActionNode(actionNode, seed))

  private[this] def createViewCommonData(
      rootView: TransactionViewDecomposition.NewView,
      salt: Salt,
  ): ViewCommonData =
    ViewCommonData.create(cryptoOps)(rootView.informees, rootView.threshold, salt, protocolVersion)

  private def createViewParticipantData(
      coreCreatedNodes: List[(LfNodeCreate, RollbackScope)],
      coreOtherNodes: List[(LfActionNode, RollbackScope)],
      childViews: Seq[TransactionView],
      createdContractInfo: collection.Map[LfContractId, SerializableContract],
      resolvedKeys: collection.Map[LfGlobalKey, KeyResolution],
      actionDescription: ActionDescription,
      salt: Salt,
      contractOfId: SerializableContractOfId,
      rbContextCore: RollbackContext,
  ): EitherT[Future, TransactionTreeConversionError, ViewParticipantData] = {

    val consumedInCore =
      coreOtherNodes.mapFilter { case (an, rbScopeOther) =>
        // to be considered consumed, archival has to happen in the same rollback scope,
        if (rbScopeOther == rbContextCore.rollbackScope)
          LfTransactionUtil.consumedContractId(an)
        else None
      }.toSet

    val created = coreCreatedNodes.map { case (n, rbScopeCreate) =>
      val cid = n.coid
      // The preconditions of tryCreate are met as we have created all contract IDs of created contracts in this class.
      checked(
        CreatedContract
          .tryCreate(
            createdContractInfo(cid),
            consumedInCore = consumedInCore.contains(cid),
            rolledBack = rbScopeCreate != rbContextCore.rollbackScope,
          )
      )
    }
    // "Allowing for" duplicate contract creations under different rollback nodes is not necessary as contracts in
    // different positions receive different contract_ids as ensured by WellformednessCheck "Create nodes have unique
    // contract ids".

    val createdInSubviewsSeq = for {
      childView <- childViews
      subView <- childView.flatten
      createdContract <- subView.viewParticipantData.tryUnwrap.createdCore
    } yield createdContract.contract.contractId

    val createdInSubviews = createdInSubviewsSeq.toSet
    val createdInSameViewOrSubviews = createdInSubviewsSeq ++ created.map(_.contract.contractId)

    val usedCore = SortedSet(
      coreOtherNodes
        .mapFilter { case (node, _) => LfTransactionUtil.usedContractIdWithMetadata(node) }
        .map(contractIdWithMetadata => contractIdWithMetadata.unwrap): _*
    )
    val coreInputs = usedCore -- createdInSameViewOrSubviews
    val archivedFromSubviews = consumedInCore intersect createdInSubviews

    def withInstance(
        contractId: LfContractId
    ): EitherT[Future, ContractLookupError, InputContract] = {
      val cons = consumedInCore.contains(contractId)
      createdContractInfo.get(contractId) match {
        case Some(info) =>
          EitherT.pure(InputContract(info, cons))
        case None =>
          contractOfId(contractId).map(serializableContract =>
            InputContract(serializableContract, cons)
          )
      }
    }

    for {
      coreInputsWithInstances <- coreInputs.toList
        .traverse(cid => withInstance(cid).map(cid -> _))
        .leftWiden[TransactionTreeConversionError]
        .map(_.toMap)
      viewParticipantData <- EitherT
        .fromEither[Future](
          ViewParticipantData.create(cryptoOps)(
            coreInputs = coreInputsWithInstances,
            createdCore = created,
            archivedFromSubviews = archivedFromSubviews,
            resolvedKeys = resolvedKeys.toMap,
            actionDescription = actionDescription,
            rollbackContext = rbContextCore,
            salt = salt,
            protocolVersion = protocolVersion,
          )
        )
        .leftMap[TransactionTreeConversionError](ViewParticipantDataError)
    } yield viewParticipantData
  }
}

object TransactionTreeFactoryImpl {

  /** Creates a `TransactionTreeFactory`. */
  def apply(
      submitterParticipant: ParticipantId,
      domainId: DomainId,
      protocolVersion: ProtocolVersion,
      cryptoOps: HashOps with HmacOps,
      packageService: PackageService,
      loggerFactory: NamedLoggerFactory,
  )(implicit ex: ExecutionContext): TransactionTreeFactoryImpl =
    new TransactionTreeFactoryImpl(
      submitterParticipant,
      domainId,
      protocolVersion,
      contractSerializer,
      packageService,
      cryptoOps,
      loggerFactory,
    )

  private[submission] def contractSerializer(
      contractInst: LfContractInst
  ): SerializableRawContractInstance =
    SerializableRawContractInstance
      .create(contractInst)
      .leftMap { err: ValueCoder.EncodeError =>
        throw new IllegalArgumentException(
          s"Unable to serialize contract instance, although it is contained in a well-formed transaction.\n$err\n$contractInst"
        )
      }
      .merge

}
