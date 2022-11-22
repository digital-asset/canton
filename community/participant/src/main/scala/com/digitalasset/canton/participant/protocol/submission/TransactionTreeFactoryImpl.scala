// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission

import cats.data.EitherT
import cats.syntax.bifunctor.*
import cats.syntax.either.*
import cats.syntax.functorFilter.*
import cats.syntax.parallel.*
import com.daml.ledger.participant.state.v2.SubmitterInfo
import com.daml.lf.CantonOnly
import com.daml.lf.data.Ref.PackageId
import com.daml.lf.value.ValueCoder
import com.digitalasset.canton.*
import com.digitalasset.canton.crypto.{HashOps, HmacOps, Salt, SaltSeed}
import com.digitalasset.canton.data.*
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.protocol.submission.TransactionTreeFactory.*
import com.digitalasset.canton.protocol.ContractIdSyntax.*
import com.digitalasset.canton.protocol.RollbackContext.RollbackScope
import com.digitalasset.canton.protocol.WellFormedTransaction.{WithSuffixes, WithoutSuffixes}
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.{DomainId, MediatorId, ParticipantId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.{ErrorUtil, LfTransactionUtil, MapsUtil, MonadUtil}
import com.digitalasset.canton.version.ProtocolVersion
import io.scalaland.chimney.dsl.*

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
abstract class TransactionTreeFactoryImpl(
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
  private val cantonContractIdVersion = CantonContractIdVersion.fromProtocolVersion(protocolVersion)

  protected type State <: TransactionTreeFactoryImpl.State

  protected def stateForSubmission(
      transactionSeed: SaltSeed,
      mediatorId: MediatorId,
      transactionUUID: UUID,
      ledgerTime: CantonTimestamp,
      nextSaltIndex: Int,
      keyResolver: LfKeyResolver,
  ): State

  protected def stateForValidation(
      mediatorId: MediatorId,
      transactionUUID: UUID,
      ledgerTime: CantonTimestamp,
      salts: Iterable[Salt],
      keyResolver: LfKeyResolver,
  ): State

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
      keyResolver: LfKeyResolver,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, TransactionTreeConversionError, GenTransactionTree] = {
    val metadata = transaction.metadata
    val state = stateForSubmission(
      transactionSeed,
      mediatorId,
      transactionUuid,
      metadata.ledgerTime,
      0,
      keyResolver,
    )

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

      checker = new DomainUsabilityCheckerVetting(
        domainId = domainId,
        snapshot = topologySnapshot,
        requiredPackagesByParty = requiredPackagesByParty(rootViewDecompositions),
        packageInfoService = packageInfoService,
        localParticipantId = submitterParticipant,
      )

      _ <- checker.isUsable.leftMap(_.transformInto[UnknownPackageError])

      rootViews <- createRootViews(rootViewDecompositions, state, contractOfId)
        .map(rootViews =>
          GenTransactionTree.tryCreate(cryptoOps)(
            submitterMetadata,
            commonMetadata,
            participantMetadata,
            MerkleSeq.fromSeq(cryptoOps)(rootViews, protocolVersion),
          )
        )
    } yield rootViews
  }

  @SuppressWarnings(Array("org.wartremover.warts.IterableOps"))
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
      keyResolver: LfKeyResolver,
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
    val state = stateForValidation(
      mediatorId,
      transactionUuid,
      metadata.ledgerTime,
      viewSalts,
      keyResolver,
    )

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

  /** compute set of required packages for each party */
  private def requiredPackagesByParty(
      rootViewDecompositions: Seq[TransactionViewDecomposition.NewView]
  ): Map[LfPartyId, Set[PackageId]] = {
    def requiredPackagesByParty(
        rootViewDecomposition: TransactionViewDecomposition.NewView,
        parentInformee: Set[LfPartyId],
    ): Map[LfPartyId, Set[PackageId]] = {
      val rootInformees = rootViewDecomposition.informees.map(_.party)
      val allInformees = parentInformee ++ rootInformees
      val childRequirements =
        rootViewDecomposition.tailNodes.foldLeft(Map.empty[LfPartyId, Set[PackageId]]) {
          case (acc, newView: TransactionViewDecomposition.NewView) =>
            MapsUtil.mergeMapsOfSets(acc, requiredPackagesByParty(newView, allInformees))
          case (acc, _) => acc
        }
      val rootPackages = rootViewDecomposition.allNodes
        .collect { case sameView: TransactionViewDecomposition.SameView =>
          LfTransactionUtil.nodeTemplates(sameView.lfNode).map(_.packageId).toSet
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
      contractOfId: SerializableContractOfId,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, TransactionTreeConversionError, Seq[TransactionView]] = {
    // Creating the views sequentially
    MonadUtil.sequentialTraverse(
      decompositions.zip(MerkleSeq.indicesFromSeq(decompositions.size))
    ) { case (rootView, index) =>
      createView(rootView, index +: ViewPosition.root, state, contractOfId)
    }
  }

  protected def createView(
      view: TransactionViewDecomposition.NewView,
      viewPosition: ViewPosition,
      state: State,
      contractOfId: SerializableContractOfId,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, TransactionTreeConversionError, TransactionView]

  protected def updateStateWithContractCreation(
      nodeId: LfNodeId,
      createNode: LfNodeCreate,
      viewParticipantDataSalt: Salt,
      viewPosition: ViewPosition,
      createIndex: Int,
      state: State,
  )(implicit traceContext: TraceContext): LfNodeCreate = {
    val cantonContractInst = checked(
      LfTransactionUtil
        .suffixContractInst(state.unicumOfCreatedContract, cantonContractIdVersion)(
          createNode.versionedCoinst
        )
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
    val (contractSalt, unicum) = unicumGenerator.generateSaltAndUnicum(
      domainId,
      state.mediatorId,
      state.transactionUUID,
      viewPosition,
      viewParticipantDataSalt,
      createIndex,
      state.ledgerTime,
      serializedCantonContractInst,
    )

    val contractId = cantonContractIdVersion.fromDiscriminator(discriminator, unicum)

    state.setUnicumFor(discriminator, unicum)

    val createdMetadata = LfTransactionUtil
      .createdContractIdWithMetadata(createNode)
      .getOrElse(throw new RuntimeException("Created metadata be defined for create node"))
      .metadata
    val createdInfo = SerializableContract(
      contractId = contractId,
      rawContractInstance = serializedCantonContractInst,
      metadata = createdMetadata,
      ledgerCreateTime = state.ledgerTime,
      contractSalt = Option.when(protocolVersion >= ProtocolVersion.v4)(contractSalt.unwrap),
    )
    state.setCreatedContractInfo(contractId, createdInfo)

    // No need to update the key because the key does not contain contract ids
    val suffixedNode =
      createNode.copy(coid = contractId, arg = cantonContractInst.unversioned.arg)
    state.addSuffixedNode(nodeId, suffixedNode)
    suffixedNode
  }

  protected def trySuffixNode(
      state: State
  )(idAndNode: (LfNodeId, LfActionNode)): LfActionNode = {
    val (nodeId, node) = idAndNode
    val suffixedNode = LfTransactionUtil
      .suffixNode(state.unicumOfCreatedContract, cantonContractIdVersion)(node)
      .fold(
        cid => throw new IllegalArgumentException(s"Invalid contract id $cid found"),
        Predef.identity,
      )
    state.addSuffixedNode(nodeId, suffixedNode)
    suffixedNode
  }

  protected def createActionDescription(
      actionNode: LfActionNode,
      seed: Option[LfHash],
  ): ActionDescription =
    checked(ActionDescription.tryFromLfActionNode(actionNode, seed, protocolVersion))

  protected def createViewCommonData(
      rootView: TransactionViewDecomposition.NewView,
      salt: Salt,
  ): ViewCommonData =
    ViewCommonData.create(cryptoOps)(rootView.informees, rootView.threshold, salt, protocolVersion)

  protected def createViewParticipantData(
      coreCreatedNodes: List[(LfNodeCreate, RollbackScope)],
      coreOtherNodes: List[(LfActionNode, RollbackScope)],
      childViews: Seq[TransactionView],
      createdContractInfo: collection.Map[LfContractId, SerializableContract],
      resolvedKeys: collection.Map[LfGlobalKey, SerializableKeyResolution],
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
    val createdInSubviewArchivedInCore = consumedInCore intersect createdInSubviews

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
      coreInputsWithInstances <- coreInputs.toSeq
        .parTraverse(cid => withInstance(cid).map(cid -> _))
        .leftWiden[TransactionTreeConversionError]
        .map(_.toMap)
      viewParticipantData <- EitherT
        .fromEither[Future](
          ViewParticipantData.create(cryptoOps)(
            coreInputs = coreInputsWithInstances,
            createdCore = created,
            createdInSubviewArchivedInCore = createdInSubviewArchivedInCore,
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
      packageService: PackageInfoService,
      uniqueContractKeys: Boolean,
      loggerFactory: NamedLoggerFactory,
  )(implicit ex: ExecutionContext): TransactionTreeFactoryImpl = {
    if (protocolVersion >= ProtocolVersion.v3) {
      new TransactionTreeFactoryImplV3(
        submitterParticipant,
        domainId,
        protocolVersion,
        contractSerializer,
        packageService,
        cryptoOps,
        uniqueContractKeys,
        loggerFactory,
      )
    } else {
      new TransactionTreeFactoryImplV2(
        submitterParticipant,
        domainId,
        protocolVersion,
        contractSerializer,
        packageService,
        cryptoOps,
        loggerFactory,
      )
    }
  }

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

  trait State {
    def mediatorId: MediatorId
    def transactionUUID: UUID
    def ledgerTime: CantonTimestamp

    protected def salts: Iterator[Salt]

    def nextSalt(): Either[TransactionTreeFactory.TooFewSalts, Salt] =
      Either.cond(salts.hasNext, salts.next(), TooFewSalts)

    def tryNextSalt()(implicit loggingContext: ErrorLoggingContext): Salt =
      nextSalt().valueOr { case TooFewSalts =>
        ErrorUtil.internalError(new IllegalStateException("No more salts available"))
      }

    private val suffixedNodesBuilder
        : mutable.Builder[(LfNodeId, LfActionNode), Map[LfNodeId, LfActionNode]] =
      Map.newBuilder[LfNodeId, LfActionNode]

    def suffixedNodes(): Map[LfNodeId, LfActionNode] = suffixedNodesBuilder.result()

    def addSuffixedNode(nodeId: LfNodeId, suffixedNode: LfActionNode): Unit = {
      suffixedNodesBuilder += nodeId -> suffixedNode
    }

    private val unicumOfCreatedContractMap: mutable.Map[LfHash, Unicum] = mutable.Map.empty

    def unicumOfCreatedContract: LfHash => Option[Unicum] = unicumOfCreatedContractMap.get

    def setUnicumFor(discriminator: LfHash, unicum: Unicum)(implicit
        loggingContext: ErrorLoggingContext
    ): Unit =
      unicumOfCreatedContractMap.put(discriminator, unicum).foreach { _ =>
        ErrorUtil.internalError(
          new IllegalStateException(
            s"Two contracts have the same discriminator: $discriminator"
          )
        )
      }

    /** All contracts created by a node that has already been processed. */
    val createdContractInfo: mutable.Map[LfContractId, SerializableContract] =
      mutable.Map.empty

    def setCreatedContractInfo(
        contractId: LfContractId,
        createdInfo: SerializableContract,
    )(implicit loggingContext: ErrorLoggingContext): Unit =
      createdContractInfo.put(contractId, createdInfo).foreach { _ =>
        ErrorUtil.internalError(
          new IllegalStateException(
            s"Two created contracts have the same contract id: $contractId"
          )
        )
      }

    /** Out parameter for contracts created in the view (including subviews). */
    @SuppressWarnings(Array("org.wartremover.warts.Var"))
    var createdContractsInView: collection.Set[LfContractId] = Set.empty
  }
}
