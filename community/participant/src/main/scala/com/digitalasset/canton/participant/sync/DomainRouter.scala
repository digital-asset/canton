// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.sync

import cats.data.EitherT
import cats.implicits._
import com.daml.ledger.participant.state.v2.{SubmitterInfo, TransactionMeta}
import com.daml.nonempty.NonEmpty
import com.daml.nonempty.NonEmptyColl._
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.lifecycle.FlagCloseable
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.domain.DomainAliasManager
import com.digitalasset.canton.participant.protocol.TransactionProcessor.{
  TransactionSubmissionError,
  TransactionSubmitted,
}
import com.digitalasset.canton.participant.protocol.transfer.TransferOutProcessingSteps
import com.digitalasset.canton.participant.store.ActiveContractStore.Active
import com.digitalasset.canton.participant.store.DomainConnectionConfigStore
import com.digitalasset.canton.participant.sync.DomainRouter.{
  ContractData,
  ContractsDomainData,
  DomainRank,
  TransferArgs,
}
import com.digitalasset.canton.participant.sync.TransactionRoutingError.ConfigurationErrors.{
  MultiDomainSupportNotEnabled,
  SubmissionDomainNotReady,
}
import com.digitalasset.canton.participant.sync.TransactionRoutingError.MalformedInputErrors.InvalidDomainAlias
import com.digitalasset.canton.participant.sync.TransactionRoutingError.TopologyErrors.{
  NotConnectedToAllContractDomains,
  SubmitterAlwaysStakeholder,
  UnknownContractDomains,
}
import com.digitalasset.canton.participant.sync.TransactionRoutingError.{
  AutomaticTransferForTransactionFailure,
  MalformedInputErrors,
  RoutingInternalError,
  TopologyErrors,
}
import com.digitalasset.canton.protocol.WellFormedTransaction.WithoutSuffixes
import com.digitalasset.canton.protocol._
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.transaction.ParticipantPermission.Submission
import com.digitalasset.canton.topology.{DomainId, ParticipantId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{EitherTUtil, LfTransactionUtil}
import com.digitalasset.canton.version.{SourceProtocolVersion, TargetProtocolVersion}
import com.digitalasset.canton.{DomainAlias, LfKeyResolver, LfPartyId, LfWorkflowId}

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

/** The domain router routes transaction submissions from upstream to the right domain.
  *
  * Submitted transactions are inspected for which domains are involved based on the location of the involved contracts.
  *
  * @param submit Submits the given transaction to the given domain.
  *               The outer future completes after the submission has been registered as in-flight.
  *               The inner future completes after the submission has been sequenced or if it will never be sequenced.
  */
class DomainRouter(
    participantId: ParticipantId,
    connectedDomains: () => Set[DomainId],
    domainOfContracts: Seq[LfContractId] => Future[Map[LfContractId, DomainId]],
    recoveredDomainOfAlias: DomainAlias => Option[DomainId],
    domainsOfSubmittersAndInformees: (
        Set[LfPartyId],
        Set[LfPartyId],
    ) => EitherT[Future, TransactionRoutingError, NonEmpty[Set[DomainId]]],
    priorityOfDomain: DomainId => Int,
    submit: DomainId => (
        SubmitterInfo,
        TransactionMeta,
        LfKeyResolver,
        WellFormedTransaction[WithoutSuffixes],
        TraceContext,
    ) => EitherT[Future, TransactionRoutingError, Future[TransactionSubmitted]],
    transfer: TransferArgs => EitherT[Future, TransactionRoutingError, Unit],
    snapshot: DomainId => Option[TopologySnapshot],
    autoTransferTransaction: Boolean,
    override protected val timeouts: ProcessingTimeout,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends FlagCloseable
    with NamedLogging {

  import com.digitalasset.canton.util.ShowUtil._

  def submitTransaction(
      submitterInfo: SubmitterInfo,
      transactionMeta: TransactionMeta,
      keyResolver: LfKeyResolver,
      transaction: LfSubmittedTransaction,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, TransactionRoutingError, Future[TransactionSubmitted]] = {

    for {
      // do some sanity checks for invalid inputs (to not conflate these with broken nodes)
      _ <- EitherT.fromEither[Future](
        WellFormedTransaction.sanityCheckInputs(transaction).leftMap {
          case WellFormedTransaction.InvalidInput.InvalidParty(err) =>
            MalformedInputErrors.InvalidPartyIdentifier.Error(err)
        }
      )

      metadata <- EitherT
        .fromEither[Future](TransactionMetadata.fromTransactionMeta(transactionMeta))
        .leftMap(RoutingInternalError.IllformedTransaction)

      wfTransaction <- EitherT.fromEither[Future](
        WellFormedTransaction
          .normalizeAndCheck(transaction, metadata, WithoutSuffixes)
          .leftMap(RoutingInternalError.IllformedTransaction)
      )

      lfSubmitters <- submitterInfo.actAs
        .traverse(submitter =>
          EitherT
            .fromEither[Future](LfPartyId.fromString(submitter))
            .leftMap[TransactionRoutingError](MalformedInputErrors.InvalidSubmitter.Error)
        )
        .map(_.toSet)

      inputContractMetadata: Set[WithContractMetadata[LfContractId]] = LfTransactionUtil
        .inputContractIdsWithMetadata(wfTransaction.unwrap)

      inputContractsDomainData <- EitherT.liftF(
        ContractsDomainData.create(domainOfContracts, inputContractMetadata)
      )

      inputDomains = inputContractsDomainData.domains
      informees = LfTransactionUtil.informees(transaction)

      isMultiDomainTx <- EitherT.liftF(isMultiDomainTx(inputDomains, informees))

      domainRankTarget <-
        if (!isMultiDomainTx) {
          logger.debug(
            s"Choosing the domain as single-domain workflow for ${submitterInfo.commandId}"
          )
          chooseDomainForSingleDomain(
            lfSubmitters,
            transactionMeta.workflowId,
            inputContractsDomainData,
            informees,
          )
        } else if (autoTransferTransaction) {
          logger.debug(
            s"Choosing the domain as multi-domain workflow for ${submitterInfo.commandId}"
          )
          chooseDomainForMultiDomain(
            inputContractsDomainData,
            lfSubmitters,
            informees,
            transactionMeta,
          )
        } else {
          EitherT.leftT[Future, DomainRank](
            MultiDomainSupportNotEnabled.Error(
              inputContractsDomainData.domains
            ): TransactionRoutingError
          )
        }
      _ <- transferContracts(domainRankTarget)
      _ = logger.info(s"submitting the transaction to the ${domainRankTarget.domainId}")
      transactionSubmittedF <- submit(domainRankTarget.domainId)(
        submitterInfo,
        transactionMeta,
        keyResolver,
        wfTransaction,
        traceContext,
      )
    } yield transactionSubmittedF
  }

  private def allInformeesOnDomain(
      informees: Set[LfPartyId]
  )(domainId: DomainId): Future[Boolean] = {
    snapshot(domainId) match {
      case None => Future.successful(false)
      case Some(topologySnapshot) =>
        topologySnapshot.allHaveActiveParticipants(informees, _.isActive).value.map(_.isRight)
    }
  }

  private def chooseDomainForMultiDomain(
      inputContractsDomainData: ContractsDomainData,
      submitters: Set[LfPartyId],
      informees: Set[LfPartyId],
      transactionMeta: TransactionMeta,
  )(implicit traceContext: TraceContext): EitherT[Future, TransactionRoutingError, DomainRank] =
    for {
      _ <- checkValidityOfMultiDomain(inputContractsDomainData, submitters)
      domainRankTarget <- chooseDomainIdForMultiDomain(
        transactionMeta,
        inputContractsDomainData.withDomainData,
        submitters,
        informees,
      )
    } yield domainRankTarget

  private def transferContracts(
      domainRankTarget: DomainRank
  )(implicit traceContext: TraceContext): EitherT[Future, TransactionRoutingError, Unit] = {
    if (domainRankTarget.transfers.nonEmpty) {
      logger.info(
        s"Automatic transaction transfer into domain ${domainRankTarget.domainId}"
      )
      domainRankTarget.transfers.toSeq.traverse_ { case (cid, (lfParty, sourceDomainId)) =>
        transfer(
          TransferArgs(
            sourceDomainId,
            domainRankTarget.domainId,
            lfParty,
            cid,
            traceContext,
          )
        )
      }
    } else {
      EitherT.pure[Future, TransactionRoutingError](())
    }
  }

  /** We have a multi-domain transaction if the input contracts are on more than one domain
    * or if the (single) input domain does not host all informees
    * (because we will need to transfer the contracts to a domain that that *does* host all informees.
    * Transactions without input contracts are always single-domain.
    */
  private def isMultiDomainTx(
      inputDomains: Set[DomainId],
      informees: Set[LfPartyId],
  ): Future[Boolean] = {
    if (inputDomains.sizeCompare(2) >= 0) Future.successful(true)
    else
      inputDomains.toList
        .traverse(allInformeesOnDomain(informees)(_))
        .map(!_.forall(identity))
  }

  private def checkValidityOfMultiDomain(
      inputContractsDomainData: ContractsDomainData,
      submitters: Set[LfPartyId],
  ): EitherT[Future, TransactionRoutingError, Unit] = {

    val allContractsHaveDomainData: Boolean = inputContractsDomainData.withoutDomainData.isEmpty
    val connectedDomains: Set[DomainId] = this.connectedDomains()
    val contractData = inputContractsDomainData.withDomainData
    val contractsDomainNotConnected = contractData.filterNot { contractData =>
      connectedDomains.contains(contractData.domain)
    }

    // Check that at least one submitter is a stakeholder so that we can transfer the contract if needed. This check
    // is overly strict on behalf of contracts that turn out not to need to be transferred.
    val submitterNotBeingStakeholder = contractData.filter { data =>
      data.stakeholders.intersect(submitters).isEmpty
    }

    for {
      // Check: submitter
      _ <- EitherTUtil.condUnitET[Future](
        submitterNotBeingStakeholder.isEmpty,
        SubmitterAlwaysStakeholder.Error(submitterNotBeingStakeholder.map(_.id)),
      )

      // Check: all contracts have domain data
      _ <- EitherTUtil.condUnitET[Future](
        allContractsHaveDomainData, {
          val ids = inputContractsDomainData.withoutDomainData.map(_.show)
          UnknownContractDomains.Error(ids.toList): TransactionRoutingError
        },
      )

      // Check: connected domains
      _ <- EitherTUtil.condUnitET[Future](
        contractsDomainNotConnected.isEmpty, {
          val contractsAndDomains: Map[String, DomainId] = contractsDomainNotConnected.map {
            contractData => contractData.id.show -> contractData.domain
          }.toMap

          NotConnectedToAllContractDomains.Error(contractsAndDomains): TransactionRoutingError
        },
      )

    } yield ()
  }

  private def pickDomainIdAndComputeTransfers(
      contracts: Seq[ContractData],
      submitters: Set[LfPartyId],
      informees: Set[LfPartyId],
      domains: Set[DomainId],
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, TransactionRoutingError, DomainRank] = {
    for {
      rankedDomains <- domains.toList
        .traverse(targetDomain => computeDomainRank(contracts, targetDomain, submitters))

      // Priority of domain
      // Number of Transfers if we use this domain
      // pick according to least amount of transfers
      chosen = rankedDomains.minOption
      domainRank <- chosen match {
        case None =>
          logger.info(
            s"Unable to find a common domain for the following set of informees ${informees}"
          )
          EitherT.leftT[Future, DomainRank](
            AutomaticTransferForTransactionFailure.Failed(
              "No domain found for the given informees to perform transaction"
            ): TransactionRoutingError
          )
        case Some(domainRank) => EitherT.rightT[Future, TransactionRoutingError](domainRank)
      }
    } yield domainRank
  }

  /** Choose the appropriate domain for a transaction.
    * The domain is chosen as follows:
    * 1. Domain whose alias equals the workflow id
    * 2. The domain with the smaller number of transfers on which all informees have active participants
    */
  private def chooseDomainIdForMultiDomain(
      transactionMeta: TransactionMeta,
      contracts: Seq[ContractData],
      submitters: Set[LfPartyId],
      informees: Set[LfPartyId],
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, TransactionRoutingError, DomainRank] = {
    val connected = this.connectedDomains()
    val maybeWorkflowId = transactionMeta.workflowId
    for {
      domainIdO <- chooseDomainWithAliasEqualToWorkflowId(maybeWorkflowId)
      informeeDomainsId <- domainsOfSubmittersAndInformees(submitters, informees)
      res <- domainIdO
        .filter(connected.contains) match {
        case None => // pick a suitable domain id
          pickDomainIdAndComputeTransfers(
            contracts,
            submitters,
            informees,
            informeeDomainsId,
          )
        // Use the domain given by the workflow ID
        case Some(targetDomain) =>
          if (!informeeDomainsId.contains(targetDomain)) {
            EitherT.leftT(
              TransactionRoutingError.ConfigurationErrors.InvalidWorkflowId
                .NotAllInformeeAreOnDomain(targetDomain, informeeDomainsId)
            ): EitherT[Future, TransactionRoutingError, DomainRank]
          } else computeDomainRank(contracts, targetDomain, submitters)
      }
    } yield res
  }

  // Includes check that submitting party has a participant with submission rights on source and target domain
  private def computeDomainRank(
      contracts: Seq[ContractData],
      targetDomain: DomainId,
      submitters: Set[LfPartyId],
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[Future, TransactionRoutingError, DomainRank] = {
    val ts = snapshot(targetDomain)
    val maybeTransfers: EitherT[Future, TransactionRoutingError, Seq[
      Option[(LfContractId, (LfPartyId, DomainId))]
    ]] =
      contracts.traverse { c =>
        if (c.domain == targetDomain) EitherT.fromEither[Future](Right(None))
        else {
          for {
            sourceSnapshot <- EitherT.fromEither[Future](
              snapshot(c.domain).toRight(
                AutomaticTransferForTransactionFailure.Failed(s"No snapshot for domain ${c.domain}")
              )
            )
            targetSnapshot <- EitherT.fromEither[Future](
              ts.toRight(
                AutomaticTransferForTransactionFailure.Failed(
                  s"No snapshot for domain $targetDomain"
                )
              )
            )
            submitter <- findSubmitterThatCanTransferContract(
              sourceSnapshot,
              targetSnapshot,
              c,
              submitters,
            )
          } yield Some(c.id -> (submitter, c.domain))
        }
      }
    maybeTransfers.map(transfers =>
      DomainRank(
        transfers.flattenOption.toMap,
        priorityOfDomain(targetDomain),
        targetDomain,
      )
    )
  }

  private def findSubmitterThatCanTransferContract(
      sourceSnapshot: TopologySnapshot,
      targetSnapshot: TopologySnapshot,
      c: ContractData,
      submitters: Set[LfPartyId],
  )(implicit traceContext: TraceContext): EitherT[Future, TransactionRoutingError, LfPartyId] = {

    // Building the transfer out requests lets us check whether contract can be transferred to target domain
    def go(
        submitters: List[LfPartyId],
        errAccum: List[String] = List.empty,
    ): EitherT[Future, String, LfPartyId] = {
      submitters match {
        case Nil =>
          EitherT.leftT(show"Cannot transfer contract ${c.id}: ${errAccum.mkString(",")}")
        case submitter :: rest =>
          TransferOutProcessingSteps
            .transferOutRequestData(
              participantId,
              submitter,
              c.stakeholders,
              sourceSnapshot,
              targetSnapshot,
              logger,
            )
            .biflatMap(
              left => go(rest, errAccum :+ show"Submitter ${submitter} cannot transfer: $left"),
              _canTransfer => EitherT.rightT(submitter),
            )
      }
    }

    go(submitters.intersect(c.stakeholders).toList).leftMap(errors =>
      AutomaticTransferForTransactionFailure.Failed(errors)
    )
  }

  /** Choose the appropriate domain for a transaction.
    * The domain is chosen as follows:
    * 1. Domain whose alias equals the workflow id
    * 1. Domain of all input contracts (fail if there is more than one)
    * 3. An arbitrary domain to which the submitter can submit and on which all informees have active participants
    */
  private def chooseDomainForSingleDomain(
      submitters: Set[LfPartyId],
      maybeWorkflowId: Option[LfWorkflowId],
      inputContractsDomainData: ContractsDomainData,
      informees: Set[LfPartyId],
  ): EitherT[Future, TransactionRoutingError, DomainRank] = {
    for {
      domainSpecifiedByWorkFlowIdO <- chooseDomainWithAliasEqualToWorkflowId(maybeWorkflowId)
      inputContractDomainId <- chooseDomainOfInputContracts(inputContractsDomainData)
      informeeDomainsId <- domainsOfSubmittersAndInformees(submitters, informees)

      domainId <- EitherT.fromEither[Future](
        domainSpecifiedByWorkFlowIdO match {
          case Some(domainSpecifiedByWorkFlowId) =>
            if (
              inputContractDomainId.nonEmpty && !inputContractDomainId
                .contains(domainSpecifiedByWorkFlowId)
            )
              Left(
                TransactionRoutingError.ConfigurationErrors.InvalidWorkflowId
                  .InputContractsNotOnDomain(domainSpecifiedByWorkFlowId, inputContractDomainId)
              )
            else if (!informeeDomainsId.contains(domainSpecifiedByWorkFlowId)) {
              Left(
                TransactionRoutingError.ConfigurationErrors.InvalidWorkflowId
                  .NotAllInformeeAreOnDomain(domainSpecifiedByWorkFlowId, informeeDomainsId)
              )
            } else Right(domainSpecifiedByWorkFlowId)

          case None =>
            Right(
              inputContractDomainId.getOrElse(
                informeeDomainsId.minBy1(id => DomainRank(Map.empty, priorityOfDomain(id), id))
              )
            )
        }
      ): EitherT[Future, TransactionRoutingError, DomainId]

    } yield DomainRank(Map.empty, priorityOfDomain(domainId), domainId)
  }

  private def chooseDomainOfInputContracts(
      inputContractsDomainData: ContractsDomainData
  ): EitherT[Future, TransactionRoutingError, Option[DomainId]] =
    inputContractsDomainData.domains.size match {
      case 0 | 1 => EitherT.rightT(inputContractsDomainData.domains.headOption)
      // Input contracts reside on different domains
      // Fail..
      case _ =>
        EitherT.leftT(
          RoutingInternalError
            .InputContractsOnDifferentDomains(inputContractsDomainData.domains)
        )
    }

  private def chooseDomainWithAliasEqualToWorkflowId(
      maybeWorkflowId: Option[LfWorkflowId]
  ): EitherT[Future, InvalidDomainAlias.Error, Option[DomainId]] = {
    for {
      domainAliasO <- maybeWorkflowId
        .traverse(DomainAlias.create(_))
        .toEitherT[Future]
        .leftMap(InvalidDomainAlias.Error)
      res = domainAliasO.flatMap(recoveredDomainOfAlias(_))
    } yield res
  }

}

private[sync] object DomainRouter {
  def apply(
      connectedDomainsMap: mutable.Map[DomainId, SyncDomain],
      domainConnectionConfigStore: DomainConnectionConfigStore,
      domainAliasManager: DomainAliasManager,
      participantId: ParticipantId,
      autoTransferTransaction: Boolean,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit ec: ExecutionContext, traceContext: TraceContext): DomainRouter = {

    new DomainRouter(
      participantId,
      () => getConnectedDomains(connectedDomainsMap),
      domainOfContract(connectedDomainsMap),
      recoveredDomainOfAlias(connectedDomainsMap, domainAliasManager),
      domainsOfSubmittersAndInformees(participantId, connectedDomainsMap),
      priorityOfDomain(domainConnectionConfigStore, domainAliasManager),
      submit(connectedDomainsMap),
      transfer(connectedDomainsMap),
      domainSnapshot(connectedDomainsMap),
      autoTransferTransaction = autoTransferTransaction,
      timeouts,
      loggerFactory,
    )
  }

  private def getConnectedDomains(
      connectedDomainsMap: mutable.Map[DomainId, SyncDomain]
  ): Set[DomainId] = {
    connectedDomainsMap.collect {
      case (domainId, syncService) if syncService.readyForSubmission => domainId
    }.toSet
  }

  private def domainOfContract(connectedDomains: collection.Map[DomainId, SyncDomain])(
      coids: Seq[LfContractId]
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): Future[Map[LfContractId, DomainId]] = {
    type Acc = (Seq[LfContractId], Map[LfContractId, DomainId])
    connectedDomains
      .collect {
        // only look at domains that are ready for submission
        case (_, syncDomain: SyncDomain) if syncDomain.readyForSubmission => syncDomain
      }
      .toList
      .foldM[Future, Acc]((coids, Map.empty[LfContractId, DomainId]): Acc) {
        // if there are no more cids for which we don't know the domain, we are done
        case ((pending, acc), _) if pending.isEmpty => Future.successful((pending, acc))
        case ((pending, acc), syncDomain) =>
          // grab the approximate state and check if the contract is currently active on the given domain
          syncDomain.ephemeral.requestTracker.getApproximateStates(pending).map { res =>
            val done = acc ++ res.collect {
              case (cid, status) if status.status == Active => (cid, syncDomain.domainId)
            }
            (pending.filterNot(cid => done.contains(cid)), done)
          }
      }
      .map(_._2)
  }

  private def recoveredDomainOfAlias(
      connectedDomains: collection.Map[DomainId, SyncDomain],
      domainAliasManager: DomainAliasManager,
  )(domainAlias: DomainAlias): Option[DomainId] = {
    domainAliasManager
      .domainIdForAlias(domainAlias)
      .filter(domainId => connectedDomains.get(domainId).exists(_.ready))
  }

  private def domainSnapshot(
      connectedDomains: collection.Map[DomainId, SyncDomain]
  )(domain: DomainId)(implicit traceContext: TraceContext): Option[TopologySnapshot] = {
    val sdO = connectedDomains.get(domain)
    sdO.map(sd => sd.topologyClient.currentSnapshotApproximation)
  }

  private def domainsOfSubmittersAndInformees(
      localParticipantId: ParticipantId,
      connectedDomains: collection.Map[DomainId, SyncDomain],
  )(submitters: Set[LfPartyId], informees: Set[LfPartyId])(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[Future, TransactionRoutingError, NonEmpty[Set[DomainId]]] = {
    val readyDomains = connectedDomains.collect {
      case r @ (_, domain) if domain.readyForSubmission => r
    }

    for {
      submitterDomainIds <- domainsOfSubmitters(localParticipantId, readyDomains, submitters)
      informeeDomainIds <- domainsOfInformees(readyDomains, informees)
      commonDomainIds <- EitherT.fromEither[Future](
        NonEmpty
          .from(submitterDomainIds.intersect(informeeDomainIds))
          .toRight(
            TopologyErrors.NoCommonDomain.Error(submitters, informees): TransactionRoutingError
          )
      )
    } yield commonDomainIds
  }

  private def domainsOfSubmitters(
      localParticipantId: ParticipantId,
      candidateDomains: collection.Map[DomainId, SyncDomain],
      submitters: Set[LfPartyId],
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[Future, TransactionRoutingError, Set[DomainId]] = {
    val snapshotsOfDomain = candidateDomains.map { case (domainId, domain) =>
      domainId -> domain.topologyClient.currentSnapshotApproximation
    }
    val submittersL = submitters.toList

    val domainsKnowingAllSubmittersF = snapshotsOfDomain.toList
      .traverse { case (domain, snapshot) =>
        submittersL.traverse(snapshot.hostedOn(_, localParticipantId)).map { res =>
          (domain, res.forall(_.exists(_.permission == Submission)))
        }
      }
      .map(_.collect {
        case (domainId, allSubmittersAreAllowedToSubmit) if allSubmittersAreAllowedToSubmit =>
          domainId
      }.toSet)

    EitherT(domainsKnowingAllSubmittersF.map { domainsKnowingAllSubmitters =>
      if (domainsKnowingAllSubmitters.isEmpty) {
        Left(submitters.toList match {
          case one :: Nil => TopologyErrors.NoDomainOnWhichAllSubmittersCanSubmit.NotAllowed(one)
          case some => TopologyErrors.NoDomainOnWhichAllSubmittersCanSubmit.NoSuitableDomain(some)
        })
      } else
        Right(domainsKnowingAllSubmitters)
    })
  }

  private def domainsOfInformees(
      candidateDomains: collection.Map[DomainId, SyncDomain],
      informees: Set[LfPartyId],
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[Future, TransactionRoutingError, Set[DomainId]] = {
    val informeesList = informees.toList

    def partiesHostedOnDomain(snapshot: TopologySnapshot): Future[Set[LfPartyId]] =
      informeesList
        .traverseFilter { partyId =>
          snapshot
            .isHostedByAtLeastOneParticipantF(partyId, _.permission.isActive)
            .map(if (_) Some(partyId) else None)
        }
        .map(_.toSet)

    val hostedOfDomainF = candidateDomains.toList.traverse { case (domainId, domain) =>
      val snapshot = domain.topologyClient.currentSnapshotApproximation
      partiesHostedOnDomain(snapshot).map { hosted =>
        domainId -> hosted
      }
    }
    val ret = hostedOfDomainF.map { hostedOfDomain =>
      val unknownInformees = informees.filter { informee =>
        hostedOfDomain.forall { case (_, hosted) =>
          !hosted.contains(informee)
        }
      }

      if (unknownInformees.isEmpty) {
        val informeeDomainIds = hostedOfDomain.collect {
          case (domainId, hosted) if informees.forall(hosted.contains) => domainId
        }.toSet
        if (informeeDomainIds.isEmpty) {
          Left(TopologyErrors.InformeesNotActive.Error(candidateDomains.keys.toSet, informees))
        } else {
          Right(informeeDomainIds)
        }
      } else {
        Left(TopologyErrors.UnknownInformees.Error(unknownInformees))
      }
    }
    EitherT(ret)
  }

  private def priorityOfDomain(
      domainConnectionConfigStore: DomainConnectionConfigStore,
      domainAliasManager: DomainAliasManager,
  )(domainId: DomainId): Int = {
    val maybePriority = for {
      domainAlias <- domainAliasManager.aliasForDomainId(domainId)
      config <- domainConnectionConfigStore.get(domainAlias).toOption.map(_.config)
    } yield config.priority

    // If the participant is disconnected from the domain while this code is evaluated,
    // we may fail to determine the priority.
    // Choose the lowest possible priority, as it will be unlikely that a submission to the domain succeeds.
    maybePriority.getOrElse(Integer.MIN_VALUE)
  }

  /** We intentially do not store the `keyResolver` in [[com.digitalasset.canton.protocol.WellFormedTransaction]]
    * because we do not (yet) need to deal with merging the mappings
    * in [[com.digitalasset.canton.protocol.WellFormedTransaction.merge]].
    */
  private def submit(connectedDomains: collection.Map[DomainId, SyncDomain])(domainId: DomainId)(
      submitterInfo: SubmitterInfo,
      transactionMeta: TransactionMeta,
      keyResolver: LfKeyResolver,
      tx: WellFormedTransaction[WithoutSuffixes],
      traceContext: TraceContext,
  )(implicit
      ec: ExecutionContext
  ): EitherT[Future, TransactionRoutingError, Future[TransactionSubmitted]] =
    for {
      domain <- EitherT.fromEither[Future](
        connectedDomains
          .get(domainId)
          .toRight[TransactionRoutingError](SubmissionDomainNotReady.Error(domainId))
      )
      _ <- EitherT.cond[Future](domain.ready, (), SubmissionDomainNotReady.Error(domainId))
      result <- wrapSubmissionError(domain.domainId)(
        domain.submitTransaction(submitterInfo, transactionMeta, keyResolver, tx)(traceContext)
      )
    } yield result

  private def transfer(connectedDomains: collection.Map[DomainId, SyncDomain])(
      args: TransferArgs
  )(implicit ec: ExecutionContext): EitherT[Future, TransactionRoutingError, Unit] = {
    val TransferArgs(sourceDomain, targetDomain, submittingParty, contractId, _traceContext) = args
    implicit val traceContext = _traceContext

    val transfer = for {
      sourceSyncDomain <- EitherT.fromEither[Future](
        connectedDomains.get(sourceDomain).toRight("Not connected to the source domain")
      )

      targetSyncDomain <- EitherT.fromEither[Future](
        connectedDomains.get(targetDomain).toRight("Not connected to the target domain")
      )

      _unit <- EitherT
        .cond[Future](sourceSyncDomain.ready, (), "The source domain is not ready for submissions")

      outResult <- sourceSyncDomain
        .submitTransferOut(
          submittingParty,
          contractId,
          targetDomain,
          TargetProtocolVersion(targetSyncDomain.staticDomainParameters.protocolVersion),
        )
        .leftMap(_.toString)
      outStatus <- EitherT.right[String](outResult.transferOutCompletionF)
      _outApprove <- EitherT.cond[Future](
        outStatus.code == com.google.rpc.Code.OK_VALUE,
        (),
        s"The transfer out for ${outResult.transferId} failed with status $outStatus",
      )

      _unit <- EitherT
        .cond[Future](targetSyncDomain.ready, (), "The target domain is not ready for submission")

      inResult <- targetSyncDomain
        .submitTransferIn(
          submittingParty,
          outResult.transferId,
          SourceProtocolVersion(sourceSyncDomain.staticDomainParameters.protocolVersion),
        )
        .leftMap[String](err => s"Transfer in failed with error ${err}")

      inStatus <- EitherT.right[String](inResult.transferInCompletionF)
      _inApprove <- EitherT.cond[Future](
        inStatus.code == com.google.rpc.Code.OK_VALUE,
        (),
        s"The transfer in for ${outResult.transferId} failed with verdict $inStatus",
      )
    } yield ()

    transfer.leftMap[TransactionRoutingError](str =>
      AutomaticTransferForTransactionFailure.Failed(str)
    )
  }

  private def wrapSubmissionError[T](domainId: DomainId)(
      eitherT: EitherT[Future, TransactionSubmissionError, T]
  )(implicit ec: ExecutionContext): EitherT[Future, TransactionRoutingError, T] =
    eitherT.leftMap(subm => TransactionRoutingError.SubmissionError(domainId, subm))

  case class DomainRank(
      transfers: Map[LfContractId, (LfPartyId, DomainId)],
      priority: Int,
      domainId: DomainId,
  )

  object DomainRank {
    //The highest priority domain should be picked first, so negate the priority
    implicit val domainRanking: Ordering[DomainRank] =
      Ordering.by(x => (x.transfers.size, -x.priority, x.domainId))
  }
  case class ContractData(id: LfContractId, domain: DomainId, stakeholders: Set[LfPartyId])

  case class TransferArgs(
      sourceDomain: DomainId,
      targetDomain: DomainId,
      submitter: LfPartyId,
      contract: LfContractId,
      traceContext: TraceContext,
  )

  case class ContractsDomainData(
      withDomainData: Seq[ContractData],
      withoutDomainData: Seq[LfContractId],
  ) {
    val domains: Set[DomainId] = withDomainData.map(_.domain).toSet
  }

  object ContractsDomainData {
    def create(
        domainOfContracts: Seq[LfContractId] => Future[Map[LfContractId, DomainId]],
        inputContractMetadata: Set[WithContractMetadata[LfContractId]],
    )(implicit ec: ExecutionContext): Future[ContractsDomainData] = {
      val inputDataSeq = inputContractMetadata.toSeq
      domainOfContracts(inputDataSeq.map(_.unwrap))
        .map { domainMap =>
          // Collect domains of input contracts, ignoring contracts that cannot be found in the ACS.
          // Such contracts need to be ignored, because they could be divulged contracts.
          val (good, bad) = inputDataSeq.map { metadata =>
            val coid = metadata.unwrap
            domainMap.get(coid) match {
              case Some(domainId) =>
                Left(ContractData(coid, domainId, metadata.metadata.stakeholders))
              case None => Right(coid)
            }
          }.separate
          ContractsDomainData(good, bad)
        }
    }
  }
}
