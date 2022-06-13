// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.sync

import cats.data.{EitherT, OptionT}
import cats.implicits._
import com.daml.ledger.participant.state.v2.{SubmitterInfo, TransactionMeta}
import com.daml.nonempty.NonEmpty
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
  AutoTransferDomainRank,
  ContractData,
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
import com.digitalasset.canton.topology.{DomainId, ParticipantId}
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.transaction.ParticipantPermission.Submission
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{ErrorUtil, LfTransactionUtil}
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
    domainOfContract: LfContractId => OptionT[Future, DomainId],
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

  /** Submits a transaction to an appropriate domain.
    * The domain is chosen as follows:
    * 1. Domain of all input contracts (fail if there is more than one)
    * 2. Domain whose alias equals the workflow id
    * 3. An arbitrary domain to which the submitter can submit and on which all informees have active participants
    */
  def submitTransaction(
      submitterInfo: SubmitterInfo,
      transactionMeta: TransactionMeta,
      keyResolver: LfKeyResolver,
      transaction: LfSubmittedTransaction,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, TransactionRoutingError, Future[TransactionSubmitted]] = {

    for {
      metadata <- EitherT
        .fromEither[Future](TransactionMetadata.fromTransactionMeta(transactionMeta))
        .leftMap(RoutingInternalError.IllformedTransaction)

      // do some sanity checks for invalid inputs (to not conflate these with broken nodes)
      _ <- EitherT.fromEither[Future](
        WellFormedTransaction.sanityCheckInputs(transaction).leftMap {
          case WellFormedTransaction.InvalidInput.InvalidParty(err) =>
            MalformedInputErrors.InvalidPartyIdentifier.Error(err)
        }
      )

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

      inputContractMetadata = LfTransactionUtil.inputContractIdsWithMetadata(wfTransaction.unwrap)

      domainOfInputContracts <- EitherT.liftF(inputContractMetadata.toList.traverse { metadata =>
        val contractId = metadata.unwrap
        domainOfContract(contractId).value.map(dO =>
          contractId -> dO.map(d => ContractData(contractId, d, metadata.metadata.stakeholders))
        )
      })

      inputDomains = domainOfInputContracts.mapFilter { case (_, contractDataO) =>
        contractDataO.map(_.domain)
      }.toSet
      contractsOnOneDomain = inputDomains.sizeCompare(2) < 0

      informees = LfTransactionUtil.informees(transaction)
      inputDomainsHostAllInformees <- EitherT.liftF(
        inputDomains.toList
          .traverse(allInformeesOnDomain(informees)(_))
          .map(_.forall(identity))
      )

      // We have a multi-domain transaction if the input contracts are on more than one domain or if the (single) input
      // domain does not host all informees (because we will need to transfer the contracts to a domain that
      // that *does* host all informees).
      isMultiDomainTx = !contractsOnOneDomain || !inputDomainsHostAllInformees

      transactionSubmittedF <-
        if (isMultiDomainTx && autoTransferTransaction) {
          logger.debug(s"Submitting as multi-domain ${submitterInfo.commandId} ")
          checkAndMaybeSubmitMultiDomain(
            domainOfInputContracts,
            submitterInfo,
            transactionMeta,
            keyResolver,
            wfTransaction,
            lfSubmitters,
            informees,
          )
        } else if (isMultiDomainTx) {
          EitherT.leftT[Future, Future[TransactionSubmitted]](
            MultiDomainSupportNotEnabled.Error(inputDomains): TransactionRoutingError
          )
        } else {
          logger.debug(s"Submitting as single-domain ${submitterInfo.commandId} ")
          submitTransactionSingleDomain(
            submitterInfo,
            transactionMeta,
            keyResolver,
            wfTransaction,
            lfSubmitters,
            inputContractMetadata.map(m => m.unwrap),
            informees,
          )
        }
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

  private def checkAndMaybeSubmitMultiDomain(
      domainOfInputContracts: List[(LfContractId, Option[ContractData])],
      submitterInfo: SubmitterInfo,
      transactionMeta: TransactionMeta,
      keyResolver: LfKeyResolver,
      wfTransaction: WellFormedTransaction[WithoutSuffixes],
      submitters: Set[LfPartyId],
      informees: Set[LfPartyId],
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, TransactionRoutingError, Future[TransactionSubmitted]] = {

    ErrorUtil.requireArgument(
      autoTransferTransaction,
      "multi-domain path called whilst not enabled?",
    )

    val connectedToDomains = domainOfInputContracts.forall {
      case (_contractId, Some(data)) => this.connectedDomains().contains(data.domain)
      case _ => false
    }

    val contractData = domainOfInputContracts.mapFilter(_._2)

    // Check that at least one submitter is a stakeholder so that we can transfer the contract if needed. This check
    // is overly strict on behalf of contracts that turn out not to need to be transferred.
    val submitterNotBeingStakeholder = contractData.filter { data =>
      data.stakeholders.intersect(submitters).isEmpty
    }

    for {
      _atLeastOneQualifyingDomain <- domainsOfSubmittersAndInformees(submitters, informees)
      transactionSubmitted <-
        if (submitterNotBeingStakeholder.isEmpty && connectedToDomains)
          submitTransactionMultiDomain(
            wfTransaction,
            contractData,
            submitterInfo,
            submitters,
            transactionMeta,
            keyResolver,
            informees,
          )
        else {
          val error = if (submitterNotBeingStakeholder.nonEmpty) {
            SubmitterAlwaysStakeholder.Error(submitterNotBeingStakeholder.map(_.id))
          } else {
            val contractsAndDomains = domainOfInputContracts
              .mapFilter(
                _._2
                  .filterNot(x => connectedDomains().contains(x.domain))
                  .map(x => show"${x.id}" -> x.domain)
              )
              .toMap

            if (contractsAndDomains.nonEmpty) {
              NotConnectedToAllContractDomains.Error(contractsAndDomains): TransactionRoutingError
            } else {
              val ids = domainOfInputContracts.filter(_._2.isEmpty).map(c => show"${c._1}")
              UnknownContractDomains.Error(ids): TransactionRoutingError
            }
          }
          EitherT.leftT[Future, Future[TransactionSubmitted]](error)
        }
    } yield transactionSubmitted
  }

  def submitTransactionSingleDomain(
      submitterInfo: SubmitterInfo,
      transactionMeta: TransactionMeta,
      keyResolver: LfKeyResolver,
      wfTransaction: WellFormedTransaction[WithoutSuffixes],
      submitters: Set[LfPartyId],
      inputContractIds: Set[LfContractId],
      informees: Set[LfPartyId],
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, TransactionRoutingError, Future[TransactionSubmitted]] = {
    for {
      domainId <- chooseDomain(submitters, transactionMeta.workflowId, inputContractIds, informees)
      _ = logger.info(
        s"Submitting transaction to domain $domainId (commandId=${submitterInfo.commandId})..."
      )
      transactionSubmitted <- submit(domainId)(
        submitterInfo,
        transactionMeta,
        keyResolver,
        wfTransaction,
        traceContext,
      )
    } yield transactionSubmitted
  }

  private def submitViaDomain(
      targetDomain: DomainId,
      wfTransaction: WellFormedTransaction[WithoutSuffixes],
      contracts: List[ContractData],
      submitterInfo: SubmitterInfo,
      submitters: Set[LfPartyId],
      transactionMeta: TransactionMeta,
      keyResolver: LfKeyResolver,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, TransactionRoutingError, Future[TransactionSubmitted]] = {
    transfersToDomain(contracts, targetDomain, submitters)
      .leftMap { reason =>
        AutomaticTransferForTransactionFailure.Failed(
          s"All contracts cannot be transferred to the specified target domain $targetDomain: $reason"
        )
      }
      .flatMap { case (_domainId, transfers) =>
        transferThenSubmit(
          targetDomain,
          contracts,
          wfTransaction,
          submitterInfo,
          transfers,
          transactionMeta,
          keyResolver,
        )
      }
  }

  private def submitPickingADomain(
      connected: Set[DomainId],
      wfTransaction: WellFormedTransaction[WithoutSuffixes],
      contracts: List[ContractData],
      submitterInfo: SubmitterInfo,
      submitters: Set[LfPartyId],
      transactionMeta: TransactionMeta,
      keyResolver: LfKeyResolver,
      informees: Set[LfPartyId],
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, TransactionRoutingError, Future[TransactionSubmitted]] = {

    val rankedDomainsF = connected.toList
      .flatTraverse(targetDomain =>
        transfersToDomain(contracts, targetDomain, submitters).value.map(_.toOption.toList.map {
          case (domainId, transfers) =>
            AutoTransferDomainRank(transfers, priorityOfDomain(domainId), domainId)
        })
      )

    // Priority of domain
    // Number of Transfers if we use this domain

    // pick according to least amount of transfers, but only pick the ones where all informees are connected to
    val chosenF = EitherT.right(rankedDomainsF.flatMap { rankedDomains =>
      rankedDomains
        .sorted(DomainRouter.domainRanking)
        .findM(rank => allInformeesOnDomain(informees)(rank.name))
    })

    chosenF.flatMap { chosen =>
      chosen.fold {
        logger.info(
          s"Unable to find a common domain for the following set of informees ${informees}"
        )
        EitherT.leftT[Future, Future[TransactionSubmitted]](
          AutomaticTransferForTransactionFailure.Failed(
            "No domain found for the given informees to perform transaction"
          ): TransactionRoutingError
        )
      } { domainRank =>
        logger.info(
          s"Automatic transaction transfer for $participantId into domain ${domainRank.name}"
        )
        transferThenSubmit(
          domainRank.name,
          contracts,
          wfTransaction,
          submitterInfo,
          domainRank.transfers,
          transactionMeta,
          keyResolver,
        )
      }
    }
  }

  private def submitTransactionMultiDomain(
      wfTransaction: WellFormedTransaction[WithoutSuffixes],
      contracts: List[ContractData],
      submitterInfo: SubmitterInfo,
      submitters: Set[LfPartyId],
      transactionMeta: TransactionMeta,
      keyResolver: LfKeyResolver,
      informees: Set[LfPartyId],
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, TransactionRoutingError, Future[TransactionSubmitted]] = {

    val connected = this.connectedDomains()
    val maybeWorkflowId = transactionMeta.workflowId
    for {
      mybDomainAlias <- maybeWorkflowId
        .traverse(DomainAlias.create)
        .toEitherT[Future]
        .leftMap(InvalidDomainAlias.Error)
      res <- mybDomainAlias
        .flatMap(recoveredDomainOfAlias(_))
        .filter(connected.contains) match {
        case None => // pick a suitable domain id
          submitPickingADomain(
            connected,
            wfTransaction,
            contracts,
            submitterInfo,
            submitters,
            transactionMeta,
            keyResolver,
            informees,
          )
        case Some(targetDomain) => // Use the domain given by the workflow ID
          submitViaDomain(
            targetDomain,
            wfTransaction,
            contracts,
            submitterInfo,
            submitters,
            transactionMeta,
            keyResolver,
          )
      }
    } yield res
  }

  // Includes check that submitting party has a participant with submission rights on origin and target domain
  private def transfersToDomain(
      contracts: List[ContractData],
      targetDomain: DomainId,
      submitters: Set[LfPartyId],
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[Future, String, (DomainId, Map[LfContractId, LfPartyId])] = {
    val ts = snapshot(targetDomain)
    val maybeTransfers: EitherT[Future, String, List[Option[(LfContractId, LfPartyId)]]] =
      contracts.traverse { c =>
        if (c.domain == targetDomain) EitherT.fromEither[Future](Right(None))
        else {
          for {
            originSnapshot <- EitherT.fromEither[Future](
              snapshot(c.domain).toRight(s"No snapshot for domain ${c.domain}")
            )
            targetSnapshot <- EitherT.fromEither[Future](
              ts.toRight(s"No snapshot for domain $targetDomain")
            )
            submitter <- findSubmitterThatCanTransferContract(
              originSnapshot,
              targetSnapshot,
              c,
              submitters,
            )
          } yield Some(c.id -> submitter)
        }
      }
    maybeTransfers.map(transfers => (targetDomain, transfers.flattenOption.toMap))
  }

  private def findSubmitterThatCanTransferContract(
      originSnapshot: TopologySnapshot,
      targetSnapshot: TopologySnapshot,
      c: ContractData,
      submitters: Set[LfPartyId],
  )(implicit traceContext: TraceContext): EitherT[Future, String, LfPartyId] = {

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
              originSnapshot,
              targetSnapshot,
              logger,
            )
            .biflatMap(
              left => go(rest, errAccum :+ show"Submitter ${submitter} cannot transfer: $left"),
              _canTransfer => EitherT.rightT(submitter),
            )
      }
    }

    go(submitters.intersect(c.stakeholders).toList)
  }

  private def transferThenSubmit(
      targetDomain: DomainId,
      contracts: List[ContractData],
      wfTransaction: WellFormedTransaction[WithoutSuffixes],
      submitterInfo: SubmitterInfo,
      transfers: Map[LfContractId, LfPartyId],
      transactionMeta: TransactionMeta,
      keyResolver: LfKeyResolver,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, TransactionRoutingError, Future[TransactionSubmitted]] = {
    for {
      _unit <- contracts.traverse_ { c =>
        if (c.domain != targetDomain) {
          transfer(TransferArgs(c.domain, targetDomain, transfers(c.id), c.id, traceContext))
        } else {
          EitherT.pure[Future, TransactionRoutingError](())
        }
      }
      submissionResult <- submit(targetDomain)(
        submitterInfo,
        transactionMeta,
        keyResolver,
        wfTransaction,
        traceContext,
      )
    } yield submissionResult
  }

  private def chooseDomain(
      submitters: Set[LfPartyId],
      maybeWorkflowId: Option[LfWorkflowId],
      inputContractIds: Set[LfContractId],
      informees: Set[LfPartyId],
  ): EitherT[Future, TransactionRoutingError, DomainId] = {
    for {
      domainId1 <- chooseDomainOfInputContracts(inputContractIds)
      domainId2 <- domainId1 match {
        case None =>
          chooseDomainWithAliasEqualToWorkflowId2(maybeWorkflowId)
            .toEitherT[Future]
            .leftMap(InvalidDomainAlias.Error)
        case Some(_id) => EitherT.rightT[Future, TransactionRoutingError](domainId1)
      }
      domainId3 <- domainId2.fold({
        // domain2 not existent
        chooseCommonDomainOfInformees(submitters, informees)
      })( // domain2 existent
        EitherT.rightT(_)
      )
    } yield domainId3
  }

  private def chooseDomainOfInputContracts(
      inputContractIds: Set[LfContractId]
  ): EitherT[Future, TransactionRoutingError, Option[DomainId]] = {
    for {
      // Collect domains of input contracts, ignoring contracts that cannot be found in the ACS.
      // Such contracts need to be ignored, because they could be divulged contracts.
      domainIdsOfInputContracts <- EitherT
        .right(inputContractIds.toList.traverseFilter(id => domainOfContract(id).value))
        .map(_.toSet)
      maybeDomainId <- EitherT.cond[Future](
        domainIdsOfInputContracts.sizeCompare(1) <= 0,
        domainIdsOfInputContracts.headOption, {
          // Input contracts reside on different domains
          // Fail...
          RoutingInternalError
            .InputContractsOnDifferentDomains(domainIdsOfInputContracts): TransactionRoutingError
        },
      )
    } yield maybeDomainId
  }

  private def chooseDomainWithAliasEqualToWorkflowId2(
      maybeWorkflowId: Option[LfWorkflowId]
  ): Either[String, Option[DomainId]] = {
    for {
      domainAliasO <- maybeWorkflowId.traverse(DomainAlias.create(_))
      res = domainAliasO.flatMap(x => recoveredDomainOfAlias(x))
    } yield res
  }

  @SuppressWarnings(Array("org.wartremover.warts.TraversableOps"))
  private def chooseCommonDomainOfInformees(
      submitters: Set[LfPartyId],
      informees: Set[LfPartyId],
  ): EitherT[Future, TransactionRoutingError, DomainId] =
    for {
      nonEmptyDomainIds <- domainsOfSubmittersAndInformees(submitters, informees): EitherT[
        Future,
        TransactionRoutingError,
        NonEmpty[Set[DomainId]],
      ]
      domainId <-
        if (nonEmptyDomainIds.size == 1) {
          EitherT
            .rightT(nonEmptyDomainIds.head1): EitherT[Future, TransactionRoutingError, DomainId]
        } else {
          EitherT.rightT(
            nonEmptyDomainIds.maxBy(id => priorityOfDomain(id) -> id.toProtoPrimitive)(
              Ordering
                .Tuple2(Ordering[Int], Ordering[String].reverse)
            )
          ): EitherT[Future, TransactionRoutingError, DomainId]
        }
    } yield domainId
}

object DomainRouter {
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
      coid: LfContractId
  )(implicit ec: ExecutionContext, traceContext: TraceContext): OptionT[Future, DomainId] = {
    connectedDomains
      .collect {
        case (domainId, syncDomain) if syncDomain.readyForSubmission =>
          syncDomain.ephemeral.requestTracker
            .getApproximateState(coid)
            .filter(_.status == Active)
            .map(_ => domainId)
      }
      .reduceOption[OptionT[Future, DomainId]] { case (d1, d2) =>
        d1.orElse(d2)
      }
      .getOrElse(OptionT.none)
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
      .map(_.filter(_._2).map(_._1).toSet)

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
    val TransferArgs(originDomain, targetDomain, submittingParty, contractId, _traceContext) = args
    implicit val traceContext = _traceContext

    val transfer = for {
      originSyncDomain <- EitherT.fromEither[Future](
        connectedDomains.get(originDomain).toRight("Not connected to the origin domain")
      )

      targetSyncDomain <- EitherT.fromEither[Future](
        connectedDomains.get(targetDomain).toRight("Not connected to the target domain")
      )

      _unit <- EitherT
        .cond[Future](originSyncDomain.ready, (), "The origin domain is not ready for submissions")

      outResult <- originSyncDomain
        .submitTransferOut(submittingParty, contractId, targetDomain)
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
        .submitTransferIn(submittingParty, outResult.transferId)
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

  case class AutoTransferDomainRank(
      transfers: Map[LfContractId, LfPartyId],
      priority: Int,
      name: DomainId,
  )
  //The highest priority domain should be picked first, so negate the priority
  val domainRanking: Ordering[AutoTransferDomainRank] =
    Ordering.by(x => (x.transfers.size, -x.priority, x.name))

  case class ContractData(id: LfContractId, domain: DomainId, stakeholders: Set[LfPartyId])

  case class TransferArgs(
      originDomain: DomainId,
      targetDomain: DomainId,
      submitter: LfPartyId,
      contract: LfContractId,
      traceContext: TraceContext,
  )

}
