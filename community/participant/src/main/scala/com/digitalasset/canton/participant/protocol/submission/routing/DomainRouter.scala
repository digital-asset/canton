// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission.routing

import cats.data.EitherT
import cats.syntax.alternative._
import cats.syntax.either._
import cats.syntax.foldable._
import cats.syntax.traverse._
import cats.syntax.traverseFilter._
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
import com.digitalasset.canton.participant.protocol.submission.routing.DomainRouter.ContractsDomainData
import com.digitalasset.canton.participant.store.ActiveContractStore.Active
import com.digitalasset.canton.participant.store.DomainConnectionConfigStore
import com.digitalasset.canton.participant.sync.TransactionRoutingError.ConfigurationErrors.{
  MultiDomainSupportNotEnabled,
  SubmissionDomainNotReady,
}
import com.digitalasset.canton.participant.sync.TransactionRoutingError.TopologyErrors.{
  NotConnectedToAllContractDomains,
  SubmitterAlwaysStakeholder,
  UnknownContractDomains,
}
import com.digitalasset.canton.participant.sync.TransactionRoutingError.{
  MalformedInputErrors,
  RoutingInternalError,
  TopologyErrors,
}
import com.digitalasset.canton.participant.sync.{SyncDomain, TransactionRoutingError}
import com.digitalasset.canton.protocol.WellFormedTransaction.WithoutSuffixes
import com.digitalasset.canton.protocol._
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.transaction.ParticipantPermission.Submission
import com.digitalasset.canton.topology.{DomainId, ParticipantId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{EitherTUtil, LfTransactionUtil}
import com.digitalasset.canton.{DomainAlias, LfKeyResolver, LfPartyId}

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
    getConnectedDomains: () => Set[DomainId],
    domainOfContracts: Seq[LfContractId] => Future[Map[LfContractId, DomainId]],
    submit: DomainId => (
        SubmitterInfo,
        TransactionMeta,
        LfKeyResolver,
        WellFormedTransaction[WithoutSuffixes],
        TraceContext,
    ) => EitherT[Future, TransactionRoutingError, Future[TransactionSubmitted]],
    contractsTransferer: ContractsTransferer,
    snapshot: DomainId => Option[TopologySnapshot],
    autoTransferTransaction: Boolean,
    domainSelector: DomainSelector,
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
          domainSelector.forSingleDomain(
            lfSubmitters,
            transactionMeta.workflowId,
            inputContractsDomainData,
            informees,
          )
        } else if (autoTransferTransaction) {
          val connectedDomains = getConnectedDomains()
          logger.debug(
            s"Choosing the domain as multi-domain workflow for ${submitterInfo.commandId}"
          )
          chooseDomainForMultiDomain(
            inputContractsDomainData,
            connectedDomains,
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
      _ <- contractsTransferer.transfer(domainRankTarget)
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
      connectedDomains: Set[DomainId],
      submitters: Set[LfPartyId],
      informees: Set[LfPartyId],
      transactionMeta: TransactionMeta,
  )(implicit traceContext: TraceContext): EitherT[Future, TransactionRoutingError, DomainRank] =
    for {
      _ <- checkValidityOfMultiDomain(connectedDomains, inputContractsDomainData, submitters)
      domainRankTarget <- domainSelector.forMultiDomain(
        transactionMeta,
        connectedDomains,
        inputContractsDomainData.withDomainData,
        submitters,
        informees,
      )
    } yield domainRankTarget

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
      connectedDomains: Set[DomainId],
      inputContractsDomainData: ContractsDomainData,
      submitters: Set[LfPartyId],
  ): EitherT[Future, TransactionRoutingError, Unit] = {

    val allContractsHaveDomainData: Boolean = inputContractsDomainData.withoutDomainData.isEmpty
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

    val transfer = new ContractsTransferer(connectedDomainsMap, loggerFactory)

    val domainIdResolver = recoveredDomainOfAlias(connectedDomainsMap, domainAliasManager) _

    val domainRankComputation = new DomainRankComputation(
      participantId = participantId,
      priorityOfDomain = priorityOfDomain(domainConnectionConfigStore, domainAliasManager),
      snapshot = domainSnapshot(connectedDomainsMap),
      loggerFactory = loggerFactory,
    )

    val domainSelection = new DomainSelector(
      domainIdResolver = domainIdResolver,
      domainsOfSubmittersAndInformees =
        domainsOfSubmittersAndInformees(participantId, connectedDomainsMap),
      priorityOfDomain = priorityOfDomain(domainConnectionConfigStore, domainAliasManager),
      domainRankComputation = domainRankComputation,
      loggerFactory = loggerFactory,
    )

    new DomainRouter(
      participantId,
      () => getConnectedDomains(connectedDomainsMap),
      domainOfContract(connectedDomainsMap),
      submit(connectedDomainsMap),
      transfer,
      domainSnapshot(connectedDomainsMap),
      autoTransferTransaction = autoTransferTransaction,
      domainSelection,
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

  private def wrapSubmissionError[T](domainId: DomainId)(
      eitherT: EitherT[Future, TransactionSubmissionError, T]
  )(implicit ec: ExecutionContext): EitherT[Future, TransactionRoutingError, T] =
    eitherT.leftMap(subm => TransactionRoutingError.SubmissionError(domainId, subm))

  case class ContractData(id: LfContractId, domain: DomainId, stakeholders: Set[LfPartyId])

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
