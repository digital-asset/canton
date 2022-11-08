// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission.routing

import cats.data.EitherT
import cats.syntax.alternative.*
import cats.syntax.parallel.*
import com.daml.lf.transaction.TransactionVersion
import com.daml.nonempty.NonEmpty
import com.daml.nonempty.NonEmptyColl.*
import com.daml.nonempty.catsinstances.*
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.admin.PackageService
import com.digitalasset.canton.participant.protocol.submission.{
  DomainUsabilityCheckerFull,
  DomainsFilter,
}
import com.digitalasset.canton.participant.sync.TransactionRoutingError.RoutingInternalError
import com.digitalasset.canton.participant.sync.TransactionRoutingError.TopologyErrors.NoDomainForSubmission
import com.digitalasset.canton.participant.sync.{
  TransactionRoutingError,
  TransactionRoutingErrorWithDomain,
}
import com.digitalasset.canton.protocol.PackageInfoService
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.{DomainId, ParticipantId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.EitherTUtil
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.{ExecutionContext, Future}

private[routing] class DomainSelectorFactory(
    participantId: ParticipantId,
    domainsOfSubmittersAndInformees: TransactionData => EitherT[
      Future,
      TransactionRoutingError,
      NonEmpty[Set[DomainId]],
    ],
    priorityOfDomain: DomainId => Int,
    domainRankComputation: DomainRankComputation,
    packageService: PackageService,
    domainStateProvider: DomainId => Either[
      TransactionRoutingErrorWithDomain,
      (TopologySnapshot, ProtocolVersion),
    ],
    loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext) {
  def create(
      transactionData: TransactionData
  ): EitherT[Future, TransactionRoutingError, DomainSelector] = {
    for {
      domainsOfSubmittersAndInformees <- domainsOfSubmittersAndInformees(transactionData)
    } yield new DomainSelector(
      participantId,
      transactionData,
      domainsOfSubmittersAndInformees,
      priorityOfDomain,
      domainRankComputation,
      packageService,
      domainStateProvider,
      loggerFactory,
    )
  }
}

/** Selects the best domain for routing.
  * @param domainsOfSubmittersAndInformees Domains that host both submitters and informees of the transaction:
  *                                          - submitters have to be hosted on the local participant
  *                                          - informees have to be hosted on some participant
  *                                        It is assumed that the participant is connected to all domains in `connectedDomains`
  * @param priorityOfDomain Priority of each domain (lowest number indicates highest priority)
  * @param domainRankComputation Utility class to compute `DomainRank`
  * @param domainStateProvider Provides state information about a domain.
  *                            Note: returns an either rather than an option since failure comes from disconnected
  *                            domains and we assume the participant to be connected to all domains in `connectedDomains`
  */
private[routing] class DomainSelector(
    participantId: ParticipantId,
    val transactionData: TransactionData,
    domainsOfSubmittersAndInformees: NonEmpty[Set[DomainId]],
    priorityOfDomain: DomainId => Int,
    domainRankComputation: DomainRankComputation,
    packageService: PackageInfoService,
    domainStateProvider: DomainId => Either[
      TransactionRoutingErrorWithDomain,
      (TopologySnapshot, ProtocolVersion),
    ],
    protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends NamedLogging {

  private val submitters = transactionData.submitters

  /** Choose the appropriate domain for a transaction.
    * The domain is chosen as follows:
    * 1. Domain whose id equals `transactionData.prescribedDomainO` (if non-empty)
    * 2. The domain with the smaller number of transfers on which all informees have active participants
    */
  def forMultiDomain(implicit
      traceContext: TraceContext
  ): EitherT[Future, TransactionRoutingError, DomainRank] = {
    val contracts = transactionData.inputContractsDomainData.withDomainData

    transactionData.prescribedDomainO match {
      case Some(prescribedDomain) =>
        for {
          _ <- validatePrescribedDomain(prescribedDomain, transactionData.version)
          domainRank <- domainRankComputation.compute(
            contracts,
            prescribedDomain,
            transactionData.submitters,
          )
        } yield domainRank

      case None =>
        for {
          admissibleDomains <- filterDomains(domainsOfSubmittersAndInformees)
          domainRank <- pickDomainIdAndComputeTransfers(contracts, admissibleDomains)
        } yield domainRank
    }
  }

  /** Choose the appropriate domain for a transaction.
    * The domain is chosen as follows:
    * 1. Domain whose alias equals the workflow id
    * 1. Domain of all input contracts (fail if there is more than one)
    * 3. An arbitrary domain to which the submitter can submit and on which all informees have active participants
    */
  def forSingleDomain(implicit
      traceContext: TraceContext
  ): EitherT[Future, TransactionRoutingError, DomainRank] = {
    for {
      inputContractsDomainIdO <- chooseDomainOfInputContracts

      domainId <- transactionData.prescribedDomainO match {
        case Some(prescribedDomainId) =>
          // If a domain is prescribed, we use the prescribed one
          singleDomainValidatePrescribedDomain(
            prescribedDomainId,
            transactionData.version,
            inputContractsDomainIdO,
          )
            .map(_ => prescribedDomainId)

        case None =>
          inputContractsDomainIdO match {
            case Some(inputContractsDomainId) =>
              // If all the contracts are on a single domain, we use this one
              singleDomainValidatePrescribedDomain(
                inputContractsDomainId,
                transactionData.version,
                inputContractsDomainIdO,
              )
                .map(_ => inputContractsDomainId)
            // TODO(#10088) If validation fails, try to re-submit as multi-domain

            case None =>
              // Pick the best valid domain in domainsOfSubmittersAndInformees
              filterDomains(domainsOfSubmittersAndInformees)
                .map(_.minBy1(id => DomainRank(Map.empty, priorityOfDomain(id), id)))
          }
      }
    } yield DomainRank(Map.empty, priorityOfDomain(domainId), domainId)
  }

  /** Filter domains using the [[com.digitalasset.canton.participant.protocol.submission.DomainUsabilityCheckerFull]]
    */
  private def filterDomains(
      domains: NonEmpty[Set[DomainId]]
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, TransactionRoutingError, NonEmpty[Set[DomainId]]] = {

    val (unableToFetchStateDomains, domainStates) = domains.forgetNE.toList.map { domainId =>
      domainStateProvider(domainId).map { case (snapshot, protocolVersion) =>
        (domainId, protocolVersion, snapshot, packageService)
      }
    }.separate

    val domainsFilter = DomainsFilter(
      localParticipantId = participantId,
      submittedTransaction = transactionData.transaction,
      domains = domainStates,
      loggerFactory = loggerFactory,
    )

    for {
      domains <- EitherT.liftF(domainsFilter.split)

      (unusableDomains, usableDomains) = domains
      allUnusableDomains =
        unusableDomains.map(d => d.domainId -> d.toString).toMap ++
          unableToFetchStateDomains.map(d => d.domainId -> d.toString).toMap

      _ = logger.debug(s"Not considering the following domains for routing: $allUnusableDomains")

      usableDomainsNE <- EitherT
        .pure[Future, TransactionRoutingError](usableDomains)
        .map(NonEmpty.from)
        .subflatMap(
          _.toRight[TransactionRoutingError](NoDomainForSubmission.Error(allUnusableDomains))
        )
    } yield usableDomainsNE.toSet
  }

  private def singleDomainValidatePrescribedDomain(
      domainId: DomainId,
      transactionVersion: TransactionVersion,
      inputContractsDomainIdO: Option[DomainId],
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, TransactionRoutingError, Unit] = {
    /*
      If there are input contracts, then they should be on domain `domainId`
     */
    def validateContainsInputContractsDomainId: EitherT[Future, TransactionRoutingError, Unit] =
      inputContractsDomainIdO match {
        case Some(inputContractsDomainId) =>
          EitherTUtil.condUnitET(
            inputContractsDomainId == domainId,
            TransactionRoutingError.ConfigurationErrors.InvalidPrescribedDomainId
              .InputContractsNotOnDomain(domainId, inputContractsDomainId),
          )

        case None => EitherT.pure(())
      }

    for {
      // Single-domain specific validations
      _ <- validateContainsInputContractsDomainId

      // Generic validations
      _ <- validatePrescribedDomain(domainId, transactionVersion)
    } yield ()
  }

  /** Validation that are shared between single- and multi- domain submission:
    *
    * - Participant is connected to `domainId`
    *
    * - List `domainsOfSubmittersAndInformees` contains `domainId`
    *
    * - Checks in [[com.digitalasset.canton.participant.protocol.submission.DomainUsabilityCheckerFull]]
    */
  private def validatePrescribedDomain(
      domainId: DomainId,
      transactionVersion: TransactionVersion,
  )(implicit traceContext: TraceContext): EitherT[Future, TransactionRoutingError, Unit] = {

    for {
      domainState <- EitherT.fromEither[Future](domainStateProvider(domainId))
      (snapshot, protocolVersion) = domainState

      // Informees and submitters should reside on the selected domain
      _ <- EitherTUtil.condUnitET[Future](
        domainsOfSubmittersAndInformees.contains(domainId),
        TransactionRoutingError.ConfigurationErrors.InvalidPrescribedDomainId
          .NotAllInformeeAreOnDomain(
            domainId,
            domainsOfSubmittersAndInformees,
          ),
      )

      // Further validations
      domainUsabilityChecker = new DomainUsabilityCheckerFull(
        domainId = domainId,
        protocolVersion = protocolVersion,
        snapshot = snapshot,
        requiredPackagesByParty = transactionData.requiredPackagesPerParty,
        packageInfoService = packageService,
        localParticipantId = participantId,
        transactionVersion = transactionVersion,
      )

      _ <- domainUsabilityChecker.isUsable.leftMap[TransactionRoutingError] { err =>
        TransactionRoutingError.ConfigurationErrors.InvalidPrescribedDomainId
          .Generic(domainId, err.toString)
      }
    } yield ()
  }

  private def pickDomainIdAndComputeTransfers(
      contracts: Seq[ContractData],
      domains: NonEmpty[Set[DomainId]],
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, TransactionRoutingError, DomainRank] =
    for {
      rankedDomains <- domains.toSeq.toNEF
        .parTraverse(targetDomain =>
          domainRankComputation.compute(contracts, targetDomain, submitters)
        )

      // Priority of domain
      // Number of Transfers if we use this domain
      // pick according to least amount of transfers
    } yield rankedDomains.min1

  private def chooseDomainOfInputContracts
      : EitherT[Future, TransactionRoutingError, Option[DomainId]] = {
    val inputContractsDomainData = transactionData.inputContractsDomainData

    inputContractsDomainData.domains.size match {
      case 0 | 1 => EitherT.rightT(inputContractsDomainData.domains.headOption)
      // Input contracts reside on different domains
      // Fail..
      case _ =>
        EitherT.leftT[Future, Option[DomainId]](
          RoutingInternalError
            .InputContractsOnDifferentDomains(inputContractsDomainData.domains)
        )
    }
  }
}
