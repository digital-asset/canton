// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission.routing

import cats.data.EitherT
import cats.implicits._
import com.daml.ledger.participant.state.v2.TransactionMeta
import com.daml.nonempty.NonEmpty
import com.daml.nonempty.NonEmptyColl._
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.protocol.submission.routing.DomainRouter.{
  ContractData,
  ContractsDomainData,
}
import com.digitalasset.canton.participant.sync.TransactionRoutingError
import com.digitalasset.canton.participant.sync.TransactionRoutingError.MalformedInputErrors.InvalidDomainAlias
import com.digitalasset.canton.participant.sync.TransactionRoutingError.{
  AutomaticTransferForTransactionFailure,
  RoutingInternalError,
}
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{DomainAlias, LfPartyId, LfWorkflowId}

import scala.concurrent.{ExecutionContext, Future}

private[routing] class DomainSelector(
    domainIdResolver: DomainAlias => Option[DomainId],
    domainsOfSubmittersAndInformees: (
        Set[LfPartyId],
        Set[LfPartyId],
    ) => EitherT[Future, TransactionRoutingError, NonEmpty[Set[DomainId]]],
    priorityOfDomain: DomainId => Int,
    domainRankComputation: DomainRankComputation,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends NamedLogging {

  /** Choose the appropriate domain for a transaction.
    * The domain is chosen as follows:
    * 1. Domain whose alias equals the workflow id
    * 2. The domain with the smaller number of transfers on which all informees have active participants
    */
  def forMultiDomain(
      transactionMeta: TransactionMeta,
      connectedDomains: Set[DomainId],
      contracts: Seq[ContractData],
      submitters: Set[LfPartyId],
      informees: Set[LfPartyId],
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, TransactionRoutingError, DomainRank] = {
    val maybeWorkflowId = transactionMeta.workflowId
    for {
      domainIdO <- toDomainId(maybeWorkflowId)
      informeeDomainsId <- domainsOfSubmittersAndInformees(submitters, informees)
      res <- domainIdO
        .filter(connectedDomains.contains) match {
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
          } else domainRankComputation.compute(contracts, targetDomain, submitters)
      }
    } yield res
  }

  def forSingleDomain(
      submitters: Set[LfPartyId],
      maybeWorkflowId: Option[LfWorkflowId],
      inputContractsDomainData: ContractsDomainData,
      informees: Set[LfPartyId],
  ): EitherT[Future, TransactionRoutingError, DomainRank] = {
    for {
      domainSpecifiedByWorkflowIdO <- toDomainId(maybeWorkflowId)
      inputContractDomainId <- chooseDomainOfInputContracts(inputContractsDomainData)
      informeeDomainsId <- domainsOfSubmittersAndInformees(submitters, informees)

      domainId <- EitherT.fromEither[Future](
        domainSpecifiedByWorkflowIdO match {
          case Some(domainSpecifiedByWorkflowId) =>
            if (
              inputContractDomainId.nonEmpty && !inputContractDomainId
                .contains(domainSpecifiedByWorkflowId)
            )
              Left(
                TransactionRoutingError.ConfigurationErrors.InvalidWorkflowId
                  .InputContractsNotOnDomain(domainSpecifiedByWorkflowId, inputContractDomainId)
              )
            else if (!informeeDomainsId.contains(domainSpecifiedByWorkflowId)) {
              Left(
                TransactionRoutingError.ConfigurationErrors.InvalidWorkflowId
                  .NotAllInformeeAreOnDomain(domainSpecifiedByWorkflowId, informeeDomainsId)
              )
            } else Right(domainSpecifiedByWorkflowId)

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
        .traverse(targetDomain =>
          domainRankComputation.compute(contracts, targetDomain, submitters)
        )

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

  private def chooseDomainOfInputContracts(
      inputContractsDomainData: ContractsDomainData
  ): EitherT[Future, TransactionRoutingError, Option[DomainId]] =
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

  private def toDomainId(
      maybeWorkflowId: Option[LfWorkflowId]
  ): EitherT[Future, InvalidDomainAlias.Error, Option[DomainId]] = {
    for {
      domainAliasO <- maybeWorkflowId
        .traverse(DomainAlias.create(_))
        .toEitherT[Future]
        .leftMap(InvalidDomainAlias.Error)
      res = domainAliasO.flatMap(domainIdResolver(_))
    } yield res
  }
}
