// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission.routing

import cats.data.EitherT
import cats.syntax.either._
import cats.syntax.traverse._
import com.daml.ledger.participant.state.v2.{SubmitterInfo, TransactionMeta}
import com.daml.lf.engine.Blinding
import com.digitalasset.canton.participant.sync.TransactionRoutingError
import com.digitalasset.canton.participant.sync.TransactionRoutingError.MalformedInputErrors
import com.digitalasset.canton.participant.sync.TransactionRoutingError.MalformedInputErrors.InvalidDomainAlias
import com.digitalasset.canton.protocol.{LfContractId, LfVersionedTransaction, WithContractMetadata}
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.{DomainAlias, LfPackageId, LfPartyId, LfWorkflowId}

import scala.concurrent.{ExecutionContext, Future}

/** Bundle together some data needed to route the transaction.
  *
  * @param requiredPackagesPerParty Required packages per informee of the transaction
  * @param submitters Submitters of the transaction.
  * @param inputContractsDomainData Information about the input contracts
  * @param prescribedDomainO If non-empty, thInvalidWorkflowIde prescribed domain will be chosen for routing.
  *                          In case this domain is not admissible, submission will fail.
  */
private[routing] sealed abstract case class TransactionData(
    transaction: LfVersionedTransaction,
    requiredPackagesPerParty: Map[LfPartyId, Set[LfPackageId]],
    submitters: Set[LfPartyId],
    inputContractsDomainData: ContractsDomainData,
    prescribedDomainO: Option[DomainId],
) {
  val informees: Set[LfPartyId] = requiredPackagesPerParty.keySet
}

private[routing] object TransactionData {
  def create(
      submitterInfo: SubmitterInfo,
      transaction: LfVersionedTransaction,
      transactionMeta: TransactionMeta,
      domainOfContracts: Seq[LfContractId] => Future[Map[LfContractId, DomainId]],
      domainIdResolver: DomainAlias => Option[DomainId],
      inputContractMetadata: Set[WithContractMetadata[LfContractId]],
  )(implicit
      ec: ExecutionContext
  ): EitherT[Future, TransactionRoutingError, TransactionData] = {
    for {
      prescribedDomainO <- EitherT.fromEither[Future](
        toDomainId(transactionMeta.workflowId, domainIdResolver)
      )

      submitters <- EitherT.fromEither[Future](
        submitterInfo.actAs
          .traverse(submitter =>
            LfPartyId
              .fromString(submitter)
              .leftMap[TransactionRoutingError](MalformedInputErrors.InvalidSubmitter.Error)
          )
          .map(_.toSet)
      )

      contractsDomainData <- EitherT.liftF(
        ContractsDomainData.create(domainOfContracts, inputContractMetadata)
      )
    } yield new TransactionData(
      transaction = transaction,
      requiredPackagesPerParty = Blinding.partyPackages(transaction),
      submitters = submitters,
      inputContractsDomainData = contractsDomainData,
      prescribedDomainO = prescribedDomainO,
    ) {}
  }

  private def toDomainId(
      maybeWorkflowId: Option[LfWorkflowId],
      domainIdResolver: DomainAlias => Option[DomainId],
  ): Either[TransactionRoutingError, Option[DomainId]] = {
    for {
      domainAliasO <- maybeWorkflowId
        .traverse(DomainAlias.create(_))
        .leftMap[TransactionRoutingError](InvalidDomainAlias.Error)
      res = domainAliasO.flatMap(domainIdResolver(_))
    } yield res
  }
}
