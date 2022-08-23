// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission.routing

import cats.syntax.alternative._
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.protocol.{LfContractId, WithContractMetadata}
import com.digitalasset.canton.topology.DomainId

import scala.concurrent.{ExecutionContext, Future}

private[routing] final case class ContractsDomainData(
    withDomainData: Seq[ContractData],
    withoutDomainData: Seq[LfContractId],
) {
  val domains: Set[DomainId] = withDomainData.map(_.domain).toSet
}

private[routing] object ContractsDomainData {
  def create(
      domainOfContracts: Seq[LfContractId] => Future[Map[LfContractId, DomainId]],
      inputContractMetadata: Set[WithContractMetadata[LfContractId]],
  )(implicit ec: ExecutionContext): Future[ContractsDomainData] = {
    val inputDataSeq = inputContractMetadata.toSeq
    domainOfContracts(inputDataSeq.map(_.unwrap))
      .map { domainMap =>
        // Collect domains of input contracts, ignoring contracts that cannot be found in the ACS.
        // Such contracts need to be ignored, because they could be divulged contracts.
        val (bad, good) = inputDataSeq.map { metadata =>
          val coid = metadata.unwrap
          domainMap.get(coid) match {
            case Some(domainId) =>
              Right(ContractData(coid, domainId, metadata.metadata.stakeholders))
            case None => Left(coid)
          }
        }.separate
        ContractsDomainData(good, bad)
      }
  }
}

private[routing] final case class ContractData(
    id: LfContractId,
    domain: DomainId,
    stakeholders: Set[LfPartyId],
)
