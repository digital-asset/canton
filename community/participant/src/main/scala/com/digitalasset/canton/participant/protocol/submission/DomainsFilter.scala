// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission

import cats.syntax.alternative._
import cats.syntax.traverse._
import com.daml.lf.engine.Blinding
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.protocol.submission.DomainUsabilityChecker.DomainNotUsedReason
import com.digitalasset.canton.protocol.{LfVersionedTransaction, PackageInfoService}
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.{DomainId, ParticipantId}
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.{ExecutionContext, Future}

private[submission] class DomainsFilter(
    localParticipantId: ParticipantId,
    submittedTransaction: LfVersionedTransaction,
    domains: List[(DomainId, TopologySnapshot, PackageInfoService)],
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext, traceContext: TraceContext)
    extends NamedLogging {
  private val requiredPackagesPerParty = Blinding.partyPackages(submittedTransaction)

  def split: Future[(List[DomainNotUsedReason], List[DomainId])] = domains
    .traverse { case (domainId, snapshot, packageInfoService) =>
      val checker = new DomainUsabilityCheckerFull(
        domainId,
        snapshot,
        requiredPackagesPerParty,
        packageInfoService,
        localParticipantId,
      )

      checker.isUsable.map(_ => domainId).value
    }
    .map(_.separate)
}
