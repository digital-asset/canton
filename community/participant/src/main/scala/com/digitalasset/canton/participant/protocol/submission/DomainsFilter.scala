// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission

import cats.syntax.alternative.*
import cats.syntax.parallel.*
import com.daml.lf.data.Ref.{PackageId, Party}
import com.daml.lf.engine.Blinding
import com.daml.lf.transaction.TransactionVersion
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.protocol.submission.DomainUsabilityChecker.DomainNotUsedReason
import com.digitalasset.canton.protocol.{LfVersionedTransaction, PackageInfoService}
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.{DomainId, ParticipantId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.{ExecutionContext, Future}

private[submission] class DomainsFilter(
    localParticipantId: ParticipantId,
    requiredPackagesPerParty: Map[Party, Set[PackageId]],
    domains: List[(DomainId, ProtocolVersion, TopologySnapshot, PackageInfoService)],
    transactionVersion: TransactionVersion,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext, traceContext: TraceContext)
    extends NamedLogging {
  def split: Future[(List[DomainNotUsedReason], List[DomainId])] = domains
    .parTraverse { case (domainId, protocolVersion, snapshot, packageInfoService) =>
      val checker = new DomainUsabilityCheckerFull(
        domainId,
        protocolVersion,
        snapshot,
        requiredPackagesPerParty,
        packageInfoService,
        localParticipantId,
        transactionVersion,
      )

      checker.isUsable.map(_ => domainId).value
    }
    .map(_.separate)
}

private[submission] object DomainsFilter {
  def apply(
      localParticipantId: ParticipantId,
      submittedTransaction: LfVersionedTransaction,
      domains: List[(DomainId, ProtocolVersion, TopologySnapshot, PackageInfoService)],
      loggerFactory: NamedLoggerFactory,
  )(implicit ec: ExecutionContext, traceContext: TraceContext) = new DomainsFilter(
    localParticipantId,
    Blinding.partyPackages(submittedTransaction),
    domains,
    submittedTransaction.version,
    loggerFactory,
  )
}
