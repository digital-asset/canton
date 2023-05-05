// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.store

import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.topology.store.StoredTopologyTransactionsX.GenericStoredTopologyTransactionsX
import com.digitalasset.canton.topology.store.TopologyStoreId.DomainStore
import com.digitalasset.canton.topology.{MediatorId, Member, ParticipantId}
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.{ExecutionContext, Future}

trait TopologyStateForInitializationService {
  def initialSnapshot(member: Member)(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): Future[GenericStoredTopologyTransactionsX]
}

final class StoreBasedTopologyStateForInitializationService(
    domainTopologyStore: TopologyStoreX[DomainStore],
    val loggerFactory: NamedLoggerFactory,
) extends TopologyStateForInitializationService
    with NamedLogging {

  override def initialSnapshot(member: Member)(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): Future[GenericStoredTopologyTransactionsX] = {
    member match {
      case participant @ ParticipantId(_) =>
        logger.debug(s"Fetching initial topology state for $participant")
        for {
          trustCert <- domainTopologyStore.findFirstTrustCertificateForParticipant(participant)
          maybeSequencedTime = trustCert.map(_.validFrom)
          storedTopologyTransactions <- maybeSequencedTime match {
            case Some(sequenced) =>
              domainTopologyStore.findEssentialStateForMember(participant, sequenced.value)
            case None =>
              // TODO(#11255) should this error out if nothing can be found?
              Future.successful(StoredTopologyTransactionsX(Seq.empty))
          }
        } yield storedTopologyTransactions

      case mediator @ MediatorId(_) =>
        logger.debug(s"Fetching initial topology state for $mediator")
        for {
          // find first MediatorDomainStateX mentioning mediator
          mediatorState <- domainTopologyStore.findFirstMediatorStateForMediator(mediator)
          // find the first one that mentions the member
          maybeSequencedTime = mediatorState.map(_.validFrom)
          storedTopologyTransactions <- maybeSequencedTime match {
            case Some(sequenced) =>
              domainTopologyStore.findEssentialStateForMember(mediator, sequenced.value)
            case None =>
              // TODO(#11255) should this error out if nothing can be found?
              Future.successful(StoredTopologyTransactionsX(Seq.empty))
          }

        } yield {
          storedTopologyTransactions
        }

      case _ =>
        // TODO(#11255) proper error
        ???
    }
  }
}
