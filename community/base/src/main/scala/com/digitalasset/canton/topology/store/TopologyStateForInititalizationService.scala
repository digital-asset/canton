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

  /** Downloading the initial topology snapshot works as follows:
    *
    * 1. Determine the first MediatorDomainStateX or DomainTrustCertificateX that mentions the member to onboard.
    * 2. Take its effective time (here t0')
    * 3. Find all transactions with sequence time <= t0'
    * 4. Find the maximum effective time of the transactions returned in 3. (here ts1')
    * 5. Set all validUntil > ts1' to None
    *
    * TODO(#13394) adapt this logic to allow onboarding of a previously offboarded member
    *
    * {{{
    *
    * t0 , t1   ... sequenced time
    * t0', t1'  ... effective time
    *
    *                           xxxxxxxxxxxxx
    *                         xxx           xxx
    *               t0        x       t0'     xx
    *      │         │        │        │       │
    *      ├─────────┼────────┼────────┼───────┼────────►
    *      │         │        │        │       │
    *                x       t1        x      t1'
    *                xx               xx
    *                 xx             xx
    *                   xx MDS/DTC xx
    *    }}}
    */
  override def initialSnapshot(member: Member)(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): Future[GenericStoredTopologyTransactionsX] = {
    member match {
      case participant @ ParticipantId(_) =>
        logger.debug(s"Fetching initial topology state for $participant")
        for {
          trustCert <- domainTopologyStore.findFirstTrustCertificateForParticipant(participant)
          maybeEffectiveTime = trustCert.map(_.validFrom)
          storedTopologyTransactions <- maybeEffectiveTime match {
            case Some(effective) =>
              domainTopologyStore.findEssentialStateForMember(participant, effective.value)
            case None =>
              // TODO(#12390) should this error out if nothing can be found?
              Future.successful(StoredTopologyTransactionsX(Seq.empty))
          }
        } yield storedTopologyTransactions

      case mediator @ MediatorId(_) =>
        logger.debug(s"Fetching initial topology state for $mediator")
        for {
          // find first MediatorDomainStateX mentioning mediator
          mediatorState <- domainTopologyStore.findFirstMediatorStateForMediator(mediator)
          // find the first one that mentions the member
          maybeEffectiveTime = mediatorState.map(_.validFrom)
          storedTopologyTransactions <- maybeEffectiveTime match {
            case Some(effective) =>
              domainTopologyStore.findEssentialStateForMember(mediator, effective.value)
            case None =>
              // TODO(#12390) should this error out if nothing can be found?
              Future.successful(StoredTopologyTransactionsX(Seq.empty))
          }

        } yield {
          storedTopologyTransactions
        }

      case _ =>
        // TODO(#12390) proper error
        ???
    }
  }
}
