// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.topology

import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.CryptoPureApi
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.topology.client.DomainTopologyClientWithInit
import com.digitalasset.canton.topology.processing.{
  TopologyTransactionProcessor,
  TopologyTransactionProcessorCommon,
  TopologyTransactionProcessorX,
}
import com.digitalasset.canton.topology.store.{TopologyStore, TopologyStoreId, TopologyStoreX}
import com.digitalasset.canton.tracing.Traced

import scala.concurrent.ExecutionContext

object TopologyTransactionProcessorFactory {

  /** factory used in sync domain */
  class FactoryOld(
      domainId: DomainId,
      topologyClient: DomainTopologyClientWithInit,
      partyNotifier: LedgerServerPartyNotifier,
      cryptoPureApi: CryptoPureApi,
      store: TopologyStore[TopologyStoreId.DomainStore],
      futureSupervisor: FutureSupervisor,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  ) extends TopologyTransactionProcessorCommon.Factory {
    override def create(
        acsCommitmentScheduleEffectiveTime: Traced[CantonTimestamp] => Unit
    )(implicit executionContext: ExecutionContext): TopologyTransactionProcessorCommon = {
      val topologyProcessor = new TopologyTransactionProcessor(
        domainId,
        cryptoPureApi,
        store,
        acsCommitmentScheduleEffectiveTime,
        futureSupervisor,
        timeouts,
        loggerFactory,
      )
      // connect domain client to processor
      topologyProcessor.subscribe(topologyClient)
      // subscribe party notifier to topology processor
      topologyProcessor.subscribe(partyNotifier.attachToTopologyProcessor())
      topologyProcessor
    }
  }

  /** factory used in sync domain */
  class FactoryX(
      domainId: DomainId,
      topologyClient: DomainTopologyClientWithInit,
      partyNotifier: LedgerServerPartyNotifier,
      cryptoPureApi: CryptoPureApi,
      store: TopologyStoreX[TopologyStoreId.DomainStore],
      futureSupervisor: FutureSupervisor,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  ) extends TopologyTransactionProcessorCommon.Factory {
    override def create(
        acsCommitmentScheduleEffectiveTime: Traced[CantonTimestamp] => Unit
    )(implicit executionContext: ExecutionContext): TopologyTransactionProcessorCommon = {
      val topologyProcessor = new TopologyTransactionProcessorX(
        domainId,
        cryptoPureApi,
        store,
        acsCommitmentScheduleEffectiveTime,
        futureSupervisor,
        timeouts,
        loggerFactory,
      )
      // connect domain client to processor
      topologyProcessor.subscribe(topologyClient)
      // subscribe party notifier to topology processor
      topologyProcessor.subscribe(partyNotifier.attachToTopologyProcessor())
      topologyProcessor
    }
  }

}
