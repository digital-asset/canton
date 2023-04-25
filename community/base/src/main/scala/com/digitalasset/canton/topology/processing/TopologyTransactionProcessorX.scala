// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.processing

import com.digitalasset.canton.SequencerCounter
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.CryptoPureApi
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.environment.CantonNodeParameters
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.protocol.messages.DefaultOpenEnvelope
import com.digitalasset.canton.sequencing.*
import com.digitalasset.canton.sequencing.protocol.{Deliver, DeliverError}
import com.digitalasset.canton.time.{Clock, DomainTimeTracker}
import com.digitalasset.canton.topology.client.{
  DomainTopologyClientWithInit,
  StoreBasedDomainTopologyClient,
  StoreBasedDomainTopologyClientX,
}
import com.digitalasset.canton.topology.store.{TopologyStoreId, TopologyStoreX}
import com.digitalasset.canton.topology.{DomainId, TopologyStateProcessorX}
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.MonadUtil
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.{ExecutionContext, Future}

class TopologyTransactionProcessorX(
    domainId: DomainId,
    cryptoPureApi: CryptoPureApi,
    store: TopologyStoreX[TopologyStoreId.DomainStore],
    acsCommitmentScheduleEffectiveTime: Traced[CantonTimestamp] => Unit,
    futureSupervisor: FutureSupervisor,
    timeouts: ProcessingTimeout,
    loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends TopologyTransactionProcessorCommon(timeouts, loggerFactory) {

  // TODO(#11255) don't close store here

  private val stateProcessor = new TopologyStateProcessorX(store, loggerFactory)

  def subscriptionStartsAt(
      start: SubscriptionStart,
      domainTimeTracker: DomainTimeTracker,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = FutureUnlessShutdown.unit

  override def createHandler(domainId: DomainId): UnsignedProtocolEventHandler =
    new UnsignedProtocolEventHandler {

      override def name: String = s"topology-processor-$domainId"

      override def apply(
          tracedBatch: BoxedEnvelope[UnsignedEnvelopeBox, DefaultOpenEnvelope]
      ): HandlerResult = {
        MonadUtil.sequentialTraverseMonoid(tracedBatch.value) {
          _.withTraceContext { implicit traceContext =>
            {
              case Deliver(sc, ts, _, _, _) =>
                logger.debug(s"Processing sequenced event with counter $sc and timestamp $ts")
                // TODO(#11255) wire up state processor
                HandlerResult.done
              case _: DeliverError => HandlerResult.done
            }
          }
        }
      }

      override def subscriptionStartsAt(
          start: SubscriptionStart,
          domainTimeTracker: DomainTimeTracker,
      )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
        TopologyTransactionProcessorX.this.subscriptionStartsAt(start, domainTimeTracker)
    }

  /** process envelopes mostly asynchronously
    *
    * Here, we return a Future[Future[Unit]]. We need to ensure the outer future finishes processing
    * before we tick the record order publisher.
    */
  override def processEnvelopes(
      sc: SequencerCounter,
      ts: CantonTimestamp,
      envelopes: Traced[List[DefaultOpenEnvelope]],
  ): HandlerResult = ???

  override def subscribe(listener: TopologyTransactionProcessingSubscriber): Unit = {
    // TODO(#11255)
  }
}

object TopologyTransactionProcessorX {
  def createProcessorAndClientForDomain(
      topologyStore: TopologyStoreX[TopologyStoreId.DomainStore],
      domainId: DomainId,
      protocolVersion: ProtocolVersion,
      pureCrypto: CryptoPureApi,
      parameters: CantonNodeParameters,
      clock: Clock,
      futureSupervisor: FutureSupervisor,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      executionContext: ExecutionContext
  ): Future[(TopologyTransactionProcessorX, DomainTopologyClientWithInit)] = {
    val processor = new TopologyTransactionProcessorX(
      domainId,
      pureCrypto,
      topologyStore,
      _ => (),
      futureSupervisor,
      parameters.processingTimeouts,
      loggerFactory,
    )

    val client = new StoreBasedDomainTopologyClientX(
      clock,
      domainId,
      protocolVersion,
      topologyStore,
      StoreBasedDomainTopologyClient.NoPackageDependencies,
      parameters.processingTimeouts,
      futureSupervisor,
      loggerFactory,
    )

    processor.subscribe(client)
    Future.successful((processor, client))
  }
}
