// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.processing

import cats.syntax.functorFilter.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.SequencerCounter
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.CryptoPureApi
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.environment.CantonNodeParameters
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.protocol.messages.{
  AcceptedTopologyTransactionsX,
  DefaultOpenEnvelope,
  ProtocolMessage,
}
import com.digitalasset.canton.time.{Clock, NonNegativeFiniteDuration}
import com.digitalasset.canton.topology.client.{
  DomainTopologyClientWithInitX,
  StoreBasedDomainTopologyClient,
  StoreBasedDomainTopologyClientX,
}
import com.digitalasset.canton.topology.store.TopologyStore.Change
import com.digitalasset.canton.topology.store.ValidatedTopologyTransactionX.GenericValidatedTopologyTransactionX
import com.digitalasset.canton.topology.store.{TopologyStoreId, TopologyStoreX}
import com.digitalasset.canton.topology.transaction.{DomainParametersStateX, TopologyChangeOpX}
import com.digitalasset.canton.topology.{DomainId, TopologyStateProcessorX}
import com.digitalasset.canton.tracing.{TraceContext, Traced}
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
    extends TopologyTransactionProcessorCommonImpl[AcceptedTopologyTransactionsX](
      domainId,
      futureSupervisor,
      store,
      acsCommitmentScheduleEffectiveTime,
      timeouts,
      loggerFactory,
    ) {

  override type SubscriberType = TopologyTransactionProcessingSubscriberX

  private val stateProcessor = new TopologyStateProcessorX(store, loggerFactory)

  override def onClosed(): Unit = {
    super.onClosed()
  }

  override protected def epsilonForTimestamp(asOfExclusive: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Change.TopologyDelay] = FutureUnlessShutdown.pure(
    Change.TopologyDelay(
      SequencedTime(CantonTimestamp.Epoch),
      EffectiveTime(CantonTimestamp.Epoch),
      epsilon = NonNegativeFiniteDuration.tryOfMillis(250),
    )
  )

  override protected def maxTimestampFromStore()(implicit
      traceContext: TraceContext
  ): Future[Option[(SequencedTime, EffectiveTime)]] = store.maxTimestamp()

  override protected def initializeTopologyTimestampPlusEpsilonTracker(
      processorTs: CantonTimestamp
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[EffectiveTime] =
    TopologyTimestampPlusEpsilonTracker.initializeX(timeAdjuster, store, processorTs)

  override protected def extractTopologyUpdates(
      envelopes: List[DefaultOpenEnvelope]
  ): List[AcceptedTopologyTransactionsX] = {
    envelopes
      .mapFilter(ProtocolMessage.select[AcceptedTopologyTransactionsX])
      .map(_.protocolMessage)
  }

  override private[processing] def process(
      sequencingTimestamp: SequencedTime,
      effectiveTimestamp: EffectiveTime,
      sc: SequencerCounter,
      messages: List[AcceptedTopologyTransactionsX],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    val tx = messages.flatMap(_.accepted).flatMap(_.transactions)
    performUnlessClosingEitherU("process-topology-transaction")(
      stateProcessor
        .validateAndApplyAuthorization(
          sequencingTimestamp,
          effectiveTimestamp,
          tx,
          abortIfCascading = false,
          abortOnError = false,
          expectFullAuthorization = false,
        )
    ).merge
      .flatMap { validated =>
        inspectAndAdvanceTopologyTransactionDelay(
          sequencingTimestamp,
          effectiveTimestamp,
          validated,
        )
        import cats.syntax.parallel.*
        performUnlessClosingUSF("notify-topology-transaction-observers")(
          listeners.toList.parTraverse_(
            _.observed(
              sequencingTimestamp,
              effectiveTimestamp,
              sc,
              validated.collect { case tx if tx.rejectionReason.isEmpty => tx.transaction },
            )
          )
        )
      }
  }

  private def inspectAndAdvanceTopologyTransactionDelay(
      sequencingTimestamp: SequencedTime,
      effectiveTimestamp: EffectiveTime,
      validated: Seq[GenericValidatedTopologyTransactionX],
  )(implicit traceContext: TraceContext): Unit = {
    def applyEpsilon(mapping: DomainParametersStateX) = {
      timeAdjuster
        .adjustEpsilon(
          effectiveTimestamp,
          sequencingTimestamp,
          mapping.parameters.topologyChangeDelay,
        )
        .foreach { previous =>
          logger.info(
            s"Updated topology change delay from=${previous} to ${mapping.parameters.topologyChangeDelay}"
          )
        }
      timeAdjuster.effectiveTimeProcessed(effectiveTimestamp)
    }

    val domainParamChanges = validated.flatMap(
      _.collectOf[TopologyChangeOpX.Replace, DomainParametersStateX]
        .filter(
          _.rejectionReason.isEmpty
        )
        .map(_.transaction.transaction.mapping)
    )

    NonEmpty.from(domainParamChanges) match {
      // normally, we shouldn't have any adjustment
      case None => timeAdjuster.effectiveTimeProcessed(effectiveTimestamp)
      case Some(changes) =>
        // if there is one, there should be exactly one
        // If we have several, let's panic now. however, we just pick the last and try to keep working
        if (changes.lengthCompare(1) > 0) {
          logger.error(
            s"Broken or malicious domain topology manager has sent (${changes.length}) domain parameter adjustments at $effectiveTimestamp, will ignore all of them except the last"
          )
        }
        applyEpsilon(changes.last1)
    }
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
  ): Future[(TopologyTransactionProcessorX, DomainTopologyClientWithInitX)] = {
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
