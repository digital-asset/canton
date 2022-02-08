// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.initialization

import akka.stream.Materializer
import cats.data.EitherT
import cats.instances.future._
import com.digitalasset.canton.DomainId
import com.digitalasset.canton.crypto.{
  Crypto,
  DomainSnapshotSyncCryptoApi,
  DomainSyncCryptoClient,
  PublicKey,
}
import com.digitalasset.canton.domain.config.{DomainBaseConfig, DomainConfig, DomainNodeParameters}
import com.digitalasset.canton.domain.topology._
import com.digitalasset.canton.domain.topology.client.DomainInitializationObserver
import com.digitalasset.canton.domain.topology.store.RegisterTopologyTransactionResponseStore
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory}
import com.digitalasset.canton.protocol.messages.DomainTopologyTransactionMessage
import com.digitalasset.canton.resource.Storage
import com.digitalasset.canton.sequencing.client.{SendType, SequencerClient, SequencerClientFactory}
import com.digitalasset.canton.sequencing.handlers.{
  DiscardIgnoredEvents,
  EnvelopeOpener,
  StripSignature,
}
import com.digitalasset.canton.sequencing.protocol.{Batch, OpenEnvelope, Recipients}
import com.digitalasset.canton.sequencing.{
  AsyncResult,
  HttpSequencerConnection,
  SequencerConnection,
  UnsignedEnvelopeBox,
}
import com.digitalasset.canton.store.db.SequencerClientDiscriminator
import com.digitalasset.canton.store.{
  IndexedStringStore,
  SendTrackerStore,
  SequencedEventStore,
  SequencerCounterTrackerStore,
}
import com.digitalasset.canton.time.{Clock, DomainTimeTracker}
import com.digitalasset.canton.topology.client.DomainTopologyClientWithInit
import com.digitalasset.canton.topology.processing.TopologyTransactionProcessor
import com.digitalasset.canton.topology.store.{StoredTopologyTransactions, TopologyStore}
import com.digitalasset.canton.topology.transaction.TopologyChangeOp
import com.digitalasset.canton.topology.{DomainMember, DomainTopologyManagerId, KeyOwner, NodeId}
import com.digitalasset.canton.tracing.TraceContext
import io.opentelemetry.api.trace.Tracer

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}

object TopologyManagementInitialization {

  def sequenceInitialTopology(
      id: DomainId,
      client: SequencerClient,
      authorizedTopologySnapshot: StoredTopologyTransactions[TopologyChangeOp],
      domainMembers: Set[DomainMember],
      recentSnapshot: DomainSnapshotSyncCryptoApi,
  )(implicit
      executionContext: ExecutionContext,
      loggingContext: ErrorLoggingContext,
  ): Future[Unit] = {
    implicit val traceContext = loggingContext.traceContext
    for {
      content <- DomainTopologyTransactionMessage
        .tryCreate(authorizedTopologySnapshot.result.map(_.transaction).toList, recentSnapshot, id)
      batch = domainMembers.map(member => OpenEnvelope(content, Recipients.cc(member)))
      _ <- SequencerClient.sendWithRetries(
        callback => client.sendAsync(Batch(batch.toList), SendType.Other, callback = callback),
        maxRetries = 600,
        delay = 1.second,
        sendDescription = "Send initial topology transaction to domain members",
        errMsg = "Failed to send initial topology transactions to domain members",
        flagCloseable = client,
      )
    } yield ()
  }

  def apply(
      config: DomainBaseConfig,
      id: DomainId,
      nodeId: NodeId,
      storage: Storage,
      clock: Clock,
      crypto: Crypto,
      syncCrypto: DomainSyncCryptoClient,
      sequencedTopologyStore: TopologyStore,
      sequencerConnection: SequencerConnection,
      domainTopologyManager: DomainTopologyManager,
      domainTopologyService: DomainTopologyManagerRequestService,
      topologyManagerSequencerCounterTrackerStore: SequencerCounterTrackerStore,
      topologyProcessor: TopologyTransactionProcessor,
      topologyClient: DomainTopologyClientWithInit,
      initialKeys: Map[KeyOwner, Seq[PublicKey]],
      sequencerClientFactory: SequencerClientFactory,
      parameters: DomainNodeParameters,
      indexedStringStore: IndexedStringStore,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      executionContext: ExecutionContextExecutor,
      materializer: Materializer,
      tracer: Tracer,
      loggingContext: ErrorLoggingContext,
  ): EitherT[Future, String, DomainTopologyDispatcher] = {
    implicit val traceContext: TraceContext = loggingContext.traceContext
    val managerId: DomainTopologyManagerId = domainTopologyManager.managerId
    val dispatcherLoggerFactory = loggerFactory.appendUnnamedKey("node", "identity")
    // if we're only running a embedded sequencer then there's no need to sequence topology transactions
    val isEmbedded = config match {
      case _: DomainConfig => true
      case _ => false
    }
    val addressSequencerAsDomainMember = !isEmbedded
    for {
      managerDiscriminator <- EitherT.right(
        SequencerClientDiscriminator.fromDomainMember(managerId, indexedStringStore)
      )
      newClient <- {
        val sequencedEventStore =
          SequencedEventStore(
            storage,
            managerDiscriminator,
            parameters.processingTimeouts,
            dispatcherLoggerFactory,
          )
        val sendTrackerStore = SendTrackerStore(storage)
        sequencerClientFactory(
          managerId,
          sequencedEventStore,
          sendTrackerStore,
        )
      }
      timeTracker = DomainTimeTracker(config.timeTracker, clock, newClient, loggerFactory)
      eventHandler = {
        val domainTopologyServiceHandler =
          new DomainTopologyManagerEventHandler(
            RegisterTopologyTransactionResponseStore(storage, crypto.pureCrypto, loggerFactory),
            domainTopologyService.newRequest,
            (env, callback) => newClient.sendAsync(Batch(List(env)), callback = callback),
            parameters.processingTimeouts,
            loggerFactory,
          )
        val topologyProcessorHandler = topologyProcessor.createHandler(id)
        DiscardIgnoredEvents {
          StripSignature {
            EnvelopeOpener[UnsignedEnvelopeBox](crypto.pureCrypto) { ev =>
              for {
                r1 <- domainTopologyServiceHandler(ev)
                r2 <- topologyProcessorHandler(ev)
              } yield AsyncResult.monoidAsyncResult.combine(r1, r2)
            }
          }
        }
      }
      _ <- EitherT.right[String](
        newClient.subscribeTracking(
          topologyManagerSequencerCounterTrackerStore,
          eventHandler,
          timeTracker,
        )
      )

      initializationObserver <- EitherT.right(
        DomainInitializationObserver(
          id,
          topologyClient,
          sequencedTopologyStore,
          parameters.processingTimeouts,
          loggerFactory,
        )
      )

      // before starting the domain identity dispatcher, we need to make sure the initial topology transactions
      // have been sequenced. in the case of external sequencers this is done with admin commands and we just need to wait,
      // but for embedded sequencers we need to explicitly sequence these transactions here if that's not already been done.
      hasInitData <- EitherT.right(initializationObserver.initialisedAtHead)
      _ <-
        if (isEmbedded && !hasInitData) {
          EitherT.right(for {
            authorizedTopologySnapshot <- domainTopologyManager.store.headTransactions(traceContext)
            _ <- sequenceInitialTopology(
              id,
              newClient,
              authorizedTopologySnapshot,
              DomainMember.list(id, addressSequencerAsDomainMember),
              syncCrypto.currentSnapshotApproximation,
            )
          } yield ())
        } else EitherT.rightT(())
      _ <- sequencerConnection match {
        // the CCF sequencer is special case in that it skips initial topology bootstrapping since it does not rely on that
        // for authentication. so there is no need to wait.
        case _: HttpSequencerConnection => EitherT.pure(())
        // GRPC-based sequencers all go through the initial topology bootstrapping so we need to wait here to make sure
        // that initial topology data has been sequenced before starting the topology dispatcher
        case _ => EitherT.right(initializationObserver.waitUntilInitialisedAndEffective.unwrap)
      }
      dispatcher <- EitherT.rightT(
        DomainTopologyDispatcher.create(
          id,
          domainTopologyManager,
          topologyClient,
          initialKeys,
          sequencedTopologyStore,
          newClient,
          timeTracker,
          crypto,
          clock,
          addressSequencerAsDomainMember,
          parameters,
          dispatcherLoggerFactory,
        )
      )
    } yield dispatcher
  }
}
