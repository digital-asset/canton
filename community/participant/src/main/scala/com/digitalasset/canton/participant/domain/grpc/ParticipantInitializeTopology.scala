// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.domain.grpc

import akka.stream.Materializer
import cats.data.EitherT
import com.digitalasset.canton.DomainAlias
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.{HashOps, RandomOps}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory}
import com.digitalasset.canton.participant.domain.DomainRegistryError
import com.digitalasset.canton.participant.topology.DomainOnboardingOutbox
import com.digitalasset.canton.protocol.messages.DefaultOpenEnvelope
import com.digitalasset.canton.sequencing.client.{SequencerClient, SequencerClientFactory}
import com.digitalasset.canton.sequencing.handlers.{
  DiscardIgnoredEvents,
  EnvelopeOpener,
  StripSignature,
}
import com.digitalasset.canton.sequencing.protocol.{Batch, Deliver}
import com.digitalasset.canton.sequencing._
import com.digitalasset.canton.store.memory.{InMemorySendTrackerStore, InMemorySequencedEventStore}
import com.digitalasset.canton.time.{Clock, DomainTimeTracker, DomainTimeTrackerConfig}
import com.digitalasset.canton.topology.store.TopologyStore
import com.digitalasset.canton.topology.{DomainId, ParticipantId, UnauthenticatedMemberId}
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.MonadUtil
import io.opentelemetry.api.trace.Tracer

import scala.concurrent.ExecutionContextExecutor

object ParticipantInitializeTopology {

  /** Takes care of requesting approval of the participant's initial topology transactions to the IDM via the sequencer.
    * Before these transactions have been approved, the participant cannot connect to the sequencer because it can't
    * authenticate without the IDM having approved the transactions. Because of that, this initial request is sent by
    * a dynamically created unauthenticated member whose sole purpose is to send this request and wait for the response.
    */
  def apply(
      domainId: DomainId,
      alias: DomainAlias,
      participantId: ParticipantId,
      clock: Clock,
      timeTracker: DomainTimeTrackerConfig,
      processingTimeout: ProcessingTimeout,
      authorizedStore: TopologyStore,
      targetDomainStore: TopologyStore,
      loggerFactory: NamedLoggerFactory,
      sequencerClientFactory: SequencerClientFactory,
      cryptoOps: HashOps with RandomOps,
  )(implicit
      executionContext: ExecutionContextExecutor,
      materializer: Materializer,
      tracer: Tracer,
      traceContext: TraceContext,
      loggingContext: ErrorLoggingContext,
  ): EitherT[FutureUnlessShutdown, DomainRegistryError, Unit] = {
    val unauthenticatedMember =
      UnauthenticatedMemberId.tryCreate(participantId.uid.namespace)(cryptoOps)

    loggingContext.logger.debug(
      s"Unauthenticated member $unauthenticatedMember will register initial topology transactions on behalf of participant $participantId"
    )

    def pushTopologyAndVerify(client: SequencerClient, domainTimeTracker: DomainTimeTracker) = {
      val handle = new SequencerBasedRegisterTopologyTransactionHandle(
        (traceContext, env) => client.sendAsyncUnauthenticated(Batch(List(env)))(traceContext),
        domainId,
        participantId,
        unauthenticatedMember,
        processingTimeout,
        loggerFactory,
      )

      val eventHandler = new UnsignedProtocolEventHandler {
        override def name: String = s"participant-initialize-topology-$alias"

        override def resubscriptionStartsAt(start: ResubscriptionStart)(implicit
            traceContext: TraceContext
        ): FutureUnlessShutdown[Unit] = FutureUnlessShutdown.unit

        override def apply(
            events: BoxedEnvelope[UnsignedEnvelopeBox, DefaultOpenEnvelope]
        ): HandlerResult =
          MonadUtil.sequentialTraverseMonoid(events.value) {
            _.withTraceContext { implicit traceContext =>
              {
                case Deliver(_, _, _, _, batch) => handle.processor(Traced(batch.envelopes))
                case _ => HandlerResult.done
              }
            }
          }
      }

      for {
        _ <- EitherT.right[DomainRegistryError](
          FutureUnlessShutdown.outcomeF(
            client.subscribeAfterUnauthenticated(
              CantonTimestamp.MinValue,
              DiscardIgnoredEvents { StripSignature { EnvelopeOpener(cryptoOps)(eventHandler) } },
              domainTimeTracker,
            )
          )
        )
        // push the initial set of topology transactions to the domain and stop using the unauthenticated member
        _ <- DomainOnboardingOutbox
          .initiateOnboarding(
            alias,
            domainId,
            participantId,
            handle,
            authorizedStore,
            targetDomainStore,
            processingTimeout,
            loggerFactory,
          )
          .leftMap(
            DomainRegistryError.DomainRegistryInternalError
              .InitialOnboardingError(_): DomainRegistryError
          )
      } yield ()
    }

    for {
      unauthenticatedSequencerClient <- sequencerClientFactory
        .create(
          unauthenticatedMember,
          new InMemorySequencedEventStore(loggerFactory),
          new InMemorySendTrackerStore(),
        )
        .leftMap[DomainRegistryError](
          DomainRegistryError.ConnectionErrors.FailedToConnectToSequencer.Error(_)
        )
        .mapK(FutureUnlessShutdown.outcomeK)

      domainTimeTracker = DomainTimeTracker(
        timeTracker,
        clock,
        unauthenticatedSequencerClient,
        loggerFactory,
      )

      _ <- pushTopologyAndVerify(unauthenticatedSequencerClient, domainTimeTracker)
      _ = {
        unauthenticatedSequencerClient.closeSubscription()
        domainTimeTracker.close()
      }
      _ <- EitherT
        .right(unauthenticatedSequencerClient.completion)
        .mapK(FutureUnlessShutdown.outcomeK)
    } yield {
      unauthenticatedSequencerClient.close()
    }
  }
}
