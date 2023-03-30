// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.domain

import akka.stream.Materializer
import cats.data.EitherT
import com.digitalasset.canton.DomainAlias
import com.digitalasset.canton.config.{DomainTimeTrackerConfig, ProcessingTimeout}
import com.digitalasset.canton.crypto.Crypto
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory}
import com.digitalasset.canton.participant.sync.SyncServiceError.SyncServiceAlarm
import com.digitalasset.canton.participant.topology.DomainOnboardingOutbox
import com.digitalasset.canton.sequencing.*
import com.digitalasset.canton.sequencing.client.RequestSigner.UnauthenticatedRequestSigner
import com.digitalasset.canton.sequencing.client.{SequencerClient, SequencerClientFactory}
import com.digitalasset.canton.sequencing.handlers.DiscardIgnoredEvents
import com.digitalasset.canton.sequencing.protocol.{Batch, ClosedEnvelope, Deliver, SequencedEvent}
import com.digitalasset.canton.store.SequencedEventStore.OrdinarySequencedEvent
import com.digitalasset.canton.store.memory.{InMemorySendTrackerStore, InMemorySequencedEventStore}
import com.digitalasset.canton.time.{Clock, DomainTimeTracker}
import com.digitalasset.canton.topology.store.{TopologyStore, TopologyStoreId}
import com.digitalasset.canton.topology.{DomainId, ParticipantId, UnauthenticatedMemberId}
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.MonadUtil
import com.digitalasset.canton.version.ProtocolVersion
import io.opentelemetry.api.trace.Tracer

import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.{Failure, Success}

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
      authorizedStore: TopologyStore[TopologyStoreId.AuthorizedStore],
      targetDomainStore: TopologyStore[TopologyStoreId.DomainStore],
      loggerFactory: NamedLoggerFactory,
      sequencerClientFactory: SequencerClientFactory,
      connection: SequencerConnection,
      crypto: Crypto,
      protocolVersion: ProtocolVersion,
  )(implicit
      executionContext: ExecutionContextExecutor,
      materializer: Materializer,
      tracer: Tracer,
      traceContext: TraceContext,
      loggingContext: ErrorLoggingContext,
  ): EitherT[FutureUnlessShutdown, DomainRegistryError, Boolean] = {
    val unauthenticatedMember =
      UnauthenticatedMemberId.tryCreate(participantId.uid.namespace)(crypto.pureCrypto)

    loggingContext.logger.debug(
      s"Unauthenticated member $unauthenticatedMember will register initial topology transactions on behalf of participant $participantId"
    )

    def pushTopologyAndVerify(client: SequencerClient, domainTimeTracker: DomainTimeTracker) = {
      val handle = new SequencerBasedRegisterTopologyTransactionHandle(
        (traceContext, env) =>
          client.sendAsyncUnauthenticated(
            Batch(List(env), client.protocolVersion)
          )(traceContext),
        domainId,
        participantId,
        unauthenticatedMember,
        protocolVersion,
        processingTimeout,
        loggerFactory,
      )

      val eventHandler = new OrdinaryApplicationHandler[ClosedEnvelope] {
        override def name: String = s"participant-initialize-topology-$alias"

        override def subscriptionStartsAt(
            start: SubscriptionStart,
            domainTimeTracker: DomainTimeTracker,
        )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
          FutureUnlessShutdown.unit

        override def apply(
            tracedEvents: Traced[Seq[BoxedEnvelope[OrdinarySequencedEvent, ClosedEnvelope]]]
        ): HandlerResult = {
          val openEvents = tracedEvents.value.map { closedSignedEvent =>
            val closedEvent = closedSignedEvent.signedEvent.content
            val (openEvent, openingErrors) = {
              SequencedEvent.openEnvelopes(closedEvent)(protocolVersion, crypto.pureCrypto)
            }

            openingErrors.foreach { error =>
              val cause =
                s"Received an envelope at ${closedEvent.timestamp} that cannot be opened. " +
                  s"Discarding envelope... Reason: $error"
              SyncServiceAlarm.Warn(cause).report()
            }

            Traced(openEvent)(closedSignedEvent.traceContext)
          }

          MonadUtil.sequentialTraverseMonoid(openEvents) {
            _.withTraceContext { implicit traceContext =>
              {
                case Deliver(_, _, _, _, batch) => handle.processor(Traced(batch.envelopes))
                case _ => HandlerResult.done
              }
            }
          }
        }
      }

      for {
        _ <- EitherT.right[DomainRegistryError](
          FutureUnlessShutdown.outcomeF(
            client.subscribeAfterUnauthenticated(
              CantonTimestamp.MinValue,
              // There is no point in ignoring events in an unauthenticated subscription
              DiscardIgnoredEvents(loggerFactory)(eventHandler),
              domainTimeTracker,
            )
          )
        )
        // push the initial set of topology transactions to the domain and stop using the unauthenticated member
        success <- DomainOnboardingOutbox
          .initiateOnboarding(
            alias,
            domainId,
            protocolVersion,
            participantId,
            handle,
            authorizedStore,
            targetDomainStore,
            processingTimeout,
            loggerFactory,
            crypto,
          )
      } yield success
    }

    for {
      unauthenticatedSequencerClient <- sequencerClientFactory
        .create(
          unauthenticatedMember,
          new InMemorySequencedEventStore(loggerFactory),
          new InMemorySendTrackerStore(),
          UnauthenticatedRequestSigner,
          connection,
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

      success <- {
        def closeEverything(): Future[Unit] = {
          unauthenticatedSequencerClient.closeSubscription()
          domainTimeTracker.close()
          unauthenticatedSequencerClient.completion.transform { _ =>
            Success(unauthenticatedSequencerClient.close())
          }
        }

        EitherT {
          FutureUnlessShutdown {
            pushTopologyAndVerify(unauthenticatedSequencerClient, domainTimeTracker).value.unwrap
              .transformWith {
                case Failure(exception) =>
                  // Close everything and then return the original failure
                  closeEverything().flatMap(_ => Future.failed(exception))
                case Success(value) =>
                  // Close everything and then return the result
                  closeEverything().map(_ => value)
              }
          }
        }
      }
    } yield {
      success
    }
  }
}
