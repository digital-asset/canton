// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.mediator.topology

import cats.data.EitherT
import cats.syntax.functor.*
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.sequencing.*
import com.digitalasset.canton.sequencing.client.{
  SendAsyncClientError,
  SendCallback,
  SequencerClient,
}
import com.digitalasset.canton.sequencing.protocol.{Deliver, OpenEnvelope, Recipients}
import com.digitalasset.canton.time.DomainTimeTracker
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.transaction.OwnerToKeyMappingX
import com.digitalasset.canton.topology.{DomainId, TopologyStateProcessorX}
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.{ErrorUtil, MonadUtil}
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

class MediatorXTopologyRequestProcessor(
    domainId: DomainId,
    sequencerSendResponse: (
        OpenEnvelope[ProtocolMessage],
        SendCallback,
    ) => EitherT[Future, SendAsyncClientError, Unit],
    protocolVersion: ProtocolVersion,
    processor: TopologyStateProcessorX,
    val timeouts: ProcessingTimeout,
    val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends UnsignedProtocolEventHandler
    with NamedLogging
    with FlagCloseable {

  {
    import TraceContext.Implicits.Empty.*
    ErrorUtil.requireState(
      protocolVersion == ProtocolVersion.dev,
      "only supported for dev version!",
    )
  }
  override def name: String = "mediator-x-topology-request-processor"

  override def subscriptionStartsAt(start: SubscriptionStart, domainTimeTracker: DomainTimeTracker)(
      implicit traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = FutureUnlessShutdown.unit

  override def apply(
      events: BoxedEnvelope[UnsignedEnvelopeBox, DefaultOpenEnvelope]
  ): HandlerResult = performUnlessClosingF(functionFullName)(Future {
    val requests: Seq[Traced[(CantonTimestamp, RegisterTopologyTransactionRequestX)]] = {
      events.value.collect { case t @ Traced(Deliver(_sc, ts, _, _, batch)) =>
        batch.envelopes.map(_.protocolMessage).collect {
          case request: RegisterTopologyTransactionRequestX => Traced((ts, request))(t.traceContext)
        }
      }.flatten
    }
    // needs to be sequential now as we are applying the changes to the state
    AsyncResult(
      MonadUtil.sequentialTraverse(requests)(process).map(_ => ())
    )
  })(ec, TraceContext.empty)

  private def process(
      event: Traced[(CantonTimestamp, RegisterTopologyTransactionRequestX)]
  ): FutureUnlessShutdown[Unit] = event.withTraceContext { implicit traceContext => event =>
    {
      val (ts, request) = event
      def genResponse(res: RegisterTopologyTransactionResponseResult.State) = {
        val response = OpenEnvelope(
          RegisterTopologyTransactionResponseX.create(
            requestedBy = request.requestedBy,
            requestedFor = request.requestedFor,
            requestId = request.requestId,
            results = request.transactions.map(_ => res),
            domainId = domainId,
            protocolVersion = protocolVersion,
          ),
          Recipients.cc(request.requestedBy),
        )(protocolVersion)
        logger.info(s"Sending ${res} for ${request.requestId}")
        send(response, "responding to request")
      }

      performUnlessClosingEitherU(functionFullName)(
        processor
          .validateAndApplyAuthorization(
            SequencedTime(ts),
            EffectiveTime(ts),
            request.transactions,
            abortIfCascading = false,
            abortOnError = true,
            expectFullAuthorization = true,
          )
      ).leftMap(x => s"Txs failed ${x}")
        .biflatMap(
          failed => {
            logger.warn(s"Topology evaluation failed with ${failed}")
            EitherT
              .right[String](genResponse(RegisterTopologyTransactionResponseResult.State.Failed))
          },
          _success => {
            EitherT.right[String](for {
              _ <- acceptTransactions(ts, request)
              _ <- genResponse(RegisterTopologyTransactionResponseResult.State.Accepted)
            } yield ())
          },
        )
        .fold(_ => (), _ => ())
    }
  }

  private def acceptTransactions(
      ts: CantonTimestamp,
      request: RegisterTopologyTransactionRequestX,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    val msg = AcceptedTopologyTransactionsX.create(
      accepted = List(
        AcceptedTopologyTransactionsX.AcceptedRequest(
          requestId = request.requestId,
          transactions = request.transactions,
        )
      ),
      domainId = domainId,
      protocolVersion = protocolVersion,
    )
    performUnlessClosingF(functionFullName)(
      processor.store.findPositiveTransactions(
        asOf = ts,
        asOfInclusive =
          false, // must be inclusive as we don't want to include folks that we just added
        isProposal = false,
        types = Seq(OwnerToKeyMappingX.code),
        filterUid = None,
        filterNamespace = None,
      )
    ).flatMap { res =>
      val members = res
        .collectOfMapping[OwnerToKeyMappingX]
        .result
        .map(_.transaction.transaction.mapping.member)
      Recipients.ofSet(members.toSet) match {
        case Some(recipients) =>
          send(OpenEnvelope(msg, recipients)(protocolVersion), "sending accepted txs to members")
        case None =>
          logger.error("Didn't find recipients")
          FutureUnlessShutdown.unit
      }
    }
  }

  private def send(
      batch: OpenEnvelope[ProtocolMessage],
      description: String,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    SequencerClient
      .sendWithRetries(
        sequencerSendResponse(batch, _),
        maxRetries = timeouts.unbounded.retries(1.second),
        delay = 1.second,
        sendDescription = description,
        errMsg = s"Failed to send: ${description}",
        flagCloseable = this,
      )
      .recover { case NonFatal(e) =>
        performUnlessClosing("recover")(
          logger.error(
            s"After many attempts, failed to send $description",
            e,
          )
        )
      }

}
