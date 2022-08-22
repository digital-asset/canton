// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.topology

import cats.data.EitherT
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.domain.topology.store.RegisterTopologyTransactionResponseStore
import com.digitalasset.canton.domain.topology.store.RegisterTopologyTransactionResponseStore.Response
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown, Lifecycle}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.messages.{
  DefaultOpenEnvelope,
  RegisterTopologyTransactionRequest,
  RegisterTopologyTransactionResponse,
}
import com.digitalasset.canton.sequencing._
import com.digitalasset.canton.sequencing.client.{
  SendAsyncClientError,
  SendCallback,
  SequencerClient,
}
import com.digitalasset.canton.sequencing.protocol._
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.MonadUtil
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.control.NonFatal

/** When a new member joins, they send register topology requests to the Topology Manager via the sequencer.
  * This handler takes care of the IDM handling these requests and sending the response back via the sequencer
  * while also supporting crashes and making sure the response is sent at least once.
  */
class DomainTopologyManagerEventHandler(
    store: RegisterTopologyTransactionResponseStore,
    requestHandler: DomainTopologyManagerRequestService.Handler,
    sequencerSendResponse: (
        OpenEnvelope[RegisterTopologyTransactionResponse.Result],
        SendCallback,
    ) => EitherT[Future, SendAsyncClientError, Unit],
    protocolVersion: ProtocolVersion,
    override protected val timeouts: ProcessingTimeout,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends UnsignedProtocolEventHandler
    with FlagCloseable
    with NamedLogging {

  /** Human-readable name of the application handler for logging and debugging */
  override def name: String = "domain-topology-manager-event-handler"

  override def apply(events: UnsignedEnvelopeBox[DefaultOpenEnvelope]): HandlerResult = {
    val requests: Seq[Traced[RegisterTopologyTransactionRequest]] = events.value.collect {
      case t @ Traced(Deliver(_sc, _ts, _, _, batch)) =>
        batch.envelopes.map(_.protocolMessage).collect {
          case request: RegisterTopologyTransactionRequest => Traced(request)(t.traceContext)
        }
    }.flatten
    FutureUnlessShutdown.outcomeF(
      MonadUtil.sequentialTraverseMonoid(requests)(Traced.lift(handle(_)(_)))
    )
  }

  override def subscriptionStartsAt(start: SubscriptionStart)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = FutureUnlessShutdown.unit

  private def handle(
      request: RegisterTopologyTransactionRequest
  )(implicit traceContext: TraceContext): Future[AsyncResult] = {
    for {
      // the logic below supports crash recovery.
      response <- store.getResponse(request.requestId).value
      result = response match {
        // if the request has no response recorded yet, it means it is a new request and let's handle it normally.
        case None =>
          logger.debug(
            s"New register topology transaction request from participant ${request.participant} with requestId = ${request.requestId}, size=${request.transactions.size}"
          )
          AsyncResult.async(handleTopologyRequest(request))
        // if the response has been recorded before, it means we're now replaying events
        case Some(Response(response, isCompleted)) =>
          logger.debug(
            s"Previous register topology transaction request from participant ${request.participant} with requestId = ${request.requestId}, size=${request.transactions.size}"
          )
          // if this response recorded before had not been sent yet, then let's send it. otherwise we're done.
          if (!isCompleted)
            AsyncResult.async(sendResponse(response))
          else AsyncResult.immediate
      }
    } yield result
  }

  private def handleTopologyRequest(
      request: RegisterTopologyTransactionRequest
  )(implicit traceContext: TraceContext): Future[Unit] = {
    for {
      // TODO(i4933) we need to add a signature to the request
      //   - signature must match participant
      //   - config flag / domain parameter ensuring that participant only sends transactions related to itself
      //   - initial registration must not contain anything other than a cert and some keys
      //   - initial registration must be limited to a handful of certs and keys (100, configurable)
      responseResults <- requestHandler.newRequest(
        request.requestedBy,
        request.participant,
        request.transactions,
      )
      pendingResponseE = RegisterTopologyTransactionResponse.create(
        request,
        responseResults,
        protocolVersion,
      )
      pendingResponse <- pendingResponseE.fold(
        { case e @ RegisterTopologyTransactionResponse.ResultVersionsMixture =>
          Future.failed(new IllegalStateException(e.message))
        },
        Future.successful,
      )
      _ <- store.savePendingResponse(pendingResponse)
      result <- sendResponse(pendingResponse)
    } yield result
  }

  private def sendResponse(
      response: RegisterTopologyTransactionResponse.Result
  )(implicit traceContext: TraceContext): Future[Unit] = {
    val batch = OpenEnvelope(
      response,
      Recipients.cc(response.requestedBy),
      protocolVersion,
    )
    SequencerClient
      .sendWithRetries(
        sequencerSendResponse(batch, _),
        maxRetries = timeouts.unbounded.retries(1.second),
        delay = 1.second,
        sendDescription =
          s"Register topology transaction response for participant ${response.participant} with requestId = ${response.requestId}",
        errMsg =
          s"Failed to send register topology transaction response for participant ${response.participant} with requestId = ${response.requestId}",
        flagCloseable = this,
      )
      .flatMap { _ =>
        store.completeResponse(response.requestId)
      }
      .recover { case NonFatal(e) =>
        logger.error(
          s"After many attempts, failed to send register topology transaction response for participant ${response.participant} with requestId = ${response.requestId}",
          e,
        )
      }
  }

  override def onClosed(): Unit = Lifecycle.close(store)(logger)
}
