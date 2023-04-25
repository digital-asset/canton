// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.domain

import cats.data.EitherT
import cats.syntax.functorFilter.*
import cats.syntax.parallel.*
import com.digitalasset.canton.DiscardOps
import com.digitalasset.canton.config.CantonRequireTypes.LengthLimitedString.TopologyRequestId
import com.digitalasset.canton.config.CantonRequireTypes.String255
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown, UnlessShutdown}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil.mapErr
import com.digitalasset.canton.participant.topology.RegisterTopologyTransactionHandleCommon
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.sequencing.HandlerResult
import com.digitalasset.canton.sequencing.client.SendAsyncClientError
import com.digitalasset.canton.sequencing.protocol.{OpenEnvelope, Recipients}
import com.digitalasset.canton.topology.transaction.SignedTopologyTransactionX.GenericSignedTopologyTransactionX
import com.digitalasset.canton.topology.transaction.{SignedTopologyTransaction, TopologyChangeOp}
import com.digitalasset.canton.topology.{DomainId, DomainTopologyManagerId, Member, ParticipantId}
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc, Traced}
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.{EitherTUtil, FutureUtil}
import com.digitalasset.canton.version.ProtocolVersion
import io.functionmeta.functionFullName

import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import scala.collection.concurrent
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.jdk.CollectionConverters.*

trait RegisterTopologyTransactionHandleWithProcessor[TX]
    extends RegisterTopologyTransactionHandleCommon[TX] {
  def processor: Traced[Seq[DefaultOpenEnvelope]] => HandlerResult
}

/** Handle used in order to request approval of participant's topology transactions by the IDM and wait for the
  * responses by sending RegisterTopologyTransactionRequest's via the sequencer.
  * This gets created in [[com.digitalasset.canton.participant.topology.ParticipantTopologyDispatcher]]
  */
class SequencerBasedRegisterTopologyTransactionHandle(
    send: (
        TraceContext,
        OpenEnvelope[ProtocolMessage],
    ) => EitherT[Future, SendAsyncClientError, Unit],
    domainId: DomainId,
    participantId: ParticipantId,
    requestedBy: Member,
    protocolVersion: ProtocolVersion,
    protected val timeouts: ProcessingTimeout,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends RegisterTopologyTransactionHandleWithProcessor[
      SignedTopologyTransaction[TopologyChangeOp]
    ]
    with NamedLogging {

  private val service =
    new ParticipantDomainTopologyService(domainId, send, protocolVersion, timeouts, loggerFactory)

  // must be used by the event handler of a sequencer subscription in order to complete the promises of requests sent with the given sequencer client
  val processor: Traced[Seq[DefaultOpenEnvelope]] => HandlerResult = service.processor

  override def submit(
      transactions: Seq[SignedTopologyTransaction[TopologyChangeOp]]
  ): FutureUnlessShutdown[Seq[RegisterTopologyTransactionResponseResult.State]] = {
    RegisterTopologyTransactionRequest
      .create(
        requestedBy = requestedBy,
        participant = participantId,
        requestId = String255.tryCreate(UUID.randomUUID().toString),
        transactions = transactions.toList,
        domainId = domainId,
        protocolVersion = protocolVersion,
      )
      .toList
      .parTraverse(service.registerTopologyTransaction)
      .map(_.flatten)
  }

  override def onClosed(): Unit = service.close()
}

class SequencerBasedRegisterTopologyTransactionHandleX(
    send: (
        TraceContext,
        OpenEnvelope[ProtocolMessage],
    ) => EitherT[Future, SendAsyncClientError, Unit],
    domainId: DomainId,
    participantId: ParticipantId,
    requestedBy: Member,
    protocolVersion: ProtocolVersion,
    protected val timeouts: ProcessingTimeout,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends RegisterTopologyTransactionHandleWithProcessor[
      GenericSignedTopologyTransactionX
    ]
    with NamedLogging {

  private val service =
    new ParticipantDomainTopologyServiceX(domainId, send, protocolVersion, timeouts, loggerFactory)

  // must be used by the event handler of a sequencer subscription in order to complete the promises of requests sent with the given sequencer client
  val processor: Traced[Seq[DefaultOpenEnvelope]] => HandlerResult = service.processor

  override def submit(
      transactions: Seq[GenericSignedTopologyTransactionX]
  ): FutureUnlessShutdown[Seq[RegisterTopologyTransactionResponseResult.State]] = {
    service.registerTopologyTransaction(
      RegisterTopologyTransactionRequestX
        .create(
          requestedBy = requestedBy,
          requestedFor = participantId,
          requestId = String255.tryCreate(UUID.randomUUID().toString),
          transactions = transactions.toList,
          domainId = domainId,
          protocolVersion = protocolVersion,
        )
    )
  }

  override def onClosed(): Unit = service.close()
}

private[domain] abstract class ParticipantDomainTopologyServiceCommon[
    Request <: ProtocolMessage,
    RequestIndex,
    Response,
    Result,
](
    send: (
        TraceContext,
        OpenEnvelope[ProtocolMessage],
    ) => EitherT[Future, SendAsyncClientError, Unit],
    recipients: Recipients,
    protocolVersion: ProtocolVersion,
    protected val timeouts: ProcessingTimeout,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit val ec: ExecutionContext)
    extends NamedLogging
    with FlagCloseable {

  protected def requestToIndex(request: Request): RequestIndex
  protected def responseToIndex(response: Response): RequestIndex
  protected def responseToResult(response: Response): Result
  protected def protocolMessageToResponse(m: ProtocolMessage): Option[Response]

  private val responsePromiseMap: concurrent.Map[RequestIndex, Promise[UnlessShutdown[Result]]] =
    new ConcurrentHashMap[RequestIndex, Promise[UnlessShutdown[Result]]]().asScala

  def registerTopologyTransaction(
      request: Request
  ): FutureUnlessShutdown[Result] = {
    implicit val traceContext: TraceContext = TraceContextGrpc.fromGrpcContext
    val responseF = getResponse(request)
    for {
      _ <- performUnlessClosingF(functionFullName)(
        EitherTUtil.toFuture(mapErr(sendRequest(request)))
      )
      response <- responseF
    } yield response
  }

  private def sendRequest(
      request: Request
  )(implicit traceContext: TraceContext): EitherT[Future, SendAsyncClientError, Unit] = {
    logger.debug(s"Sending register topology transaction request ${request}")
    EitherTUtil.logOnError(
      send(
        traceContext,
        OpenEnvelope(request, recipients)(protocolVersion),
      ),
      s"Failed sending register topology transaction request ${request}",
    )
  }

  private def getResponse(request: Request)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Result] =
    FutureUnlessShutdown {
      val promise = Promise[UnlessShutdown[Result]]()
      responsePromiseMap.put(requestToIndex(request), promise).discard
      FutureUtil.logOnFailure(
        promise.future.map { result =>
          result match {
            case _ @UnlessShutdown.Outcome(_) =>
              logger.debug(
                s"Received register topology transaction response ${request}"
              )
            case _: UnlessShutdown.AbortedDueToShutdown =>
              logger.info(
                show"Shutdown before receiving register topology transaction response ${request}"
              )
          }
          result
        },
        show"Failed to receive register topology transaction response ${request}",
      )
    }

  private[domain] val processor: Traced[Seq[DefaultOpenEnvelope]] => HandlerResult = envs =>
    envs.withTraceContext { implicit traceContext => envs =>
      HandlerResult.asynchronous(performUnlessClosingF(s"${getClass.getSimpleName}-processor") {
        Future {
          envs.mapFilter(env => protocolMessageToResponse(env.protocolMessage)).foreach {
            response =>
              responsePromiseMap
                .get(responseToIndex(response))
                .foreach(_.trySuccess(UnlessShutdown.Outcome(responseToResult(response))))
          }
        }
      })
    }

  override def onClosed(): Unit = {
    responsePromiseMap.values.foreach(
      _.trySuccess(UnlessShutdown.AbortedDueToShutdown).discard[Boolean]
    )
  }
}
class ParticipantDomainTopologyService[
    Request,
    RequestIndex,
    Response,
    Result,
](
    domainId: DomainId,
    send: (
        TraceContext,
        OpenEnvelope[ProtocolMessage],
    ) => EitherT[Future, SendAsyncClientError, Unit],
    protocolVersion: ProtocolVersion,
    timeouts: ProcessingTimeout,
    loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends ParticipantDomainTopologyServiceCommon[
      RegisterTopologyTransactionRequest,
      (TopologyRequestId, ParticipantId),
      RegisterTopologyTransactionResponse.Result,
      Seq[RegisterTopologyTransactionResponseResult.State],
    ](
      send,
      Recipients.cc(DomainTopologyManagerId(domainId)),
      protocolVersion,
      timeouts,
      loggerFactory,
    ) {

  override protected def requestToIndex(
      request: RegisterTopologyTransactionRequest
  ): (TopologyRequestId, ParticipantId) = (request.requestId, request.participant)
  override protected def responseToIndex(
      response: RegisterTopologyTransactionResponse.Result
  ): (TopologyRequestId, ParticipantId) = (response.requestId, response.participant)
  override protected def responseToResult(
      response: RegisterTopologyTransactionResponse.Result
  ): Seq[RegisterTopologyTransactionResponseResult.State] = response.results.map(_.state)

  override protected def protocolMessageToResponse(
      m: ProtocolMessage
  ): Option[RegisterTopologyTransactionResponse.Result] = m match {
    case m: RegisterTopologyTransactionResponse.Result => Some(m)
    case _ => None
  }

}

class ParticipantDomainTopologyServiceX[
    Request,
    RequestIndex,
    Response,
    Result,
](
    domainId: DomainId,
    send: (
        TraceContext,
        OpenEnvelope[ProtocolMessage],
    ) => EitherT[Future, SendAsyncClientError, Unit],
    protocolVersion: ProtocolVersion,
    timeouts: ProcessingTimeout,
    loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends ParticipantDomainTopologyServiceCommon[
      RegisterTopologyTransactionRequestX,
      (TopologyRequestId, Member),
      RegisterTopologyTransactionResponseX,
      Seq[RegisterTopologyTransactionResponseResult.State],
    ](
      send,
      // TODO(#11255) get the mediator id via connect client temporarily until we have group notifications
      Recipients.cc(DomainTopologyManagerId(domainId)),
      protocolVersion,
      timeouts,
      loggerFactory,
    ) {

  override protected def requestToIndex(
      request: RegisterTopologyTransactionRequestX
  ): (TopologyRequestId, Member) = (request.requestId, request.requestedFor)
  override protected def responseToIndex(
      response: RegisterTopologyTransactionResponseX
  ): (TopologyRequestId, Member) = (response.requestId, response.requestedFor)
  override protected def responseToResult(
      response: RegisterTopologyTransactionResponseX
  ): Seq[RegisterTopologyTransactionResponseResult.State] = response.results.map(_.state)

  override protected def protocolMessageToResponse(
      m: ProtocolMessage
  ): Option[RegisterTopologyTransactionResponseX] = m match {
    case m: RegisterTopologyTransactionResponseX => Some(m)
    case _ => None
  }

}
