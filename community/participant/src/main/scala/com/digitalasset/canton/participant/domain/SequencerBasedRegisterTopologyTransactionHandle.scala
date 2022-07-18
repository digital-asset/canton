// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.domain

import cats.data.EitherT
import cats.syntax.traverse._
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.LengthLimitedString.TopologyRequestId
import com.digitalasset.canton.config.RequireTypes.String255
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown, UnlessShutdown}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.grpc.CantonGrpcUtil.mapErr
import com.digitalasset.canton.participant.topology.RegisterTopologyTransactionHandle
import com.digitalasset.canton.protocol.messages.{
  DefaultOpenEnvelope,
  RegisterTopologyTransactionRequest,
  RegisterTopologyTransactionResponse,
  RegisterTopologyTransactionResponseResult,
}
import com.digitalasset.canton.sequencing.HandlerResult
import com.digitalasset.canton.sequencing.client.SendAsyncClientError
import com.digitalasset.canton.sequencing.protocol.{OpenEnvelope, Recipients}
import com.digitalasset.canton.topology.transaction.{SignedTopologyTransaction, TopologyChangeOp}
import com.digitalasset.canton.topology.{DomainId, DomainTopologyManagerId, Member, ParticipantId}
import com.digitalasset.canton.tracing.TraceContext.fromGrpcContext
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.{EitherTUtil, FutureUtil}
import com.digitalasset.canton.version.ProtocolVersion
import io.functionmeta.functionFullName

import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import scala.collection.concurrent
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.jdk.CollectionConverters._

/** Handle used in order to request approval of participant's topology transactions by the IDM and wait for the
  * responses by sending RegisterTopologyTransactionRequest's via the sequencer.
  * This gets passed to [[com.digitalasset.canton.participant.topology.ParticipantTopologyDispatcher.domainConnected]]
  */
class SequencerBasedRegisterTopologyTransactionHandle(
    send: (
        TraceContext,
        OpenEnvelope[RegisterTopologyTransactionRequest],
    ) => EitherT[Future, SendAsyncClientError, Unit],
    domainId: DomainId,
    participantId: ParticipantId,
    requestedBy: Member,
    protocolVersion: ProtocolVersion,
    protected val timeouts: ProcessingTimeout,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends RegisterTopologyTransactionHandle
    with NamedLogging {
  private val service =
    new ParticipantDomainTopologyService(domainId, send, protocolVersion, timeouts, loggerFactory)

  // must be used by the event handler of a sequencer subscription in order to complete the promises of requests sent with the given sequencer client
  val processor: Traced[Seq[DefaultOpenEnvelope]] => HandlerResult = service.processor

  override def submit(
      transactions: Seq[SignedTopologyTransaction[TopologyChangeOp]]
  ): FutureUnlessShutdown[Seq[RegisterTopologyTransactionResponseResult]] =
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
      .traverse(service.registerTopologyTransaction)
      .map(_.map(_.results))
      .map(_.flatten)

  override def onClosed(): Unit = service.close()
}

private[domain] class ParticipantDomainTopologyService(
    domainId: DomainId,
    send: (
        TraceContext,
        OpenEnvelope[RegisterTopologyTransactionRequest],
    ) => EitherT[Future, SendAsyncClientError, Unit],
    protocolVersion: ProtocolVersion,
    protected val timeouts: ProcessingTimeout,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit val ec: ExecutionContext)
    extends NamedLogging
    with FlagCloseable {

  private val responsePromiseMap: concurrent.Map[(ParticipantId, TopologyRequestId), Promise[
    UnlessShutdown[RegisterTopologyTransactionResponse.Result]
  ]] =
    new ConcurrentHashMap[(ParticipantId, TopologyRequestId), Promise[
      UnlessShutdown[RegisterTopologyTransactionResponse.Result]
    ]]().asScala

  def registerTopologyTransaction(
      request: RegisterTopologyTransactionRequest
  ): FutureUnlessShutdown[
    RegisterTopologyTransactionResponse.Result
  ] =
    fromGrpcContext { implicit traceContext =>
      val responseF = getResponse(request)
      for {
        _ <- performUnlessClosingF(functionFullName)(
          EitherTUtil.toFuture(mapErr(sendRequest(request)))
        )
        response <- responseF
      } yield response
    }

  private def sendRequest(
      request: RegisterTopologyTransactionRequest
  )(implicit traceContext: TraceContext): EitherT[Future, SendAsyncClientError, Unit] = {
    logger.debug(s"Sending register topology transaction request ${requestDescription(request)}")
    EitherTUtil.logOnError(
      send(
        traceContext,
        OpenEnvelope(request, Recipients.cc(DomainTopologyManagerId(domainId)), protocolVersion),
      ),
      s"Failed sending register topology transaction request ${requestDescription(request)}",
    )
  }

  private def getResponse(request: RegisterTopologyTransactionRequest)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[
    RegisterTopologyTransactionResponse.Result
  ] =
    FutureUnlessShutdown {
      val promise = Promise[UnlessShutdown[
        RegisterTopologyTransactionResponse.Result
      ]]()
      responsePromiseMap.put((request.participant, request.requestId), promise)
      FutureUtil.logOnFailure(
        promise.future.map { result =>
          result match {
            case _ @UnlessShutdown.Outcome(_) =>
              logger.debug(
                s"Received register topology transaction response ${requestDescription(request)}"
              )
            case _: UnlessShutdown.AbortedDueToShutdown =>
              logger.info(
                s"Shutdown before receiving register topology transaction response ${requestDescription(request)}"
              )
          }
          result
        },
        s"Failed to receive register topology transaction response ${requestDescription(request)}",
      )
    }

  private def requestDescription(request: RegisterTopologyTransactionRequest): String =
    s"on behalf of participant ${request.participant}${if (request.requestedBy != request.participant)
      s" requested by ${request.requestedBy} "
    else " "}with requestId = ${request.requestId}"

  private[domain] val processor: Traced[Seq[DefaultOpenEnvelope]] => HandlerResult = envs =>
    envs.withTraceContext { implicit traceContext => envs =>
      HandlerResult.asynchronous(performUnlessClosingF(s"${getClass.getSimpleName}-processor") {
        Future {
          envs.foreach { env =>
            env.protocolMessage match {
              case response: RegisterTopologyTransactionResponse[_] =>
                responsePromiseMap
                  .get((response.participant, response.requestId))
                  .foreach(_.trySuccess(UnlessShutdown.Outcome(response)))
              case _ =>
            }
          }
        }
      })
    }

  override def onClosed(): Unit = {
    responsePromiseMap.values.foreach(_.trySuccess(UnlessShutdown.AbortedDueToShutdown))
  }
}
