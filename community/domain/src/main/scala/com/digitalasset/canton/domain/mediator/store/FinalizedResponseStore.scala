// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.mediator.store

import cats.data.EitherT
import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.CryptoPureApi
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.mediator.{MediatorRequestNotFound, ResponseAggregation}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.MetricHandle.GaugeM
import com.digitalasset.canton.metrics.TimedLoadGauge
import com.digitalasset.canton.protocol.RequestId
import com.digitalasset.canton.protocol.messages.{MediatorRequest, ProtocolMessage, Verdict}
import com.digitalasset.canton.resource.{DbStorage, DbStore, MemoryStorage, Storage}
import com.digitalasset.canton.store.db.DbDeserializationException
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion
import com.google.protobuf.ByteString
import io.functionmeta.functionFullName
import slick.jdbc.{GetResult, PositionedParameters, SetParameter}

import java.util.concurrent.ConcurrentHashMap
import scala.concurrent.{ExecutionContext, Future}

/** Stores and retrieves finalized mediator response aggregations
  */
trait FinalizedResponseStore extends AutoCloseable {

  /** Stores finalized mediator response aggregations (whose state is a Left(verdict)).
    * In the event of a crash we may attempt to store an existing finalized request so the store
    * should behave in an idempotent manner.
    * TODO(#4335): If there is an existing value ensure that it matches the value we want to insert
    */
  def store(request: ResponseAggregation)(implicit traceContext: TraceContext): Future[Unit]

  /** Fetch previously stored finalized mediator response aggregation by requestId.
    */
  def fetch(requestId: RequestId)(implicit
      traceContext: TraceContext
  ): EitherT[Future, MediatorRequestNotFound, ResponseAggregation]

  /** Remove all responses up to and including the provided timestamp. */
  def prune(timestamp: CantonTimestamp)(implicit traceContext: TraceContext): Future[Unit]

  /** Count how many finalized responses we have stored.
    * Primarily used for testing mediator pruning.
    */
  def count()(implicit traceContext: TraceContext): Future[Long]
}

object FinalizedResponseStore {
  def apply(
      storage: Storage,
      cryptoApi: CryptoPureApi,
      protocolVersion: ProtocolVersion,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      ec: ExecutionContext
  ): FinalizedResponseStore = storage match {
    case _: MemoryStorage => new InMemoryFinalizedResponseStore(loggerFactory)
    case jdbc: DbStorage =>
      new DbFinalizedResponseStore(jdbc, cryptoApi, protocolVersion, timeouts, loggerFactory)
  }
}

class InMemoryFinalizedResponseStore(override protected val loggerFactory: NamedLoggerFactory)
    extends FinalizedResponseStore
    with NamedLogging {
  private implicit val ec: ExecutionContext = DirectExecutionContext(logger)

  import scala.jdk.CollectionConverters._
  private val finalizedRequests =
    new ConcurrentHashMap[CantonTimestamp, ResponseAggregation].asScala

  override def store(
      request: ResponseAggregation
  )(implicit traceContext: TraceContext): Future[Unit] = {
    finalizedRequests.putIfAbsent(request.requestId.unwrap, request)
    Future.unit
  }

  override def fetch(requestId: RequestId)(implicit
      traceContext: TraceContext
  ): EitherT[Future, MediatorRequestNotFound, ResponseAggregation] =
    EitherT.fromEither[Future](
      finalizedRequests.get(requestId.unwrap).toRight(MediatorRequestNotFound(requestId))
    )

  override def prune(
      timestamp: CantonTimestamp
  )(implicit traceContext: TraceContext): Future[Unit] =
    Future.successful {
      finalizedRequests.keys.filterNot(_.isAfter(timestamp)).foreach(finalizedRequests.remove)
    }

  override def count()(implicit traceContext: TraceContext): Future[Long] =
    Future.successful(finalizedRequests.size.toLong)

  override def close(): Unit = ()
}

class DbFinalizedResponseStore(
    override protected val storage: DbStorage,
    cryptoApi: CryptoPureApi,
    protocolVersion: ProtocolVersion,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit val ec: ExecutionContext)
    extends FinalizedResponseStore
    with DbStore {
  import storage.api._
  import storage.converters._

  implicit val getResultRequestId: GetResult[RequestId] =
    GetResult[CantonTimestamp].andThen(ts => RequestId(ts))
  implicit val setParameterRequestId: SetParameter[RequestId] =
    (r: RequestId, pp: PositionedParameters) => SetParameter[CantonTimestamp].apply(r.unwrap, pp)

  implicit val getResultMediatorRequest: GetResult[MediatorRequest] = GetResult(r =>
    ProtocolMessage
      .fromEnvelopeContentByteString(protocolVersion, cryptoApi)(
        ByteString.copyFrom(r.<<[Array[Byte]])
      )
      .fold[MediatorRequest](
        error =>
          throw new DbDeserializationException(s"Error deserializing mediator request $error"),
        {
          case mediatorRequest: MediatorRequest => mediatorRequest
          case _ =>
            sys.error("Deserialized request was not a MediatorRequest!") // should never happen
        },
      )
  )
  implicit val setParameterMediatorRequest: SetParameter[MediatorRequest] =
    (r: MediatorRequest, pp: PositionedParameters) =>
      pp >> ProtocolMessage
        .toEnvelopeContentByteString(r, protocolVersion)
        .toByteArray

  private val processingTime: GaugeM[TimedLoadGauge, Double] =
    storage.metrics.loadGaugeM("finalized-response-store")

  override def store(
      request: ResponseAggregation
  )(implicit traceContext: TraceContext): Future[Unit] =
    processingTime.metric.event {
      request.state match {
        case Left(verdict) =>
          val insert = storage.profile match {
            case _: DbStorage.Profile.Oracle =>
              sqlu"""insert 
                     /*+  IGNORE_ROW_ON_DUPKEY_INDEX ( response_aggregations ( request_id ) ) */
                     into response_aggregations(request_id, mediator_request, version, verdict, request_trace_context)
                     values(${request.requestId},${request.request},${request.version},${verdict}, ${request.requestTraceContext})"""
            case _ =>
              sqlu"""insert into response_aggregations(request_id, mediator_request, version, verdict, request_trace_context)
                          values(${request.requestId},${request.request},${request.version},${verdict}, ${request.requestTraceContext})
                          on conflict do nothing"""
          }

          storage.update_(
            insert,
            operationName = s"${this.getClass}: store request ${request.requestId}",
          )
        case Right(_) =>
          throw new IllegalArgumentException(s"Given request has not been finalized")
      }
    }

  override def fetch(requestId: RequestId)(implicit
      traceContext: TraceContext
  ): EitherT[Future, MediatorRequestNotFound, ResponseAggregation] =
    processingTime.metric.eitherTEvent {
      EitherT(
        storage.query(
          {
            sql"""
      select request_id, mediator_request, version, verdict, request_trace_context from response_aggregations where request_id=${requestId.unwrap}
    """.as[(RequestId, MediatorRequest, CantonTimestamp, Verdict, TraceContext)].map { result =>
              result.headOption.toRight(MediatorRequestNotFound(requestId)).map {
                case (reqId, mediatorRequest, version, verdict, requestTraceContext) =>
                  ResponseAggregation(
                    reqId,
                    mediatorRequest,
                    version,
                    verdict,
                    requestTraceContext,
                  )(loggerFactory)
              }
            }
          },
          operationName = s"${this.getClass}: fetch request $requestId",
        )
      )
    }

  override def prune(
      timestamp: CantonTimestamp
  )(implicit traceContext: TraceContext): Future[Unit] = {
    for {
      removedCount <- storage.update(
        sqlu"delete from response_aggregations where request_id <= ${timestamp}",
        functionFullName,
      )
    } yield logger.debug(s"Removed $removedCount finalized responses")
  }

  override def count()(implicit traceContext: TraceContext): Future[Long] =
    storage.query(
      sql"select count(request_id) from response_aggregations".as[Long].head,
      functionFullName,
    )
}
