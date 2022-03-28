// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.topology.store

import cats.data.OptionT
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.LengthLimitedString.TopologyRequestId
import com.digitalasset.canton.crypto.CryptoPureApi
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.protocol.messages.{
  ProtocolMessage,
  RegisterTopologyTransactionRequest,
  RegisterTopologyTransactionResponse,
}
import com.digitalasset.canton.resource.IdempotentInsert.insertIgnoringConflicts
import com.digitalasset.canton.resource.{DbStorage, DbStore, MemoryStorage, Storage}
import com.digitalasset.canton.store.db.DbDeserializationException
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion
import com.google.common.annotations.VisibleForTesting
import com.google.protobuf.ByteString
import io.functionmeta.functionFullName
import slick.jdbc.{GetResult, PositionedParameters, SetParameter}

import scala.concurrent.{ExecutionContext, Future, blocking}

trait RegisterTopologyTransactionResponseStore extends AutoCloseable {
  def savePendingResponse(response: RegisterTopologyTransactionResponse)(implicit
      traceContext: TraceContext
  ): Future[Unit]

  @VisibleForTesting
  def pendingResponses()(implicit
      traceContext: TraceContext
  ): Future[Seq[RegisterTopologyTransactionResponse]]

  def completeResponse(requestId: TopologyRequestId)(implicit
      traceContext: TraceContext
  ): Future[Unit]

  def exists(requestId: TopologyRequestId)(implicit traceContext: TraceContext): Future[Boolean]

  def getResponse(requestId: TopologyRequestId)(implicit
      traceContext: TraceContext
  ): OptionT[Future, RegisterTopologyTransactionResponseStore.Response]
}

object RegisterTopologyTransactionResponseStore {
  case class Response(response: RegisterTopologyTransactionResponse, isCompleted: Boolean)

  def apply(
      storage: Storage,
      cryptoApi: CryptoPureApi,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      ec: ExecutionContext
  ): RegisterTopologyTransactionResponseStore = storage match {
    case _: MemoryStorage => new InMemoryRegisterTopologyTransactionResponseStore()
    case jdbc: DbStorage =>
      new DbRegisterTopologyTransactionResponseStore(jdbc, cryptoApi, timeouts, loggerFactory)
  }
}

class InMemoryRegisterTopologyTransactionResponseStore(implicit ec: ExecutionContext)
    extends RegisterTopologyTransactionResponseStore {
  import java.util.concurrent.ConcurrentHashMap
  import scala.jdk.CollectionConverters._

  private val responseMap =
    new ConcurrentHashMap[
      TopologyRequestId,
      RegisterTopologyTransactionResponseStore.Response,
    ].asScala

  override def savePendingResponse(
      response: RegisterTopologyTransactionResponse
  )(implicit traceContext: TraceContext): Future[Unit] = {
    responseMap.put(
      response.requestId,
      RegisterTopologyTransactionResponseStore.Response(response, isCompleted = false),
    )
    Future.unit
  }

  override def pendingResponses()(implicit
      traceContext: TraceContext
  ): Future[Seq[RegisterTopologyTransactionResponse]] =
    Future.successful(responseMap.values.filterNot(_.isCompleted).map(_.response).toSeq)

  override def completeResponse(
      requestId: TopologyRequestId
  )(implicit traceContext: TraceContext): Future[Unit] = blocking {
    synchronized {
      responseMap
        .get(requestId)
        .foreach(response => responseMap.put(requestId, response.copy(isCompleted = true)))
      Future.unit
    }
  }

  override def getResponse(requestId: TopologyRequestId)(implicit
      traceContext: TraceContext
  ): OptionT[Future, RegisterTopologyTransactionResponseStore.Response] =
    OptionT.fromOption(responseMap.get(requestId))

  override def exists(requestId: TopologyRequestId)(implicit
      traceContext: TraceContext
  ): Future[Boolean] =
    Future.successful(responseMap.contains(requestId))

  override def close(): Unit = ()
}

class DbRegisterTopologyTransactionResponseStore(
    override protected val storage: DbStorage,
    cryptoApi: CryptoPureApi,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit val ec: ExecutionContext)
    extends RegisterTopologyTransactionResponseStore
    with DbStore {

  import storage.api._
  import storage.converters._

  implicit val getRegisterTopologyTransactionRequest
      : GetResult[RegisterTopologyTransactionRequest] = GetResult(r =>
    ProtocolMessage
      .fromEnvelopeContentByteStringV0(cryptoApi)(ByteString.copyFrom(r.<<[Array[Byte]]))
      .fold[RegisterTopologyTransactionRequest](
        error =>
          throw new DbDeserializationException(
            s"Error deserializing register topology transaction request $error"
          ),
        {
          case request: RegisterTopologyTransactionRequest => request
          case _ =>
            sys.error(
              "Deserialized request was not a RegisterTopologyTransactionRequest!"
            ) // should never happen
        },
      )
  )
  implicit val setParameterRegisterTopologyTransactionRequest
      : SetParameter[RegisterTopologyTransactionRequest] =
    (r: RegisterTopologyTransactionRequest, pp: PositionedParameters) =>
      pp >> r.toEnvelopeContentByteString(ProtocolVersion.v2_0_0_Todo_i8793).toByteArray

  implicit val getRegisterTopologyTransactionResponse
      : GetResult[RegisterTopologyTransactionResponse] = GetResult(r =>
    ProtocolMessage
      .fromEnvelopeContentByteStringV0(cryptoApi)(ByteString.copyFrom(r.<<[Array[Byte]]))
      .fold[RegisterTopologyTransactionResponse](
        error =>
          throw new DbDeserializationException(
            s"Error deserializing register topology transaction response $error"
          ),
        {
          case request: RegisterTopologyTransactionResponse => request
          case _ =>
            sys.error(
              "Deserialized response was not a RegisterTopologyTransactionResponse!"
            ) // should never happen
        },
      )
  )
  implicit val getPendingRegisterTopologyTransactionRequestStoreResponse
      : GetResult[RegisterTopologyTransactionResponseStore.Response] = GetResult(r =>
    RegisterTopologyTransactionResponseStore.Response(
      getRegisterTopologyTransactionResponse(r),
      r.nextBoolean(),
    )
  )
  implicit val setRegisterTopologyTransactionResponse
      : SetParameter[RegisterTopologyTransactionResponse] =
    (r: RegisterTopologyTransactionResponse, pp: PositionedParameters) =>
      pp >> r.toEnvelopeContentByteString(ProtocolVersion.v2_0_0_Todo_i8793).toByteArray

  override def savePendingResponse(response: RegisterTopologyTransactionResponse)(implicit
      traceContext: TraceContext
  ): Future[Unit] =
    storage.update_(
      // request_id is a uuid so can safely assume conflicts will only be caused by retries of this request
      insertIgnoringConflicts(
        storage,
        "register_topology_transaction_responses(request_id)",
        sql"""register_topology_transaction_responses(request_id, response, completed)
                   values(${response.requestId},${response},${false})""",
      ),
      functionFullName,
    )

  override def pendingResponses()(implicit
      traceContext: TraceContext
  ): Future[Seq[RegisterTopologyTransactionResponse]] =
    storage.query(
      sql""" select response from register_topology_transaction_responses where completed = ${false}"""
        .as[RegisterTopologyTransactionResponse],
      functionFullName,
    )

  override def completeResponse(
      requestId: TopologyRequestId
  )(implicit traceContext: TraceContext): Future[Unit] =
    storage
      .update_(
        sqlu"""update register_topology_transaction_responses set completed = ${true} where request_id=$requestId""",
        functionFullName,
      )

  override def exists(
      requestId: TopologyRequestId
  )(implicit traceContext: TraceContext): Future[Boolean] =
    storage.query(
      sql"""select 1 from register_topology_transaction_responses where request_id=$requestId"""
        .as[Int]
        .map(_.nonEmpty),
      functionFullName,
    )

  override def getResponse(requestId: TopologyRequestId)(implicit
      traceContext: TraceContext
  ): OptionT[Future, RegisterTopologyTransactionResponseStore.Response] =
    OptionT(
      storage.query(
        sql""" select response, completed from register_topology_transaction_responses where request_id=$requestId"""
          .as[RegisterTopologyTransactionResponseStore.Response]
          .headOption,
        functionFullName,
      )
    )
}
