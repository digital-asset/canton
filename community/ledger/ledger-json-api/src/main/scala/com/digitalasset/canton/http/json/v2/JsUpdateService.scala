// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http.json.v2

import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.ledger.api.v2 as lapi
import com.daml.ledger.api.v2.transaction_filter.ParticipantAuthorizationTopologyFormat
import com.daml.ledger.api.v2.{offset_checkpoint, transaction_filter, update_service}
import com.digitalasset.canton.auth.AuthInterceptor
import com.digitalasset.canton.http.WebsocketConfig
import com.digitalasset.canton.http.json.v2.CirceRelaxedCodec.deriveRelaxedCodec
import com.digitalasset.canton.http.json.v2.Endpoints.{CallerContext, TracedInput}
import com.digitalasset.canton.http.json.v2.JsSchema.DirectScalaPbRwImplicits.*
import com.digitalasset.canton.http.json.v2.JsSchema.{
  JsCantonError,
  JsReassignment,
  JsTransaction,
  OneOfSchemaExtension,
}
import com.digitalasset.canton.http.json.v2.damldefinitionsservice.Schema.Codecs.*
import com.digitalasset.canton.ledger.client.LedgerClient
import com.digitalasset.canton.logging.audit.ApiRequestLogger
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.TraceContext
import com.google.protobuf.ByteString
import io.circe.Codec
import io.circe.generic.extras.semiauto.deriveConfiguredCodec
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.Flow
import sttp.capabilities.pekko.PekkoStreams
import sttp.tapir.Schema.SName
import sttp.tapir.generic.auto.*
import sttp.tapir.json.circe.*
import sttp.tapir.{AnyEndpoint, CodecFormat, Schema, webSocketBody}

import scala.concurrent.{ExecutionContext, Future}

@SuppressWarnings(Array("com.digitalasset.canton.DirectGrpcServiceInvocation"))
class JsUpdateService(
    ledgerClient: LedgerClient,
    protocolConverters: ProtocolConverters,
    override protected val requestLogger: ApiRequestLogger,
    val loggerFactory: NamedLoggerFactory,
)(implicit
    val executionContext: ExecutionContext,
    esf: ExecutionSequencerFactory,
    wsConfig: WebsocketConfig,
    materializer: Materializer,
    val authInterceptor: AuthInterceptor,
) extends Endpoints
    with NamedLogging {

  private def updateServiceClient(token: Option[String])(implicit
      traceContext: TraceContext
  ): update_service.UpdateServiceGrpc.UpdateServiceStub =
    ledgerClient.serviceClient(update_service.UpdateServiceGrpc.stub, token)

  def endpoints() = List(
    websocket(
      JsUpdateService.getUpdatesEndpoint,
      getUpdates,
    ),
    asList(
      JsUpdateService.getUpdatesListEndpoint,
      getUpdates,
      timeoutOpenEndedStream = (r: update_service.GetUpdatesRequest) => r.endInclusive.isEmpty,
    ),
    withServerLogic(
      JsUpdateService.getUpdateByOffsetEndpoint,
      getUpdateByOffset,
    ),
    withServerLogic(
      JsUpdateService.getUpdateByIdEndpoint,
      getUpdateById,
    ),
    withServerLogic(
      JsUpdateService.getUpdateByHashEndpoint,
      getUpdateByHash,
    ),
    withServerLogic(
      JsUpdateService.getUpdatesPageEndpoint,
      getUpdatesPage,
    ),
  )

  private def getUpdateByOffset(
      caller: CallerContext
  ): TracedInput[update_service.GetUpdateByOffsetRequest] => Future[
    Either[JsCantonError, JsGetUpdateResponse]
  ] =
    req => {
      implicit val tc = caller.traceContext()
      updateServiceClient(caller.token())
        .getUpdateByOffset(req.in)
        .flatMap(protocolConverters.GetUpdateResponse.toJson(_))
        .resultToRight
    }

  private def getUpdateById(
      caller: CallerContext
  ): TracedInput[update_service.GetUpdateByIdRequest] => Future[
    Either[JsCantonError, JsGetUpdateResponse]
  ] =
    req => {
      implicit val tc = caller.traceContext()
      updateServiceClient(caller.token())
        .getUpdateById(req.in)
        .flatMap(protocolConverters.GetUpdateResponse.toJson(_))
        .resultToRight
    }

  private def getUpdateByHash(
      caller: CallerContext
  ): TracedInput[update_service.GetUpdateByHashRequest] => Future[
    Either[JsCantonError, JsGetUpdateResponse]
  ] =
    req => {
      implicit val tc = caller.traceContext()
      updateServiceClient(caller.token())
        .getUpdateByHash(req.in)
        .flatMap(protocolConverters.GetUpdateResponse.toJson(_))
        .resultToRight
    }

  private def getUpdates(
      caller: CallerContext
  ): TracedInput[Unit] => Flow[update_service.GetUpdatesRequest, JsGetUpdatesResponse, NotUsed] =
    _ => {
      implicit val tc = caller.traceContext()
      prepareSingleWsStream(
        updateServiceClient(caller.token()).getUpdates,
        (r: update_service.GetUpdatesResponse) => protocolConverters.GetUpdatesResponse.toJson(r),
      )
    }

  private def getUpdatesPage(
      caller: CallerContext
  ): TracedInput[update_service.GetUpdatesPageRequest] => Future[
    Either[JsCantonError, JsGetUpdatesPageResponse]
  ] =
    req => {
      implicit val tc = caller.traceContext()
      updateServiceClient(caller.token())
        .getUpdatesPage(req.in)
        .flatMap(protocolConverters.GetUpdatesPageResponse.toJson(_))
        .resultToRight
    }

}

object JsUpdateService extends DocumentationEndpoints {
  import Endpoints.*
  import JsUpdateServiceCodecs.*
  import JsSchema.JsServicesCommonCodecs.*

  private lazy val updates = v2Endpoint.in(sttp.tapir.stringToPath("updates"))

  val getUpdatesEndpoint = updates.get
    .out(
      webSocketBody[
        update_service.GetUpdatesRequest,
        CodecFormat.Json,
        Either[JsCantonError, JsGetUpdatesResponse],
        CodecFormat.Json,
      ](PekkoStreams)
    )
    .protoRef(update_service.UpdateServiceGrpc.METHOD_GET_UPDATES)

  val getUpdatesListEndpoint =
    updates.post
      .in(jsonBody[update_service.GetUpdatesRequest])
      .out(jsonBody[Seq[JsGetUpdatesResponse]])
      .protoRef(update_service.UpdateServiceGrpc.METHOD_GET_UPDATES)
      .inStreamListParamsAndDescription()

  val getUpdateByOffsetEndpoint =
    updates.post
      .in(sttp.tapir.stringToPath("update-by-offset"))
      .in(jsonBody[update_service.GetUpdateByOffsetRequest])
      .out(jsonBody[JsGetUpdateResponse])
      .protoRef(update_service.UpdateServiceGrpc.METHOD_GET_UPDATE_BY_OFFSET)

  val getUpdateByIdEndpoint =
    updates.post
      .in(sttp.tapir.stringToPath("update-by-id"))
      .in(jsonBody[update_service.GetUpdateByIdRequest])
      .out(jsonBody[JsGetUpdateResponse])
      .protoRef(update_service.UpdateServiceGrpc.METHOD_GET_UPDATE_BY_ID)

  val getUpdateByHashEndpoint =
    updates.post
      .in(sttp.tapir.stringToPath("update-by-hash"))
      .in(jsonBody[update_service.GetUpdateByHashRequest])
      .out(jsonBody[JsGetUpdateResponse])
      .protoRef(update_service.UpdateServiceGrpc.METHOD_GET_UPDATE_BY_HASH)

  val getUpdatesPageEndpoint =
    updates.post
      .in(sttp.tapir.stringToPath("get-updates-page"))
      .in(jsonBody[update_service.GetUpdatesPageRequest])
      .out(jsonBody[JsGetUpdatesPageResponse])
      .protoRef(update_service.UpdateServiceGrpc.METHOD_GET_UPDATES_PAGE)

  override def documentation: Seq[AnyEndpoint] = List(
    getUpdatesEndpoint,
    getUpdatesListEndpoint,
    getUpdateByOffsetEndpoint,
    getUpdateByIdEndpoint,
    getUpdateByHashEndpoint,
    getUpdatesPageEndpoint,
  )
}

object JsUpdate {
  sealed trait Update
  final case class OffsetCheckpoint(value: offset_checkpoint.OffsetCheckpoint) extends Update
  final case class Reassignment(value: JsReassignment) extends Update
  final case class Transaction(value: JsTransaction) extends Update
  final case class TopologyTransaction(value: lapi.topology_transaction.TopologyTransaction)
      extends Update
}

final case class JsGetUpdateResponse(update: JsUpdate.Update)

final case class JsGetUpdatesResponse(
    update: JsUpdate.Update
)

final case class JsGetUpdatesPageResponse(
    updates: Seq[
      JsGetUpdateResponse
    ],
    lowestPageOffsetExclusive: Long,
    highestPageOffsetInclusive: Long,
    nextPageToken: Option[ByteString],
)

object JsUpdateServiceCodecs {
  import JsSchema.config
  import JsSchema.JsServicesCommonCodecs.*

  implicit val participantAuthorizationTopologyFormatRW
      : Codec[ParticipantAuthorizationTopologyFormat] = deriveRelaxedCodec
  implicit val topologyFormatRW: Codec[transaction_filter.TopologyFormat] = deriveRelaxedCodec
  implicit val updateFormatRW: Codec[transaction_filter.UpdateFormat] = deriveRelaxedCodec
  implicit val getUpdatesRequestRW: Codec[update_service.GetUpdatesRequest] = deriveRelaxedCodec
  implicit val getUpdateByIdRequestRW: Codec[update_service.GetUpdateByIdRequest] =
    deriveRelaxedCodec
  implicit val getUpdateByOffsetRequestRW: Codec[update_service.GetUpdateByOffsetRequest] =
    deriveRelaxedCodec
  implicit val getUpdateByHashRequestRW: Codec[update_service.GetUpdateByHashRequest] =
    deriveRelaxedCodec

  implicit val jsGetUpdatesResponseRW: Codec[JsGetUpdatesResponse] = deriveConfiguredCodec

  implicit val jsUpdateRW: Codec[JsUpdate.Update] = deriveConfiguredCodec

  implicit val jsUpdateOffsetCheckpointRW: Codec[JsUpdate.OffsetCheckpoint] = deriveConfiguredCodec

  implicit val jsUpdateReassignmentRW: Codec[JsUpdate.Reassignment] = deriveConfiguredCodec
  implicit val jsUpdateTransactionRW: Codec[JsUpdate.Transaction] = deriveConfiguredCodec
  implicit val jsUpdateTopologyTransactionRW: Codec[JsUpdate.TopologyTransaction] =
    deriveConfiguredCodec

  implicit val jsGetUpdateResponseRW: Codec[JsGetUpdateResponse] = deriveConfiguredCodec

  implicit val jsGetUpdatesPageResponseRW: Codec[JsGetUpdatesPageResponse] = deriveConfiguredCodec
  implicit val getUpdatesPageRequest: Codec[update_service.GetUpdatesPageRequest] =
    deriveRelaxedCodec

  implicit val jsTopologyParticipantAuthorizationAddedRW
      : Codec[lapi.topology_transaction.ParticipantAuthorizationAdded] =
    deriveRelaxedCodec

  implicit val jsTopologyParticipantAuthorizationChangedRW
      : Codec[lapi.topology_transaction.ParticipantAuthorizationChanged] =
    deriveRelaxedCodec

  implicit val jsTopologyParticipantAuthorizationRevokedRW
      : Codec[lapi.topology_transaction.ParticipantAuthorizationRevoked] =
    deriveRelaxedCodec
  implicit val jsTopologyParticipantAuthorizationOnboardingRW
      : Codec[lapi.topology_transaction.ParticipantAuthorizationOnboarding] =
    deriveRelaxedCodec
  implicit val jsTopologyEventEventRW: Codec[lapi.topology_transaction.TopologyEvent.Event] =
    deriveConfiguredCodec
  implicit val jsTopologyEventParticipantAuthorizationAddedRW
      : Codec[lapi.topology_transaction.TopologyEvent.Event.ParticipantAuthorizationAdded] =
    deriveRelaxedCodec
  implicit val jsParticipantAuthorizationChangedRW
      : Codec[lapi.topology_transaction.TopologyEvent.Event.ParticipantAuthorizationChanged] =
    deriveRelaxedCodec
  implicit val jsParticipantAuthorizationRevokedRW
      : Codec[lapi.topology_transaction.TopologyEvent.Event.ParticipantAuthorizationRevoked] =
    deriveRelaxedCodec
  implicit val jsParticipantAuthorizationOnboardingRW
      : Codec[lapi.topology_transaction.TopologyEvent.Event.ParticipantAuthorizationOnboarding] =
    deriveRelaxedCodec

  implicit val jsTopologyEventRW: Codec[lapi.topology_transaction.TopologyEvent] =
    deriveRelaxedCodec
  implicit val jsTopologyTransactionRW: Codec[lapi.topology_transaction.TopologyTransaction] =
    deriveRelaxedCodec
  // Schema mappings are added to align generated tapir docs with a circe mapping of ADTs

  implicit val jsTopologyTransactionSchema: Schema[lapi.topology_transaction.TopologyTransaction] =
    Schema.derived.name(Some(SName("JsTopologyTransaction")))

  @SuppressWarnings(Array("org.wartremover.warts.Product", "org.wartremover.warts.Serializable"))
  implicit val jsUpdateSchema: Schema[JsUpdate.Update] =
    Schema.oneOfWrapped[JsUpdate.Update].oneOfExtension()

}
