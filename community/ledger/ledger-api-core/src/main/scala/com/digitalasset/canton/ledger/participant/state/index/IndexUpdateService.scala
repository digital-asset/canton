// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.participant.state.index

import com.daml.ledger.api.v2.update_service.{
  GetUpdateResponse,
  GetUpdatesPageResponse,
  GetUpdatesResponse,
}
import com.digitalasset.canton.data.{CantonTimestamp, Offset}
import com.digitalasset.canton.ledger.api.UpdateFormat
import com.digitalasset.canton.ledger.api.messages.update.GetUpdatesPageRequest
import com.digitalasset.canton.ledger.participant.state.AcsChange
import com.digitalasset.canton.ledger.participant.state.index.IndexUpdateService.UpdatesResponse
import com.digitalasset.canton.logging.LoggingContextWithTrace
import com.digitalasset.canton.platform.store.backend.common.UpdatePointwiseQueries.LookupKey
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.daml.lf.data.Time.Timestamp
import com.google.protobuf.ByteString
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Source

import scala.concurrent.Future

/** Serves as a backend to implement
  * [[com.daml.ledger.api.v2.update_service.UpdateServiceGrpc.UpdateService]]
  */
trait IndexUpdateService extends LedgerEndService {
  def updates(
      begin: Option[Offset],
      endAt: Option[Offset],
      updateFormat: UpdateFormat,
      descendingOrder: Boolean,
      skipPruningChecks: Boolean,
  )(implicit loggingContext: LoggingContextWithTrace): Source[UpdatesResponse, NotUsed]

  def getUpdateBy(
      lookupKey: LookupKey,
      updateFormat: UpdateFormat,
  )(implicit loggingContext: LoggingContextWithTrace): Future[Option[GetUpdateResponse]]

  def latestPrunedOffset()(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[Option[Offset]]

  def updatesPage(
      getUpdatesPageRequest: GetUpdatesPageRequest
  )(implicit
      loggingContext: LoggingContextWithTrace
  ): Future[GetUpdatesPageResponse]
}

object IndexUpdateService {
  sealed trait UpdateResponse extends Product with Serializable
  sealed trait UpdatesResponse extends Product with Serializable

  object UpdateResponse {
    final case class ProtoUpdate(response: GetUpdateResponse) extends UpdateResponse
    final case class AcsCommitment(commitment: ReceivedAcsCommitment)
        extends UpdateResponse
        with UpdatesResponse
    final case class AcsChange(change: AcsChangeUpdate) extends UpdateResponse with UpdatesResponse

    def expandToGetUpdatesResponse(response: UpdateResponse): UpdatesResponse =
      response match {
        case ProtoUpdate(response) =>
          response.update match {
            case GetUpdateResponse.Update.Empty =>
              UpdatesResponse.ProtoUpdates(GetUpdatesResponse(GetUpdatesResponse.Update.Empty))
            case GetUpdateResponse.Update.Transaction(value) =>
              UpdatesResponse.ProtoUpdates(
                GetUpdatesResponse(GetUpdatesResponse.Update.Transaction(value))
              )
            case GetUpdateResponse.Update.Reassignment(value) =>
              UpdatesResponse.ProtoUpdates(
                GetUpdatesResponse(GetUpdatesResponse.Update.Reassignment(value))
              )
            case GetUpdateResponse.Update.TopologyTransaction(value) =>
              UpdatesResponse.ProtoUpdates(
                GetUpdatesResponse(GetUpdatesResponse.Update.TopologyTransaction(value))
              )
          }
        case v: AcsCommitment => v
        case v: AcsChange => v
      }
  }

  object UpdatesResponse {
    final case class ProtoUpdates(response: GetUpdatesResponse) extends UpdatesResponse
    type AcsCommitment = UpdateResponse.AcsCommitment
    val AcsCommitment = UpdateResponse.AcsCommitment
    type AcsChange = UpdateResponse.AcsChange
    val AcsChange = UpdateResponse.AcsChange
  }

  final case class ReceivedAcsCommitment(
      offset: Offset,
      updateId: String,
      synchronizerId: String,
      recordTime: Timestamp,
      payload: ByteString,
      traceContext: TraceContext,
  )

  final case class AcsChangeUpdate(
      acsChange: AcsChange,
      offset: Offset,
      recordTime: CantonTimestamp,
      traceContext: TraceContext,
  )
}
