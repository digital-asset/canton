// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.validation

import cats.instances.either.*
import cats.instances.seq.*
import cats.syntax.traverse.*
import com.daml.ledger.api.v2.state_service
import com.daml.ledger.api.v2.state_service.HostedPartiesFilter
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.ledger.api.messages.state
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors
import com.digitalasset.canton.ledger.participant.state.SynchronizerIndex
import com.digitalasset.canton.ledger.participant.state.index.IndexerHostedPartyFilter
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.nonempty.NonEmpty
import io.grpc.StatusRuntimeException

object StateServiceRequestValidator {
  type Result[X] = Either[StatusRuntimeException, X]
  import ValueValidator.*
  import FieldValidator.*

  def validateConvertRecordTimeToOffsetRequest(
      request: state_service.ConvertRecordTimeToOffsetRequest,
      knownSynchronizers: Map[SynchronizerId, SynchronizerIndex],
  )(implicit
      errorLoggingContext: ErrorLoggingContext
  ): Result[state.ConvertRecordTimeToOffsetRequest] = for {
    recordTimeProto <- requirePresence(request.recordTime, "record_time")
    recordTimeParsed <- validateLfTime(recordTimeProto).map(CantonTimestamp.apply _)
    parsedSynchronizerId <- requireSynchronizerId(
      request.synchronizerId,
      "synchronizer_id",
    )
    lastRecordTime <- resolveLastRecordTimeForSynchronizer(
      parsedSynchronizerId,
      knownSynchronizers,
      "synchronizer_id",
    )

    _ <- Either.cond(
      recordTimeParsed <= lastRecordTime,
      (),
      RequestValidationErrors.RecordTimeNotObservedYet
        .Reject(parsedSynchronizerId, recordTimeParsed, lastRecordTime)
        .asGrpcError,
    )
  } yield state.ConvertRecordTimeToOffsetRequest(
    recordTime = recordTimeParsed,
    synchronizerId = parsedSynchronizerId,
  )

  def validateHostedPartiesFilter(
      filter: Option[HostedPartiesFilter]
  )(implicit
      errorLoggingContext: ErrorLoggingContext,
      sizeLimitsConfig: LedgerApiSizeLimitsConfig,
  ): Result[IndexerHostedPartyFilter] = {
    val requested = filter.getOrElse(HostedPartiesFilter.defaultInstance)
    val limits = sizeLimitsConfig.publicLapiSizeConfig.hostedPartiesFilter
    for {
      _ <- validateCardinality(
        values = requested.partyFilter,
        fieldName = "party_filter",
        limit = limits.partyFilterSizeLimit,
      )
      _ <- validateCardinality(
        values = requested.synchronizerIdFilter,
        fieldName = "synchronizer_id_filter",
        limit = limits.synchronizerIdFilterSizeLimit,
      )
      parties <- requireParties(requested.partyFilter.toSet)
      synchronizerIds <- requested.synchronizerIdFilter.traverse(
        requireSynchronizerId(_, "synchronizer_id_filter")
      )
    } yield IndexerHostedPartyFilter(
      NonEmpty.from(parties),
      requested.allParticipants,
      NonEmpty.from(synchronizerIds.toSet),
    )
  }

  private def resolveLastRecordTimeForSynchronizer(
      synchronizerId: SynchronizerId,
      knownSynchronizers: Map[SynchronizerId, SynchronizerIndex],
      fieldName: String,
  )(implicit
      errorLoggingContext: ErrorLoggingContext
  ): Result[CantonTimestamp] =
    knownSynchronizers
      .get(synchronizerId)
      .toRight(
        ValidationErrors.invalidField(
          fieldName = fieldName,
          message =
            s"Synchronizer id '${synchronizerId.toProtoPrimitive}' is not initialized on this participant.",
        )
      )
      .map(_.recordTime)
}
