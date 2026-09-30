// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.validation

import com.daml.ledger.api.v2.state_service
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.ledger.api.messages.state
import com.digitalasset.canton.ledger.error.groups.RequestValidationErrors
import com.digitalasset.canton.ledger.participant.state.SynchronizerIndex
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.topology.SynchronizerId
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
