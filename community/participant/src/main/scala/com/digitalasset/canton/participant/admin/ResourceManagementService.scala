// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin

import com.daml.error.definitions.LedgerApiErrors.ParticipantBackpressure
import com.daml.ledger.participant.state.v2.SubmissionResult
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.networking.grpc.StaticGrpcServices
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.Future

trait ResourceManagementService {

  def checkOverloaded(currentLoad: Int)(implicit
      loggingContext: ErrorLoggingContext
  ): Option[SubmissionResult] =
    checkNumberOfDirtyRequests(currentLoad).orElse(checkAndUpdateRate())

  protected def checkNumberOfDirtyRequests(
      currentLoad: Int
  )(implicit loggingContext: ErrorLoggingContext): Option[SubmissionResult] =
    resourceLimits.maxDirtyRequests
      .filter(currentLoad >= _.unwrap)
      .map(limit => {
        val status =
          ParticipantBackpressure
            .Rejection(s"too many requests (count: $currentLoad, limit: $limit)")
            .rpcStatus(loggingContext.correlationId)
        // Choosing SynchronousReject instead of Overloaded, because that allows us to specify a custom error message.
        SubmissionResult.SynchronousError(status)
      })

  protected def checkAndUpdateRate()(implicit
      loggingContext: ErrorLoggingContext
  ): Option[SubmissionResult]

  def resourceLimits: ResourceLimits

  def writeResourceLimits(limits: ResourceLimits)(implicit traceContext: TraceContext): Future[Unit]

  def refreshCache()(implicit traceContext: TraceContext): Future[Unit]
}

object ResourceManagementService {
  class CommunityResourceManagementService extends ResourceManagementService {

    override protected def checkAndUpdateRate()(implicit
        loggingContext: ErrorLoggingContext
    ): Option[SubmissionResult] =
      None

    /** The community edition only supports a fixed configuration that cannot be changed.
      */
    override def resourceLimits: ResourceLimits = ResourceLimits.community

    override def writeResourceLimits(limits: ResourceLimits)(implicit
        traceContext: TraceContext
    ): Future[Unit] =
      Future.failed(StaticGrpcServices.notSupportedByCommunityStatus.asRuntimeException())

    override def refreshCache()(implicit traceContext: TraceContext): Future[Unit] = Future.unit
  }
}
