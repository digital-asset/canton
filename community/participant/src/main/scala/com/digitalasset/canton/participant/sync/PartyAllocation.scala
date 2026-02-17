// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.sync

import cats.data.EitherT
import cats.implicits.showInterpolator
import cats.syntax.bifunctor.*
import cats.syntax.either.*
import com.digitalasset.canton.LedgerSubmissionId
import com.digitalasset.canton.config.CantonRequireTypes.String255
import com.digitalasset.canton.ledger.participant.state.*
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.topology.ParticipantTopologyManagerError.IdentityManagerParentError
import com.digitalasset.canton.participant.topology.PartyOps
import com.digitalasset.canton.topology.TopologyManagerError.MappingAlreadyExists
import com.digitalasset.canton.topology.{
  ExternalPartyOnboardingDetails,
  ParticipantId,
  PartyId,
  PhysicalSynchronizerId,
}
import com.digitalasset.canton.tracing.{Spanning, TraceContext}
import io.opentelemetry.api.trace.Tracer

import scala.concurrent.ExecutionContext
import scala.util.chaining.*

private[sync] class PartyAllocation(
    participantId: ParticipantId,
    isActive: () => Boolean,
    connectedSynchronizersLookup: ConnectedSynchronizersLookup,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext, val tracer: Tracer)
    extends Spanning
    with NamedLogging {
  def allocate(
      partyId: PartyId,
      rawSubmissionId: LedgerSubmissionId,
      synchronizerId: PhysicalSynchronizerId,
      externalPartyOnboardingDetails: Option[ExternalPartyOnboardingDetails],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[SubmissionResult] =
    withSpan("CantonSyncService.allocateParty") { implicit traceContext => span =>
      span.setAttribute("submission_id", rawSubmissionId)

      allocateInternal(partyId, rawSubmissionId, synchronizerId, externalPartyOnboardingDetails)
    }

  private def allocateInternal(
      partyId: PartyId,
      rawSubmissionId: LedgerSubmissionId,
      synchronizerId: PhysicalSynchronizerId,
      externalPartyOnboardingDetails: Option[ExternalPartyOnboardingDetails],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[SubmissionResult] = {
    import com.google.rpc.status.Status
    import io.grpc.Status.Code

    def reject(reason: String, statusCode: Option[Code]): SubmissionResult.SynchronousError =
      SubmissionResult.SynchronousError(
        Status.of(statusCode.getOrElse(Code.UNKNOWN).value(), reason, Seq())
      )

    val result =
      for {
        _ <- EitherT
          .cond[FutureUnlessShutdown](isActive(), (), SyncServiceError.Synchronous.PassiveNode)
          .leftWiden[SubmissionResult]
        validatedSubmissionId <- EitherT.fromEither[FutureUnlessShutdown](
          String255
            .fromProtoPrimitive(rawSubmissionId, "LedgerSubmissionId")
            .leftMap(err => SyncServiceError.Synchronous.internalError(err.toString))
        )
        // Allow party allocation via ledger API only if the participant is connected to the synchronizer.
        // Otherwise the gRPC call will just timeout without a meaningful error message
        connectedSynchronizer <- EitherT.fromOption[FutureUnlessShutdown](
          connectedSynchronizersLookup.get(synchronizerId),
          SubmissionResult.SynchronousError(
            SyncServiceInjectionError.NotConnectedToSynchronizer
              .Error(synchronizerId.toProtoPrimitive)
              .rpcStatus()
          ),
        )
        _ <- (externalPartyOnboardingDetails match {
          case Some(details) =>
            PartyOps.allocateExternalParty(
              participantId,
              details,
              synchronizerId,
              connectedSynchronizer.topologyManager,
            )
          case None =>
            PartyOps.allocateParty(
              partyId,
              participantId,
              synchronizerId,
              connectedSynchronizer.topologyManager,
            )
        })
          .leftMap[SubmissionResult] {
            case IdentityManagerParentError(e) if e.code == MappingAlreadyExists =>
              reject(
                show"Party already exists: party $partyId is already allocated${if (externalPartyOnboardingDetails.isEmpty) { " on this node" }
                  else ""}",
                e.code.category.grpcCode,
              )
            case IdentityManagerParentError(e) => reject(e.cause, e.code.category.grpcCode)
            case e => reject(e.cause, e.code.category.grpcCode)
          }
      } yield SubmissionResult.Acknowledged

    result.fold(
      _.tap { l =>
        logger.info(
          s"Failed to allocate party $partyId: ${l.toString}"
        )
      },
      _.tap { _ =>
        logger.debug(s"Allocated party $partyId")
      },
    )
  }
}
