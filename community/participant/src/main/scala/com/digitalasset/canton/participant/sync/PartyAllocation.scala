// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.sync

import cats.data.EitherT
import cats.implicits.showInterpolator
import cats.syntax.bifunctor._
import cats.syntax.either._
import cats.syntax.traverse._
import com.daml.ledger.participant.state.v2._
import com.daml.telemetry.TelemetryContext
import com.digitalasset.canton.config.RequireTypes.String255
import com.digitalasset.canton.error.TransactionError
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.LedgerSyncEvent
import com.digitalasset.canton.participant.config.{
  ParticipantNodeParameters,
  PartyNotificationConfig,
}
import com.digitalasset.canton.participant.store.ParticipantNodeEphemeralState
import com.digitalasset.canton.participant.topology.ParticipantTopologyManagerError.IdentityManagerParentError
import com.digitalasset.canton.participant.topology.{
  LedgerServerPartyNotifier,
  ParticipantTopologyManager,
}
import com.digitalasset.canton.topology.TopologyManagerError.MappingAlreadyExists
import com.digitalasset.canton.topology.transaction._
import com.digitalasset.canton.topology.{DomainId, Identifier, ParticipantId, PartyId}
import com.digitalasset.canton.tracing.{Spanning, TraceContext}
import com.digitalasset.canton.util._
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{LedgerSubmissionId, LfPartyId, LfTimestamp}
import io.opentelemetry.api.trace.Tracer

import java.util.UUID
import java.util.concurrent.CompletionStage
import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.FutureConverters._
import scala.util.chaining._
import scala.util.{Failure, Success}

private[sync] class PartyAllocation(
    participantId: ParticipantId,
    participantNodeEphemeralState: ParticipantNodeEphemeralState,
    topologyManager: ParticipantTopologyManager,
    partyNotifier: LedgerServerPartyNotifier,
    parameters: ParticipantNodeParameters,
    isActive: () => Boolean,
    connectedDomainsMap: TrieMap[DomainId, SyncDomain],
    protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext, val tracer: Tracer)
    extends Spanning
    with NamedLogging {
  def allocate(
      hint: Option[LfPartyId],
      displayName: Option[String],
      rawSubmissionId: LedgerSubmissionId,
  )(implicit telemetryContext: TelemetryContext): CompletionStage[SubmissionResult] = {
    implicit val traceContext: TraceContext =
      TraceContext.fromDamlTelemetryContext(telemetryContext)

    withSpan("CantonSyncService.allocateParty") { implicit traceContext => span =>
      span.setAttribute("submission_id", rawSubmissionId)

      allocateInternal(hint, displayName, rawSubmissionId)
    }.asJava
  }

  private def allocateInternal(
      hint: Option[LfPartyId],
      displayName: Option[String],
      rawSubmissionId: LedgerSubmissionId,
  )(implicit traceContext: TraceContext): Future[SubmissionResult] = {
    def reject(reason: String, result: SubmissionResult): SubmissionResult = {
      publishReject(reason, rawSubmissionId, displayName, result)
      result
    }

    val partyName = hint.getOrElse(s"party-${UUID.randomUUID().toString}")
    val protocolVersion = parameters.protocolConfig.initialProtocolVersion

    val result =
      for {
        _ <- EitherT
          .cond[Future](isActive(), (), TransactionError.NotSupported)
          .leftWiden[SubmissionResult]
        id <- Identifier
          .create(partyName)
          .leftMap(TransactionError.internalError)
          .toEitherT[Future]
        partyId = PartyId(id, participantId.uid.namespace)
        validatedDisplayName <- displayName
          .traverse(n => String255.create(n, Some("DisplayName")))
          .leftMap(TransactionError.internalError)
          .toEitherT[Future]
        validatedSubmissionId <- EitherT.fromEither[Future](
          String255
            .fromProtoPrimitive(rawSubmissionId, "LedgerSubmissionId")
            .leftMap(err => TransactionError.internalError(err.toString))
        )
        // Allow party allocation via ledger API only if notification is Eager or the participant is connected to a domain
        // Otherwise the gRPC call will just timeout without a meaning error message
        _ <- EitherT.cond[Future](
          parameters.partyChangeNotification == PartyNotificationConfig.Eager ||
            connectedDomainsMap.nonEmpty,
          (),
          SubmissionResult.SynchronousError(
            SyncServiceError.PartyAllocationNoDomainError.Error(rawSubmissionId).rpcStatus()
          ),
        )
        _ <- topologyManager
          .authorize(
            topologyTransaction(partyId, validatedSubmissionId, protocolVersion),
            None,
            protocolVersion,
            force = false,
          )
          .leftMap[SubmissionResult] {
            case IdentityManagerParentError(e) if e.code == MappingAlreadyExists =>
              reject(
                show"Party already exists: party $partyId is already allocated on this node",
                SubmissionResult.Acknowledged,
              )
            case IdentityManagerParentError(e) => reject(e.cause, SubmissionResult.Acknowledged)
            case e => reject(e.toString, TransactionError.internalError(e.toString))
          }

        // Notify upstream of display name using the participant party notifier (as display name is a participant setting)
        _ <- EitherT(
          validatedDisplayName
            .fold(Future.unit)(dn => partyNotifier.setDisplayName(partyId, dn))
            .transform {
              case Success(_) => Success(Right(()))
              case Failure(t) =>
                Success(
                  Left(
                    TransactionError.internalError(s"Failed to set display name ${t.getMessage}")
                  )
                )
            }
        ).leftWiden[SubmissionResult]

      } yield SubmissionResult.Acknowledged

    result.fold(
      _.tap { l =>
        logger.info(
          s"Failed to allocate party $partyName::${participantId.uid.namespace}: ${l.toString}"
        )
      },
      _.tap { _ =>
        logger.debug(s"Allocated party $partyName::${participantId.uid.namespace}")
      },
    )
  }

  private def publishReject(
      reason: String,
      rawSubmissionId: LedgerSubmissionId,
      displayName: Option[String],
      result: SubmissionResult,
  )(implicit
      traceContext: TraceContext
  ): SubmissionResult = {
    FutureUtil.doNotAwait(
      participantNodeEphemeralState.participantEventPublisher.publish(
        LedgerSyncEvent.PartyAllocationRejected(
          rawSubmissionId,
          participantId.toLf,
          recordTime =
            LfTimestamp.Epoch, // The actual record time will be filled in by the ParticipantEventPublisher
          rejectionReason = reason,
        )
      ),
      s"Failed to publish allocation rejection for party $displayName",
    )
    result
  }

  private def topologyTransaction(
      partyId: PartyId,
      validatedSubmissionId: String255,
      protocolVersion: ProtocolVersion,
  ): TopologyStateUpdate[TopologyChangeOp.Add] = TopologyStateUpdate(
    TopologyChangeOp.Add,
    TopologyStateUpdateElement(
      TopologyElementId.adopt(validatedSubmissionId),
      PartyToParticipant(
        RequestSide.Both,
        partyId,
        participantId,
        ParticipantPermission.Submission,
      ),
    ),
    protocolVersion,
  )
}
