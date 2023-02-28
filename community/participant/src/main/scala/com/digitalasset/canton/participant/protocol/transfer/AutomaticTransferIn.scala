// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.transfer

import cats.data.*
import cats.syntax.bifunctor.*
import cats.syntax.parallel.*
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.data.{CantonTimestamp, TransferSubmitterMetadata}
import com.digitalasset.canton.error.{BaseCantonError, MediatorError}
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.participant.protocol.transfer.TransferInValidation.NoTransferData
import com.digitalasset.canton.participant.protocol.transfer.TransferOutRequestValidation.AutomaticTransferInError
import com.digitalasset.canton.participant.protocol.transfer.TransferProcessingSteps.*
import com.digitalasset.canton.participant.store.TransferStore.TransferCompleted
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.{EitherTUtil, MonadUtil}
import com.digitalasset.canton.version.Transfer.SourceProtocolVersion
import org.slf4j.event.Level

import scala.concurrent.{ExecutionContext, Future}

private[participant] object AutomaticTransferIn {
  def perform(
      id: TransferId,
      targetDomain: DomainId,
      transferCoordination: TransferCoordination,
      stks: Set[LfPartyId],
      transferOutSubmitterMetadata: TransferSubmitterMetadata,
      participantId: ParticipantId,
      t0: CantonTimestamp,
  )(implicit
      ec: ExecutionContext,
      elc: ErrorLoggingContext,
  ): EitherT[Future, TransferProcessorError, Unit] = {
    val logger = elc.logger
    implicit val tc = elc.traceContext

    def hostedStakeholders(snapshot: TopologySnapshot): Future[Set[LfPartyId]] = {
      stks.toList
        .parTraverseFilter { partyId =>
          snapshot
            .hostedOn(partyId, participantId)
            .map(x => x.filter(_.permission == ParticipantPermission.Submission).map(_ => partyId))
        }
        .map(_.toSet)
    }

    def performAutoInOnce: EitherT[Future, TransferProcessorError, com.google.rpc.status.Status] = {
      for {
        targetIps <- transferCoordination
          .getTimeProofAndSnapshot(targetDomain)
          .map(_._2)
          .onShutdown(Left(DomainNotReady(targetDomain, "Shutdown of time tracker")))
        possibleSubmittingParties <- EitherT.right(hostedStakeholders(targetIps.ipsSnapshot))
        inParty <- EitherT.fromOption[Future](
          possibleSubmittingParties.headOption,
          AutomaticTransferInError("No possible submitting party for automatic transfer-in"),
        )
        sourceProtocolVersion <- EitherT(
          transferCoordination
            .protocolVersion(Traced(id.sourceDomain))
            .map(
              _.toRight(
                AutomaticTransferInError(
                  s"Unable to get protocol version of source domain ${id.sourceDomain}"
                )
              )
            )
        ).map(SourceProtocolVersion(_))
        submissionResult <- transferCoordination
          .transferIn(
            targetDomain,
            TransferSubmitterMetadata(
              inParty,
              transferOutSubmitterMetadata.applicationId,
              participantId.toLf,
              transferOutSubmitterMetadata.commandId,
              None,
            ),
            workflowId = None,
            id,
            sourceProtocolVersion,
          )(
            TraceContext.empty
          )
        TransferInProcessingSteps.SubmissionResult(completionF) = submissionResult
        status <- EitherT.liftF(completionF)
      } yield status
    }

    def performAutoInRepeatedly: EitherT[Future, TransferProcessorError, Unit] = {
      case class StopRetry(result: Either[TransferProcessorError, com.google.rpc.status.Status])
      val retryCount = 5

      def tryAgain(
          previous: com.google.rpc.status.Status
      ): EitherT[Future, StopRetry, com.google.rpc.status.Status] = {
        if (BaseCantonError.isStatusErrorCode(MediatorError.Timeout, previous))
          performAutoInOnce.leftMap(error => StopRetry(Left(error)))
        else EitherT.leftT[Future, com.google.rpc.status.Status](StopRetry(Right(previous)))
      }

      val initial = performAutoInOnce.leftMap(error => StopRetry(Left(error)))
      val result = MonadUtil.repeatFlatmap(initial, tryAgain, retryCount)

      result.transform {
        case Left(StopRetry(Left(error))) => Left(error)
        case Left(StopRetry(Right(_verdict))) => Right(())
        case Right(_verdict) => Right(())
      }
    }

    def triggerAutoIn(
        targetSnapshot: TopologySnapshot,
        targetDomainParameters: DynamicDomainParametersWithValidity,
    ): Unit = {

      val autoIn = for {
        exclusivityLimit <- EitherT
          .fromEither[Future](
            targetDomainParameters
              .transferExclusivityLimitFor(t0)
              .leftMap(TransferParametersError(targetDomain, _))
          )
          .leftWiden[TransferProcessorError]

        targetHostedStakeholders <- EitherT.right(hostedStakeholders(targetSnapshot))
        _unit <-
          if (targetHostedStakeholders.nonEmpty) {
            logger.info(
              s"Registering automatic submission of transfer-in with ID ${id} at time $exclusivityLimit, where base timestamp is $t0"
            )
            for {
              timeoutFuture <- EitherT.fromEither[Future](
                transferCoordination.awaitTimestamp(
                  targetDomain,
                  exclusivityLimit,
                  waitForEffectiveTime = false,
                )
              )
              _ <- EitherT.liftF[Future, TransferProcessorError, Unit](timeoutFuture.getOrElse {
                logger.debug(s"Automatic transfer-in triggered immediately")
                Future.unit
              })
              _unit <- EitherTUtil.leftSubflatMap(performAutoInRepeatedly) {
                // Filter out submission errors occurring because the transfer is already completed
                case NoTransferData(_id, TransferCompleted(_transferId, _timeOfCompletion)) =>
                  Right(())
                // Filter out the case that the participant has disconnected from the target domain in the meantime.
                case UnknownDomain(domain, _reason) if domain == targetDomain =>
                  Right(())
                case DomainNotReady(domain, _reason) if domain == targetDomain =>
                  Right(())
                // Filter out the case that the target domain is closing right now
                case other => Left(other)
              }
            } yield ()
          } else EitherT.pure[Future, TransferProcessorError](())
      } yield ()

      EitherTUtil.doNotAwait(autoIn, "Automatic transfer-in failed", Level.INFO)
    }

    for {
      targetIps <- transferCoordination.cryptoSnapshot(targetDomain, t0)
      targetSnapshot = targetIps.ipsSnapshot

      targetDomainParameters <- EitherT(
        targetSnapshot
          .findDynamicDomainParameters()
          .map(_.leftMap(DomainNotReady(targetDomain, _)))
      ).leftWiden[TransferProcessorError]
    } yield {

      if (targetDomainParameters.automaticTransferInEnabled)
        triggerAutoIn(targetSnapshot, targetDomainParameters)
      else ()
    }
  }
}
