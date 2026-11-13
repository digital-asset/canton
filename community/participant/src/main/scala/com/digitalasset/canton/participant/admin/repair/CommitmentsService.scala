// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.repair

import cats.Eval
import cats.data.EitherT
import cats.syntax.traverse.*
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
import com.digitalasset.canton.lifecycle.{
  FlagCloseable,
  FutureUnlessShutdown,
  HasCloseContext,
  UnlessShutdown,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.ParticipantNodeParameters
import com.digitalasset.canton.participant.ledger.api.LedgerApiIndexer
import com.digitalasset.canton.participant.store.*
import com.digitalasset.canton.participant.store.AcsCommitmentStore.ReinitializationStatus
import com.digitalasset.canton.participant.sync.{
  ConnectedSynchronizersLookup,
  SyncEphemeralStateFactory,
  SyncPersistentStateLookup,
}
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.topology.{ParticipantId, PartyId, SynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.*
import com.google.common.annotations.VisibleForTesting

import scala.concurrent.duration.DurationLong
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

/** Implements the repair commands for ACS commitments. These do not require that the participants
  * disconnects from synchronizers.
  */
final class CommitmentsService(
    ledgerApiIndexer: Eval[LedgerApiIndexer],
    parameters: ParticipantNodeParameters,
    syncPersistentStateLookup: SyncPersistentStateLookup,
    connectedSynchronizersLookup: ConnectedSynchronizersLookup,
    @VisibleForTesting
    protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends NamedLogging
    with FlagCloseable
    with HasCloseContext {

  override protected def timeouts: ProcessingTimeout = parameters.processingTimeouts

  /** Waits for the reinitialization task, for at most `timeout` (granularity: seconds), and then
    * reports the completion timestamp persisted in the store.
    *
    * We wait on the task itself rather than polling the store's completion marker: the marker is
    * written before the task finishes, so a caller that returned on seeing the marker could issue
    * the next reinitialization while the processor still considers this one in progress.
    */
  private def awaitCommitmentReinitialization(
      reinitialization: FutureUnlessShutdown[Unit],
      synchronizerState: SyncPersistentState,
      expectedCompletionTs: CantonTimestamp,
      timeout: NonNegativeFiniteDuration,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Either[String, Option[CantonTimestamp]]] = {
    def notCompleted(actual: Option[CantonTimestamp]): String =
      s"Commitment reinitialization not completed yet, expected completion timestamp: $expectedCompletionTs, actual: $actual"

    val completed: FutureUnlessShutdown[Either[String, Option[CantonTimestamp]]] =
      reinitialization.transformWith {
        case Success(UnlessShutdown.Outcome(())) =>
          synchronizerState.acsCommitmentStore.runningCommitments
            .readReinitilizationStatus()
            .map { case ReinitializationStatus(_, reinitCompletedTs) =>
              Either.cond(
                reinitCompletedTs.contains(expectedCompletionTs),
                reinitCompletedTs,
                notCompleted(reinitCompletedTs),
              )
            }
        case Success(UnlessShutdown.AbortedDueToShutdown) =>
          FutureUnlessShutdown.abortedDueToShutdown
        case Failure(ex) =>
          FutureUnlessShutdown.pure(
            Left(s"Commitment reinitialization at $expectedCompletionTs failed: ${ex.getMessage}")
          )
      }

    val timedOut: FutureUnlessShutdown[Either[String, Option[CantonTimestamp]]] =
      DelayUtil
        .delayIfNotClosing(
          "commitment reinitialization timeout",
          timeout.duration.getSeconds.seconds,
          this,
        )
        .map(_ => Left(notCompleted(None)))

    FutureUnlessShutdown(Future.firstCompletedOf(Seq(completed.unwrap, timedOut.unwrap)))
  }

  def reinitializeLegacyCommitmentsUsingAcs(
      paramSynchronizerIds: Set[SynchronizerId],
      filterCounterParticipants: Seq[ParticipantId],
      filterParties: Seq[PartyId],
      timeoutSeconds: NonNegativeFiniteDuration,
  )(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Map[SynchronizerId, Either[String, Option[CantonTimestamp]]]] = {
    // TODO(#27370) Wire through filterCounterParticipants and filterParties
    logger.debug(
      "Called reinitializeCommitmentsUsingAcs with parameters",
      Map(
        "synchronizerIds" -> paramSynchronizerIds,
        "filterCounterParticipants" -> filterCounterParticipants,
        "filterParties" -> filterParties,
        "timeoutSeconds" -> timeoutSeconds,
      ),
    )
    val synchronizers = connectedSynchronizersLookup.snapshot.values.filter(synchronizer =>
      paramSynchronizerIds.isEmpty || paramSynchronizerIds.contains(synchronizer.psid.logical)
    )

    val resultPerSynchronizer = synchronizers.map { synchronizer =>
      val synchronizerId = synchronizer.psid.logical
      val synchronizerIndex = ledgerApiIndexer.value.ledgerApiStore
        .cleanSynchronizerIndex(
          synchronizerId
        )
      val reinitRecordTime = SyncEphemeralStateFactory.currentTimeOfChange(synchronizerIndex)
      val statusPerSynchronizer =
        for {
          processor <- EitherT.fromOption[FutureUnlessShutdown](
            synchronizer.acsCommitmentProcessorO,
            s"Commitment reinitialization is disabled for ${synchronizer.psid} because the old ACS commitment processor is turned off.",
          )

          // Check the storage before scheduling, so that an unsupported request does not start a reinitialization.
          persistentState <- EitherT.fromEither[FutureUnlessShutdown](
            syncPersistentStateLookup
              .get(synchronizer.psid)
              .toRight(
                s"Could not find persistent state for ${synchronizer.psid}"
              )
          )
          _ <- EitherT.fromEither[FutureUnlessShutdown](
            Either.cond(
              !persistentState.isMemory,
              (),
              s"${synchronizer.psid} is in memory which is not supported by commitment reinitialization. Use db persistence.",
            )
          )

          reinitialization <- EitherT.fromOption[FutureUnlessShutdown](
            processor.reinitializeCommitments(reinitRecordTime),
            s"Reinitialization is already scheduled or in progress for ${synchronizer.psid}.",
          )

          res <- EitherT(
            awaitCommitmentReinitialization(
              reinitialization,
              persistentState,
              reinitRecordTime.timestamp,
              timeoutSeconds,
            )
          )
        } yield res
      statusPerSynchronizer.value.map(status => synchronizerId -> status)
    }
    resultPerSynchronizer.toSeq.sequence.map(_.toMap)
  }
}
