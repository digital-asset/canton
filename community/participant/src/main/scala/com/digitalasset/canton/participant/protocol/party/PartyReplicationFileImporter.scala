// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.party

import cats.Eval
import cats.data.EitherT
import com.digitalasset.canton.RepairCounter
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.config.RequireTypes.NonNegativeLong
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
import com.digitalasset.canton.lifecycle.UnlessShutdown.Outcome
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.participant.admin.data.ActiveContract
import com.digitalasset.canton.participant.admin.party.PartyReplicator.AddPartyRequestId
import com.digitalasset.canton.participant.admin.party.{
  PartyReplicationStatus,
  PartyReplicationTestInterceptor,
}
import com.digitalasset.canton.participant.protocol.conflictdetection.RequestTracker
import com.digitalasset.canton.participant.protocol.party.PartyReplicationFileImporter.ImportFailedException
import com.digitalasset.canton.participant.store.{
  AcsReplicationProgress,
  ParticipantNodePersistentState,
  PartyReplicationIndexingStore,
}
import com.digitalasset.canton.participant.sync.ConnectedSynchronizer
import com.digitalasset.canton.topology.processing.EffectiveTime
import com.digitalasset.canton.topology.{PartyId, PhysicalSynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.nonempty.NonEmpty
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.{Sink, Source}

import java.util.concurrent.atomic.AtomicLong
import scala.annotation.tailrec
import scala.concurrent.{ExecutionContext, Future, blocking}
import scala.util.control.NonFatal

/** The party replication file importer imports a party's active contracts on a specific
  * synchronizer and timestamp previously exported from a source participant.
  *
  * @param requestId
  *   The "add party" request id that this replication is associated with.
  * @param psid
  *   The physical id of the synchronizer to replicate active contracts in.
  * @param partyOnboardingAt
  *   The timestamp immediately on which the ACS snapshot is based.
  * @param replicationProgressState
  *   Interface for processor to read and update ACS replication progress.
  * @param persistsContracts
  *   Interface to persist imported contracts to the ContractStore.
  * @param requestTracker
  *   Canton protocol request tracker used to persist to the ActiveContractStore and update the
  *   in-memory request state in a consistent way.
  * @param acsReader
  *   Pekko Source providing the stream of active contracts to import.
  * @param indexingStore
  *   Store to insert imported contract activations for subsequent indexing.
  * @param testOnlyInterceptorO
  *   Test interceptor only alters behavior in integration tests.
  * @param isClosing
  *   A callback function to check for active node shutdown. It is used exclusively to safely break
  *   out of blocking wait loops introduced by the `testOnlyInterceptorO` during integration tests.
  */
class PartyReplicationFileImporter(
    requestId: AddPartyRequestId,
    protected val psid: PhysicalSynchronizerId,
    partyOnboardingAt: EffectiveTime,
    protected val replicationProgressState: AcsReplicationProgress,
    persistsContracts: TargetParticipantAcsPersistence.PersistsContracts,
    requestTracker: RequestTracker,
    acsReader: Source[ActiveContract, NotUsed],
    indexingStore: PartyReplicationIndexingStore,
    testOnlyInterceptorO: Option[PartyReplicationTestInterceptor],
    isClosing: () => Boolean,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit val executionContext: ExecutionContext, mat: Materializer)
    extends TargetParticipantAcsPersistence(
      requestId,
      psid,
      partyOnboardingAt,
      replicationProgressState,
      persistsContracts,
      requestTracker,
      indexingStore,
    ) {

  /** Imports the Active Contract Set (ACS) snapshot from the provided stream.
    *
    * This method processes the stream of active contracts asynchronously and will only return
    * successfully (i.e., yielding a Right) once the entire ACS has been fully imported and the
    * replication progress state is marked as complete.
    *
    * @param traceContext
    *   The trace context for logging and telemetry.
    * @return
    *   An EitherT that completes with Unit when the full import is successful, or a String error
    *   message if it fails.
    */
  def importAcsSnapshot()(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] = {

    // Helper to pipe EitherT errors into the Pekko Stream future
    def toFuture[T](resET: EitherT[FutureUnlessShutdown, String, T]): Future[T] =
      resET.value
        .flatMap(
          _.fold(
            err => FutureUnlessShutdown.failed[T](ImportFailedException(err)),
            res => FutureUnlessShutdown.pure(res),
          )
        )
        .failOnShutdownToAbortException("importAcsSnapshot")

    // Note: We process the file from the beginning upon every invocation.
    val numContractsImported = new AtomicLong(0L)

    // Run the stream
    val doneF = acsReader
      .grouped(TargetParticipantAcsPersistence.contractsToRequestEachTime.unwrap)
      .mapAsync(1) { contracts =>
        toFuture(
          for {
            _ <- EitherT.liftF[FutureUnlessShutdown, String, Unit](awaitTestInterceptor())
            _ = logger.info(
              s"About to import contracts starting at ordinal ${numContractsImported.get}"
            )
            contractsNE <- EitherT.fromEither[FutureUnlessShutdown](
              NonEmpty.from(contracts).toRight("Grouped ACS must be nonempty")
            )

            // Import the chunk
            totalNumContractsImported <- importContracts(contractsNE)

            // Update the state incrementally after every chunk (= checkpoint)
            // We must fetch the current progress here to access the correct nextPersistenceCounter
            currentProgress <- EitherT.fromEither[FutureUnlessShutdown](
              replicationProgressState
                .getAcsReplicationProgress(requestId)
                .toRight(s"Party replication $requestId is unexpectedly unknown")
            )

            _ <- replicationProgressState.updateAcsReplicationProgress(
              requestId,
              PartyReplicationStatus.EphemeralFileImporterProgress(
                totalNumContractsImported,
                currentProgress.nextPersistenceCounter,
                fullyProcessedAcs = false,
                this,
              ),
            )
          } yield numContractsImported.set(totalNumContractsImported.unwrap)
        )
      }
      .runWith(Sink.ignore)

    for {
      // Translate exceptions back to EitherT:
      // - recoverFromAbortException safely extracts the private AbortedDueToShutdownException
      // - We then map the successful completion to Right(())
      // - We use FutureUnlessShutdown's built-in .recover to catch stream failures and turn them into Lefts
      _ <- EitherT(
        FutureUnlessShutdown
          .recoverFromAbortException(doneF)
          .map[Either[String, Unit]](_ => Right(()))
          .recover {
            case ImportFailedException(msg) => Outcome(Left(msg))
            case NonFatal(ex) => Outcome(Left(ex.getMessage))
          }
      )

      finalProgress <- EitherT.fromEither[FutureUnlessShutdown](
        replicationProgressState
          .getAcsReplicationProgress(requestId)
          .toRight(s"Party replication $requestId is unexpectedly unknown")
      )

      // Mark the replication as fully complete
      _ <- replicationProgressState.updateAcsReplicationProgress(
        requestId,
        PartyReplicationStatus.EphemeralFileImporterProgress(
          finalProgress.processedContractCount,
          finalProgress.nextPersistenceCounter,
          fullyProcessedAcs = true,
          this,
        ),
      )
    } yield ()
  }

  private def awaitTestInterceptor()(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = testOnlyInterceptorO match {
    case None => FutureUnlessShutdown.unit
    case Some(interceptor) =>
      @tailrec
      def go(): FutureUnlessShutdown[Unit] =
        if (isClosing()) {
          logger.info(s"Interceptor breaking wait loop due to active shutdown for $requestId")
          FutureUnlessShutdown.abortedDueToShutdown
        } else {
          replicationProgressState
            .getAcsReplicationProgress(requestId)
            .map(interceptor.onTargetParticipantProgress) match {
            case None =>
              FutureUnlessShutdown.unit
            case Some(PartyReplicationTestInterceptor.Proceed) =>
              FutureUnlessShutdown.unit
            case Some(PartyReplicationTestInterceptor.Wait) =>
              blocking(Threading.sleep(200))
              go()
            case Some(PartyReplicationTestInterceptor.Fail(reason)) =>
              FutureUnlessShutdown.failed(ImportFailedException(reason))
          }
        }

      go()
  }
  override protected def newProgress(
      updatedProcessedContractsCount: NonNegativeLong,
      usedRepairCounter: RepairCounter,
  ): PartyReplicationStatus.AcsReplicationProgress =
    PartyReplicationStatus.EphemeralFileImporterProgress(
      updatedProcessedContractsCount,
      usedRepairCounter + 1,
      fullyProcessedAcs = false,
      this,
    )
}

object PartyReplicationFileImporter {

  // Exception wrapper used to tunnel EitherT Left values (business logic errors) through Pekko Stream's Future-based
  // mapAsync and safely recover them back into EitherT Lefts.
  final case class ImportFailedException(msg: String) extends RuntimeException(msg)

  def apply(
      partyId: PartyId,
      requestId: AddPartyRequestId,
      partyOnboardingAt: EffectiveTime,
      replicationProgressState: AcsReplicationProgress,
      participantNodePersistentState: Eval[ParticipantNodePersistentState],
      connectedSynchronizer: ConnectedSynchronizer,
      acsReader: Source[ActiveContract, NotUsed],
      testOnlyInterceptorO: Option[PartyReplicationTestInterceptor],
      isClosing: () => Boolean,
      loggerFactory: NamedLoggerFactory,
  )(implicit executionContext: ExecutionContext, mat: Materializer) = {
    val partyReplicationIndexingStore =
      connectedSynchronizer.synchronizerHandle.syncPersistentState.partyReplicationIndexingStoreIfOnPREnabled
        .getOrElse(throw new IllegalStateException("Expect store when OnPR enabled"))
    new PartyReplicationFileImporter(
      requestId,
      connectedSynchronizer.psid,
      partyOnboardingAt,
      replicationProgressState,
      new TargetParticipantAcsPersistence.PersistsContractsImpl(participantNodePersistentState),
      connectedSynchronizer.ephemeral.requestTracker,
      acsReader,
      partyReplicationIndexingStore,
      testOnlyInterceptorO,
      isClosing,
      loggerFactory
        .append("psid", connectedSynchronizer.psid.toProtoPrimitive)
        .append("partyId", partyId.toProtoPrimitive)
        .append("requestId", requestId.toHexString),
    )
  }
}
