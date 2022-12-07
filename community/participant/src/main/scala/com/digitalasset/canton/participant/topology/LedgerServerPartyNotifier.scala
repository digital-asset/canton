// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.topology

import cats.syntax.parallel.*
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.LengthLimitedString.DisplayName
import com.digitalasset.canton.config.RequireTypes.{LengthLimitedString, String255}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.{
  AsyncOrSyncCloseable,
  FlagCloseableAsync,
  FutureUnlessShutdown,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.sync.{LedgerSyncEvent, ParticipantEventPublisher}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.processing.{
  ApproximateTime,
  EffectiveTime,
  SequencedTime,
  TopologyTransactionProcessingSubscriber,
}
import com.digitalasset.canton.topology.store.{PartyMetadata, PartyMetadataStore}
import com.digitalasset.canton.topology.transaction.{
  ParticipantState,
  PartyToParticipant,
  SignedTopologyTransaction,
  TopologyChangeOp,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.{FutureUtil, SimpleExecutionQueue}
import com.digitalasset.canton.{LedgerSubmissionId, SequencerCounter}
import com.google.common.annotations.VisibleForTesting
import io.functionmeta.functionFullName

import scala.concurrent.{ExecutionContext, Future}

/** Listens to changes of the identity stores and notifies the ledger-api server
  *
  * We need to send PartyAddedToParticipant messages to LedgerApi server for every
  * successful addition.
  */
class LedgerServerPartyNotifier(
    participantId: ParticipantId,
    eventPublisher: ParticipantEventPublisher,
    store: PartyMetadataStore,
    clock: Clock,
    override protected val timeouts: ProcessingTimeout,
    val loggerFactory: NamedLoggerFactory,
)(implicit val ec: ExecutionContext)
    extends NamedLogging
    with FlagCloseableAsync {

  def resumePending(): Future[Unit] = {
    import TraceContext.Implicits.Empty.*
    store.fetchNotNotified().map { todo =>
      if (todo.nonEmpty)
        logger.debug(s"Resuming party notification with ${todo.size} pending notifications")
      todo.foreach { partyMetadata =>
        val participantIdO = partyMetadata.participantId
        participantIdO.foreach(
          scheduleUpdate(partyMetadata, _, SequencedTime(clock.now))
        )
      }
    }
  }

  @VisibleForTesting
  def flush(): Future[Unit] = sequentialQueue.flush()

  def attachToTopologyProcessor(): TopologyTransactionProcessingSubscriber =
    new TopologyTransactionProcessingSubscriber {

      override def updateHead(
          effectiveTimestamp: EffectiveTime,
          approximateTimestamp: ApproximateTime,
          potentialTopologyChange: Boolean,
      )(implicit
          traceContext: TraceContext
      ): Unit = {}

      override def observed(
          sequencerTimestamp: SequencedTime,
          effectiveTimestamp: EffectiveTime,
          sequencerCounter: SequencerCounter,
          transactions: Seq[SignedTopologyTransaction[TopologyChangeOp]],
      )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
        transactions.parTraverse_(tx => observedF(sequencerTimestamp, effectiveTimestamp, tx))

    }

  def attachToIdentityManager(): ParticipantTopologyManagerObserver =
    new ParticipantTopologyManagerObserver {
      override def addedNewTransactions(
          timestamp: CantonTimestamp,
          transactions: Seq[SignedTopologyTransaction[TopologyChangeOp]],
      )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
        transactions.parTraverse_(observedF(SequencedTime(clock.now), EffectiveTime(clock.now), _))
    }

  private val sequentialQueue = new SimpleExecutionQueue()

  def setDisplayName(partyId: PartyId, displayName: DisplayName)(implicit
      traceContext: TraceContext
  ): Future[Unit] =
    sequentialQueue.execute(
      {
        val currentTime = clock.now
        updateAndNotify(
          partyId,
          Some(displayName),
          None,
          SequencedTime(currentTime),
          EffectiveTime(currentTime),
        )
      },
      s"set display name for $partyId",
    )

  private def updateAndNotify(
      partyId: PartyId,
      displayName: Option[DisplayName],
      targetParticipantId: Option[ParticipantId],
      sequencerTimestamp: SequencedTime,
      effectiveTimestamp: EffectiveTime,
      submissionIdRaw: String255 = LengthLimitedString.getUuid.asString255,
  )(implicit traceContext: TraceContext): Future[Unit] = {

    def desiredDisplayName(
        stored: Option[DisplayName],
        passed: Option[DisplayName],
    ): Option[DisplayName] =
      if (passed.isDefined) passed else stored

    def desiredParticipantId(
        stored: Option[ParticipantId],
        passed: Option[ParticipantId],
    ): Option[ParticipantId] =
      // prefer ours over any passed
      if (passed.isDefined && !stored.contains(participantId))
        passed
      else stored
    for {
      currentO <- store.metadataForParty(partyId)
      current = currentO.getOrElse(
        PartyMetadata(partyId, None, None)(
          effectiveTimestamp = effectiveTimestamp.value,
          submissionId = submissionIdRaw,
        )
      )
      desired = PartyMetadata(
        partyId,
        displayName = desiredDisplayName(current.displayName, displayName),
        participantId = desiredParticipantId(current.participantId, targetParticipantId),
      )(
        effectiveTimestamp = effectiveTimestamp.value,
        submissionId = submissionIdRaw,
      )

      // update or insert if necessary
      _ <-
        (if (current != desired)
           updateToNewMetadata(desired, sequencerTimestamp)
         else Future.unit)
    } yield ()
  }

  private def updateToNewMetadata(
      metadata: PartyMetadata,
      sequencerTimestamp: SequencedTime,
  )(implicit traceContext: TraceContext): Future[Unit] = {
    val storedF = store.insertOrUpdatePartyMetadata(
      partyId = metadata.partyId,
      participantId = metadata.participantId,
      displayName = metadata.displayName,
      effectiveTimestamp = metadata.effectiveTimestamp,
      submissionId = metadata.submissionId,
    )
    metadata.participantId match {
      case Some(desiredParticipantId) =>
        scheduleUpdate(metadata, desiredParticipantId, sequencerTimestamp)
        Future.unit
      case None =>
        storedF.flatMap { _ =>
          store.markNotified(metadata)
        }
    }
  }

  private def scheduleUpdate(
      metadata: PartyMetadata,
      desiredParticipantId: ParticipantId,
      sequencerTimestamp: SequencedTime,
  )(implicit
      traceContext: TraceContext
  ): Unit = {
    // if the update is in the future, ensure we wait until the effective time is valid
    if (metadata.effectiveTimestamp > sequencerTimestamp.value) {
      // we don't await on this future
      val _ = clock.scheduleAfter(
        _ => notifyLedgerServer(metadata, desiredParticipantId),
        metadata.effectiveTimestamp - sequencerTimestamp.value,
      )
    } else notifyLedgerServer(metadata, desiredParticipantId)
  }

  private def notifyLedgerServer(
      metadata: PartyMetadata,
      targetParticipantId: ParticipantId,
  )(implicit traceContext: TraceContext): Unit = {
    val notifyF = sequentialQueue.execute(
      performUnlessClosingF(functionFullName) {
        logger.debug(show"Pushing ${metadata.partyId} on ${targetParticipantId} to ledger server")
        val event = LedgerSyncEvent.PartyAddedToParticipant(
          metadata.partyId.toLf,
          metadata.displayName.map(_.unwrap).getOrElse(""),
          targetParticipantId.toLf,
          ParticipantEventPublisher.now.toLf,
          LedgerSubmissionId.fromString(metadata.submissionId.unwrap).toOption,
        )
        for {
          _ <- eventPublisher.publish(event)
          _ <- store.markNotified(metadata)
        } yield ()
      }.unwrap,
      s"notifying ledger server about ${metadata}",
    )
    FutureUtil.doNotAwait(notifyF, s"failed to notify ledger server about ${metadata}")
  }

  @VisibleForTesting
  private[topology] def observedF(
      sequencerTimestamp: SequencedTime,
      effectiveTimestamp: EffectiveTime,
      transaction: SignedTopologyTransaction[TopologyChangeOp],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    def dispatch(
        party: PartyId,
        participant: ParticipantId,
        submissionId: String255,
    ): FutureUnlessShutdown[Unit] = {
      // start the notification in the background
      // note, that if this fails, we have an issue as ledger server will not have
      // received the event. this is generally an issue with everything we send to the
      // index server
      performUnlessClosingF(functionFullName)(
        FutureUtil.logOnFailure(
          sequentialQueue.execute(
            updateAndNotify(
              party,
              displayName = None,
              targetParticipantId = Some(participant),
              sequencerTimestamp,
              effectiveTimestamp,
              submissionId,
            ),
            s"notify ledger server about $party",
          ),
          s"Notifying ledger server about ${transaction} failed",
        )
      )
    }

    if (transaction.operation == TopologyChangeOp.Add) {
      transaction.transaction.element.mapping match {
        // TODO(rv): this will also pick mappings which are only one-sided. we should fix this by looking at the aggregated topology state once the metadata in the server is consolidated and allows us to match it to our metadata
        case PartyToParticipant(_, party, participant, permission) if permission.isActive =>
          dispatch(party, participant, transaction.transaction.element.id.toLengthLimitedString)
        // propagate admin parties
        case ParticipantState(_, _, participant, permission, _) if permission.isActive =>
          dispatch(participant.adminParty, participant, LengthLimitedString.getUuid.asString255)
        case _ => FutureUnlessShutdown.unit
      }
    } else {
      FutureUnlessShutdown.unit
    }
  }

  override protected def closeAsync(): Seq[AsyncOrSyncCloseable] = {
    import TraceContext.Implicits.Empty.*
    Seq(
      sequentialQueue.asCloseable(
        "ledger-server-party-notifier-queue",
        timeouts.shutdownShort.unwrap,
      )
    )
  }

}
