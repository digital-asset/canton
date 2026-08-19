// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.ledger.api

import com.digitalasset.canton.ledger.participant.state.Update.{
  AcsChangeSequencedUpdate,
  EmptyAcsPublicationRequired,
  LsuTimeReached,
  OnPRReassignmentAccepted,
}
import com.digitalasset.canton.ledger.participant.state.{
  AcsChangeFactory,
  IndexingWatermark,
  SynchronizerIndex,
  Update,
}
import com.digitalasset.canton.lifecycle.CloseContext
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.event.{AcsChangeListener, RecordTime}
import com.digitalasset.canton.participant.sync.ConnectedSynchronizersLookupContainer
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.TraceContext

class AcsChangePublicationPostProcessor(
    connectedSynchronizersLookupContainer: ConnectedSynchronizersLookupContainer,
    override val loggerFactory: NamedLoggerFactory,
)(implicit closeContext: CloseContext)
    extends NamedLogging
    with (Update => Unit) {

  def apply(update: Update): Unit = {
    def publishAcsChange(
        synchronizerId: SynchronizerId,
        synchronizerIndex: SynchronizerIndex,
        acsChangeFactoryO: Option[AcsChangeFactory],
        onprIndexingWatermark: Option[IndexingWatermark],
    ): Unit =
      acsChangeListenerFor(synchronizerId).foreach { listener =>
        val recordTime =
          onprIndexingWatermark.fold(RecordTime.fromSynchronizerIndex(synchronizerIndex))(wm =>
            RecordTime(synchronizerIndex.recordTime, wm.acsCommitmentTiebreaker.unwrap.toLong)
          )
        // The trace context is deliberately generated here instead of continuing the one for the Update
        // to unlink the asynchronous acs commitment processing from message processing trace.
        implicit val traceContext: TraceContext = TraceContext.createNew("publish_acs_change")
        listener.publish(recordTime, acsChangeFactoryO)
      }

    update match {
      case updateWithAcsChangeFactory: OnPRReassignmentAccepted =>
        publishAcsChange(
          updateWithAcsChangeFactory.synchronizerId,
          updateWithAcsChangeFactory.synchronizerIndex,
          Some(updateWithAcsChangeFactory.acsChangeFactory),
          Some(updateWithAcsChangeFactory.watermark),
        )

      case updateWithAcsChangeFactory: AcsChangeSequencedUpdate =>
        publishAcsChange(
          updateWithAcsChangeFactory.synchronizerId,
          updateWithAcsChangeFactory.synchronizerIndex,
          Some(updateWithAcsChangeFactory.acsChangeFactory),
          onprIndexingWatermark = None,
        )

      case emptyAcsPublicationRequired: EmptyAcsPublicationRequired =>
        publishAcsChange(
          emptyAcsPublicationRequired.synchronizerId,
          emptyAcsPublicationRequired.synchronizerIndex,
          acsChangeFactoryO = None,
          onprIndexingWatermark = None,
        )

      case upgradeTimeReached: LsuTimeReached =>
        acsChangeListenerFor(upgradeTimeReached.synchronizerId).foreach { listener =>
          // The trace context is deliberately generated here instead of continuing the one for the Update
          // to unlink the asynchronous acs commitment processing from message processing trace.
          implicit val traceContext: TraceContext = TraceContext.createNew("publish_upgrade_time")
          listener.publishForUpgradeTime(upgradeTimeReached.synchronizerIndex.recordTime)
        }

      // not publishing otherwise
      case _ => ()
    }
  }

  private def acsChangeListenerFor(synchronizerId: SynchronizerId): Option[AcsChangeListener] =
    connectedSynchronizersLookupContainer.get(synchronizerId).map(_.acsChangeListener)
}
