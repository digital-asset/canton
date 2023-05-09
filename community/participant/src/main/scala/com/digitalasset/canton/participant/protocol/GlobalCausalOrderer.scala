// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol

import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.{
  FlagCloseable,
  FutureUnlessShutdown,
  Lifecycle,
  PromiseUnlessShutdown,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.protocol.SingleDomainCausalTracker.EventClock
import com.digitalasset.canton.participant.store.MultiDomainCausalityStore
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.messages.{CausalityMessage, VectorClock}
import com.digitalasset.canton.topology.{DomainId, ParticipantId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{FutureUtil, SimpleExecutionQueueWithShutdown}
import com.digitalasset.canton.{DiscardOps, LfPartyId}
import com.google.common.annotations.VisibleForTesting

import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

class GlobalCausalOrderer(
    val participantId: ParticipantId,
    connectedDomains: DomainId => Boolean,
    protected val timeouts: ProcessingTimeout,
    val domainCausalityStore: MultiDomainCausalityStore,
    futureSupervisor: FutureSupervisor,
    val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends NamedLogging
    with FlagCloseable {

  // Stores events that are waiting for their causal dependencies to get published
  // Is not accessed concurrently
  private val pendingEvents: mutable.Map[DomainId, List[PendingEvent]] = new TrieMap()

  private case class PendingEvent(clock: EventClock)(
      val stillWaiting: mutable.Map[DomainId, CantonTimestamp],
      val promise: PromiseUnlessShutdown[Unit],
  )

  private val exQueue: SimpleExecutionQueueWithShutdown = new SimpleExecutionQueueWithShutdown(
    "global-causal-orderer-queue",
    futureSupervisor,
    timeouts,
    loggerFactory,
  )

  /** @return A future that completes when all previously spawned futures have finished
    */
  @VisibleForTesting
  def flush(): Future[Unit] = exQueue.flush()

  def waitPublishable(clock: EventClock)(implicit tc: TraceContext): FutureUnlessShutdown[Unit] = {

    val mutableDependencies: mutable.Map[DomainId, CantonTimestamp] =
      mutable.Map(clock.waitOn.toList.map(e => e._1 -> e._2): _*)

    val pending = PendingEvent(clock)(
      mutableDependencies,
      new PromiseUnlessShutdown[Unit](s"pending-promise-$tc", futureSupervisor),
    )

    lazy val msg =
      s"Waiting for causal dependencies of event with clock $clock, waits on ${clock.waitOn} [tc : $tc]"
    logger.debug(msg)

    FutureUtil.doNotAwait(
      exQueue
        .execute(
          Future {
            val finished = waitingOn(mutableDependencies)

            if (!finished) {
              mutableDependencies.foreach { case (id, timestamp) =>
                val previous = pendingEvents.getOrElse(id, List.empty)
                pendingEvents.put(id, pending :: previous).discard[Option[List[PendingEvent]]]
              }
              ()
            } else {
              pending.promise.outcome(())
            }
          },
          msg,
        )
        .onShutdown {
          logger.debug(s"Aborted due to shutdown: $msg")
          pending.promise.shutdown()
        },
      msg,
    )

    pending.promise.futureUS
  }

  def registerPublished(clock: EventClock)(implicit tc: TraceContext): Unit = {
    lazy val msg =
      s"Register that event with $clock has been published, and release causal dependencies"
    FutureUtil.doNotAwait(
      exQueue
        .execute(
          {
            Future {
              val eventLogId = clock.domainId
              val ts = clock.localTs

              domainCausalityStore.registerSeen(eventLogId, ts)

              val waiting = pendingEvents.getOrElse(eventLogId, List.empty)

              if (waiting.nonEmpty) {
                val (release, stay) = waiting.partition { p =>
                  waitingOn(p.stillWaiting)
                }

                release.foreach(p => p.promise.outcome(()))

                if (stay.isEmpty) {
                  pendingEvents.remove(eventLogId)
                } else {
                  pendingEvents.put(eventLogId, stay)
                }

              }
            }
          },
          msg,
        )
        .onShutdown {
          logger.debug(s"Aborted due to shutdown: $msg")
        },
      msg,
    )
  }

  def awaitTransferOutRegistered(id: TransferId, parties: Set[LfPartyId])(implicit
      tc: TraceContext
  ): Future[Map[LfPartyId, VectorClock]] = {
    domainCausalityStore.awaitTransferOutRegistered(id, parties)
  }

  def registerCausalityMessages(
      causalityMessages: List[CausalityMessage]
  )(implicit tc: TraceContext): Future[Unit] = {
    domainCausalityStore.registerCausalityMessages(causalityMessages)
  }

  // Get the remaining timestamps that an event is waiting on. These timestamps have not yet been observed in their
  // corresponding event logs.
  private def waitingOn(clk: mutable.Map[DomainId, CantonTimestamp]): Boolean = {
    clk.filterInPlace { case (id, timestamp) =>
      val seen = domainCausalityStore.highestSeenOn(id)
      val finished = seen.exists(t => !t.isBefore(timestamp))
      val connected = connectedDomains(id)
      // TODO(i6180): Revisit this code with respect to changing domain topologies
      !finished && connected
    }
    clk.isEmpty
  }

  override protected def onClosed(): Unit = {
    Lifecycle.close(exQueue)(logger)

    // Complete all the pending events promises with shutdown
    pendingEvents.values.foreach(_.foreach(_.promise.shutdown()))
  }

}
