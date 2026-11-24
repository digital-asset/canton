// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.testtool.infrastructure

import com.daml.ledger.api.v2.topology_transaction.TopologyEvent.Event
import com.daml.ledger.api.v2.update_service.GetUpdatesResponse
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.util.DelayUtil
import io.grpc.Context
import io.grpc.stub.StreamObserver

import java.util.concurrent.{CancellationException, TimeoutException}
import scala.collection.concurrent.TrieMap
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future, Promise}

/** Waits until an observing participant has indexed that each of `parties` is hosted by
  * `hostingParticipantId` on `expectedSynchronizers` synchronizers.
  *
  * Instead of polling, it subscribes to the participant authorization topology events of the
  * parties and tracks the synchronizers on which the hosting participant is authorized. Topology
  * events are published at the effective time of the topology change, so an observed event implies
  * that the observing participant knows about the party and its hosting on that synchronizer.
  *
  * The subscription must start at an offset before the parties were allocated, otherwise the
  * relevant events are missed and the wait times out.
  */
private[testtool] object AwaitPartiesHosted {

  /** Matches the budget of the two `Eventually` loops this wait replaced. */
  val DefaultTimeout: FiniteDuration = Eventually.DefaultMaxDeadline * 2

  def apply(
      subscribe: StreamObserver[GetUpdatesResponse] => Unit,
      parties: Set[String],
      hostingParticipantId: String,
      expectedSynchronizers: Int,
      timeout: FiniteDuration,
      description: String,
  )(implicit ec: ExecutionContext): Future[Unit] =
    if (parties.isEmpty) Future.unit
    else {
      val observer =
        new Observer(parties, hostingParticipantId, expectedSynchronizers, description, timeout)
      val context = Context.ROOT.withCancellation()
      context.run(() => subscribe(observer))
      DelayUtil.delay(timeout).foreach(_ => observer.timedOut())
      observer.result.transform { result =>
        context.cancel(new CancellationException(s"$description finished")).discard
        result
      }
    }

  private final class Observer(
      parties: Set[String],
      hostingParticipantId: String,
      expectedSynchronizers: Int,
      description: String,
      timeout: FiniteDuration,
  ) extends StreamObserver[GetUpdatesResponse] {

    private val promise = Promise[Unit]()
    private val hostingSynchronizers: TrieMap[String, Set[String]] =
      TrieMap.from(parties.map(_ -> Set.empty[String]))

    def result: Future[Unit] = promise.future

    override def onNext(response: GetUpdatesResponse): Unit =
      response.update.topologyTransaction.foreach { transaction =>
        val synchronizerId = transaction.synchronizerId
        transaction.events.foreach(_.event match {
          case Event.ParticipantAuthorizationAdded(added) =>
            track(added.partyId, added.participantId)(_ + synchronizerId)
          case Event.ParticipantAuthorizationChanged(changed) =>
            track(changed.partyId, changed.participantId)(_ + synchronizerId)
          case Event.ParticipantAuthorizationRevoked(revoked) =>
            track(revoked.partyId, revoked.participantId)(_ - synchronizerId)
          case Event.ParticipantAuthorizationOnboarding(_) | Event.Empty => ()
        })
        if (
          hostingSynchronizers.readOnlySnapshot().values.forall(_.sizeIs >= expectedSynchronizers)
        )
          promise.trySuccess(()).discard
      }

    override def onError(t: Throwable): Unit =
      promise.tryFailure(new IllegalStateException(s"$description: stream failed", t)).discard

    override def onCompleted(): Unit =
      promise
        .tryFailure(
          new IllegalStateException(s"$description: stream completed unexpectedly; ${progress()}")
        )
        .discard

    def timedOut(): Unit =
      promise
        .tryFailure(new TimeoutException(s"$description timed out after $timeout; ${progress()}"))
        .discard

    private def track(party: String, participantId: String)(
        update: Set[String] => Set[String]
    ): Unit =
      if (participantId == hostingParticipantId)
        hostingSynchronizers.updateWith(party)(_.map(update)).discard

    private def progress(): String = {
      val missing = hostingSynchronizers.readOnlySnapshot().collect {
        case (party, synchronizers) if synchronizers.sizeIs < expectedSynchronizers =>
          s"$party (${synchronizers.size})"
      }
      s"expected $expectedSynchronizers synchronizers hosting on $hostingParticipantId, still waiting for ${missing
          .mkString(", ")}"
    }
  }
}
