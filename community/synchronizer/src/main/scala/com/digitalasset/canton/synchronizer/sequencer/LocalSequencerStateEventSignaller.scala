// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer

import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.{
  AsyncOrSyncCloseable,
  FlagCloseableAsync,
  SyncCloseable,
  UnlessShutdown,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.synchronizer.sequencer.store.SequencerMemberId
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.TryUtil.*
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.*
import org.apache.pekko.stream.scaladsl.Source

import java.util.concurrent.ConcurrentHashMap
import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.concurrent.Future
import scala.jdk.CollectionConverters.*
import scala.util.{Failure, Success, Try}

/** If all Sequencer writes are occurring locally we pipe write notifications to read subscriptions
  * allowing the [[SequencerReader]] to immediately read from the backing store rather than polling.
  *
  * An important caveat is that we only supply signals when a write for a member occurs. If there
  * are no writes from starting the process the member will never receive a read signal. The
  * [[SequencerReader]] is responsible for performing at least one initial read from the store to
  * ensure that all prior events are served as required.
  *
  * Not suitable or at least very sub-optimal for a horizontally scaled sequencer setup where a
  * reader will not have visibility of all writes locally.
  */
class LocalSequencerStateEventSignaller(
    protected val timeouts: ProcessingTimeout,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit materializer: Materializer)
    extends EventSignaller
    with FlagCloseableAsync
    with NamedLogging {

  override def isLegacySignaller: Boolean = false

  private val memberQueues =
    new TrieMap[
      SequencerMemberId,
      // support multiple subscriptions for members. This shouldn't be the normal case, but during token expiry or crash recovery
      // a member could subscribe again, while the old queue might still be active.
      // The actual set behind this mutable.Set interface is threadsafe.
      // The TraceContext carried along is from the readSignalsForMember call, to correlate later errors with the initial request.
      mutable.Set[Traced[BoundedSourceQueue[ReadSignal]]],
    ]()

  override def notifyOfLocalWrite(
      notification: WriteNotification
  ): Future[Unit] = {
    val membersToNotify =
      if (notification.isBroadcast) memberQueues.iterator
      else
        notification.memberIds.iterator.flatMap(member => memberQueues.get(member).map(member -> _))

    membersToNotify.foreach { case (member, queues) =>
      queues.foreach { tracedQueue =>
        tracedQueue.value.offer(ReadSignal) match {
          case QueueOfferResult.Enqueued =>
          // happy case
          case QueueOfferResult.Dropped =>
          // nothing to do, the queue is just full and therefore a notification is already buffered
          case QueueOfferResult.QueueClosed =>
            // the queue was closed, so let's remove the entry
            queues.remove(tracedQueue).discard
          case QueueOfferResult.Failure(ex) =>
            logger.info(
              s"Unable to queue signal for member $member, because the queue failed with an error.",
              ex,
            )(tracedQueue.traceContext)
            queues.remove(tracedQueue).discard
        }
      }
      if (queues.isEmpty) {
        // if a queue was added between the if above and the updateWith call,
        // then the member doesn't get removed and we don't "lose" queue entries.
        memberQueues.updateWith(member)(_.filter(queues => queues.nonEmpty)).discard
      }
    }
    Future.unit
  }

  override def readSignalsForMember(
      member: Member,
      memberId: SequencerMemberId,
  )(implicit traceContext: TraceContext): Source[ReadSignal, NotUsed] = {
    logger.info(s"Creating signal source for $member")
    val (queue, source) = Source.queue[ReadSignal](1).preMaterialize()
    val registeredUS = synchronizeWithClosingSync(s"readSignalsForMember($member)")(
      memberQueues
        .updateWith(memberId)(
          // create a concurrent set based on ConcurrentHashMap, but use the scala api for it
          _.orElse(
            Some(ConcurrentHashMap.newKeySet[Traced[BoundedSourceQueue[ReadSignal]]]().asScala)
          )
            .map(_.addOne(Traced(queue)(traceContext)))
        )
        .discard
    )

    UnlessShutdown.failOnShutdownToAbortException(
      Success(registeredUS),
      s"readSignalsForMember($member)",
    ) match {
      case Success(_) =>
        // if registration was successful, just return the Source the corresponds to the queue
        source
      case Failure(shutdown) =>
        // registration happened during shutdown: fail the queue and return a failed Source
        logger.info(s"Could not register queue for member=$member due to an ongoing shutdown.")
        queue.fail(shutdown)
        Source.failed(shutdown)
    }
  }

  protected override def closeAsync(): Seq[AsyncOrSyncCloseable] = {
    val closables = memberQueues.flatMap { case (member, queues) =>
      queues
        .filter(!_.value.isCompleted)
        .map(q =>
          SyncCloseable(
            s"queues.complete(memberId=$member)",
            Try(q.value.complete()).forFailed(ex =>
              logger.info(s"Error while completing the queue for seqMemberId=$member", ex)(
                q.traceContext
              )
            ),
          )
        )
    }.toSeq
    memberQueues.clear()
    closables

  }
}
