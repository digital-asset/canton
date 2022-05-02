// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.store
import cats.data.EitherT
import cats.syntax.bifunctor._
import cats.syntax.either._
import cats.syntax.functor._
import cats.syntax.option._
import cats.syntax.traverse._
import com.daml.nonempty.NonEmpty
import com.daml.nonempty.catsinstances._
import com.digitalasset.canton.SequencerCounter
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.sequencing.sequencer._
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{EitherTUtil, ErrorUtil}
import com.google.common.annotations.VisibleForTesting
import com.google.protobuf.ByteString

import java.util.UUID
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import java.util.concurrent.{ConcurrentHashMap, ConcurrentSkipListMap}
import scala.Ordering.Implicits._
import scala.collection.concurrent.TrieMap
import scala.collection.immutable.SortedSet
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._

/** Thrown when a record that must be unique is inserted with a non-unique key.
  * Mirrors the type of exceptions that we would expect to see from a database.
  */
class UniqueKeyViolationException(message: String) extends RuntimeException(message)

class InMemorySequencerStore(protected val loggerFactory: NamedLoggerFactory)(implicit
    protected val executionContext: ExecutionContext
) extends SequencerStore {
  private case class StoredPayload(instanceDiscriminator: UUID, content: ByteString)

  private val nextNewMemberId = new AtomicInteger()
  private val members = TrieMap[Member, RegisteredMember]()
  private val payloads = new ConcurrentSkipListMap[CantonTimestamp, StoredPayload]()
  private val events = new ConcurrentSkipListMap[CantonTimestamp, StoreEvent[PayloadId]]()
  private val watermark = new AtomicReference[Option[CantonTimestamp]](None)
  private val checkpoints =
    TrieMap[SequencerMemberId, ConcurrentSkipListMap[SequencerCounter, CantonTimestamp]]()
  // using a concurrent hash map for the thread safe computeIfPresent updates
  private val acknowledgements =
    new ConcurrentHashMap[SequencerMemberId, CantonTimestamp]()
  private val lowerBound = new AtomicReference[Option[CantonTimestamp]](None)
  private val disabledClientsRef = new AtomicReference[SequencerClients](SequencerClients())

  override def validateCommitMode(
      configuredCommitMode: CommitMode
  )(implicit traceContext: TraceContext): EitherT[Future, String, Unit] = {
    // we're running in-memory so we immediately committing to the "store" we have
    EitherTUtil.unit
  }

  override def registerMember(member: Member, timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[SequencerMemberId] =
    Future.successful {
      members
        .getOrElseUpdate(
          member,
          RegisteredMember(SequencerMemberId(nextNewMemberId.getAndIncrement()), timestamp),
        )
        .memberId
    }

  protected override def lookupMemberInternal(member: Member)(implicit
      traceContext: TraceContext
  ): Future[Option[RegisteredMember]] =
    Future.successful(
      members.get(member)
    )

  override def savePayloads(payloadsToInsert: NonEmpty[Seq[Payload]], instanceDiscriminator: UUID)(
      implicit traceContext: TraceContext
  ): EitherT[Future, SavePayloadsError, Unit] =
    payloadsToInsert.toNEF.traverse { case Payload(id, content) =>
      Option(payloads.putIfAbsent(id.unwrap, StoredPayload(instanceDiscriminator, content)))
        .flatMap { existingPayload =>
          // if we found an existing payload it must have a matching instance discriminator
          if (existingPayload.instanceDiscriminator == instanceDiscriminator) None // no error
          else
            SavePayloadsError.ConflictingPayloadId(id, existingPayload.instanceDiscriminator).some
        }
        .toLeft(())
        .leftWiden[SavePayloadsError]
        .toEitherT[Future]
    }.void

  override def saveEvents(instanceIndex: Int, eventsToInsert: NonEmpty[Seq[Sequenced[PayloadId]]])(
      implicit traceContext: TraceContext
  ): Future[Unit] =
    Future.successful(
      eventsToInsert.foreach { event =>
        Option(events.putIfAbsent(event.timestamp, event.event))
          .foreach(_ =>
            throw new UniqueKeyViolationException(
              s"Event timestamp is not unique [${event.timestamp}]"
            )
          )
      }
    )

  override def saveWatermark(instanceIndex: Int, ts: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): EitherT[Future, SaveWatermarkError, Unit] =
    EitherT.pure[Future, SaveWatermarkError] {
      watermark.set(Some(ts))
    }

  override def fetchWatermark(instanceIndex: Int)(implicit
      traceContext: TraceContext
  ): Future[Option[Watermark]] =
    Future.successful(watermark.get.map(Watermark(_, online = true)))

  override def goOffline(instanceIndex: Int)(implicit traceContext: TraceContext): Future[Unit] =
    Future.unit

  override def goOnline(instanceIndex: Int, now: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[CantonTimestamp] =
    Future.successful {
      // we're the only sequencer that can write the watermark so just take the provided value
      watermark.set(now.some)
      now
    }

  override def readEvents(
      member: SequencerMemberId,
      fromTimestampO: Option[CantonTimestamp] = None,
      limit: Int = 100,
  )(implicit traceContext: TraceContext): Future[Seq[Sequenced[Payload]]] = Future.successful {
    import scala.jdk.CollectionConverters._

    def isMemberRecipient(member: SequencerMemberId)(event: StoreEvent[_]): Boolean = event match {
      case DeliverStoreEvent(sender, messageId, recipients, payload, signingTimestampO, _trace) =>
        recipients.contains(
          member
        ) // only if they're a recipient (sender should already be a recipient)
      case DeliverErrorStoreEvent(sender, _messageId, _message, _trace) =>
        sender == member // only if we're the sender
    }

    def lookupPayloadForDeliver(event: Sequenced[PayloadId]): Sequenced[Payload] =
      event.map {
        case deliver: DeliverStoreEvent[PayloadId] =>
          val payloadId = deliver.payload
          val storedPayload =
            Option(payloads.get(payloadId.unwrap))
              .getOrElse(sys.error(s"payload not found for id [$payloadId]"))

          deliver.copy(payload = Payload(payloadId, storedPayload.content))
        case error: DeliverErrorStoreEvent => error
      }

    // if there's no watermark, we can't return any events
    watermark.get().fold(Seq.empty[Sequenced[Payload]]) { watermark =>
      fromTimestampO
        .fold(events.tailMap(CantonTimestamp.MinValue, true))(events.tailMap(_, false))
        .entrySet()
        .iterator()
        .asScala
        .takeWhile(e => e.getKey <= watermark)
        .filter(e => isMemberRecipient(member)(e.getValue))
        .take(limit)
        .map(entry => Sequenced(entry.getKey, entry.getValue))
        .map(lookupPayloadForDeliver)
        .toList
    }
  }

  /** No implementation as only required for crash recovery */
  override def deleteEventsPastWatermark(instanceIndex: Int)(implicit
      traceContext: TraceContext
  ): Future[Unit] =
    Future.unit

  override def saveCounterCheckpoint(
      memberId: SequencerMemberId,
      counter: SequencerCounter,
      ts: CantonTimestamp,
  )(implicit traceContext: TraceContext): EitherT[Future, SaveCounterCheckpointError, Unit] = {
    val memberCheckpoints =
      checkpoints.getOrElseUpdate(
        memberId,
        new ConcurrentSkipListMap[SequencerCounter, CantonTimestamp](),
      )
    val existingTimestampO = Option(memberCheckpoints.putIfAbsent(counter, ts))

    existingTimestampO
      .filter(_ != ts) // ignore if the timestamp doesn't match
      .map[SaveCounterCheckpointError](SaveCounterCheckpointError.CounterCheckpointInconsistent)
      .toLeft(())
      .toEitherT[Future]
  }

  override def fetchClosestCheckpointBefore(memberId: SequencerMemberId, counter: SequencerCounter)(
      implicit traceContext: TraceContext
  ): Future[Option[CounterCheckpoint]] =
    Future.successful {
      checkpoints
        .get(memberId)
        .flatMap { memberCheckpoints =>
          Option(memberCheckpoints.headMap(counter, false).lastEntry())
        }
        .map(entry => CounterCheckpoint(entry.getKey, entry.getValue))
    }

  override def acknowledge(
      member: SequencerMemberId,
      timestamp: CantonTimestamp,
  )(implicit traceContext: TraceContext): Future[Unit] = Future.successful {
    // update the acknowledges with this timestamp only if greater than a existing value
    val _ = acknowledgements
      .compute(
        member,
        (_, existingOrNull) => Option(existingOrNull).map(_ max timestamp).getOrElse(timestamp),
      )
  }

  override def latestAcknowledgements()(implicit
      traceContext: TraceContext
  ): Future[Map[SequencerMemberId, CantonTimestamp]] =
    Future.successful(acknowledgements.asScala.toMap)

  override def fetchLowerBound()(implicit
      traceContext: TraceContext
  ): Future[Option[CantonTimestamp]] =
    Future.successful(lowerBound.get())

  override def saveLowerBound(
      ts: CantonTimestamp
  )(implicit traceContext: TraceContext): EitherT[Future, SaveLowerBoundError, Unit] = {
    val newValueO = lowerBound.updateAndGet { existingO =>
      existingO.map(_ max ts).getOrElse(ts).some
    }

    newValueO match {
      case Some(updatedValue) =>
        EitherT.cond[Future](
          updatedValue == ts,
          (),
          SaveLowerBoundError.BoundLowerThanExisting(updatedValue, ts),
        )
      case None => // shouldn't happen
        ErrorUtil.internalError(new IllegalStateException("Lower bound should have been updated"))
    }
  }

  override protected[store] def pruneEvents(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[Int] =
    Future.successful(prune(events, timestamp))

  override protected[store] def prunePayloads(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[Int] =
    Future.successful(prune(payloads, timestamp))

  private def prune[A](
      timeOrderedSkipMap: ConcurrentSkipListMap[CantonTimestamp, A],
      pruningTimestamp: CantonTimestamp,
  ): Int = {
    val removed = new AtomicInteger()

    // .headMap returns everything before and not including the timestamp.
    // as the pruning timestamp should have been calculated based on the safe watermarks, and we ensure no records earlier
    // than these are inserted we can safely do this iteration to remove.
    timeOrderedSkipMap.headMap(pruningTimestamp) forEach { case (timestamp, _) =>
      removed.incrementAndGet()
      val _ = timeOrderedSkipMap.remove(timestamp)
    }

    removed.get()
  }

  @SuppressWarnings(Array("org.wartremover.warts.Var", "org.wartremover.warts.While"))
  override protected[store] def pruneCheckpoints(
      timestamp: CantonTimestamp
  )(implicit traceContext: TraceContext): Future[Int] = Future.successful {
    val removedCheckpointsCounter = new AtomicInteger()
    checkpoints.foreach { case (_member, checkpoints) =>
      var completed = false

      val entriesIterator = checkpoints.entrySet().iterator()

      while (!completed && entriesIterator.hasNext) {
        val checkpointTimestamp = entriesIterator.next().getValue

        completed = checkpointTimestamp >= timestamp

        if (!completed) {
          removedCheckpointsCounter.incrementAndGet()
          entriesIterator.remove()
        }
      }
    }

    removedCheckpointsCounter.get()
  }

  override protected[store] def adjustPruningTimestampForCounterCheckpoints(
      timestamp: CantonTimestamp,
      disabledMembers: Seq[SequencerMemberId],
  )(implicit traceContext: TraceContext): Future[Option[CantonTimestamp]] =
    Future.successful {
      // find the timestamp of the checkpoint for or immediately preceding the desired pruning timestamp
      // returns None if there is no appropriate checkpoint
      def checkpointTimestamp(memberId: SequencerMemberId): Option[CantonTimestamp] = {
        checkpoints.get(memberId).flatMap { memberCheckpoints =>
          // we assume that timestamps are increasing monotonically with the counter value, so we look in descending order
          memberCheckpoints
            .descendingMap()
            .asScala
            .find { case (_counter, checkpointTimestamp) =>
              checkpointTimestamp < timestamp
            }
            .map(_._2)
        }
      }

      val timestamps = members.values.filterNot(m => disabledMembers.contains(m.memberId)).map {
        case RegisteredMember(memberId, registeredFrom) =>
          // if the member doesn't have a counter checkpoint to use we'll have to use their registered timestamp
          // which is effectively a counter checkpoint for where to start
          checkpointTimestamp(memberId).getOrElse(registeredFrom)
      }

      // if there are some timestamps then take the minimum
      // (the only reason there wouldn't be any timestamps would be no registered members or every member being ignored)
      NonEmpty.from(timestamps.toSeq).map(_.reduceLeft(_ min _))
    }

  override def status(
      now: CantonTimestamp
  )(implicit traceContext: TraceContext): Future[SequencerPruningStatus] =
    Future.successful {
      val disabledClients = disabledClientsRef.get()

      SequencerPruningStatus(
        lowerBound = lowerBound.get().getOrElse(CantonTimestamp.Epoch),
        now = now,
        members = members.map { case (member, RegisteredMember(memberId, registeredFrom)) =>
          SequencerMemberStatus(
            member,
            registeredFrom,
            lastAcknowledged = acknowledgements.asScala.get(memberId),
            enabled = !disabledClients.members.contains(member),
          )
        }.toSeq,
      )
    }

  /** This store does not support multiple concurrent instances so will do nothing. */
  override def markLaggingSequencersOffline(cutoffTime: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[Unit] = Future.unit

  /** Members must be registered to receive a memberId, so can typically assume they exist in this structure */
  private def lookupExpectedMember(memberId: SequencerMemberId): Member =
    members
      .collectFirst { case (member, RegisteredMember(`memberId`, _)) =>
        member
      }
      .getOrElse(sys.error(s"Member id [$memberId] is not registered"))

  override def disableMember(
      memberId: SequencerMemberId
  )(implicit traceContext: TraceContext): Future[Unit] =
    Future.successful {
      val _ = disabledClientsRef.updateAndGet { disabledClients =>
        disabledClients.copy(members = disabledClients.members + lookupExpectedMember(memberId))
      }
    }

  override def isEnabled(memberId: SequencerMemberId)(implicit
      traceContext: TraceContext
  ): EitherT[Future, MemberDisabledError.type, Unit] = {
    val member = lookupExpectedMember(memberId)
    EitherT
      .cond[Future](
        !disabledClientsRef.get().members.contains(member),
        (),
        MemberDisabledError,
      )
  }

  /** There can be no other sequencers sharing this storage */
  override def fetchOnlineInstances(implicit traceContext: TraceContext): Future[SortedSet[Int]] =
    Future.successful(SortedSet.empty)

  @VisibleForTesting
  override protected[store] def countRecords(implicit
      traceContext: TraceContext
  ): Future[SequencerStoreRecordCounts] =
    Future.successful(
      SequencerStoreRecordCounts(
        events.size().toLong,
        payloads.size.toLong,
        checkpoints.values.map(_.size()).sum.toLong,
      )
    )

  override def close(): Unit = ()
}
