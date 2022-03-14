// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer

import akka.NotUsed
import akka.stream._
import akka.stream.scaladsl.{Flow, GraphDSL, Keep, Merge, Source}
import cats.data.{EitherT, NonEmptyList, OptionT, Validated, ValidatedNel}
import cats.syntax.either._
import cats.syntax.list._
import cats.syntax.option._
import cats.syntax.traverse._
import com.digitalasset.canton.config.RequireTypes.String256M
import com.digitalasset.canton.crypto.DomainSyncCryptoClient
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.sequencing.sequencer.store._
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging, TracedLogger}
import com.digitalasset.canton.sequencing.protocol.{SendAsyncError, SubmissionRequest}
import com.digitalasset.canton.time.{Clock, NonNegativeFiniteDuration}
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.BatchTracing.withNelTracedBatch
import com.digitalasset.canton.tracing.{HasTraceContext, TraceContext, Traced}
import com.digitalasset.canton.util.EitherTUtil
import com.digitalasset.canton.version.ProtocolVersion
import com.google.common.annotations.VisibleForTesting

import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.{ExecutionContext, Future}

/** A write we want to make to the db */
sealed trait Write
object Write {
  case class Event(event: Presequenced[StoreEvent[PayloadId]]) extends Write
  case object KeepAlive extends Write
}

/** A write that we've assigned a timestamp to.
  * We drag these over the same clock so we can ensure earlier items have lower timestamps and later items have higher timestamps.
  * This is very helpful, essential you may say, for correctly setting the watermark while ensuring an event with
  * an earlier timestamp will not be written.
  */
sealed trait SequencedWrite extends HasTraceContext {
  def timestamp: CantonTimestamp
}

object SequencedWrite {
  case class Event(event: Sequenced[PayloadId]) extends SequencedWrite {
    override lazy val timestamp: CantonTimestamp = event.timestamp
    override def traceContext: TraceContext = event.traceContext
  }
  case class KeepAlive(override val timestamp: CantonTimestamp) extends SequencedWrite {
    override def traceContext: TraceContext = TraceContext.empty
  }
}

case class BatchWritten(notifies: WriteNotification, latestTimestamp: CantonTimestamp)
object BatchWritten {

  /** Assumes events are ordered by timestamp */
  def apply(events: NonEmptyList[Sequenced[_]]): BatchWritten =
    BatchWritten(
      notifies = WriteNotification(events),
      latestTimestamp = events.last.timestamp,
    )
}

/** Base class for exceptions intentionally thrown during Akka stream to flag errors */
sealed abstract class SequencerWriterException(message: String) extends RuntimeException(message)

/** Throw as an error in the akka stream when we discover that our currently running sequencer writer has been
  * marked as offline.
  */
class SequencerOfflineException(instanceIndex: Int)
    extends SequencerWriterException(
      s"This sequencer (instance:$instanceIndex) has been marked as offline"
    )

/** We intentionally use an unsafe storage method for writing payloads to take advantage of a full connection pool
  * for performance. However this means if a HA Sequencer Writer has lost its instance lock it may still attempt to
  * write payloads while another Sequencer Writer is active with the same instance index. As we use this instance
  * index to generate an (almost) conflict free payload id, in this circumstance there is a slim chance that we
  * may attempt to write conflicting payloads with the same id. If we were using a simple idempotent write approach
  * this could result in the active sequencer writing an event with a payload from the offline writer process (and
  * not the payload it is expecting). This would be a terrible and difficult to diagnose corruption issue.
  *
  * If this exception is raised we currently just halt the writer and run crash recovery. This is slightly suboptimal
  * as in the above scenario we may crash the active writer (if they were second to write a conflicting payload id).
  * However this will be safe. We could optimise this by checking the active lock status and only halting
  * if this is found to be false.
  */
class ConflictingPayloadIdException(payloadId: PayloadId, conflictingInstanceDiscriminator: UUID)
    extends SequencerWriterException(
      s"We attempted to write a payload with an id that already exists [$payloadId] written by instance $conflictingInstanceDiscriminator"
    )

/** A payload that we should have just stored now seems to be missing. */
class PayloadMissingException(payloadId: PayloadId)
    extends SequencerWriterException(s"Payload missing after storing [$payloadId]")

class SequencerWriterQueues private[sequencer] (
    eventGenerator: SendEventGenerator,
    protected val loggerFactory: NamedLoggerFactory,
)(
    @VisibleForTesting
    private[sequencer] val deliverEventQueue: BoundedSourceQueue[Presequenced[StoreEvent[Payload]]],
    keepAliveKillSwitch: UniqueKillSwitch,
)(implicit executionContext: ExecutionContext)
    extends NamedLogging {
  private val closing = new AtomicBoolean(false)

  def send(
      submission: SubmissionRequest
  )(implicit traceContext: TraceContext): EitherT[Future, SendAsyncError, Unit] =
    for {
      event <- eventGenerator.generate(submission)
      enqueueResult = deliverEventQueue.offer(event)
      _ <- EitherT.fromEither[Future](enqueueResult match {
        case QueueOfferResult.Enqueued => Right(())
        case QueueOfferResult.Dropped =>
          Left(SendAsyncError.Overloaded("Sequencer event buffer is full"): SendAsyncError)
        case QueueOfferResult.QueueClosed => Left(SendAsyncError.ShuttingDown())
        case other =>
          logger.warn(s"Unexpected result from payload queue offer: $other")
          Right(())
      })
    } yield ()

  def complete(): Unit = {
    implicit val tc: TraceContext = TraceContext.empty
    // the queue completions throw IllegalStateExceptions if you call close more than once
    // so guard to ensure they're only called once
    if (closing.compareAndSet(false, true)) {
      logger.debug(s"Shutting down keep-alive kill switch")
      keepAliveKillSwitch.shutdown()
      logger.debug(s"Completing deliver event queue")
      deliverEventQueue.complete()
    }
  }
}

/** Akka stream for writing as a Sequencer */
object SequencerWriterSource {
  def apply(
      writerConfig: SequencerWriterConfig,
      totalNodeCount: Int,
      keepAliveInterval: Option[NonNegativeFiniteDuration],
      cryptoApi: DomainSyncCryptoClient,
      store: SequencerWriterStore,
      clock: Clock,
      eventSignaller: EventSignaller,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): Source[Traced[BatchWritten], SequencerWriterQueues] = {
    val logger = TracedLogger(SequencerWriterSource.getClass, loggerFactory)

    val eventTimestampGenerator =
      new PartitionedTimestampGenerator(clock, store.instanceIndex, totalNodeCount)
    val payloadIdGenerator =
      new PartitionedTimestampGenerator(clock, store.instanceIndex, totalNodeCount)
    // when running an HA sequencer we typically rely on the lock based [[resource.DbStorageMulti]] to ensure that
    // there are no other writers sharing the same instance index concurrently writing. however for performance reasons
    // we in places forgo this and use a non-lock protected based "unsafe" methods. We then separately use a unique
    // instance discriminator to check that our writes are conflict free (currently used solely for payloads).
    val instanceDiscriminator = UUID.randomUUID()

    // log this instance discriminator so in the exceptionally unlikely event that we actually hit conflicts we have
    // an available approach for determining which instances were conflicting
    logger.debug(
      s"Starting sequencer writer stream with index ${store.instanceIndex} of $totalNodeCount and instance discriminator [$instanceDiscriminator]"
    )

    val eventGenerator =
      new SendEventGenerator(store, () => PayloadId(payloadIdGenerator.generateNext))

    // Take deliver events with full payloads and first write them before adding them to the events queue
    val deliverEventSource = Source
      .queue[Presequenced[StoreEvent[Payload]]](writerConfig.payloadQueueSize)
      .via(WritePayloadsFlow(writerConfig, store, instanceDiscriminator, loggerFactory))
      .map(Write.Event)

    // push keep alive writes at the specified interval, or never if not set
    val keepAliveSource = keepAliveInterval
      .fold(Source.never[Write.KeepAlive.type]) { frequency =>
        Source.repeat(Write.KeepAlive).throttle(1, frequency.toScala)
      }
      .viaMat(KillSwitches.single)(Keep.right)

    val mkMaterialized = new SequencerWriterQueues(eventGenerator, loggerFactory)(_, _)

    // merge the sources of deliver events and keep-alive writes
    val mergedEventsSource =
      Source.fromGraph(
        GraphDSL.createGraph(deliverEventSource, keepAliveSource)(mkMaterialized) {
          implicit builder => (deliverEventSourceS, keepAliveSourceS) =>
            import GraphDSL.Implicits._

            val merge = builder.add(Merge[Write](inputPorts = 2))

            deliverEventSourceS ~> merge.in(0)
            keepAliveSourceS ~> merge.in(1)

            SourceShape(merge.out)
        }
      )

    mergedEventsSource
      .via(
        SequenceWritesFlow(
          writerConfig,
          store,
          eventTimestampGenerator,
          cryptoApi,
          loggerFactory,
        )
      )
      .via(UpdateWatermarkFlow(store))
      .via(NotifyEventSignallerFlow(eventSignaller))
  }
}

class SendEventGenerator(store: SequencerWriterStore, payloadIdGenerator: () => PayloadId)(implicit
    executionContext: ExecutionContext
) {
  def generate(submission: SubmissionRequest)(implicit
      traceContext: TraceContext
  ): EitherT[Future, SendAsyncError, Presequenced[StoreEvent[Payload]]] = {
    def lookupSender: EitherT[Future, SendAsyncError, SequencerMemberId] = EitherT(
      store
        .lookupMember(submission.sender)
        .map(
          _.map(_.memberId)
            .toRight(
              SendAsyncError
                .SenderUnknown(s"sender [${submission.sender}] is unknown"): SendAsyncError
            )
        )
    )

    def validateRecipient(member: Member): Future[Validated[Member, SequencerMemberId]] =
      for {
        registeredMember <- store.lookupMember(member)
        memberIdO = registeredMember.map(_.memberId)
      } yield memberIdO.toRight(member).toValidated

    def validateRecipients: Future[ValidatedNel[Member, Set[SequencerMemberId]]] =
      for {
        validatedList <- submission.batch.allRecipients.toList
          .traverse(validateRecipient)
        validated = validatedList.traverse(_.toValidatedNel)
      } yield validated.map(_.toSet)

    def validateAndGenerateEvent(senderId: SequencerMemberId): Future[StoreEvent[Payload]] = {
      def deliverError(unknownRecipients: NonEmptyList[Member]): DeliverErrorStoreEvent = {
        val message = String256M.tryCreate(
          s"Unknown recipients: ${unknownRecipients.toList.take(1000).mkString(", ")}"
        )
        DeliverErrorStoreEvent(senderId, submission.messageId, message, traceContext)
      }

      def deliver(recipientIds: Set[SequencerMemberId]): StoreEvent[Payload] = {
        val payload =
          Payload(
            payloadIdGenerator(),
            submission.batch.toByteString(ProtocolVersion.v2_0_0_Todo_i8793),
          )
        DeliverStoreEvent.ensureSenderReceivesEvent(
          senderId,
          submission.messageId,
          recipientIds,
          payload,
          submission.timestampOfSigningKey,
        )
      }

      for {
        validatedRecipients <- validateRecipients
      } yield validatedRecipients.fold(deliverError, deliver)
    }

    for {
      senderId <- lookupSender
      event <- EitherT.right(validateAndGenerateEvent(senderId))
    } yield Presequenced.withMaxSequencingTime(event, submission.maxSequencingTime)
  }
}

object SequenceWritesFlow {
  def apply(
      writerConfig: SequencerWriterConfig,
      store: SequencerWriterStore,
      eventTimestampGenerator: PartitionedTimestampGenerator,
      cryptoApi: DomainSyncCryptoClient,
      loggerFactory: NamedLoggerFactory,
  )(implicit executionContext: ExecutionContext): Flow[Write, Traced[BatchWritten], NotUsed] = {
    val logger = TracedLogger(WritePayloadsFlow.getClass, loggerFactory)

    def sequenceWritesAndStoreEvents(writes: Seq[Write]): Future[Traced[Option[BatchWritten]]] =
      writes
        .traverse(sequenceWrite)
        .flatMap {
          _.toList.toNel // due to the groupedWithin we should likely always have items
            .fold(Future.successful[Traced[Option[BatchWritten]]](Traced.empty(None))) { writes =>
              withNelTracedBatch(logger, writes) { implicit traceContext => writes =>
                {
                  val events = writes.collect { case SequencedWrite.Event(event) => event }.toNel
                  val notifies =
                    events.fold[WriteNotification](WriteNotification.None)(WriteNotification(_))
                  for {
                    // if this write batch had any events then save them
                    _ <- events.fold(Future.unit)(store.saveEvents)
                  } yield Traced(BatchWritten(notifies, writes.last.timestamp).some)
                }
              }
            }
        }

    def sequenceWrite(write: Write): Future[SequencedWrite] = {
      val timestamp = eventTimestampGenerator.generateNext

      write match {
        case Write.KeepAlive => Future.successful(SequencedWrite.KeepAlive(timestamp))
        // if we opt not to write the event as we're past the max-sequencing-time, just replace with a keep alive as we're still alive
        case Write.Event(event) =>
          sequenceEvent(timestamp, event)(TraceContext.empty)
            .map(SequencedWrite.Event)
            .getOrElse[SequencedWrite](SequencedWrite.KeepAlive(timestamp))
      }
    }

    /* Performs checks and validations that require knowing the sequencing timestamp of the event.
     * May transform the event into an error (if the requested signing timestamp is out of bounds).
     * May drop the event entirely if the max sequencing time has been exceeded.
     */
    def sequenceEvent(
        timestamp: CantonTimestamp,
        presequencedEvent: Presequenced[StoreEvent[PayloadId]],
    )(implicit traceContext: TraceContext): OptionT[Future, Sequenced[PayloadId]] = {
      def checkMaxSequencingTime(
          event: Presequenced[StoreEvent[PayloadId]]
      ): Either[String, Presequenced[StoreEvent[PayloadId]]] =
        event.maxSequencingTimeO
          .toLeft(event)
          .leftFlatMap { maxSequencingTime =>
            Either.cond(
              timestamp <= maxSequencingTime,
              event,
              // TODO(Error handling) Revisit whether this should be logged at WARN
              s"The sequencer time [$timestamp] has exceeded the max-sequencing-time of the send request [$maxSequencingTime]: ${event.event.description}",
            )
          }

      def checkSigningTimestamp(
          event: Presequenced[StoreEvent[PayloadId]]
      ): Future[Presequenced[StoreEvent[PayloadId]]] =
        event.traverse {
          // we only do this validation for deliver events
          case deliver @ DeliverStoreEvent(sender, messageId, _, _, signingTimestampO, _) =>
            EitherT
              .fromEither[Future](signingTimestampO.toLeft(()))
              .leftFlatMap { signingTimestamp =>
                /*
                  If nothing happened for a while, then `timestamp` can be arbitrarily
                  further from `topologyKnownUntilTimestamp`. However, in that case,
                  it means that no change was recently made in the domain parameters,
                  so it safe to take the min.
                 */
                val t = CantonTimestamp.min(timestamp, cryptoApi.topologyKnownUntilTimestamp)

                EitherT(
                  cryptoApi.ips
                    .awaitSnapshot(t)
                    .flatMap(_.findDynamicDomainParametersOrDefault())
                    .map { domainParameters =>
                      Sequencer
                        .validateSigningTimestamp(domainParameters.sequencerSigningTolerance)(
                          timestamp,
                          signingTimestamp,
                        )
                    }
                )
              }
              .fold[StoreEvent[PayloadId]](
                DeliverErrorStoreEvent(sender, messageId, _, event.traceContext),
                _ => deliver,
              )

          case other => Future.successful(other)
        }

      def checkPayloadToEventBound(
          presequencedEvent: Presequenced[StoreEvent[PayloadId]]
      ): Either[String, Presequenced[StoreEvent[PayloadId]]] =
        presequencedEvent match {
          // we only need to check deliver events for payloads
          case presequencedDeliver @ Presequenced(deliver: DeliverStoreEvent[PayloadId], _) =>
            val payloadTs = deliver.payload.unwrap
            val bound = writerConfig.payloadToEventBound.unwrap
            val maxAllowableEventTime = payloadTs.add(bound)

            Either
              .cond(
                timestamp <= maxAllowableEventTime,
                presequencedDeliver,
                s"The payload to event time bound [$bound] has been been exceeded by payload time [$payloadTs] and sequenced event time [$timestamp]",
              )
          case other =>
            Right(other)
        }

      val resultE = for {
        event <- checkPayloadToEventBound(presequencedEvent)
        event <- checkMaxSequencingTime(event)
      } yield event

      resultE match {
        case Left(error) =>
          logger.warn(error)(presequencedEvent.traceContext)
          OptionT.none
        case Right(event) =>
          OptionT(checkSigningTimestamp(event).map { event =>
            Option(Sequenced(timestamp, event.event))
          })
      }
    }

    Flow[Write]
      .groupedWithin(
        writerConfig.eventWriteBatchMaxSize,
        writerConfig.eventWriteBatchMaxDuration.toScala,
      )
      .mapAsync(1)(sequenceWritesAndStoreEvents(_))
      .collect { case tew @ Traced(Some(ew)) => tew.map(_ => ew) }
      .named("sequenceAndWriteEvents")
  }
}

/** Extract the payloads of events and write them in batches to the payloads table.
  * As order does not matter at this point allow writing batches concurrently up to
  * the concurrency specified by [[SequencerWriterConfig.payloadWriteMaxConcurrency]].
  * Pass on the events with the payloads dropped and replaced by their payload ids.
  */
object WritePayloadsFlow {
  def apply(
      writerConfig: SequencerWriterConfig,
      store: SequencerWriterStore,
      instanceDiscriminator: UUID,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      executionContext: ExecutionContext
  ): Flow[Presequenced[StoreEvent[Payload]], Presequenced[StoreEvent[PayloadId]], NotUsed] = {
    val logger = TracedLogger(WritePayloadsFlow.getClass, loggerFactory)

    def writePayloads(
        events: Seq[Presequenced[StoreEvent[Payload]]]
    ): Future[List[Presequenced[StoreEvent[PayloadId]]]] =
      NonEmptyList
        .fromList(events.toList)
        .fold(Future.successful(List.empty[Presequenced[StoreEvent[PayloadId]]])) { events =>
          withNelTracedBatch(logger, events) { implicit traceContext => events =>
            // extract the payloads themselves for storing
            val payloads = events.toList.map(_.event).flatMap(extractPayload(_).toList)

            // strip out the payloads and replace with their id as the content itself is not needed downstream
            val eventsWithPayloadId = events.map(_.map(e => dropPayloadContent(e))).toList
            logger.debug(s"Writing ${payloads.size} payloads from batch of ${events.size}")

            // save the payloads if there are any
            EitherTUtil.toFuture {
              payloads.toNel
                .traverse(store.savePayloads(_, instanceDiscriminator))
                .leftMap {
                  case SavePayloadsError.ConflictingPayloadId(id, conflictingInstance) =>
                    new ConflictingPayloadIdException(id, conflictingInstance)
                  case SavePayloadsError.PayloadMissing(id) => new PayloadMissingException(id)
                }
                .map(_ => eventsWithPayloadId)
            }
          }
        }

    def extractPayload(event: StoreEvent[Payload]): Option[Payload] = event match {
      case DeliverStoreEvent(_, _, _, payload, _, _) => payload.some
      case _other => None
    }

    def dropPayloadContent(event: StoreEvent[Payload]): StoreEvent[PayloadId] = event match {
      case deliver: DeliverStoreEvent[Payload] => deliver.mapPayload(_.id)
      case error: DeliverErrorStoreEvent => error
    }

    Flow[Presequenced[StoreEvent[Payload]]]
      .groupedWithin(
        writerConfig.payloadWriteBatchMaxSize,
        writerConfig.payloadWriteBatchMaxDuration.toScala,
      )
      .mapAsyncUnordered(writerConfig.payloadWriteMaxConcurrency)(writePayloads(_))
      .mapConcat(identity)
      .named("writePayloads")
  }

}

object UpdateWatermarkFlow {
  def apply(store: SequencerWriterStore)(implicit
      executionContext: ExecutionContext
  ): Flow[Traced[BatchWritten], Traced[BatchWritten], NotUsed] =
    Flow[Traced[BatchWritten]]
      .mapAsync(1)(_.withTraceContext { implicit traceContext => written =>
        for {
          _ <- store.saveWatermark(written.latestTimestamp).value.map {
            case Left(SaveWatermarkError.WatermarkFlaggedOffline) =>
              // intentionally throwing exception that will bubble up through the akka stream and handled by the
              // recovery process in SequencerWriter
              throw new SequencerOfflineException(store.instanceIndex)
            case _ => ()
          }
        } yield Traced(written)
      })
      .named("updateWatermark")
}

object NotifyEventSignallerFlow {
  def apply(eventSignaller: EventSignaller)(implicit
      executionContext: ExecutionContext
  ): Flow[Traced[BatchWritten], Traced[BatchWritten], NotUsed] =
    Flow[Traced[BatchWritten]]
      .mapAsync(1)(_.withTraceContext { implicit traceContext => batchWritten =>
        eventSignaller.notifyOfLocalWrite(batchWritten.notifies) map { _ =>
          Traced(batchWritten)
        }
      })
}
