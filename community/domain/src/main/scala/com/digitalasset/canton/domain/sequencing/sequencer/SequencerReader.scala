// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer

import akka.stream._
import akka.stream.scaladsl.{Flow, Keep, Sink}
import cats.data.EitherT
import cats.syntax.bifunctor._
import cats.syntax.option._
import cats.syntax.traverse._
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.{SyncCryptoApi, SyncCryptoClient}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.sequencing.sequencer.errors.CreateSubscriptionError
import com.digitalasset.canton.domain.sequencing.sequencer.store._
import com.digitalasset.canton.lifecycle.FlagCloseable
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.sequencing.OrdinarySerializedEvent
import com.digitalasset.canton.sequencing.protocol._
import com.digitalasset.canton.store.SequencedEventStore.OrdinarySequencedEvent
import com.digitalasset.canton.store.db.DbDeserializationException
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.topology.{DomainId, Member}
import com.digitalasset.canton.tracing.Traced.withTraceContext
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.ErrorUtil
import com.digitalasset.canton.util.ShowUtil._
import com.digitalasset.canton.{SequencerCounter, checked}

import scala.concurrent.{ExecutionContext, Future}

/** We throw this if a [[store.SaveCounterCheckpointError.CounterCheckpointInconsistent]] error is returned when saving a new member
  * counter checkpoint. This is exceptionally concerning as may suggest that we are streaming events with inconsistent counters.
  * Should only be caused by a bug or the datastore being corrupted.
  */
class CounterCheckpointInconsistentException(message: String) extends RuntimeException(message)

/** Configuration for the database based sequence reader.
  * @param readBatchSize max number of events to fetch from the datastore in one page
  * @param checkpointInterval how frequently to checkpoint state
  * @param pollingInterval how frequently to poll for new events from the database.
  *                        only used if high availability has been configured,
  *                        otherwise will rely on local writes performed by this sequencer to indicate that new events are available.
  */
case class SequencerReaderConfig(
    readBatchSize: Int = 100,
    checkpointInterval: NonNegativeFiniteDuration = NonNegativeFiniteDuration.ofSeconds(5),
    pollingInterval: NonNegativeFiniteDuration = NonNegativeFiniteDuration.ofMillis(100),
)

class SequencerReader(
    config: SequencerReaderConfig,
    domainId: DomainId,
    store: SequencerStore,
    syncCryptoApi: SyncCryptoClient,
    eventSignaller: EventSignaller,
    override protected val timeouts: ProcessingTimeout,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends NamedLogging
    with FlagCloseable {

  /** State to keep track of when serving a read subscription */
  case class ReadState(
      member: Member,
      memberId: SequencerMemberId,
      requestedCounter: SequencerCounter,
      nextReadTimestamp: CantonTimestamp,
      lastBatchWasFull: Boolean = false,
      nextCounterAccumulator: SequencerCounter = 0L,
  ) extends PrettyPrinting {

    /** Update the state after reading a new page of results */
    def update(storedEvents: Seq[Sequenced[_]], batchSize: Int): ReadState =
      copy(
        // increment the counter by the number of events we've now processed
        nextCounterAccumulator = nextCounterAccumulator + storedEvents.size,
        // set the timestamp to the last record of the batch, or our current timestamp if we got no results
        nextReadTimestamp = storedEvents.lastOption.map(_.timestamp).getOrElse(nextReadTimestamp),
        // did we receive a full batch of events on this update
        lastBatchWasFull = storedEvents.sizeCompare(batchSize) == 0,
      )

    /** Apply a previously recorded counter checkpoint so that we don't have to start from 0 on every subscription */
    def startFromCheckpoint(checkpoint: CounterCheckpoint): ReadState = {
      // with this checkpoint we'll start reading from this timestamp and as reads are not inclusive we'll receive the next event after this checkpoint first
      copy(
        nextCounterAccumulator = checkpoint.counter + 1,
        nextReadTimestamp = checkpoint.timestamp,
      )
    }

    override def pretty: Pretty[ReadState] = prettyOfClass(
      param("member", _.member),
      param("memberId", _.memberId),
      param("requestedCounter", _.requestedCounter),
      param("nextReadTimestamp", _.nextReadTimestamp),
      param("lastBatchWasFull", _.lastBatchWasFull),
      param("nextCounterAccumulator", _.nextCounterAccumulator),
    )
  }

  object ReadState {
    def initial(member: Member, requestedCounter: SequencerCounter)(
        registeredMember: RegisteredMember
    ): ReadState =
      ReadState(
        member,
        registeredMember.memberId,
        requestedCounter,
        registeredMember.registeredFrom,
      )
  }

  def read(member: Member, offset: SequencerCounter)(implicit
      traceContext: TraceContext
  ): EitherT[Future, CreateSubscriptionError, Sequencer.EventSource] = {
    def readEventsSource(
        registeredMember: RegisteredMember,
        initialReadState: ReadState,
    ): Sequencer.EventSource = {
      val eventsSource = eventSignaller
        .readSignalsForMember(member, registeredMember.memberId)
        .via(
          FetchLatestEventsFlow[OrdinarySerializedEvent, ReadState](
            initialReadState,
            state => fetchEventsBatch(state)(traceContext),
            (state, _events) => !state.lastBatchWasFull,
          )
        )

      val recordCheckpointSink = Flow[OrdinarySerializedEvent]
        .map(_.signedEvent.content)
        .map(CounterCheckpoint(_))
        .buffer(1, OverflowStrategy.dropTail) // we only really need one event and can drop others
        .throttle(1, config.checkpointInterval.toScala)
        .drop(1) // it's pointless writing the first checkpoint as this was likely the starting point we've just read
        .mapAsync(1) { checkpoint =>
          performUnlessClosingF {
            saveCounterCheckpoint(member, registeredMember.memberId, checkpoint)
          }.onShutdown {
            logger.info("Skip saving the counter checkpoint due to shutdown")
          }
        }
        .to(Sink.ignore)

      eventsSource
        .viaMat(KillSwitches.single)(Keep.right)
        .wireTap(recordCheckpointSink) // setup a separate sink to periodically record checkpoints
    }

    performUnlessClosingEitherT(CreateSubscriptionError.ShutdownError: CreateSubscriptionError) {
      for {
        registeredMember <- EitherT
          .fromOptionF(store.lookupMember(member), CreateSubscriptionError.UnknownMember(member))
          .leftWiden[CreateSubscriptionError]
        // check they haven't been disabled
        _ <- store
          .isEnabled(registeredMember.memberId)
          .leftMap[CreateSubscriptionError] { case MemberDisabledError =>
            CreateSubscriptionError.MemberDisabled(member)
          }
        initialReadState <- EitherT.right(
          startFromClosestCounterCheckpoint(ReadState.initial(member, offset)(registeredMember))
        )
        // validate we are in the bounds of the data that this sequencer can serve
        lowerBoundO <- EitherT.right(store.fetchLowerBound())
        _ <- EitherT
          .cond[Future](
            lowerBoundO.forall(_ <= initialReadState.nextReadTimestamp),
            (), {
              val lowerBoundText = lowerBoundO.map(_.toString).getOrElse("epoch")
              val errorMessage =
                show"Subscription for $member@$offset would require reading data from ${initialReadState.nextReadTimestamp} but our lower bound is ${lowerBoundText.unquoted}."

              logger.error(errorMessage)
              CreateSubscriptionError.EventsUnavailable(offset, errorMessage)
            },
          )
          .leftWiden[CreateSubscriptionError]
      } yield readEventsSource(registeredMember, initialReadState)
    }
  }

  /** Attempt to save the counter checkpoint and fail horribly if we find this is an inconsistent checkpoint update. */
  private def saveCounterCheckpoint(
      member: Member,
      memberId: SequencerMemberId,
      checkpoint: CounterCheckpoint,
  )(implicit traceContext: TraceContext): Future[Unit] = {
    logger.trace(s"Saving counter checkpoint for [$member] with value [$checkpoint]")
    store.saveCounterCheckpoint(memberId, checkpoint.counter, checkpoint.timestamp).valueOr {
      case SaveCounterCheckpointError.CounterCheckpointInconsistent(existingTimestamp) =>
        val message =
          s"""|There is an existing checkpoint for member [$member] ($memberId) at counter ${checkpoint.counter} with timestamp $existingTimestamp. 
                |We attempted to write ${checkpoint.timestamp}.""".stripMargin
        ErrorUtil.internalError(new CounterCheckpointInconsistentException(message))
    }
  }

  /** Update the read state to start from the closest counter checkpoint if available */
  private def startFromClosestCounterCheckpoint(
      readState: ReadState
  )(implicit traceContext: TraceContext): Future[ReadState] =
    for {
      closestCheckpoint <- store.fetchClosestCheckpointBefore(
        readState.memberId,
        readState.requestedCounter,
      )
      _ = {
        val startText = closestCheckpoint.fold("the beginning")(_.toString)
        logger.debug(
          s"Subscription for ${readState.member} at ${readState.requestedCounter} will start from $startText"
        )
      }
    } yield closestCheckpoint.fold(readState)(readState.startFromCheckpoint)

  private def fetchEventsBatch(
      readState: ReadState
  )(implicit traceContext: TraceContext): Future[(ReadState, Seq[OrdinarySerializedEvent])] = {
    logger.debug(s"Reading events from $readState...")
    for {
      storedEvents <- store.readEvents(
        readState.memberId,
        readState.nextReadTimestamp.some,
        config.readBatchSize,
      )
      eventsWithCounter = storedEvents.zipWithIndex.map { case (event, n) =>
        (readState.nextCounterAccumulator + n, event)
      }
      // we may be rebuilding counters from a checkpoint before what was actually requested
      // in which case don't sign or return events that we don't need to serve
      requestedEvents = eventsWithCounter.filter(_._1 >= readState.requestedCounter)
      signedSerializedEvents <- requestedEvents.traverse { case (counter, event) =>
        signAndSerializeEvent(readState.member, readState.memberId, counter, event)
      }
    } yield {
      val newReadState = readState.update(storedEvents, config.readBatchSize)
      logger.debug(s"New state is $newReadState.")
      (newReadState, signedSerializedEvents)
    }
  }

  private def signAndSerializeEvent(
      member: Member,
      memberId: SequencerMemberId,
      counter: SequencerCounter,
      storedEvent: Sequenced[Payload],
  ): Future[OrdinarySerializedEvent] =
    withTraceContext(storedEvent) { implicit traceContext => storedEvent =>
      {
        val (event, signingTimestampO) =
          mkSequencedEvent(member, memberId, counter, storedEvent.timestamp, storedEvent.event)
        val timestamp = signingTimestampO.getOrElse(storedEvent.timestamp)
        val syncCrypto = SequencerReader.getSnapshotForSequencerSigning(syncCryptoApi, timestamp)
        for {
          signedEvent <- SignedContent.tryCreate(
            syncCrypto.pureCrypto,
            syncCrypto,
            event.value,
            signingTimestampO,
          )
        } yield OrdinarySequencedEvent(signedEvent)(traceContext)
      }
    }

  /** Takes our stored event and turns it back into a real sequenced event.
    * Also returns the optional signing timestamp to use if set.
    */
  private def mkSequencedEvent(
      member: Member,
      memberId: SequencerMemberId,
      counter: SequencerCounter,
      timestamp: CantonTimestamp,
      event: StoreEvent[Payload],
  ): (Traced[SequencedEvent[ClosedEnvelope]], Option[CantonTimestamp]) =
    withTraceContext(event) { implicit traceContext =>
      {
        case DeliverStoreEvent(sender, messageId, _recipients, payload, signingTimestampO, _tc) =>
          val messageIdO =
            Option(messageId).filter(_ => memberId == sender) // message id only goes to sender
          val batch: Batch[ClosedEnvelope] = Batch
            .fromByteString(ClosedEnvelope.fromProtoV0, payload.content)
            .fold(err => throw new DbDeserializationException(err.toString), identity)
          val filteredBatch = Batch.filterClosedEnvelopesFor(batch, member)

          (
            Traced(
              Deliver
                .create[ClosedEnvelope](counter, timestamp, domainId, messageIdO, filteredBatch)
            ),
            signingTimestampO,
          )
        case DeliverErrorStoreEvent(_, messageId, message, _tc) =>
          (
            Traced(
              DeliverError
                .create(
                  counter,
                  timestamp,
                  domainId,
                  messageId,
                  reason = DeliverErrorReason.BatchRefused(message.unwrap),
                )
            ),
            None,
          )
      }
    }
}

object SequencerReader {
  def getSnapshotForSequencerSigning(cryptoApi: SyncCryptoClient, timestamp: CantonTimestamp)(
      implicit traceContext: TraceContext
  ): SyncCryptoApi = {
    // TODO(i4639) improve how we deal with sequencer keys
    // below code works due to the following convention: we always use the "newest" key for signing.
    // if we roll sequencer keys, then we add a new key first and then revoke the old one in a subsequent
    // transaction. therefore, for some time, the sequencer will have two active keys and suddenly
    // start to use the new key. while the system is racing to update the topology state, the sequencer
    // will still sign with the old key, which is still valid.
    checked(
      cryptoApi.trySnapshot(CantonTimestamp.min(cryptoApi.topologyKnownUntilTimestamp, timestamp))
    )
  }
}
