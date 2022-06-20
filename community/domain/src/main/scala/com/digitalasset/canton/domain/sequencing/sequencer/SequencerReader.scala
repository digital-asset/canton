// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer

import akka.NotUsed
import akka.stream._
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import cats.data.EitherT
import cats.syntax.bifunctor._
import cats.syntax.option._
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.{SyncCryptoApi, SyncCryptoClient}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.sequencing.sequencer.SequencerReader.ReadState
import com.digitalasset.canton.domain.sequencing.sequencer.errors.CreateSubscriptionError
import com.digitalasset.canton.domain.sequencing.sequencer.store._
import com.digitalasset.canton.lifecycle.FlagCloseable
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.sequencing.OrdinarySerializedEvent
import com.digitalasset.canton.sequencing.protocol._
import com.digitalasset.canton.store.SequencedEventStore.OrdinarySequencedEvent
import com.digitalasset.canton.store.db.DbDeserializationException
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.topology.{DomainId, Member}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ShowUtil._
import com.digitalasset.canton.util.{AkkaUtil, ErrorUtil}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{GenesisSequencerCounter, SequencerCounter, checked}
import com.google.common.annotations.VisibleForTesting
import io.functionmeta.functionFullName

import scala.concurrent.{ExecutionContext, Future}

/** We throw this if a [[store.SaveCounterCheckpointError.CounterCheckpointInconsistent]] error is returned when saving a new member
  * counter checkpoint. This is exceptionally concerning as may suggest that we are streaming events with inconsistent counters.
  * Should only be caused by a bug or the datastore being corrupted.
  */
class CounterCheckpointInconsistentException(message: String) extends RuntimeException(message)

/** Configuration for the database based sequence reader. */
trait SequencerReaderConfig {

  /** max number of events to fetch from the datastore in one page */
  def readBatchSize: Int

  /** how frequently to checkpoint state */
  def checkpointInterval: NonNegativeFiniteDuration
}

object SequencerReaderConfig {
  val defaultReadBatchSize: Int = 100
  val defaultCheckpointInterval: NonNegativeFiniteDuration = NonNegativeFiniteDuration.ofSeconds(5)
}

class SequencerReader(
    config: SequencerReaderConfig,
    domainId: DomainId,
    store: SequencerStore,
    syncCryptoApi: SyncCryptoClient[SyncCryptoApi],
    eventSignaller: EventSignaller,
    topologyClientMember: Member,
    protocolVersion: ProtocolVersion,
    override protected val timeouts: ProcessingTimeout,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends NamedLogging
    with FlagCloseable {

  def read(member: Member, offset: SequencerCounter)(implicit
      traceContext: TraceContext
  ): EitherT[Future, CreateSubscriptionError, Sequencer.EventSource] = {

    performUnlessClosingEitherT(
      functionFullName,
      CreateSubscriptionError.ShutdownError: CreateSubscriptionError,
    ) {
      for {
        registeredTopologyClientMember <- EitherT
          .fromOptionF(
            store.lookupMember(topologyClientMember),
            CreateSubscriptionError.UnknownMember(topologyClientMember),
          )
          .leftWiden[CreateSubscriptionError]
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
          startFromClosestCounterCheckpoint(ReadState.initial(member)(registeredMember), offset)
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
      } yield {
        val loggerFactoryForMember = loggerFactory.append("subscriber", member.toString)
        val reader = new EventsReader(
          member,
          registeredMember,
          registeredTopologyClientMember.memberId,
          loggerFactoryForMember,
        )
        reader.from(offset, initialReadState)
      }
    }
  }

  private[SequencerReader] class EventsReader(
      member: Member,
      registeredMember: RegisteredMember,
      topologyClientMemberId: SequencerMemberId,
      override protected val loggerFactory: NamedLoggerFactory,
  ) extends NamedLogging {

    import SequencerReader._

    private def unvalidatedEventsSourceFromCheckpoint(initialReadState: ReadState)(implicit
        traceContext: TraceContext
    ): Source[(SequencerCounter, Sequenced[Payload]), NotUsed] = {
      eventSignaller
        .readSignalsForMember(member, registeredMember.memberId)
        .via(
          FetchLatestEventsFlow[(SequencerCounter, Sequenced[Payload]), ReadState](
            initialReadState,
            state => fetchUnvalidatedEventsBatchFromCheckpoint(state)(traceContext),
            (state, _events) => !state.lastBatchWasFull,
          )
        )
    }

    private def validateSigningTimestamp(
        counter: SequencerCounter,
        signingTimestamp: CantonTimestamp,
        sequencingTimestamp: CantonTimestamp,
        latestTopologyClientTimestamp: Option[CantonTimestamp],
    )(implicit traceContext: TraceContext): Future[Option[SyncCryptoApi]] = {
      // The SequencerWriter makes sure that the signing timestamp is at most the sequencing timestamp
      ErrorUtil.requireArgument(
        signingTimestamp <= sequencingTimestamp,
        s"The signing timestamp $signingTimestamp must be before or at the sequencing timestamp $sequencingTimestamp for sequencer counter $counter of member $member",
      )
      for {
        snapshot <- SyncCryptoClient.getSnapshotForTimestamp(
          syncCryptoApi,
          signingTimestamp,
          latestTopologyClientTimestamp,
          protocolVersion,
          // This warning should only trigger on unauthenticated members,
          // but batches addressed to unauthenticated members must not specify a signing key timestamp.
          warnIfApproximate = true,
        )
        dynamicDomainParametersO <- snapshot.ipsSnapshot.findDynamicDomainParameters()
      } yield {
        val withinSigningTolerance = dynamicDomainParametersO.exists { dynamicDomainParameters =>
          import scala.Ordered.orderingToOrdered
          dynamicDomainParameters.sequencerSigningTolerance.unwrap >= sequencingTimestamp - signingTimestamp
        }
        if (withinSigningTolerance) Some(snapshot) else None
      }
    }

    private def recordCheckpointSink(implicit
        traceContext: TraceContext
    ): Sink[UnsignedEventData, NotUsed] = Flow[UnsignedEventData]
      .map {
        case UnsignedEventData(
              event,
              _signingTimestampAndSnapshotO,
              previousTopologyClientTimestamp,
              latestTopologyClientTimestamp,
              eventTraceContext,
            ) =>
          logger.debug(s"Preparing counter checkpoint for $member at ${event.timestamp}")
          CounterCheckpoint(event, latestTopologyClientTimestamp)
      }
      .buffer(1, OverflowStrategy.dropTail) // we only really need one event and can drop others
      .throttle(1, config.checkpointInterval.toScala)
      .mapAsync(1) { checkpoint =>
        performUnlessClosingF(functionFullName) {
          saveCounterCheckpoint(member, registeredMember.memberId, checkpoint)
        }.onShutdown {
          logger.info("Skip saving the counter checkpoint due to shutdown")
        }
      }
      .to(Sink.ignore)

    private def signValidatedEvent(
        unsignedEventData: UnsignedEventData
    )(implicit traceContext: TraceContext): Future[OrdinarySerializedEvent] = {
      val UnsignedEventData(
        event,
        signingTimestampAndSnapshotO,
        previousTopologyClientTimestamp,
        latestTopologyClientTimestamp,
        eventTraceContext,
      ) = unsignedEventData
      logger.trace(
        s"Latest topology client timestamp for $member at counter ${event.counter} / ${event.timestamp} is $previousTopologyClientTimestamp / $latestTopologyClientTimestamp"
      )(eventTraceContext)

      val signingTimestampOAndSnapshotF = signingTimestampAndSnapshotO match {
        case Some((signingTimestamp, signingSnaphot)) =>
          Future.successful(Some(signingTimestamp) -> signingSnaphot)
        case None =>
          val warnIfApproximate =
            (event.counter > GenesisSequencerCounter) && member.isAuthenticated
          SyncCryptoClient
            .getSnapshotForTimestamp(
              syncCryptoApi,
              event.timestamp,
              previousTopologyClientTimestamp,
              protocolVersion,
              warnIfApproximate = warnIfApproximate,
            )(implicitly, ErrorLoggingContext.fromTracedLogger(logger)(eventTraceContext))
            .map(None -> _)
      }
      signingTimestampOAndSnapshotF.flatMap { case (signingTimestampO, signingSnapshot) =>
        logger.debug(
          s"Signing event with counter ${event.counter} / timestamp ${event.timestamp} for $member"
        )
        signEvent(event, signingTimestampO, signingSnapshot)(eventTraceContext)
      }
    }

    def latestTopologyClientTimestampAfter(
        topologyClientTimestampBefore: Option[CantonTimestamp],
        event: Sequenced[Payload],
    ): Option[CantonTimestamp] = {
      val addressedToTopologyClient = event.event.members.contains(topologyClientMemberId)
      if (addressedToTopologyClient) Some(event.timestamp)
      else topologyClientTimestampBefore
    }

    def validateEvent(
        topologyClientTimestampBefore: Option[CantonTimestamp],
        sequenced: (SequencerCounter, Sequenced[Payload]),
    ): Future[(Option[CantonTimestamp], UnsignedEventData)] = {
      val (counter, unvalidatedEvent) = sequenced

      def validationSuccess(
          event: SequencedEvent[ClosedEnvelope],
          signingInfo: Option[(CantonTimestamp, SyncCryptoApi)],
      ): Future[(Option[CantonTimestamp], UnsignedEventData)] = {
        val topologyClientTimestampAfter =
          latestTopologyClientTimestampAfter(topologyClientTimestampBefore, unvalidatedEvent)
        Future.successful(
          topologyClientTimestampAfter ->
            UnsignedEventData(
              event,
              signingInfo,
              topologyClientTimestampBefore,
              topologyClientTimestampAfter,
              unvalidatedEvent.traceContext,
            )
        )
      }

      val sequencingTimestamp = unvalidatedEvent.timestamp
      unvalidatedEvent.event match {
        case DeliverStoreEvent(
              sender,
              messageId,
              _members,
              _payload,
              Some(signingTimestamp),
              eventTraceContext,
            ) =>
          validateSigningTimestamp(
            counter,
            signingTimestamp,
            sequencingTimestamp,
            topologyClientTimestampBefore,
          )(eventTraceContext).flatMap {
            case Some(snapshot) =>
              val event =
                mkSequencedEvent(member, registeredMember.memberId, counter, unvalidatedEvent)
              validationSuccess(event, Some(signingTimestamp -> snapshot))

            case None =>
              // The signing timestamp is too old for the sequencing time.
              // Replace the event with an error that is only sent to the sender
              // To not introduce gaps in the sequencer counters,
              // we deliver an empty batch to the member if it is not the sender.
              // This way, we can avoid revalidating the skipped events after the checkpoint we resubscribe from.
              val event = if (registeredMember.memberId == sender) {
                // TODO(#5990) use an error code
                val reason = SequencerReader.invalidSigningTimestampError(
                  signingTimestamp,
                  sequencingTimestamp,
                )
                DeliverError.create(
                  counter,
                  sequencingTimestamp,
                  domainId,
                  messageId,
                  reason,
                  protocolVersion,
                )
              } else
                Deliver.create(
                  counter,
                  sequencingTimestamp,
                  domainId,
                  None,
                  Batch.empty[ClosedEnvelope](protocolVersion),
                  protocolVersion,
                )
              Future.successful(
                // This event cannot change the topology state of the client
                // and might not reach the topology client even
                // if it was originally addressed to it.
                // So keep the before timestamp
                topologyClientTimestampBefore ->
                  UnsignedEventData(
                    event,
                    None,
                    topologyClientTimestampBefore,
                    topologyClientTimestampBefore,
                    unvalidatedEvent.traceContext,
                  )
              )
          }

        case _ =>
          val event =
            mkSequencedEvent(member, registeredMember.memberId, counter, unvalidatedEvent)
          validationSuccess(event, None)
      }
    }

    def from(startAt: SequencerCounter, initialReadState: ReadState)(implicit
        traceContext: TraceContext
    ): Sequencer.EventSource = {
      val unvalidatedEventsSrc = unvalidatedEventsSourceFromCheckpoint(initialReadState)
      val validatedEventSrc = AkkaUtil.statefulMapAsync(
        unvalidatedEventsSrc,
        initialReadState.latestTopologyClientRecipientTimestamp,
      )(validateEvent)
      val eventsSource = validatedEventSrc.dropWhile(_.event.counter < startAt)
      eventsSource
        .viaMat(KillSwitches.single)(Keep.right)
        .wireTap(recordCheckpointSink) // setup a separate sink to periodically record checkpoints
        .mapAsync(
          // We technically do not need to process everything sequentially here.
          // Neither do we have evidence that parallel processing helps, as a single sequencer reader
          // will typically serve many subscriptions in parallel.
          parallelism = 1
        )(signValidatedEvent)
    }

    /** Attempt to save the counter checkpoint and fail horribly if we find this is an inconsistent checkpoint update. */
    private def saveCounterCheckpoint(
        member: Member,
        memberId: SequencerMemberId,
        checkpoint: CounterCheckpoint,
    )(implicit traceContext: TraceContext): Future[Unit] = {
      logger.debug(s"Saving counter checkpoint for [$member] with value [$checkpoint]")
      store.saveCounterCheckpoint(memberId, checkpoint).valueOr {
        case SaveCounterCheckpointError.CounterCheckpointInconsistent(
              existingTimestamp,
              existingLatestTopologyClientTimestampg,
            ) =>
          val message =
            s"""|There is an existing checkpoint for member [$member] ($memberId) at counter ${checkpoint.counter} with timestamp $existingTimestamp and latest topology client timestamp $existingLatestTopologyClientTimestampg. 
                |We attempted to write ${checkpoint.timestamp} and ${checkpoint.latestTopologyClientTimestamp}.""".stripMargin
          ErrorUtil.internalError(new CounterCheckpointInconsistentException(message))
      }
    }

    private def fetchUnvalidatedEventsBatchFromCheckpoint(
        readState: ReadState
    )(implicit
        traceContext: TraceContext
    ): Future[(ReadState, Seq[(SequencerCounter, Sequenced[Payload])])] = {
      logger.debug(s"Reading events from $readState...")
      for {
        storedEvents <- store.readEvents(
          readState.memberId,
          readState.nextReadTimestamp.some,
          config.readBatchSize,
        )
      } yield {
        // we may be rebuilding counters from a checkpoint before what was actually requested
        // in which case don't return events that we don't need to serve
        val nextSequencerCounter = readState.nextCounterAccumulator
        val eventsWithCounter = storedEvents.zipWithIndex.map { case (event, n) =>
          (nextSequencerCounter + n, event)
        }
        val newReadState = readState.update(storedEvents, config.readBatchSize)
        logger.debug(s"New state is $newReadState.")
        (newReadState, eventsWithCounter)
      }
    }

    private def signEvent(
        event: SequencedEvent[ClosedEnvelope],
        signingTimestampO: Option[CantonTimestamp],
        signingSnapshot: SyncCryptoApi,
    )(implicit traceContext: TraceContext): Future[OrdinarySerializedEvent] = {
      for {
        signedEvent <- SignedContent.tryCreate(
          signingSnapshot.pureCrypto,
          signingSnapshot,
          event,
          signingTimestampO,
        )
      } yield OrdinarySequencedEvent(signedEvent)(traceContext)
    }

    /** Takes our stored event and turns it back into a real sequenced event.
      */
    private def mkSequencedEvent(
        member: Member,
        memberId: SequencerMemberId,
        counter: SequencerCounter,
        event: Sequenced[Payload],
    ): SequencedEvent[ClosedEnvelope] = {
      val timestamp = event.timestamp
      event.event match {
        case DeliverStoreEvent(
              sender,
              messageId,
              _recipients,
              payload,
              signingTimestampO,
              traceContext,
            ) =>
          val messageIdO =
            Option(messageId).filter(_ => memberId == sender) // message id only goes to sender
          val batch: Batch[ClosedEnvelope] = Batch
            .batchClosedEnvelopesFromByteString(payload.content)
            .fold(err => throw new DbDeserializationException(err.toString), identity)
          val filteredBatch = Batch.filterClosedEnvelopesFor(batch, member)
          Deliver.create[ClosedEnvelope](
            counter,
            timestamp,
            domainId,
            messageIdO,
            filteredBatch,
            protocolVersion,
          )
        case DeliverErrorStoreEvent(_, messageId, message, traceContext) =>
          DeliverError.create(
            counter,
            timestamp,
            domainId,
            messageId,
            reason = DeliverErrorReason.BatchRefused(message.unwrap),
            protocolVersion,
          )
      }
    }
  }

  /** Update the read state to start from the closest counter checkpoint if available */
  private def startFromClosestCounterCheckpoint(
      readState: ReadState,
      requestedCounter: SequencerCounter,
  )(implicit traceContext: TraceContext): Future[ReadState] =
    for {
      closestCheckpoint <- store.fetchClosestCheckpointBefore(
        readState.memberId,
        requestedCounter,
      )
    } yield {
      val startText = closestCheckpoint.fold("the beginning")(_.toString)
      logger.debug(
        s"Subscription for ${readState.member} at $requestedCounter will start from $startText"
      )
      closestCheckpoint.fold(readState)(readState.startFromCheckpoint)
    }
}

object SequencerReader {

  /** State to keep track of when serving a read subscription */
  private[SequencerReader] case class ReadState(
      member: Member,
      memberId: SequencerMemberId,
      nextReadTimestamp: CantonTimestamp,
      latestTopologyClientRecipientTimestamp: Option[CantonTimestamp],
      lastBatchWasFull: Boolean = false,
      nextCounterAccumulator: SequencerCounter = 0L,
  ) extends PrettyPrinting {

    /** Update the state after reading a new page of results */
    def update(storedEvents: Seq[Sequenced[_]], batchSize: Int): ReadState = {
      copy(
        // increment the counter by the number of events we've now processed
        nextCounterAccumulator = nextCounterAccumulator + storedEvents.size,
        // set the timestamp to the last record of the batch, or our current timestamp if we got no results
        nextReadTimestamp = storedEvents.lastOption.map(_.timestamp).getOrElse(nextReadTimestamp),
        // did we receive a full batch of events on this update
        lastBatchWasFull = storedEvents.sizeCompare(batchSize) == 0,
      )
    }

    /** Apply a previously recorded counter checkpoint so that we don't have to start from 0 on every subscription */
    def startFromCheckpoint(checkpoint: CounterCheckpoint): ReadState = {
      // with this checkpoint we'll start reading from this timestamp and as reads are not inclusive we'll receive the next event after this checkpoint first
      copy(
        nextCounterAccumulator = checkpoint.counter + 1,
        nextReadTimestamp = checkpoint.timestamp,
        latestTopologyClientRecipientTimestamp = checkpoint.latestTopologyClientTimestamp,
      )
    }

    override def pretty: Pretty[ReadState] = prettyOfClass(
      param("member", _.member),
      param("memberId", _.memberId),
      param("nextReadTimestamp", _.nextReadTimestamp),
      param("latestTopologyClientRecipientTimestamp", _.latestTopologyClientRecipientTimestamp),
      param("lastBatchWasFull", _.lastBatchWasFull),
      param("nextCounterAccumulator", _.nextCounterAccumulator),
    )
  }

  private[SequencerReader] object ReadState {
    def initial(member: Member)(registeredMember: RegisteredMember): ReadState =
      ReadState(
        member,
        registeredMember.memberId,
        registeredMember.registeredFrom,
        None,
      )
  }

  private[SequencerReader] case class UnvalidatedEventWithMetadata(
      unvalidatedEvent: Sequenced[Payload],
      counter: SequencerCounter,
      topologyClientTimestampBefore: Option[CantonTimestamp],
      topologyClientTimestampAfter: Option[CantonTimestamp],
  )

  private[SequencerReader] case class UnsignedEventData(
      event: SequencedEvent[ClosedEnvelope],
      signingTimestampAndSnapshotO: Option[(CantonTimestamp, SyncCryptoApi)],
      previousTopologyClientTimestamp: Option[CantonTimestamp],
      latestTopologyClientTimestamp: Option[CantonTimestamp],
      eventTraceContext: TraceContext,
  )

  def getSnapshotForSequencerSigning[T <: SyncCryptoApi](
      cryptoApi: SyncCryptoClient[T],
      timestamp: CantonTimestamp,
  )(implicit
      traceContext: TraceContext
  ): T = {
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

  @VisibleForTesting
  def invalidSigningTimestampError(
      signingTimestamp: CantonTimestamp,
      sequencingTimestamp: CantonTimestamp,
  ): DeliverErrorReason.BatchRefused = {
    val errorMsg =
      s"Invalid signing timestamp $signingTimestamp for sequencing time $sequencingTimestamp."
    DeliverErrorReason.BatchRefused(errorMsg)
  }
}
