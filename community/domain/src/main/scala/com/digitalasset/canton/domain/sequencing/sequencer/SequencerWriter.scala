// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer

import akka.stream._
import akka.stream.scaladsl.{Keep, Sink}
import cats.data.EitherT
import cats.syntax.bifunctor._
import cats.syntax.functor._
import cats.syntax.option._
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.DomainSyncCryptoClient
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.sequencing.sequencer.store._
import com.digitalasset.canton.lifecycle.{
  AsyncCloseable,
  AsyncOrSyncCloseable,
  FlagCloseableAsync,
  SyncCloseable,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging, TracedLogger}
import com.digitalasset.canton.resource.Storage
import com.digitalasset.canton.sequencing.protocol.{SendAsyncError, SubmissionRequest}
import com.digitalasset.canton.time.{Clock, NonNegativeFiniteDuration}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.tracing.TraceContext.withNewTraceContext
import com.digitalasset.canton.util.Thereafter.syntax._
import com.digitalasset.canton.util.retry.RetryUtil.AllExnRetryable
import com.digitalasset.canton.util.retry.{Pause, Success}
import com.digitalasset.canton.util.{AkkaUtil, EitherTUtil, FutureUtil, retry}
import com.google.common.annotations.VisibleForTesting

import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.Failure
import scala.util.control.NonFatal

/** Configuration for how many sequencers are concurrently operating within the domain.
  * @param enabled Set to true to enable HA for the sequencer.
  * @param totalNodeCount how many sequencer writers will there ever be in this domain.
  *                       recommend setting to a value larger than the current topology to allow for expansion.
  * @param keepAliveInterval how frequently will we ensure the sequencer watermark is updated to ensure it still appears alive
  * @param onlineCheckInterval how frequently should this sequencer check that nodes are still online
  * @param offlineDuration how long should a sequencer watermark be lagging for it to be flagged as offline
  */
case class SequencerHighAvailabilityConfig(
    enabled: Boolean = false,
    totalNodeCount: Int = 10,
    keepAliveInterval: NonNegativeFiniteDuration = NonNegativeFiniteDuration.ofMillis(100L),
    onlineCheckInterval: NonNegativeFiniteDuration = NonNegativeFiniteDuration.ofSeconds(5L),
    offlineDuration: NonNegativeFiniteDuration = NonNegativeFiniteDuration.ofSeconds(8L),
)

object SequencerHighAvailabilityConfig {

  /** Used for when we know there can only be one node (when HA is not available and will never be used such as community edition).
    * Expect only one node that always has the index 0.
    */
  val SingleSequencerNodeIndex: Int = 0
  val SingleSequencerTotalNodeCount: Int = 1

  /** We need to allocate a range of available DbLockCounters so need to specify a maximum number of sequencer writers
    * that can concurrently exist.
    */
  val MaxNodeCount = 32
}

@VisibleForTesting
private[sequencer] class RunningSequencerWriterFlow(
    val queues: SequencerWriterQueues,
    doneF: Future[Unit],
)(implicit executionContext: ExecutionContext) {
  private val completed = new AtomicBoolean(false)

  /** Future for when the underlying stream has completed.
    * We intentionally hand out a transformed future that ensures our completed flag is set first.
    * This is to in most cases avoiding the race where we may call `queues.complete` on an already completed stream
    * which will cause a `IllegalStateException`.
    * However as we can't actually synchronize the akka stream completing due to an error with close being called there
    * is likely still a short window when this situation could occur, however at this point it should only result in an
    * unclean shutdown.
    */
  val done: Future[Unit] = doneF.thereafter(_ => completed.set(true))

  def complete(): Future[Unit] =
    if (!completed.get()) queues.complete().flatMap(_ => done) else done
}

/** Create instances for a [[store.SequencerWriterStore]] and a predicate to know whether we can recreate a sequencer writer
  * on failures encountered potentially during storage.
  * Implements `AutoClosable` so implementations can use [[lifecycle.FlagCloseable]] to short circuit retry attempts.
  */
trait SequencerWriterStoreFactory extends AutoCloseable {
  def create(storage: Storage, generalStore: SequencerStore)(implicit
      traceContext: TraceContext
  ): EitherT[Future, WriterStartupError, SequencerWriterStore]

  /** When the sequencer goes offline Exceptions may be thrown by the [[sequencer.store.SequencerStore]] and [[sequencer.SequencerWriterSource]].
    * This allows callers to check whether the captured exception is expected when offline and indicates that the
    * [[sequencer.SequencerWriter]] can still be recreated.
    */
  def expectedOfflineException(error: Throwable): Boolean = false
}

object SequencerWriterStoreFactory {
  def singleInstance(implicit executionContext: ExecutionContext): SequencerWriterStoreFactory =
    new SequencerWriterStoreFactory {
      override def create(storage: Storage, generalStore: SequencerStore)(implicit
          traceContext: TraceContext
      ): EitherT[Future, WriterStartupError, SequencerWriterStore] =
        EitherT.pure(SequencerWriterStore.singleInstance(generalStore))
      override def close(): Unit = ()
    }
}

/** The Writer component is in practice a little state machine that will run crash recovery on startup then
  * create a running [[SequencerWriterSource]]. If this materialized Sequencer flow then crashes with an exception
  * that can be recovered by running crash recovery it will then go through this process and attempt to restart the flow.
  *
  * Callers must call [[start]] to start the writer and will likely have to wait for this completing before accepting
  * calls for the sequencer to direct at [[send]]. Note that the crash recovery steps may take a long
  * duration to run:
  *  - we delete invalid events previously written by the sequencer and then attempt to insert a new online watermark,
  *    and these database queries may simply take a long time to run.
  *  - the [[SequencerWriter]] will wait until our clock has reached the new online watermark timestamp before starting
  *    the writer to ensure that no events before this timestamp are written. If this online watermark is significantly
  *    ahead of the current clock value it will just wait until this is reached. In practice assuming all sequencers in
  *    the local topology are kept in sync through NTP or similar, this duration should be very small (<1s).
  */
class SequencerWriter(
    writerStoreFactory: SequencerWriterStoreFactory,
    createWriterFlow: (SequencerWriterStore, TraceContext) => RunningSequencerWriterFlow,
    storage: Storage,
    generalStore: SequencerStore,
    clock: Clock,
    expectedCommitMode: Option[CommitMode],
    override protected val timeouts: ProcessingTimeout,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends NamedLogging
    with FlagCloseableAsync {

  private case class RunningWriter(flow: RunningSequencerWriterFlow, store: SequencerWriterStore) {

    def isActive: Boolean = store.isActive

    /** Ensures that all resources for the writer flow are halted and cleaned up.
      * The store should not be used after calling this operation (the HA implementation will close its exclusive storage instance).
      */
    def close()(implicit traceContext: TraceContext): Future[Unit] = {
      logger.debug(s"Completing writer flow")
      val future = for {
        _ <- flow.complete()
        // in the HA sequencer there's a chance the writer store may have already lost its writer lock,
        // in which case this will throw a PassiveInstanceException
        _ = logger.debug(s"Taking store offline")
        _ <- goOffline(store).recover {
          case throwable if writerStoreFactory.expectedOfflineException(throwable) =>
            logger.debug(
              s"Exception was thrown while setting the sequencer as offline but this is expected if already offline so suppressing",
              throwable,
            )
            ()
        }
      } yield ()
      future.thereafter { _ =>
        logger.debug(s"Closing store")
        store.close()
      }
    }
  }

  private val runningWriterRef = new AtomicReference[Option[RunningWriter]](None)

  @VisibleForTesting
  private[sequencer] def isRunning: Boolean = runningWriterRef.get().isDefined
  private[sequencer] def isActive: Boolean = runningWriterRef.get().exists(_.isActive)

  private def sequencerQueues: Option[SequencerWriterQueues] =
    runningWriterRef.get().map(_.flow.queues)

  /** The startup of a [[SequencerWriter]] can fail at runtime.
    * Currently if this occurs we will log a message at error level but as we have no ability to forcibly
    * crash the node we will likely continue running in an unhealthy state.
    * This will however be visible to anyone calling the status operation and could be used by a process monitor
    * to externally restart the sequencer.
    */
  def startOrLogError()(implicit traceContext: TraceContext): Future[Unit] =
    start().fold(
      err => logger.error(s"Failed to startup sequencer writer: $err"),
      identity,
    )

  def start()(implicit traceContext: TraceContext): EitherT[Future, WriterStartupError, Unit] =
    performUnlessClosingEitherT[WriterStartupError, Unit](WriterStartupError.WriterShuttingDown) {
      def createStoreAndRunCrashRecovery()
          : Future[Either[WriterStartupError, SequencerWriterStore]] = {
        // only retry errors that are flagged as retryable
        implicit val success: Success[Either[WriterStartupError, SequencerWriterStore]] = Success {
          case Left(error) => !error.retryable
          case Right(_) => true
        }

        // continuously attempt to start the writer as we can't meaningfully proactively shutdown or crash
        // when this fails
        Pause(logger, this, retry.Forever, 100.millis, "start-sequencer-writer").apply(
          {
            logger.debug("Starting sequencer writer")
            for {
              writerStore <- writerStoreFactory.create(storage, generalStore)
              _ <- EitherTUtil.onErrorOrFailure(() => writerStore.close()) {
                for {
                  // validate that the datastore has an appropriate commit mode set in order to run the writer
                  _ <- expectedCommitMode
                    .fold(EitherTUtil.unit[String])(writerStore.validateCommitMode)
                    .leftMap(WriterStartupError.BadCommitMode)
                  onlineTimestamp <- EitherT.right[WriterStartupError](runRecovery(writerStore))
                  _ <- EitherT.right[WriterStartupError](waitForOnline(onlineTimestamp))
                } yield ()
              }
            } yield writerStore
          }.value,
          AllExnRetryable,
        )
      }

      EitherT(createStoreAndRunCrashRecovery()) map { store =>
        startWriter(store)
      }
    }

  def send(
      submission: SubmissionRequest
  )(implicit traceContext: TraceContext): EitherT[Future, SendAsyncError, Unit] = {
    lazy val sendET = sequencerQueues
      .fold(
        EitherT
          .leftT[Future, Unit](SendAsyncError.Unavailable("Unavailable"))
          .leftWiden[SendAsyncError]
      )(_.send(submission))

    val sendUnlessShutdown = performUnlessClosingF(sendET.value)
    EitherT(
      sendUnlessShutdown.onShutdown(Left[SendAsyncError, Unit](SendAsyncError.ShuttingDown()))
    )
  }

  private def runRecovery(
      store: SequencerWriterStore
  )(implicit traceContext: TraceContext): Future[CantonTimestamp] =
    for {
      _ <- store.deleteEventsPastWatermark()
      onlineTimestamp <- store.goOnline(clock.now)
    } yield onlineTimestamp

  /** When we go online we're given the value of the new watermark that is inserted for this sequencer.
    * We cannot start the writer before this point to ensure that no events before this point are inserted.
    * It may have already be surpassed in which case we can immediately start.
    * Otherwise we wait until this point has been reached.
    */
  private def waitForOnline(
      onlineTimestamp: CantonTimestamp
  )(implicit traceContext: TraceContext): Future[Unit] =
    if (clock.now.isBefore(onlineTimestamp)) {
      val durationToWait =
        java.time.Duration.between(clock.now.toInstant, onlineTimestamp.toInstant)
      logger.debug(
        s"Delaying sequencer writer start for $durationToWait to reach our online watermark"
      )

      val onlineP = Promise[Unit]()
      clock.scheduleAt(_ => onlineP.success(()), onlineTimestamp)
      onlineP.future
    } else Future.unit

  private def startWriter(
      store: SequencerWriterStore
  )(implicit traceContext: TraceContext): Unit = {
    // if these actions fail we want to ensure that the store is closed
    try {
      val writerFlow = createWriterFlow(store, traceContext)

      setupWriterRecovery(writerFlow.done)

      runningWriterRef.set(RunningWriter(writerFlow, store).some)
    } catch {
      case NonFatal(ex) =>
        store.close()
        throw ex
    }
  }

  private def setupWriterRecovery(doneF: Future[Unit]): Unit =
    doneF.onComplete { result =>
      withNewTraceContext { implicit traceContext =>
        performUnlessClosing { // close will take care of shutting down a running writer if close is invoked
          // close the running writer and reset the reference
          runningWriterRef.getAndSet(None).foreach(_.close())

          // determine whether we can run recovery or not
          val shouldRecover = result match {
            case Failure(_writerException: SequencerWriterException) => true
            case Failure(exception) => writerStoreFactory.expectedOfflineException(exception)
            case _other => false
          }

          if (shouldRecover) {
            // include the exception in our info log message if it was the cause of restarting the writer
            val message = "Running Sequencer recovery process"
            result.fold(ex => logger.info(message, ex), _ => logger.info(message))

            FutureUtil.doNotAwait(
              startOrLogError(),
              "SequencerWriter recovery",
            )
          } else {
            // if we encountered an exception and have opted not to recover log a warning
            result.fold(
              ex => logger.warn(s"Sequencer writer has completed with an unrecoverable error", ex),
              _ => logger.debug("Sequencer writer has successfully completed"),
            )
          }
        }
      }
    }

  private def goOffline(
      store: SequencerWriterStore
  )(implicit traceContext: TraceContext): Future[Unit] = {
    logger.debug("Going offline so marking our sequencer as offline")
    store.goOffline()
  }

  override protected def closeAsync(): Seq[AsyncOrSyncCloseable] = withNewTraceContext {
    implicit traceContext =>
      logger.debug("Shutting down sequencer writer")
      val sequencerFlow = runningWriterRef.get()
      Seq(
        SyncCloseable("sequencerWriterStoreFactory", writerStoreFactory.close()),
        AsyncCloseable(
          "sequencingFlow",
          sequencerFlow.map(_.close()).getOrElse(Future.unit),
          // Use timeouts.closing.duration (as opposed to `shutdownShort`) as closing the sequencerFlow can be slow
          timeouts.closing.duration,
        ),
      )
  }
}

object SequencerWriter {

  def apply(
      writerConfig: SequencerWriterConfig,
      writerStorageFactory: SequencerWriterStoreFactory,
      highAvailabilityConfig: SequencerHighAvailabilityConfig,
      processingTimeout: ProcessingTimeout,
      storage: Storage,
      generalStore: SequencerStore,
      clock: Clock,
      cryptoApi: DomainSyncCryptoClient,
      eventSignaller: EventSignaller,
      loggerFactory: NamedLoggerFactory,
  )(implicit materializer: Materializer, executionContext: ExecutionContext): SequencerWriter = {
    val logger = TracedLogger(SequencerWriter.getClass, loggerFactory)

    def createWriterFlow(store: SequencerWriterStore)(implicit
        traceContext: TraceContext
    ): RunningSequencerWriterFlow =
      AkkaUtil.runSupervised(
        logger.error(s"Sequencer writer flow error", _)(TraceContext.empty),
        SequencerWriterSource(
          writerConfig,
          highAvailabilityConfig,
          cryptoApi,
          store,
          clock,
          eventSignaller,
          loggerFactory,
        )
          .toMat(Sink.ignore)(Keep.both)
          .mapMaterializedValue(m => new RunningSequencerWriterFlow(m._1, m._2.void)),
      )

    new SequencerWriter(
      writerStorageFactory,
      createWriterFlow(_)(_),
      storage,
      generalStore,
      clock,
      writerConfig.commitModeValidation,
      processingTimeout,
      loggerFactory,
    )
  }

}
