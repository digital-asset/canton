// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output

import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.{
  AsyncCloseable,
  AsyncOrSyncCloseable,
  FlagCloseableAsync,
  FutureUnlessShutdown,
  HasCloseContext,
  PromiseUnlessShutdown,
  SyncCloseable,
  UnlessShutdown,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging, TracedLogger}
import com.digitalasset.canton.synchronizer.block.BlockFormat
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftBlockOrdererConfig.{
  DefaultOutputEnqueueMaxRetries,
  DefaultOutputEnqueueMaxRetryDelay,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.output.PekkoBlockSubscription.{
  PekkoQueueSourceBufferSize,
  RetryPolicy,
  State,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.BlockNumber
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.{
  BlockSubscription,
  Env,
}
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.Thereafter.syntax.ThereafterAsyncOps
import com.digitalasset.canton.util.retry.ErrorKind.{FatalErrorKind, TransientErrorKind}
import com.digitalasset.canton.util.retry.{ErrorKind, ExceptionRetryPolicy}
import com.digitalasset.canton.util.{AtomicUtil, retry}
import org.apache.pekko.stream.ActorAttributes.streamSubscriptionTimeout
import org.apache.pekko.stream.scaladsl.{Keep, Sink, Source}
import org.apache.pekko.stream.{
  KillSwitch,
  KillSwitches,
  Materializer,
  OverflowStrategy,
  QueueOfferResult,
  StreamDetachedException,
  StreamSubscriptionTimeoutTerminationMode,
}

import java.util.concurrent.atomic.AtomicReference
import scala.collection.immutable.Queue
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.*
import scala.util.{Failure, Success}

class PekkoBlockSubscription[E <: Env[E]](
    initialHeight: BlockNumber,
    override val timeouts: ProcessingTimeout,
    override val loggerFactory: NamedLoggerFactory,
    maxRetries: Int = DefaultOutputEnqueueMaxRetries,
    maxRetryDelay: FiniteDuration = DefaultOutputEnqueueMaxRetryDelay,
)(abort: String => Nothing)(implicit
    executionContext: ExecutionContext,
    materializer: Materializer,
) extends BlockSubscription
    with NamedLogging
    with FlagCloseableAsync
    with HasCloseContext {

  private val stateRef = new AtomicReference[State](State())

  private lazy val (pekkoQueue, pekkoSource) = {
    val attributes =
      streamSubscriptionTimeout(
        0.milli, // this value won't be used
        StreamSubscriptionTimeoutTerminationMode.noop, // instead of .cancel
      )
    val queueSource =
      Source
        .queue[Traced[BlockFormat.Block]](PekkoQueueSourceBufferSize, OverflowStrategy.backpressure)
    // Normally we'd simply call queueSource.preMaterialize() in order to materialize the queue from here.
    // We need to do that because we don't have access to the materialized values of the stream that uses
    // the source returned by subscription() but we need to have access to the queue that gets materialized
    // from the Source.queue in order to push the blocks that we want to serve.
    // However, we're explicitly spelling out the code from preMaterialize() here because there is a small
    // change that needs to be done, which is the addition of the custom attributes such that the stream
    // won't fail with a StreamDetachedException after a timeout (which is by default 5 seconds) from the time
    // the source gets created to the time it starts getting used.
    val (mat, pub) =
      materializer.materialize(
        queueSource.toMat(Sink.asPublisher(fanout = true))(Keep.both).addAttributes(attributes)
      )
    (mat, Source.fromPublisher(pub))
  }

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  @volatile private var subscriptionStarted = false

  override def subscription(): Source[Traced[BlockFormat.Block], KillSwitch] = {
    subscriptionStarted = true
    pekkoSource
      .statefulMapConcat { () =>
        val blocksPeanoQueue =
          new PeanoQueue[BlockNumber, Traced[BlockFormat.Block]](initialHeight)(abort)
        block => {
          val blockHeight = block.value.blockHeight
          logger.debug(
            s"Inserting block $blockHeight into subscription Peano queue (head=${blocksPeanoQueue.head})"
          )(block.traceContext)
          blocksPeanoQueue.insert(BlockNumber(blockHeight), block)
          blocksPeanoQueue.pollAvailable()
        }
      }
      .viaMat(KillSwitches.single)(Keep.right)
  }

  override def receiveBlock(
      block: BlockFormat.Block
  )(implicit traceContext: TraceContext): Unit = {
    val height = block.blockHeight

    logger.debug(
      s"Received block $height from output module, enqueueing it to sequencer core"
    )

    // don't add new messages to queue if we are closing the queue, or we get a StreamDetached exception
    // We merely synchronize the call to the queue, but don't wait until the queue actually has space
    // to avoid long delays upon closing.
    synchronizeWithClosingSync("DABFT enqueue block to sequencer core")(
      advance(newTracedBlockO = Some(Traced(block)))
    ).discard
  }

  override def closeAsync(): Seq[AsyncOrSyncCloseable] = {
    import TraceContext.Implicits.Empty.*
    SyncCloseable("dabft-output-queue.complete", pekkoQueue.complete()) +: Option
      .when(subscriptionStarted)(
        AsyncCloseable(
          "dabft-output-queue.watchCompletion",
          pekkoQueue.watchCompletion(),
          timeouts.closing,
        )
      )
      .toList
  }

  // Advances the enqueueing state and returns an optional callback that starts the next enqueueing task
  private def advance(
      newTracedBlockO: Option[Traced[BlockFormat.Block]] = None,
      taskComplete: Boolean = false,
  ): Unit = {
    // Create the promise outside the CAS block, rather than inside it, in order to avoid
    //  registering untriggerable futures with the execution context when CAS fails due contention,
    //  which may constitute a memory leak.
    val promise = PromiseUnlessShutdown.unsupervised[Unit]()
    AtomicUtil
      .updateAndGetComputed(stateRef) { case State(blocksToEnqueue, taskO) =>
        // noinspection ConvertibleToMethodValue
        val updateBlocksToEnqueue =
          newTracedBlockO.fold(blocksToEnqueue)(blocksToEnqueue.enqueue(_))

        if (taskO.isEmpty || taskComplete) {
          val (tracedBlockToEnqueueO, restOfBlocksToEnqueue) =
            updateBlocksToEnqueue.dequeueOption.fold(
              Option.empty[Traced[BlockFormat.Block]] -> Queue.empty[Traced[BlockFormat.Block]]
            ) { case (tracedBlock, restOfBlocks) =>
              Some(tracedBlock) -> restOfBlocks
            }
          State(
            restOfBlocksToEnqueue,
            tracedBlockToEnqueueO.map(tracedBlockToEnqueue =>
              // The promise's continuation future, i.e. the insertion into the Pekko queue source,
              //  is registered and triggered multiple times for the same block height when CAS fails
              //  due to contention, but it's fine to insert a block out-of-order and/or multiple
              //  times in the Pekko queue because inserting into the Peano queue afterward
              //  (see body of `statefulMapConcat` on the Pekko queue) is an idempotent operation.
              promise.futureUS.flatMap(_ => pekkoEnqueue(tracedBlockToEnqueue))
            ),
          ) -> Some(() => promise.outcome_(()))
        } else {
          State(updateBlocksToEnqueue, taskO) -> None
        }
      }
      .foreach(_()) // Start the next enqueueing task if it exists
  }

  private def pekkoEnqueue(
      tracedBlockToEnqueue: Traced[BlockFormat.Block]
  ): FutureUnlessShutdown[QueueOfferResult] = {
    implicit val success: retry.Success[QueueOfferResult] = retry.Success.always
    val height = tracedBlockToEnqueue.value.blockHeight
    locally {
      implicit val traceContext: TraceContext = tracedBlockToEnqueue.traceContext
      retry
        .Backoff(
          logger,
          closeContext.context,
          maxRetries = maxRetries,
          initialDelay = 50.milliseconds,
          maxDelay = maxRetryDelay,
          operationName = "DABFT enqueue block to sequencer core",
        )
        .unlessShutdown(
          FutureUnlessShutdown.outcomeF(pekkoQueue.offer(tracedBlockToEnqueue)),
          RetryPolicy,
        )
        .thereafter { result =>
          result match {

            case Success(UnlessShutdown.Outcome(result)) =>
              result match {

                case QueueOfferResult.Enqueued =>
                  logger.debug(s"Successfully enqueued block $height to the sequencer core")

                case QueueOfferResult.Dropped =>
                  logger.error(s"Internal error: block $height was dropped by the Pekko queue")

                case QueueOfferResult.QueueClosed =>
                  logger.info(
                    s"Block $height was not enqueued to the sequencer core due to shutting down"
                  )

                case QueueOfferResult.Failure(t) =>
                  logger.error(
                    s"Internal error: block $height was not enqueued to the sequencer core due to an exception",
                    t,
                  )
              }

            case Success(UnlessShutdown.AbortedDueToShutdown) |
                Failure(_: StreamDetachedException) =>
              logger.info(
                s"Block $height was not enqueued to the sequencer core due to shutting down"
              )

            case f @ Failure(t) =>
              logger.error(
                s"Internal error: block $height was not enqueued to the sequencer core due to an exception",
                t,
              )
          }

          advance(taskComplete = true)
        }
    }
  }
}

object PekkoBlockSubscription {

  private val PekkoQueueSourceBufferSize = 5000

  /** The state of the subscription, which consists of the blocks that are waiting to be enqueued to
    * the sequencer core via the Pekko queue/source, and the current enqueueing task, if it exists.
    */
  private final case class State(
      blocksToEnqueue: Queue[Traced[BlockFormat.Block]] = Queue.empty,
      runningEnqueueTask: Option[FutureUnlessShutdown[QueueOfferResult]] = None,
  )

  private object RetryPolicy extends ExceptionRetryPolicy {
    override protected def determineExceptionErrorKind(
        t: Throwable,
        logger: TracedLogger,
    )(implicit tc: TraceContext): ErrorKind =
      t match {

        case ree: java.util.concurrent.RejectedExecutionException =>
          logger.warn(
            "Enqueuing block to sequencer core failed with `RejectedExecutionException`, " +
              "which indicates that the thread pool is overloaded",
            ree,
          )(tc)
          TransientErrorKind()

        case _ => FatalErrorKind
      }
  }
}
