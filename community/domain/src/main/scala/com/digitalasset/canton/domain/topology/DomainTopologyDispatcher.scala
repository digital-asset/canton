// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.topology

import cats.data.{EitherT, NonEmptyList}
import cats.syntax.foldable._
import cats.syntax.traverseFilter._
import com.daml.error.{ErrorCategory, ErrorCode, Explanation, Resolution}
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto._
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.config.DomainNodeParameters
import com.digitalasset.canton.error.CantonErrorGroups.TopologyManagementErrorGroup.TopologyDispatchingErrorGroup
import com.digitalasset.canton.error.{CantonError, HasDegradationState}
import com.digitalasset.canton.lifecycle.{
  FlagCloseable,
  FutureUnlessShutdown,
  Lifecycle,
  UnlessShutdown,
}
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.messages.DomainTopologyTransactionMessage
import com.digitalasset.canton.sequencing.client.SendAsyncClientError.RequestRefused
import com.digitalasset.canton.sequencing.client._
import com.digitalasset.canton.sequencing.protocol._
import com.digitalasset.canton.time.{Clock, DomainTimeTracker}
import com.digitalasset.canton.topology._
import com.digitalasset.canton.topology.client.{
  DomainTopologyClient,
  DomainTopologyClientWithInit,
  StoreBasedDomainTopologyClient,
  StoreBasedTopologySnapshot,
}
import com.digitalasset.canton.topology.processing.{ApproximateTime, EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.store.{
  StoredTopologyTransaction,
  StoredTopologyTransactions,
  TopologyStore,
}
import com.digitalasset.canton.topology.transaction._
import com.digitalasset.canton.tracing.{BatchTracing, TraceContext, Traced}
import com.digitalasset.canton.util.ShowUtil._
import com.digitalasset.canton.util.{EitherTUtil, ErrorUtil, FutureUtil}
import com.digitalasset.canton.{DiscardOps, DomainId, SequencerCounter, checked}

import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicReference}
import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future, Promise, blocking}
import scala.math.Ordered.orderingToOrdered
import scala.util.{Failure, Success}

/** Class that dispatches topology transactions added to the domain topology manager to the members
  *
  * When topology transactions are added to a domain, the dispatcher sends them
  * to the respective members. Same is true if a participant joins a domain, the
  * dispatcher will send him the current snapshot of topology transactions.
  *
  * The dispatcher is very simple:
  * - [source] as the source topology transaction store of the domain topology manager
  * - [target] as the target topology transaction store of the domain client
  * - [queue] as the difference of the stores [source] - [target]
  *
  * Now, the dispatcher takes x :: rest from [queue], sends x over the sequencer and monitors the [target] list.
  * Once x is observed, we repeat.
  *
  * The only special thing in here is the case when x = ParticipantState update. Because in this case, a participant
  * might change his state (i.e. become online for the first time or resume operation after being disabled).
  *
  * In this case, before x is sent from [source] to [target], we send all transactions in [target] to him. And once we
  * did that, we follow-up with x.
  *
  * Given that we want that all members have the same [target] store, we send the x not to one member, but to all addressable
  * members in a batch.
  *
  * One final remark: the present code will properly resume from a crash, except that it might send a snapshot to a participant
  * a second time. I.e. it assumes idempotency of the topology transaction message processing.
  */
class DomainTopologyDispatcher(
    domainId: DomainId,
    authorizedStore: TopologyStore,
    val targetClient: DomainTopologyClientWithInit,
    initialKeys: Map[KeyOwner, Seq[PublicKey]],
    targetStore: TopologyStore,
    crypto: Crypto,
    clock: Clock,
    addressSequencerAsDomainMember: Boolean,
    parameters: DomainNodeParameters,
    sender: DomainTopologySender,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit val ec: ExecutionContext)
    extends NamedLogging
    with FlagCloseable
    with DomainIdentityStateObserver {

  override protected val timeouts: ProcessingTimeout = parameters.processingTimeouts

  def queueSize: Int = lock.synchronized { queue.size + inflight.get() }

  private val topologyClient =
    new StoreBasedDomainTopologyClient(
      clock,
      domainId,
      authorizedStore,
      SigningPublicKey.collect(initialKeys),
      StoreBasedDomainTopologyClient.NoPackageDependencies,
      timeouts,
      loggerFactory,
      useStateTxs = false,
    )
  private val domainSyncCryptoClient = new DomainSyncCryptoClient(
    DomainTopologyManagerId(domainId.unwrap),
    domainId,
    topologyClient,
    crypto,
    parameters.cachingConfigs,
    loggerFactory,
  )
  private val staticDomainMembers = DomainMember.list(domainId, addressSequencerAsDomainMember)
  private val flushing = new AtomicBoolean(false)
  private val queue = mutable.Queue[Traced[StoredTopologyTransaction[TopologyChangeOp]]]()
  private val lock = new Object()
  private val catchup = new ParticipantTopologyCatchup(authorizedStore, sender, loggerFactory)
  private val lastTs = new AtomicReference(CantonTimestamp.MinValue)
  private val inflight = new AtomicInteger(0)

  def init(flushSequencer: => Future[Unit])(implicit traceContext: TraceContext): Future[Unit] = {
    for {
      initiallyPending <- determinePendingTransactions()
      // synchronise topology processing if necessary to avoid duplicate submissions and ugly warnings
      actuallyPending <-
        if (initiallyPending.nonEmpty) {
          logger.debug(
            s"It seems that ${initiallyPending.size} topology transactions are still pending. Attempting to flush the sequencer before recomputing."
          )
          for {
            // flush sequencer (so we are sure that there is nothing pending)
            _ <- flushSequencer
            // now, go back to the db and re-fetch the transactions
            pending <- determinePendingTransactions()
          } yield pending
        } else Future.successful(initiallyPending)
    } yield {
      queue.appendAll(actuallyPending)
      actuallyPending.lastOption match {
        case Some(ts) =>
          updateTopologyClientTs(ts.value.validFrom)
          logger.info(
            show"Resuming topology dispatching with ${actuallyPending.length} transactions: ${actuallyPending
              .map(x => (x.value.transaction.operation, x.value.transaction.transaction.element.mapping))}"
          )
          flush()
        case None => logger.debug("Started domain topology dispatching (nothing to catch up)")
      }
    }
  }

  override def addedSignedTopologyTransaction(
      timestamp: CantonTimestamp,
      transactions: Seq[SignedTopologyTransaction[TopologyChangeOp]],
  )(implicit traceContext: TraceContext): Unit = {
    val last = lastTs.getAndSet(timestamp)
    // we assume that we get txs in strict ascending timestamp order
    ErrorUtil.requireArgument(
      last < timestamp,
      s"received new topology txs with ts $timestamp which is not strictly higher than last $last",
    )
    // we assume that there is only one participant state or domain parameters change per timestamp
    // otherwise, we need to make the dispatching stuff a bit smarter and deal with "multiple participants could become active in this batch"
    // and we also can't deal with multiple replaces with the same timestamp
    val (ps, dpc) = transactions.map(_.transaction.element.mapping).foldLeft((0, 0)) {
      case ((ps, dpc), _: DomainParametersChange) =>
        (ps, dpc + 1)
      case ((ps, dpc), _: ParticipantState) =>
        (ps + 1, dpc)
      case (acc, _) => acc
    }
    ErrorUtil.requireArgument(
      ps < 2 && dpc < 2,
      s"received batch of topology transactions with multiple participant state changes ${transactions}",
    )
    updateTopologyClientTs(timestamp)
    blocking {
      lock.synchronized {
        queue ++= transactions.iterator.map(x =>
          Traced(StoredTopologyTransaction(timestamp, None, x))
        )
        flush()
      }
    }
  }

  // subscribe to target
  targetClient.subscribe(new DomainTopologyClient.Subscriber {
    override def observed(
        sequencedTimestamp: SequencedTime,
        effectiveTimestamp: EffectiveTime,
        sc: SequencerCounter,
        transactions: Seq[SignedTopologyTransaction[TopologyChangeOp]],
    )(implicit traceContext: TraceContext): Unit = {
      transactions.foreach { tx =>
        logger.debug(
          show"Sequenced topology transaction ${tx.transaction.op} ${tx.transaction.element.mapping}"
        )
      }
      // remove the observed txs from our queue stats. as we might have some tx in flight on startup,
      // we'll just ensure we never go below 0 which will mean that once the system has resumed operation
      // the queue stats are correct, while just on startup, they might be a bit too low
      inflight.updateAndGet(x => Math.max(0, x - transactions.size)).discard
    }
  })

  private def updateTopologyClientTs(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Unit =
    topologyClient.updateHead(
      EffectiveTime(timestamp),
      ApproximateTime(timestamp),
      potentialTopologyChange = true,
    )

  private def determinePendingTransactions()(implicit
      traceContext: TraceContext
  ): Future[List[Traced[StoredTopologyTransaction[TopologyChangeOp]]]] =
    for {
      // fetch watermark of the domain
      watermarkTs <- targetStore.currentDispatchingWatermark
      // fetch any transaction that is potentially not dispatched
      transactions <- authorizedStore.findDispatchingTransactionsAfter(
        watermarkTs.getOrElse(CantonTimestamp.MinValue)
      )
      // filter out anything that might have ended up somehow in the target store
      // despite us not having succeeded updating the watermark
      filteredTx <- transactions.result.toList
        .traverseFilter { tx =>
          targetStore
            .exists(tx.transaction)
            .map(exists => Option.when(!exists)(tx))
        }
    } yield filteredTx.map(Traced(_))

  /** Determines the next batch of topology transactions to send.
    *
    * We send the entire queue up and including of the first `ParticipantState` update at once for efficiency reasons.
    */
  private def determineBatchFromQueue()
      : List[Traced[StoredTopologyTransaction[TopologyChangeOp]]] = {
    val items = queue.dequeueWhile(_.value.transaction.transaction.element.mapping match {
      case _: ParticipantState | _: DomainParametersChange => false
      case _ => true
    })
    (if (queue.nonEmpty)
       items :+ queue.dequeue()
     else items).toList
  }

  /** wait an epsilon if the effective time is reduced with this change */
  private def waitIfEffectiveTimeIsReduced(
      transactions: Traced[NonEmptyList[StoredTopologyTransaction[TopologyChangeOp]]]
  ): EitherT[FutureUnlessShutdown, String, Unit] = transactions.withTraceContext {
    implicit traceContext => txs =>
      val empty = EitherT.rightT[FutureUnlessShutdown, String](())
      txs.last.transaction.transaction.element.mapping match {
        case mapping: DomainParametersChange =>
          EitherT
            .right(
              performUnlessClosingF(
                authorizedStoreSnapshot(txs.last.validFrom).findDynamicDomainParameters
              )
            )
            .flatMap(_.fold(empty) { param =>
              // if new epsilon is smaller than current, then wait current epsilon before dispatching this set of txs
              val old = param.topologyChangeDelay.duration
              if (old > mapping.domainParameters.topologyChangeDelay.duration) {
                logger.debug(
                  s"Waiting $old due to topology change delay before resuming dispatching"
                )
                EitherT.right(
                  clock.scheduleAfter(_ => (), param.topologyChangeDelay.duration)
                )
              } else empty
            })
        case _ => empty
      }
  }

  def flush()(implicit traceContext: TraceContext): Unit = {
    if (!flushing.getAndSet(true)) {
      val ret = for {
        pending <- EitherT.right(performUnlessClosingF(Future {
          blocking {
            lock.synchronized {
              val tmp = determineBatchFromQueue()
              inflight.updateAndGet(_ + tmp.size).discard
              tmp
            }
          }
        }))
        tracedTxO = NonEmptyList.fromList(pending).map { tracedNel =>
          BatchTracing.withNelTracedBatch(logger, tracedNel)(implicit traceContext =>
            txs => Traced(txs.map(_.value))
          )
        }
        _ <- tracedTxO.fold(EitherT.pure[FutureUnlessShutdown, String](()))(txs =>
          for {
            _ <- bootstrapAndDispatch(txs)
            _ <- waitIfEffectiveTimeIsReduced(txs)
          } yield ()
        )
      } yield {
        val flushAgain = blocking {
          lock.synchronized {
            flushing.getAndSet(false)
            queue.nonEmpty
          }
        }
        if (flushAgain)
          flush()
      }
      EitherTUtil.doNotAwait(
        EitherT(ret.value.onShutdown(Right(()))),
        "Halting topology dispatching due to unexpected error. Please restart.",
      )
    }

  }

  private def safeTimestamp(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): CantonTimestamp = {
    if (timestamp > topologyClient.topologyKnownUntilTimestamp) {
      logger.error(
        s"Requesting invalid authorized topology timestamp at ${timestamp} while I only have ${topologyClient.topologyKnownUntilTimestamp}"
      )
      topologyClient.topologyKnownUntilTimestamp
    } else timestamp
  }

  private def authorizedCryptoSnapshot(
      timestamp: CantonTimestamp
  )(implicit traceContext: TraceContext): DomainSnapshotSyncCryptoApi = {
    checked(domainSyncCryptoClient.trySnapshot(safeTimestamp(timestamp)))
  }

  private def authorizedStoreSnapshot(
      timestamp: CantonTimestamp
  )(implicit traceContext: TraceContext): StoreBasedTopologySnapshot = {
    checked(topologyClient.trySnapshot(safeTimestamp(timestamp)))
  }

  private def bootstrapAndDispatch(
      tracedTransaction: Traced[
        NonEmptyList[StoredTopologyTransaction[TopologyChangeOp]]
      ]
  ): EitherT[FutureUnlessShutdown, String, Unit] = {
    tracedTransaction withTraceContext { implicit traceContext => transactions =>
      val flushToParticipantET: EitherT[FutureUnlessShutdown, String, Option[ParticipantId]] =
        transactions.last.transaction.transaction.element.mapping match {
          case ParticipantState(_, _, participant, _, _) =>
            for {
              catchupForParticipant <- EitherT.right(
                performUnlessClosingF(
                  catchup
                    .determineCatchupForParticipant(
                      transactions.head.validFrom,
                      transactions.last.validFrom,
                      participant,
                    )
                )
              )
              // if we crash during dispatching, we will resend all these transactions again
              // to the participant. this will for now produce some ugly warnings, but the
              // bootstrapping transactions will just be deduplicated at the transaction processor
              // once we move to topology privacy, the participant will take care of this as
              // rather than pushing the initial state, the participants will pull it on demand
              _ <- catchupForParticipant.fold(EitherT.rightT[FutureUnlessShutdown, String](())) {
                txs =>
                  sender.sendTransactions(
                    authorizedCryptoSnapshot(transactions.head.validFrom),
                    txs.toDomainTopologyTransactions,
                    Set(participant),
                  )
              }
            } yield catchupForParticipant.map(_ => participant)
          case _ =>
            EitherT.rightT(None)
        }
      for {
        includeParticipant <- flushToParticipantET
        _ <- sendTransactions(transactions, includeParticipant)
        // update watermark, which we can as we successfully registered all transactions with the domain
        // we don't need to wait until they are processed
        _ <- EitherT.right(
          performUnlessClosingF(targetStore.updateDispatchingWatermark(transactions.last.validFrom))
        )
      } yield ()
    }
  }

  private def sendTransactions(
      transactions: NonEmptyList[StoredTopologyTransaction[TopologyChangeOp]],
      add: Option[ParticipantId],
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, String, Unit] = {
    val headSnapshot = authorizedStoreSnapshot(transactions.head.validFrom)
    val receivingParticipantsF = performUnlessClosingF(
      headSnapshot
        .participants()
        .map(_.collect {
          case (participantId, perm) if perm.isActive => participantId
        })
    )
    val mediatorsF = performUnlessClosingF(headSnapshot.mediators())
    for {
      receivingParticipants <- EitherT.right(receivingParticipantsF)
      mediators <- EitherT.right(mediatorsF)
      _ <- sender.sendTransactions(
        authorizedCryptoSnapshot(transactions.head.validFrom),
        transactions.map(_.transaction).toList,
        (receivingParticipants ++ staticDomainMembers ++ mediators ++ add.toList).toSet,
      )
    } yield ()
  }

  override protected def onClosed(): Unit =
    Lifecycle.close(sender, topologyClient)(logger)

}

object DomainTopologyDispatcher {
  def create(
      domainId: DomainId,
      domainTopologyManager: DomainTopologyManager,
      targetClient: DomainTopologyClientWithInit,
      initialKeys: Map[KeyOwner, Seq[PublicKey]],
      targetStore: TopologyStore,
      client: SequencerClient,
      timeTracker: DomainTimeTracker,
      crypto: Crypto,
      clock: Clock,
      addressSequencerAsDomainMember: Boolean,
      parameters: DomainNodeParameters,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): DomainTopologyDispatcher = {

    val sender = new DomainTopologySender.Impl(
      domainId,
      client,
      timeTracker,
      clock,
      maxBatchSize = 1000,
      retryInterval = 10.seconds,
      parameters.processingTimeouts,
      loggerFactory,
    )

    val dispatcher = new DomainTopologyDispatcher(
      domainId,
      domainTopologyManager.store,
      targetClient,
      initialKeys,
      targetStore,
      crypto,
      clock,
      addressSequencerAsDomainMember,
      parameters,
      sender,
      loggerFactory,
    )

    // schedule init on domain topology manager to avoid race conditions between
    // queue filling and store scanning. however, we can't do that synchronously here
    // as starting the domain might be kicked off by the domain manager (in manual init scenarios)
    domainTopologyManager
      .executeSequential(
        for {
          _ <- dispatcher.init(flushSequencerWithTimeProof(timeTracker, targetClient))
        } yield {
          domainTopologyManager.addObserver(dispatcher)
        },
        "initializing domain topology dispatcher",
      )

    dispatcher

  }

  private def flushSequencerWithTimeProof(
      timeTracker: DomainTimeTracker,
      targetClient: DomainTopologyClient,
  )(implicit executionContext: ExecutionContext, traceContext: TraceContext): Future[Unit] = for {
    // flush the sequencer client with a time-proof. this should ensure that we give the
    // topology processor time to catch up with any pending submissions
    timestamp <- timeTracker.fetchTimeProof().map(_.timestamp)
    // wait until the topology client has seen this timestamp
    _ <- targetClient
      .awaitTimestamp(timestamp, waitForEffectiveTime = false)
      .getOrElse(Future.unit)
  } yield ()

}

trait DomainTopologySender extends AutoCloseable {
  def sendTransactions(
      snapshot: DomainSnapshotSyncCryptoApi,
      transactions: Seq[SignedTopologyTransaction[TopologyChangeOp]],
      recipients: Set[Member],
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, String, Unit]
}

object DomainTopologySender extends TopologyDispatchingErrorGroup {

  class Impl(
      domainId: DomainId,
      client: SequencerClient,
      timeTracker: DomainTimeTracker,
      clock: Clock,
      maxBatchSize: Int,
      retryInterval: FiniteDuration,
      val timeouts: ProcessingTimeout,
      val loggerFactory: NamedLoggerFactory,
  )(implicit executionContext: ExecutionContext)
      extends NamedLogging
      with HasDegradationState[DomainTopologySender.TopologyDispatchingDegradation]
      with DomainTopologySender
      with FlagCloseable {

    private val currentJob =
      new AtomicReference[Option[Promise[UnlessShutdown[Either[String, Unit]]]]](None)

    def sendTransactions(
        snapshot: DomainSnapshotSyncCryptoApi,
        transactions: Seq[SignedTopologyTransaction[TopologyChangeOp]],
        recipients: Set[Member],
    )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, String, Unit] =
      for {
        nonEmptyRecipients <- EitherT.fromOption[FutureUnlessShutdown](
          Recipients.ofSet(recipients),
          show"Empty set of recipients for ${transactions}",
        )
        _ <- transactions
          .grouped(maxBatchSize)
          .foldLeft(EitherT.pure[FutureUnlessShutdown, String](())) { case (res, batch) =>
            res.flatMap(_ =>
              for {
                message <-
                  DomainTopologyTransactionMessage
                    .create(batch.toList, snapshot, domainId)
                    .leftMap(_.toString)
                    .mapK(FutureUnlessShutdown.outcomeK)
                _ <- ensureDelivery(
                  Batch(List(OpenEnvelope(message, nonEmptyRecipients))),
                  s"${transactions.size} topology transactions for ${recipients.size} recipients",
                )
              } yield ()
            )
          }
      } yield ()

    private def finalizeCurrentJob(outcome: UnlessShutdown[Either[String, Unit]]): Unit = {
      currentJob.getAndSet(None).foreach { promise =>
        promise.trySuccess(outcome).discard
      }
    }

    override def onClosed(): Unit = {
      // invoked once all performUnlessClosed tasks are done.
      // due to our retry scheduling using the clock, we might not get to complete the promise
      finalizeCurrentJob(UnlessShutdown.AbortedDueToShutdown)
      Lifecycle.close(timeTracker, client)(logger)
    }

    protected def send(
        batch: Batch[OpenEnvelope[DomainTopologyTransactionMessage]],
        callback: SendCallback,
    )(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[Either[SendAsyncClientError, Unit]] =
      performUnlessClosingF(
        client
          .sendAsync(batch, SendType.Other, callback = callback)
          .value
      )

    private def ensureDelivery(
        batch: Batch[OpenEnvelope[DomainTopologyTransactionMessage]],
        message: String,
    )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, String, Unit] = {

      val promise = Promise[UnlessShutdown[Either[String, Unit]]]()
      // as we care a lot about the order of the transactions, we should never get
      // concurrent invocations of this here
      ErrorUtil.requireArgument(
        currentJob.getAndSet(Some(promise)).isEmpty,
        "Ensure delivery called while we already had a pending call!",
      )

      def stopDispatching(str: String): Unit = {
        finalizeCurrentJob(UnlessShutdown.Outcome(Left(str)))
      }

      def abortDueToShutdown(): Unit = {
        logger.info("Aborting batch dispatching due to shutdown")
        finalizeCurrentJob(UnlessShutdown.AbortedDueToShutdown)
      }

      lazy val callback: SendCallback = {
        case _: SendResult.Success =>
          val msg = s"Successfully registered $message with the sequencer."
          if (isDegraded)
            resolveDegradationIfExists(_ => msg)
          else
            logger.debug(msg)
          finalizeCurrentJob(UnlessShutdown.Outcome(Right(())))
        case SendResult.Error(error) =>
          TopologyDispatchingInternalError.SendResultError(error)
          stopDispatching("Stopping due to an internal send result error")
        case _: SendResult.Timeout =>
          degradationOccurred(TopologyDispatchingDegradation.SendTrackerTimeout())
          dispatch()
      }

      def dispatch(): Unit = {
        logger.debug(s"Attempting to dispatch ${message}")
        FutureUtil.doNotAwait(
          send(batch, callback).transform {
            case x @ Success(UnlessShutdown.Outcome(Left(RequestRefused(error))))
                if error.category.retryable.nonEmpty =>
              degradationOccurred(TopologyDispatchingDegradation.SendRefused(error))
              // delay the retry, assuming that the retry is much smaller than our shutdown interval
              // the perform unless closing will ensure we don't close the ec before the delay
              // has kicked in
              logger.debug(s"Scheduling topology dispatching retry in ${retryInterval}")
              clock
                .scheduleAfter(_ => dispatch(), java.time.Duration.ofMillis(retryInterval.toMillis))
                .discard
              x
            case x @ Success(UnlessShutdown.Outcome(Left(error))) =>
              TopologyDispatchingInternalError.AsyncResultError(error)
              stopDispatching("Stopping due to an unexpected async result error")
              x
            case x @ Success(UnlessShutdown.Outcome(Right(_))) =>
              // nice, the sequencer seems to be accepting our request
              x
            case x @ Success(UnlessShutdown.AbortedDueToShutdown) =>
              abortDueToShutdown()
              x
            case x @ Failure(ex) =>
              TopologyDispatchingInternalError.UnexpectedException(ex)
              stopDispatching("Stopping due to an unexpected exception")
              x
          }.unwrap,
          "dispatch future threw an unexpected exception",
        )
      }

      dispatch()
      EitherT(FutureUnlessShutdown(promise.future))
    }

  }

  trait TopologyDispatchingDegradation extends CantonError

  @Explanation(
    "This warning occurs when the topology dispatcher experiences timeouts while trying to register topology transactions."
  )
  @Resolution(
    "This error should normally self-recover due to retries. If issue persist, contact support and investigate system state."
  )
  object TopologyDispatchingDegradation
      extends ErrorCode(
        id = "TOPOLOGY_DISPATCHING_DEGRADATION",
        ErrorCategory.BackgroundProcessDegradationWarning,
      ) {
    case class SendTrackerTimeout()(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause =
            "The submitted domain topology transactions never appeared on the domain. Will try again."
        )
        with TopologyDispatchingDegradation
    case class SendRefused(err: SendAsyncError)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause =
            "Did not succeed to register the domain topology transactions with the domain. Will try again."
        )
        with TopologyDispatchingDegradation
  }

  @Explanation(
    "This error is emitted if there is a fundamental, un-expected situation during topology dispatching. " +
      "In such a situation, the topology state of a newly onboarded participant or of all domain members might become outdated"
  )
  @Resolution(
    "Contact support."
  )
  object TopologyDispatchingInternalError
      extends ErrorCode(
        id = "TOPOLOGY_DISPATCHING_INTERNAL_ERROR",
        ErrorCategory.SystemInternalAssumptionViolated,
      ) {
    case class SendResultError(err: DeliverError)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = "Received an unexpected deliver error during topology dispatching."
        )
    case class AsyncResultError(err: SendAsyncClientError)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = "Received an unexpected send async client error during topology dispatching."
        )
    case class UnexpectedException(throwable: Throwable)(implicit
        val loggingContext: ErrorLoggingContext
    ) extends CantonError.Impl(
          cause = "Send async threw an unexpected exception during topology dispatching.",
          throwableO = Some(throwable),
        )
  }

}

class ParticipantTopologyCatchup(
    store: TopologyStore,
    sender: DomainTopologySender,
    val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends NamedLogging {

  /** determine the set of transactions that needs to be sent to this participant
    *
    * participants that were disabled or are joining new need to get an initial set
    * of topology transactions.
    *
    * @param asOf timestamp of the topology transaction in the authorized store that potentially activated a participant
    * @param upToExclusive timestamp up to which we have to send the topology transaction
    * @param participantId the participant of interest
    */
  def determineCatchupForParticipant(
      upToExclusive: CantonTimestamp,
      asOf: CantonTimestamp,
      participantId: ParticipantId,
  )(implicit
      traceContext: TraceContext
  ): Future[Option[StoredTopologyTransactions[TopologyChangeOp]]] = {
    determinePermissionChange(asOf, participantId).flatMap {
      case (from, to) if !from.isActive && to.isActive =>
        for {
          participantInactiveSince <- findDeactivationTsOfParticipantBefore(
            asOf,
            participantId,
          )
          participantCatchupTxs <- store.findTransactionsInRange(
            // exclusive, as last message a participant will receive is the one that is deactivating it
            asOfExclusive = participantInactiveSince,
            // exclusive as the participant will receive the rest subsequently
            upToExclusive = upToExclusive,
          )
        } yield {
          val cleanedTxs = deduplicateDomainParameterChanges(participantCatchupTxs)
          logger.info(
            s"Participant $participantId is changing from ${from} to ${to}, requires to catch up ${cleanedTxs.result.size}"
          )
          Option.when(cleanedTxs.result.nonEmpty)(cleanedTxs)
        }
      case _ => Future.successful(None)
    }
  }

  /** deduplicate domain parameter changes
    *
    * we anyway only need the most recent one.
    */
  private def deduplicateDomainParameterChanges(
      original: StoredTopologyTransactions[TopologyChangeOp]
  ): StoredTopologyTransactions[TopologyChangeOp] = {
    val start = (
      None: Option[StoredTopologyTransaction[TopologyChangeOp]],
      Seq.empty[StoredTopologyTransaction[TopologyChangeOp]],
    )
    // iterate over result set and keep last dpc
    val (dpcO, rest) = original.result.foldLeft(start) {
      case ((_, acc), elem)
          if elem.transaction.transaction.element.mapping.dbType == DomainTopologyTransactionType.DomainParameters =>
        (Some(elem), acc) // override previous dpc
      case ((cur, acc), elem) => (cur, acc :+ elem)
    }
    // append dpc
    val result = dpcO.fold(rest)(rest :+ _)
    StoredTopologyTransactions(result)
  }

  private def findDeactivationTsOfParticipantBefore(
      ts: CantonTimestamp,
      participantId: ParticipantId,
  )(implicit
      traceContext: TraceContext
  ): Future[CantonTimestamp] = {
    val limit = 1000
    // let's find the last time this node got deactivated. note, we assume that the participant is
    // not active as of ts
    store
      .findTsOfParticipantStateChangesBefore(ts, participantId, limit)
      .flatMap {
        // if there is no participant state change, then just return min value
        case Nil => Future.successful(CantonTimestamp.MinValue)
        case some =>
          // list is ordered desc
          some.toList
            .findM { ts =>
              determinePermissionChange(ts, participantId)
                .map { case (pre, post) =>
                  pre.isActive && !post.isActive
                }
            }
            .flatMap {
              // found last point in time
              case Some(resTs) => Future.successful(resTs)
              // we found many transactions but no deactivation, keep on looking
              // this should only happen if we had 1000 times a participant state update that never deactivated
              // a node. unlikely but could happen
              case None if some.length == limit =>
                findDeactivationTsOfParticipantBefore(
                  // continue from lowest ts here
                  some.lastOption.getOrElse(CantonTimestamp.MinValue), // is never empty
                  participantId,
                )
              case None => Future.successful(CantonTimestamp.MinValue)
            }
      }
  }

  private def determinePermissionChange(
      timestamp: CantonTimestamp,
      participant: ParticipantId,
  ): Future[(ParticipantPermission, ParticipantPermission)] = {
    def get(ts: CantonTimestamp): Future[ParticipantPermission] =
      snapshot(ts)
        .findParticipantState(participant)
        .map(_.map(_.permission).getOrElse(ParticipantPermission.Disabled))
    // as topology timestamps are "as of exclusive", if we want to access the impact
    // of a topology tx, we need to look at ts + immediate successor
    val curF = get(timestamp.immediateSuccessor)
    val prevF = get(timestamp)
    curF.flatMap { cur => prevF.map { prev => (prev, cur) } }
  }

  private def snapshot(timestamp: CantonTimestamp): StoreBasedTopologySnapshot =
    new StoreBasedTopologySnapshot(
      timestamp,
      store,
      Map(),
      false,
      StoreBasedDomainTopologyClient.NoPackageDependencies,
      loggerFactory,
    )

}
