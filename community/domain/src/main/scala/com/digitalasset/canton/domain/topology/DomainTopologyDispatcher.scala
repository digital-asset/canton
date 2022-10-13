// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.topology

import cats.data.EitherT
import cats.syntax.foldable.*
import cats.syntax.traverseFilter.*
import com.daml.error.{ErrorCategory, ErrorCode, Explanation, Resolution}
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.*
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
import com.digitalasset.canton.sequencing.client.*
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.time.{Clock, DomainTimeTracker}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.topology.client.{
  DomainTopologyClient,
  DomainTopologyClientWithInit,
  StoreBasedDomainTopologyClient,
  StoreBasedTopologySnapshot,
}
import com.digitalasset.canton.topology.processing.{
  ApproximateTime,
  EffectiveTime,
  SequencedTime,
  TopologyTransactionProcessingSubscriber,
  TopologyTransactionProcessor,
}
import com.digitalasset.canton.topology.store.{
  StoredTopologyTransaction,
  StoredTopologyTransactions,
  TopologyStore,
  TopologyStoreId,
}
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.tracing.{BatchTracing, TraceContext, Traced}
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.{EitherTUtil, ErrorUtil, FutureUtil}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{DiscardOps, SequencerCounter, checked}
import io.functionmeta.functionFullName

import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicReference}
import scala.collection.mutable
import scala.concurrent.duration.*
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
  * might change its state (i.e. become online for the first time or resume operation after being disabled).
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
private[domain] class DomainTopologyDispatcher(
    domainId: DomainId,
    authorizedStore: TopologyStore[TopologyStoreId.AuthorizedStore],
    processor: TopologyTransactionProcessor,
    initialKeys: Map[KeyOwner, Seq[PublicKey]],
    targetStore: TopologyStore[TopologyStoreId.DomainStore],
    crypto: Crypto,
    clock: Clock,
    addressSequencerAsDomainMember: Boolean,
    parameters: DomainNodeParameters,
    futureSupervisor: FutureSupervisor,
    sender: DomainTopologySender,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit val ec: ExecutionContext)
    extends NamedLogging
    with FlagCloseable
    with DomainIdentityStateObserver {

  override protected val timeouts: ProcessingTimeout = parameters.processingTimeouts

  def queueSize: Int = blocking { lock.synchronized { queue.size + inflight.get() } }

  private val topologyClient =
    new StoreBasedDomainTopologyClient(
      clock,
      domainId,
      authorizedStore,
      SigningPublicKey.collect(initialKeys),
      StoreBasedDomainTopologyClient.NoPackageDependencies,
      timeouts,
      futureSupervisor,
      loggerFactory,
      useStateTxs = false,
    )
  private val domainSyncCryptoClient = new DomainSyncCryptoClient(
    DomainTopologyManagerId(domainId.unwrap),
    domainId,
    topologyClient,
    crypto,
    parameters.cachingConfigs,
    timeouts,
    futureSupervisor,
    loggerFactory,
  )
  private val staticDomainMembers = DomainMember.list(domainId, addressSequencerAsDomainMember)
  private val flushing = new AtomicBoolean(false)
  private val queue = mutable.Queue[Traced[StoredTopologyTransaction[TopologyChangeOp]]]()
  private val lock = new Object()
  private val catchup = new MemberTopologyCatchup(authorizedStore, loggerFactory)
  private val lastTs = new AtomicReference(CantonTimestamp.MinValue)
  private val inflight = new AtomicInteger(0)

  private sealed trait DispatchStrategy extends Product with Serializable
  private case object Alone extends DispatchStrategy
  private case object Batched extends DispatchStrategy
  private case object Last extends DispatchStrategy

  private def dispatchStrategy(mapping: TopologyMapping): DispatchStrategy = mapping match {
    case _: ParticipantState | _: DomainParametersChange => Last
    case _: MediatorDomainState => Alone
    case _: IdentifierDelegation | _: NamespaceDelegation | _: OwnerToKeyMapping |
        _: PartyToParticipant | _: SignedLegalIdentityClaim | _: VettedPackages =>
      Batched
  }

  def init(
      flushSequencer: => FutureUnlessShutdown[Unit]
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    for {
      initiallyPending <- FutureUnlessShutdown.outcomeF(determinePendingTransactions())
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
            pending <- FutureUnlessShutdown.outcomeF(determinePendingTransactions())
          } yield pending
        } else FutureUnlessShutdown.pure(initiallyPending)
    } yield {
      queue.appendAll(actuallyPending)
      actuallyPending.lastOption match {
        case Some(ts) =>
          updateTopologyClientTs(ts.value.validFrom.value)
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
  )(implicit traceContext: TraceContext): Unit = performUnlessClosing(functionFullName) {
    val last = lastTs.getAndSet(timestamp)
    // we assume that we get txs in strict ascending timestamp order
    ErrorUtil.requireArgument(
      last < timestamp,
      s"received new topology txs with ts $timestamp which is not strictly higher than last $last",
    )
    // we assume that there is only one participant state or domain parameters change per timestamp
    // otherwise, we need to make the dispatching stuff a bit smarter and deal with "multiple participants could become active in this batch"
    // and we also can't deal with multiple replaces with the same timestamp
    val byStrategy = transactions.map(_.transaction.element.mapping).groupBy(dispatchStrategy).map {
      case (k, v) => (k, v.length)
    }
    val alone = byStrategy.getOrElse(Alone, 0)
    val batched = byStrategy.getOrElse(Batched, 0)
    val lst = byStrategy.getOrElse(Last, 0)
    ErrorUtil.requireArgument(
      (lst < 2 && alone == 0) || (alone < 2 && lst == 0 && batched == 0),
      s"received batch of topology transactions with multiple changes $byStrategy that can't be batched ${transactions}",
    )
    updateTopologyClientTs(timestamp)
    blocking {
      lock.synchronized {
        queue ++= transactions.iterator.map(x =>
          Traced(
            StoredTopologyTransaction(SequencedTime(timestamp), EffectiveTime(timestamp), None, x)
          )
        )
        flush()
      }
    }
  }.discard

  // subscribe to target
  processor.subscribe(new TopologyTransactionProcessingSubscriber {
    override def observed(
        sequencedTimestamp: SequencedTime,
        effectiveTimestamp: EffectiveTime,
        sc: SequencerCounter,
        transactions: Seq[SignedTopologyTransaction[TopologyChangeOp]],
    )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = FutureUnlessShutdown.pure {
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
    override def updateHead(
        effectiveTimestamp: EffectiveTime,
        approximateTimestamp: ApproximateTime,
        potentialTopologyChange: Boolean,
    )(implicit traceContext: TraceContext): Unit = {}
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
    * We send the entire queue up and including of the first `ParticipantState`, `MediatorDomainState` or `DomainParametersChange` update at once for efficiency reasons.
    */
  private def determineBatchFromQueue()
      : Seq[Traced[StoredTopologyTransaction[TopologyChangeOp]]] = {

    val items =
      queue.dequeueWhile(tx =>
        dispatchStrategy(tx.value.transaction.transaction.element.mapping) == Batched
      )

    val includeNext = queue.headOption
      .map(tx => dispatchStrategy(tx.value.transaction.transaction.element.mapping))
      .exists {
        case Alone => items.isEmpty
        case Batched => true // shouldn't actually happen ...
        case Last => true
      }

    (if (includeNext)
       items :+ queue.dequeue()
     else items).toSeq
  }

  /** wait an epsilon if the effective time is reduced with this change */
  private def waitIfEffectiveTimeIsReduced(
      transactions: Traced[NonEmpty[Seq[StoredTopologyTransaction[TopologyChangeOp]]]]
  ): EitherT[FutureUnlessShutdown, String, Unit] = transactions.withTraceContext {
    implicit traceContext => txs =>
      val empty = EitherT.rightT[FutureUnlessShutdown, String](())
      txs.last1.transaction.transaction.element.mapping match {
        case mapping: DomainParametersChange =>
          EitherT
            .right(
              performUnlessClosingF(functionFullName)(
                authorizedStoreSnapshot(txs.last1.validFrom.value).findDynamicDomainParameters()
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
        pending <- EitherT.right(performUnlessClosingF(functionFullName)(Future {
          blocking {
            lock.synchronized {
              val tmp = determineBatchFromQueue()
              inflight.updateAndGet(_ + tmp.size).discard
              tmp
            }
          }
        }))
        tracedTxO = NonEmpty.from(pending).map { tracedNE =>
          BatchTracing.withTracedBatch(logger, tracedNE)(implicit traceContext =>
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
        NonEmpty[Seq[StoredTopologyTransaction[TopologyChangeOp]]]
      ]
  ): EitherT[FutureUnlessShutdown, String, Unit] = {
    tracedTransaction withTraceContext { implicit traceContext => transactions =>
      val flushToParticipantET: EitherT[FutureUnlessShutdown, String, Option[ParticipantId]] =
        transactions.last1.transaction.transaction.element.mapping match {
          case ParticipantState(_, _, participant, _, _) =>
            for {
              catchupForParticipant <- EitherT.right(
                performUnlessClosingF(functionFullName)(
                  catchup
                    .determineCatchupForParticipant(
                      transactions.head1.validFrom.value,
                      transactions.last1.validFrom.value,
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
                    authorizedCryptoSnapshot(transactions.head1.validFrom.value),
                    txs.toDomainTopologyTransactions,
                    Set(participant),
                  )
              }
            } yield catchupForParticipant.map(_ => participant)
          case _ =>
            EitherT.rightT(None)
        }
      val checkForMediatorActivationET: EitherT[FutureUnlessShutdown, String, Option[MediatorId]] =
        transactions.last1.transaction.transaction.element.mapping match {
          case MediatorDomainState(_, _, mediator) =>
            EitherT.right(
              performUnlessClosingF(functionFullName)(
                catchup
                  .determinePermissionChangeForMediator(
                    transactions.last1.validFrom.value,
                    mediator,
                  )
                  .map {
                    case (true, true) => None
                    case (false, true) => Some(mediator)
                    case (false, false) => None
                    case (true, false) =>
                      // TODO(#1251) implement catchup for mediator
                      logger.warn(
                        s"Mediator ${mediator} is deactivated and will miss out on topology transactions. This will break it"
                      )
                      None
                  }
              )
            )
          case _ =>
            EitherT.rightT(None)
        }
      for {
        includeParticipant <- flushToParticipantET
        includeMediator <- checkForMediatorActivationET
        _ <- sendTransactions(
          transactions,
          includeParticipant.toList ++ includeMediator.toList,
        )
        // update watermark, which we can as we successfully registered all transactions with the domain
        // we don't need to wait until they are processed
        _ <- EitherT.right(
          performUnlessClosingF(functionFullName)(
            targetStore.updateDispatchingWatermark(transactions.last1.validFrom.value)
          )
        )
      } yield ()
    }
  }

  private def sendTransactions(
      transactions: NonEmpty[Seq[StoredTopologyTransaction[TopologyChangeOp]]],
      add: Seq[Member],
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, String, Unit] = {
    val headSnapshot = authorizedStoreSnapshot(transactions.head1.validFrom.value)
    val receivingParticipantsF = performUnlessClosingF(functionFullName)(
      headSnapshot
        .participants()
        .map(_.collect {
          case (participantId, perm) if perm.isActive => participantId
        })
    )
    val mediatorsF = performUnlessClosingF(functionFullName)(headSnapshot.mediators())
    for {
      receivingParticipants <- EitherT.right(receivingParticipantsF)
      mediators <- EitherT.right(mediatorsF)
      _ <- sender.sendTransactions(
        authorizedCryptoSnapshot(transactions.head1.validFrom.value),
        transactions.map(_.transaction).toList,
        (receivingParticipants ++ staticDomainMembers ++ mediators ++ add).toSet,
      )
    } yield ()
  }

  override protected def onClosed(): Unit =
    Lifecycle.close(sender)(logger)

}

private[domain] object DomainTopologyDispatcher {
  def create(
      domainId: DomainId,
      domainTopologyManager: DomainTopologyManager,
      targetClient: DomainTopologyClientWithInit,
      processor: TopologyTransactionProcessor,
      initialKeys: Map[KeyOwner, Seq[PublicKey]],
      targetStore: TopologyStore[TopologyStoreId.DomainStore],
      client: SequencerClient,
      timeTracker: DomainTimeTracker,
      crypto: Crypto,
      clock: Clock,
      addressSequencerAsDomainMember: Boolean,
      parameters: DomainNodeParameters,
      futureSupervisor: FutureSupervisor,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): DomainTopologyDispatcher = {

    val logger = loggerFactory.getTracedLogger(DomainTopologyDispatcher.getClass)

    val sender = new DomainTopologySender.Impl(
      domainId,
      domainTopologyManager.protocolVersion,
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
      processor,
      initialKeys,
      targetStore,
      crypto,
      clock,
      addressSequencerAsDomainMember,
      parameters,
      futureSupervisor,
      sender,
      loggerFactory,
    )

    // schedule init on domain topology manager to avoid race conditions between
    // queue filling and store scanning. however, we can't do that synchronously here
    // as starting the domain might be kicked off by the domain manager (in manual init scenarios)
    FutureUtil.doNotAwait(
      domainTopologyManager.executeSequential(
        {
          for {
            _ <- dispatcher.init(flushSequencerWithTimeProof(timeTracker, targetClient))
          } yield {
            domainTopologyManager.addObserver(dispatcher)
          }
        }.onShutdown(
          logger.debug("Stopped dispatcher initialization due to shutdown")(
            TraceContext.empty
          )
        ),
        "initializing domain topology dispatcher",
      ),
      "domain topology manager sequential init failed",
    )(ErrorLoggingContext.fromTracedLogger(logger))

    dispatcher

  }

  private def flushSequencerWithTimeProof(
      timeTracker: DomainTimeTracker,
      targetClient: DomainTopologyClient,
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): FutureUnlessShutdown[Unit] = {
    for {
      // flush the sequencer client with a time-proof. this should ensure that we give the
      // topology processor time to catch up with any pending submissions
      //
      // This is not fool-proof as the time proof may be processed by a different sequencer replica
      // than where earlier transactions have been submitted and therefore overtake them.
      timestamp <- timeTracker.fetchTimeProof().map(_.timestamp)
      // wait until the topology client has seen this timestamp
      _ <- targetClient
        .awaitTimestampUS(timestamp, waitForEffectiveTime = false)
        .getOrElse(FutureUnlessShutdown.unit)
    } yield ()
  }

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
      protocolVersion: ProtocolVersion,
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
                    .create(batch.toList, snapshot, domainId, protocolVersion)
                    .leftMap(_.toString)
                    .mapK(FutureUnlessShutdown.outcomeK)
                _ <- ensureDelivery(
                  Batch(
                    List(
                      OpenEnvelope(message, nonEmptyRecipients, protocolVersion)
                    ),
                    protocolVersion,
                  ),
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
      performUnlessClosingF(functionFullName)(
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
          TopologyDispatchingInternalError.SendResultError(error).discard
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
              TopologyDispatchingInternalError.AsyncResultError(error).discard
              stopDispatching("Stopping due to an unexpected async result error")
              x
            case x @ Success(UnlessShutdown.Outcome(Right(_))) =>
              // nice, the sequencer seems to be accepting our request
              x
            case x @ Success(UnlessShutdown.AbortedDueToShutdown) =>
              abortDueToShutdown()
              x
            case x @ Failure(ex) =>
              TopologyDispatchingInternalError.UnexpectedException(ex).discard
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

class MemberTopologyCatchup(
    store: TopologyStore[TopologyStoreId.AuthorizedStore],
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
  ): Future[(ParticipantPermission, ParticipantPermission)] =
    determineChange(
      timestamp,
      _.findParticipantState(participant)
        .map(_.map(_.permission).getOrElse(ParticipantPermission.Disabled)),
    )

  private def determineChange[T](
      timestamp: CantonTimestamp,
      get: StoreBasedTopologySnapshot => Future[T],
  ): Future[(T, T)] = {
    // as topology timestamps are "as of exclusive", if we want to access the impact
    // of a topology tx, we need to look at ts + immediate successor
    val curF = get(snapshot(timestamp.immediateSuccessor))
    val prevF = get(snapshot(timestamp))
    prevF.zip(curF)
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

  def determinePermissionChangeForMediator(
      timestamp: CantonTimestamp,
      mediator: MediatorId,
  ): Future[(Boolean, Boolean)] = determineChange(timestamp, _.isMediatorActive(mediator))

}
