// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology

import cats.data.EitherT
import cats.syntax.either.*
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.common.domain.{
  RegisterTopologyTransactionHandleCommon,
  SequencerBasedRegisterTopologyTransactionHandleX,
}
import com.digitalasset.canton.config.{ProcessingTimeout, TopologyXConfig}
import com.digitalasset.canton.crypto.Crypto
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.*
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.messages.RegisterTopologyTransactionResponseResult
import com.digitalasset.canton.sequencing.EnvelopeHandler
import com.digitalasset.canton.sequencing.client.SequencerClientSend
import com.digitalasset.canton.sequencing.protocol.Batch
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.client.{DomainTopologyClient, DomainTopologyClientWithInit}
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.store.TopologyStoreId.DomainStore
import com.digitalasset.canton.topology.store.{
  TopologyStore,
  TopologyStoreCommon,
  TopologyStoreId,
  TopologyStoreX,
}
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import com.digitalasset.canton.topology.transaction.SignedTopologyTransactionX.GenericSignedTopologyTransactionX
import com.digitalasset.canton.topology.transaction.{
  SignedTopologyTransactionX,
  TopologyChangeOpX,
  TopologyMappingX,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{DelayUtil, EitherTUtil, ErrorUtil, SingleUseCell}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{DiscardOps, DomainAlias}

import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Future, Promise}

class DomainOutbox(
    domain: DomainAlias,
    domainId: DomainId,
    memberId: Member,
    protocolVersion: ProtocolVersion,
    val handle: RegisterTopologyTransactionHandleCommon[GenericSignedTopologyTransaction],
    targetClient: DomainTopologyClientWithInit,
    val authorizedStore: TopologyStore[TopologyStoreId.AuthorizedStore],
    val targetStore: TopologyStore[TopologyStoreId.DomainStore],
    timeouts: ProcessingTimeout,
    loggerFactory: NamedLoggerFactory,
    crypto: Crypto,
    // TODO(#9270) clean up how we parameterize our nodes
    batchSize: Int = 100,
)(implicit ec: ExecutionContext)
    extends DomainOutboxCommon[
      GenericSignedTopologyTransaction,
      RegisterTopologyTransactionHandleCommon[GenericSignedTopologyTransaction],
      TopologyStore[TopologyStoreId.DomainStore],
    ](
      domain,
      domainId,
      memberId,
      protocolVersion,
      targetClient,
      timeouts,
      loggerFactory,
      crypto,
    )
    with DomainOutboxDispatchHelperOld {
  override protected def awaitTransactionObserved(
      client: DomainTopologyClient,
      transaction: GenericSignedTopologyTransaction,
      timeout: Duration,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Boolean] =
    TopologyStore.awaitTxObserved(targetClient, transaction, targetStore, timeout)

  override protected def findPendingTransactions(
      watermarks: Watermarks
  )(implicit
      traceContext: TraceContext
  ): Future[PendingTransactions[GenericSignedTopologyTransaction]] =
    authorizedStore
      .findDispatchingTransactionsAfter(
        timestampExclusive = watermarks.dispatched,
        limit = Some(batchSize),
      )
      .map(storedTransactions =>
        PendingTransactions(
          storedTransactions.result.map(_.transaction),
          storedTransactions.result.map(_.validFrom.value).fold(watermarks.dispatched)(_ max _),
        )
      )

  override def maxAuthorizedStoreTimestamp()(implicit
      traceContext: TraceContext
  ): Future[Option[(SequencedTime, EffectiveTime)]] =
    authorizedStore.timestamp(useStateStore =
      false // can't use TopologyStore.maxTimestamp which uses state-store
    )
}

class DomainOutboxX(
    domain: DomainAlias,
    domainId: DomainId,
    memberId: Member,
    protocolVersion: ProtocolVersion,
    val handle: RegisterTopologyTransactionHandleCommon[GenericSignedTopologyTransactionX],
    targetClient: DomainTopologyClientWithInit,
    val authorizedStore: TopologyStoreX[TopologyStoreId.AuthorizedStore],
    val targetStore: TopologyStoreX[TopologyStoreId.DomainStore],
    timeouts: ProcessingTimeout,
    loggerFactory: NamedLoggerFactory,
    crypto: Crypto,
    batchSize: Int = 100,
)(implicit ec: ExecutionContext)
    extends DomainOutboxCommon[
      GenericSignedTopologyTransactionX,
      RegisterTopologyTransactionHandleCommon[GenericSignedTopologyTransactionX],
      TopologyStoreX[TopologyStoreId.DomainStore],
    ](
      domain,
      domainId,
      memberId,
      protocolVersion,
      targetClient,
      timeouts,
      loggerFactory,
      crypto,
    )
    with DomainOutboxDispatchHelperX {
  override protected def awaitTransactionObserved(
      client: DomainTopologyClient,
      transaction: GenericSignedTopologyTransactionX,
      timeout: Duration,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Boolean] =
    TopologyStoreX.awaitTxObserved(targetClient, transaction, targetStore, timeout)

  override protected def findPendingTransactions(watermarks: Watermarks)(implicit
      traceContext: TraceContext
  ): Future[PendingTransactions[GenericSignedTopologyTransactionX]] =
    authorizedStore
      .findDispatchingTransactionsAfter(
        timestampExclusive = watermarks.dispatched,
        limit = Some(batchSize),
      )
      .map(storedTransactions =>
        PendingTransactions(
          storedTransactions.result.map(_.transaction),
          storedTransactions.result.map(_.validFrom.value).fold(watermarks.dispatched)(_ max _),
        )
      )

  override def maxAuthorizedStoreTimestamp()(implicit
      traceContext: TraceContext
  ): Future[Option[(SequencedTime, EffectiveTime)]] = authorizedStore.maxTimestamp()

  override protected def onClosed(): Unit = {
    Lifecycle.close(handle)(logger)
    super.onClosed()
  }
}

abstract class DomainOutboxCommon[
    TX,
    +H <: RegisterTopologyTransactionHandleCommon[TX],
    +DTS <: TopologyStoreCommon[TopologyStoreId.DomainStore, ?, ?, TX],
](
    domain: DomainAlias,
    val domainId: DomainId,
    val memberId: Member,
    val protocolVersion: ProtocolVersion,
    val targetClient: DomainTopologyClientWithInit,
    override protected val timeouts: ProcessingTimeout,
    val loggerFactory: NamedLoggerFactory,
    override protected val crypto: Crypto,
)(implicit val ec: ExecutionContext)
    extends DomainOutboxDispatch[TX, H, DTS] {

  def handle: H
  def targetStore: DTS

  runOnShutdown(new RunOnShutdown {
    override def name: String = "close-participant-topology-outbox"
    override def done: Boolean = idleFuture.get().forall(_.isCompleted)
    override def run(): Unit =
      idleFuture.get().foreach(_.trySuccess(UnlessShutdown.AbortedDueToShutdown))
  })(TraceContext.empty)

  protected def awaitTransactionObserved(
      client: DomainTopologyClient,
      transaction: TX,
      timeout: Duration,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Boolean]

  def awaitIdle(
      timeout: Duration
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Boolean] = {
    // first, we wait until the idle future is idle again
    // this is the case when we've updated the dispatching watermark such that
    // there are no more topology transactions to read
    idleFuture
      .get()
      .map(x => FutureUnlessShutdown(x.future))
      .getOrElse(FutureUnlessShutdown.unit)
      .flatMap { _ =>
        // now, we've left the last transaction that got dispatched to the domain
        // in the last dispatched reference. we can now wait on the domain topology client
        // and domain store for it to appear.
        // as the transactions get sent sequentially we know that once the last transaction is out
        // we are idle again.
        lastDispatched.get().fold(FutureUnlessShutdown.pure(true)) { last =>
          awaitTransactionObserved(targetClient, last, timeout)
        }
      }
  }

  protected case class Watermarks(
      queuedApprox: Int,
      running: Boolean,
      authorized: CantonTimestamp, // last time a transaction was added to the store
      dispatched: CantonTimestamp,
  ) {
    def updateAuthorized(updated: CantonTimestamp, queuedNum: Int): Watermarks = {
      val ret = copy(
        authorized = authorized.max(updated),
        queuedApprox = queuedApprox + queuedNum,
      )
      if (ret.hasPending) {
        idleFuture.updateAndGet {
          case None =>
            Some(Promise())
          case x => x
        }
      }
      ret
    }

    def hasPending: Boolean = authorized != dispatched

    def done(): Watermarks = {
      if (!hasPending) {
        idleFuture.getAndSet(None).foreach(_.trySuccess(UnlessShutdown.unit))
      }
      copy(running = false)
    }
    def setRunning(): Watermarks = {
      if (!running) {
        ensureIdleFutureIsSet()
      }
      copy(running = true)
    }

  }

  private val watermarks =
    new AtomicReference[Watermarks](
      Watermarks(
        0,
        false,
        CantonTimestamp.MinValue,
        CantonTimestamp.MinValue,
      )
    )
  private val initialized = new AtomicBoolean(false)

  /** a future we provide that gets fulfilled once we are done dispatching */
  private val idleFuture = new AtomicReference[Option[Promise[UnlessShutdown[Unit]]]](None)
  private val lastDispatched =
    new AtomicReference[Option[TX]](None)
  private def ensureIdleFutureIsSet(): Unit = idleFuture.updateAndGet {
    case None =>
      Some(Promise())
    case x => x
  }.discard

  def queueSize: Int = watermarks.get().queuedApprox

  def newTransactionsAddedToAuthorizedStore(
      asOf: CantonTimestamp,
      num: Int,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    watermarks.updateAndGet(_.updateAuthorized(asOf, num)).discard
    kickOffFlush()
    FutureUnlessShutdown.unit
  }

  def maxAuthorizedStoreTimestamp()(implicit
      traceContext: TraceContext
  ): Future[Option[(SequencedTime, EffectiveTime)]]

  def startup()(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, /*DomainRegistryError*/ String, Unit] = {
    val loadWatermarksF = performUnlessClosingF(functionFullName)(for {
      // find the current target watermark
      watermarkTsO <- targetStore.currentDispatchingWatermark
      watermarkTs = watermarkTsO.getOrElse(CantonTimestamp.MinValue)
      authorizedTsO <- maxAuthorizedStoreTimestamp()
      authorizedTs = authorizedTsO
        .map { case (_, effectiveTime) => effectiveTime.value }
        .getOrElse(CantonTimestamp.MinValue)
      // update cached watermark
    } yield {
      val cur = watermarks.updateAndGet { c =>
        val newAuthorized = c.authorized.max(authorizedTs)
        val newDispatched = c.dispatched.max(watermarkTs)
        val next = c.copy(
          // queuing statistics during startup will be a bit off, we just ensure that we signal that we have something in our queue
          // we might improve by querying the store, checking for the number of pending tx
          queuedApprox = if (newAuthorized == newDispatched) c.queuedApprox else c.queuedApprox + 1,
          authorized = newAuthorized,
          dispatched = newDispatched,
        )
        if (next.hasPending) ensureIdleFutureIsSet()
        next
      }
      logger.debug(
        s"Resuming dispatching, pending=${cur.hasPending}, authorized=${cur.authorized}, dispatched=${cur.dispatched}"
      )
      ()
    })
    for {
      // load current authorized timestamp and watermark
      _ <- EitherT.right(loadWatermarksF)
      // run initial flush
      _ <- flush(initialize = true)
    } yield ()
  }

  private def kickOffFlush()(implicit traceContext: TraceContext): Unit = {
    // It's fine to ignore shutdown because we do not await the future anyway.
    if (initialized.get()) {
      EitherTUtil.doNotAwait(flush().onShutdown(Either.unit), "domain outbox flusher")
    }
  }

  protected def findPendingTransactions(
      watermarks: Watermarks
  )(implicit traceContext: TraceContext): Future[PendingTransactions[TX]]

  private def flush(initialize: Boolean = false)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, /*DomainRegistryError*/ String, Unit] = {
    def markDone(delayRetry: Boolean = false): Unit = {
      val updated = watermarks.getAndUpdate(_.done())
      // if anything has been pushed in the meantime, we need to kick off a new flush
      if (updated.hasPending) {
        if (delayRetry) {
          // kick off new flush in the background
          DelayUtil.delay(functionFullName, 10.seconds, this).map(_ => kickOffFlush()).discard
        } else {
          kickOffFlush()
        }
      }
    }

    val cur = watermarks.getAndUpdate(_.setRunning())

    // only flush if we are not running yet
    if (cur.running) {
      EitherT.rightT(())
    } else {
      // mark as initialised (it's safe now for a concurrent thread to invoke flush as well)
      if (initialize)
        initialized.set(true)
      if (cur.hasPending) {
        val pendingAndApplicableF = performUnlessClosingF(functionFullName)(for {
          // find pending transactions
          pending <- findPendingTransactions(cur)
          // filter out applicable
          applicablePotentiallyPresent <- onlyApplicable(pending.transactions)
          // not already present
          applicable <- notAlreadyPresent(applicablePotentiallyPresent)
        } yield (pending, applicable))
        val ret = for {
          pendingAndApplicable <- EitherT.right(pendingAndApplicableF)
          (pending, applicable) = pendingAndApplicable
          _ = lastDispatched.set(applicable.lastOption)
          // Try to convert if necessary the topology transactions for the required protocol version of the domain
          convertedTxs <- performUnlessClosingEitherU(functionFullName) {
            convertTransactions(applicable)
          }
          // dispatch to domain
          responses <- dispatch(domain, transactions = convertedTxs)
          // update watermark according to responses
          _ <- EitherT.right[ /*DomainRegistryError*/ String](
            updateWatermark(pending, applicable, responses)
          )
        } yield ()
        ret.transform {
          case x @ Left(_) =>
            markDone(delayRetry = true)
            x
          case x @ Right(_) =>
            markDone()
            x
        }
      } else {
        markDone()
        EitherT.rightT(())
      }
    }
  }

  private def updateWatermark(
      found: PendingTransactions[TX],
      applicable: Seq[TX],
      responses: Seq[RegisterTopologyTransactionResponseResult.State],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    val valid = applicable.zipWithIndex.zip(responses).foldLeft(true) {
      case (valid, ((item, idx), response)) =>
        val expectedResult =
          RegisterTopologyTransactionResponseResult.State.isExpectedState(response)
        if (!expectedResult) {
          logger.warn(
            s"Topology transaction ${topologyTransaction(item)} got ${response}. Will not update watermark."
          )
          false
        } else valid
    }
    if (valid) {
      val newWatermark = found.newWatermark
      watermarks.updateAndGet { c =>
        c.copy(
          // this will ensure that we have a queue count of at least 1 during catchup
          queuedApprox =
            if (c.authorized != newWatermark)
              Math.max(c.queuedApprox - found.transactions.length, 1)
            else 0,
          dispatched = newWatermark,
        )
      }.discard
      performUnlessClosingF(functionFullName)(targetStore.updateDispatchingWatermark(newWatermark))
    } else {
      FutureUnlessShutdown.unit
    }
  }
}

class DomainOutboxXFactory(
    domainId: DomainId,
    memberId: Member,
    manager: TopologyManagerX,
    domainTopologyStore: TopologyStoreX[DomainStore],
    crypto: Crypto,
    topologyXConfig: TopologyXConfig,
    override val timeouts: ProcessingTimeout,
    override val loggerFactory: NamedLoggerFactory,
) extends FlagCloseable
    with NamedLogging {
  val outboxRef = new SingleUseCell[DomainOutboxX]

  def create(
      protocolVersion: ProtocolVersion,
      targetTopologyClient: DomainTopologyClientWithInit,
      sequencerClient: SequencerClientSend,
      clock: Clock,
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): (DomainOutboxCommon[?, ?, ?], EnvelopeHandler) = {
    outboxRef.get.foreach { outbox =>
      ErrorUtil.invalidState(s"DomainOutbox was already created. This is a bug.")
    }
    val handle = new SequencerBasedRegisterTopologyTransactionHandleX(
      (traceContext, env) =>
        sequencerClient.sendAsync(Batch(List(env), protocolVersion))(traceContext),
      domainId,
      memberId,
      memberId,
      maybeClockForRetries = Some(clock),
      topologyXConfig,
      protocolVersion,
      timeouts,
      loggerFactory,
    )
    val outbox =
      new DomainOutboxX(
        DomainAlias(domainId.uid.toLengthLimitedString),
        domainId,
        memberId = memberId,
        protocolVersion = protocolVersion,
        handle = handle,
        targetClient = targetTopologyClient,
        authorizedStore = manager.store,
        targetStore = domainTopologyStore,
        timeouts = timeouts,
        loggerFactory = loggerFactory,
        crypto = crypto,
      )
    outboxRef.putIfAbsent(outbox).discard
    manager.addObserver(new TopologyManagerObserver {
      override def addedNewTransactions(
          timestamp: CantonTimestamp,
          transactions: Seq[SignedTopologyTransactionX[TopologyChangeOpX, TopologyMappingX]],
      )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
        outbox.newTransactionsAddedToAuthorizedStore(timestamp, transactions.size)
      }
    })
    (outbox, handle.processor)
  }

  override protected def onClosed(): Unit =
    Lifecycle.close(outboxRef.get.toList: _*)(logger)
}

final case class PendingTransactions[TX](
    transactions: Seq[TX],
    newWatermark: CantonTimestamp,
)
