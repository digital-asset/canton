// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.topology

import cats.data.EitherT
import cats.syntax.traverse._
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.health.admin.data.TopologyQueueStatus
import com.digitalasset.canton.lifecycle.{
  FlagCloseable,
  FutureUnlessShutdown,
  Lifecycle,
  RunOnShutdown,
  UnlessShutdown,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.domain.DomainRegistryError
import com.digitalasset.canton.protocol.messages.RegisterTopologyTransactionResponse
import com.digitalasset.canton.topology.client.DomainTopologyClientWithInit
import com.digitalasset.canton.topology.store.{
  SignedTopologyTransactions,
  StoredTopologyTransaction,
  TopologyStore,
  TopologyStoreId,
}
import com.digitalasset.canton.topology.transaction.{
  OwnerToKeyMapping,
  SignedTopologyTransaction,
  TopologyChangeOp,
}
import com.digitalasset.canton.topology.{DomainId, ParticipantId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.Thereafter.syntax._
import com.digitalasset.canton.util.retry.RetryUtil.AllExnRetryable
import com.digitalasset.canton.util.{DelayUtil, ErrorUtil, FutureUtil, retry}
import com.digitalasset.canton.version.{ProtocolVersion, RepresentativeProtocolVersion}
import com.digitalasset.canton.{DiscardOps, DomainAlias}
import io.functionmeta.functionFullName

import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import scala.collection.concurrent.TrieMap
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future, Promise}

sealed trait ParticipantIdentityDispatcherError

trait RegisterTopologyTransactionHandle extends FlagCloseable {
  def submit(
      transactions: Seq[SignedTopologyTransaction[TopologyChangeOp]]
  ): FutureUnlessShutdown[Seq[RegisterTopologyTransactionResponse.Result]]
}

/** Identity dispatcher, registering identities with a domain
  *
  * The dispatcher observes the participant topology manager and tries to shovel
  * new topology transactions added to the manager to all connected domains.
  */
class ParticipantTopologyDispatcher(
    val manager: ParticipantTopologyManager,
    override protected val timeouts: ProcessingTimeout,
    val loggerFactory: NamedLoggerFactory,
)(implicit val ec: ExecutionContext)
    extends NamedLogging
    with FlagCloseable {

  def queueStatus: TopologyQueueStatus = {
    val (dispatcher, clients) = domains.values.foldLeft((0, 0)) { case ((disp, clts), outbox) =>
      (disp + outbox.queueSize, clts + outbox.targetClient.numPendingChanges)
    }
    TopologyQueueStatus(
      manager = manager.queueSize,
      dispatcher = dispatcher,
      clients = clients,
    )
  }

  // connect to manager
  manager.addObserver(new ParticipantTopologyManagerObserver {
    override def addedNewTransactions(
        timestamp: CantonTimestamp,
        transactions: Seq[SignedTopologyTransaction[TopologyChangeOp]],
    )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
      val num = transactions.size
      domains.values.toList
        .traverse(_.newTransactionsAddedToAuthorizedStore(timestamp, num))
        .map(_ => ())
    }
  })

  /** map of active domain outboxes, i.e. where we are connected and actively try to push topology state onto the domains */
  private val domains = new TrieMap[DomainAlias, DomainOutbox]()

  def domainDisconnected(domain: DomainAlias)(implicit traceContext: TraceContext): Unit = {
    domains.remove(domain) match {
      case Some(outbox) =>
        outbox.close()
      case None =>
        logger.debug(s"Topology pusher already disconnected from $domain")
    }
  }

  def awaitIdle(domain: DomainAlias, timeout: Duration)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, DomainRegistryError, Boolean] = {
    domains
      .get(domain)
      .fold(
        EitherT.leftT[FutureUnlessShutdown, Boolean](
          DomainRegistryError.DomainRegistryInternalError
            .InvalidState(
              "Can not await idle without the domain being connected"
            ): DomainRegistryError
        )
      )(x => EitherT.right[DomainRegistryError](x.awaitIdle(timeout)))
  }

  /** Signal domain connection such that we resume topology transaction dispatching
    *
    * When connecting / reconnecting to a domain, we will first attempt to push out all
    * pending topology transactions until we have caught up with the authorized store.
    *
    * This will guarantee that all parties known on this participant are active once the domain
    * is marked as ready to process transactions.
    */
  def domainConnected(
      domain: DomainAlias,
      domainId: DomainId,
      protocolVersion: ProtocolVersion,
      handle: RegisterTopologyTransactionHandle,
      client: DomainTopologyClientWithInit,
      targetStore: TopologyStore[TopologyStoreId.DomainStore],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, DomainRegistryError, Unit] = {

    val outbox = new DomainOutbox(
      domain,
      domainId,
      protocolVersion,
      handle,
      client,
      manager.store,
      targetStore,
      timeouts,
      loggerFactory.appendUnnamedKey("domain", domain.unwrap),
    )
    ErrorUtil.requireState(!domains.contains(domain), s"topology pusher for $domain already exists")
    domains += domain -> outbox
    outbox.startup()

  }

}

private class DomainOutbox(
    domain: DomainAlias,
    val domainId: DomainId,
    val protocolVersion: ProtocolVersion,
    val handle: RegisterTopologyTransactionHandle,
    val targetClient: DomainTopologyClientWithInit,
    authorizedStore: TopologyStore[TopologyStoreId.AuthorizedStore],
    val targetStore: TopologyStore[TopologyStoreId.DomainStore],
    override protected val timeouts: ProcessingTimeout,
    val loggerFactory: NamedLoggerFactory,
    // TODO(#9270) clean up how we parameterize our nodes
    batchSize: Int = 100,
)(implicit val ec: ExecutionContext)
    extends DomainOutboxDispatch {

  runOnShutdown(new RunOnShutdown {
    override def name: String = "close-participant-topology-outbox"
    override def done: Boolean = idleFuture.get().forall(_.isCompleted)
    override def run(): Unit =
      idleFuture.get().foreach(_.trySuccess(UnlessShutdown.AbortedDueToShutdown))
  })(TraceContext.empty)

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
          TopologyStore
            .awaitTxObserved(targetClient, last, targetStore, timeout)
        }
      }
  }

  private case class Watermarks(
      queuedApprox: Int,
      running: Boolean,
      authorized: CantonTimestamp, // last time a transaction was added to the store
      dispatched: CantonTimestamp,
  ) {
    def updateAuthorized(updated: CantonTimestamp, queuedNum: Int): Watermarks = {
      val newAuthorized = CantonTimestamp.max(authorized, updated)
      val ret = copy(
        authorized = newAuthorized,
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
    new AtomicReference[Option[SignedTopologyTransaction[TopologyChangeOp]]](None)
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

  def startup()(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, DomainRegistryError, Unit] = {
    val loadWatermarksF = performUnlessClosingF(functionFullName)(for {
      // find the current target watermark
      watermarkTsO <- targetStore.currentDispatchingWatermark
      watermarkTs = watermarkTsO.getOrElse(CantonTimestamp.MinValue)
      authorizedTsO <- authorizedStore.timestamp()
      authorizedTs = authorizedTsO
        .map { case (_, effectiveTime) => effectiveTime.value }
        .getOrElse(CantonTimestamp.MinValue)
      // update cached watermark
    } yield {
      val cur = watermarks.updateAndGet { c =>
        val newAuthorized = CantonTimestamp.max(c.authorized, authorizedTs)
        val newDispatched = CantonTimestamp.max(c.dispatched, watermarkTs)
        val next = c.copy(
          // queuing statistics during startup will be a bit off, we just ensure that we signal that we have something in our queue
          // we might improve by querying the store, checking for the number of pending tx
          queuedApprox = c.queuedApprox + (if (newAuthorized != newDispatched) 1 else 0),
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
      _ <- flush(initialize = true).leftMap[DomainRegistryError](
        DomainRegistryError.DomainRegistryInternalError.InitialOnboardingError(
          _
        )
      )
    } yield ()
  }

  private def kickOffFlush()(implicit traceContext: TraceContext): Unit = {
    // It's fine to ignore shutdown because we do not await the future anyway.
    if (initialized.get()) {
      FutureUtil.doNotAwait(flush().value.unwrap, "domain outbox flusher")
    }
  }

  private def findPendingTransactions(
      watermarks: Watermarks
  )(implicit traceContext: TraceContext): Future[Seq[StoredTopologyTransaction[TopologyChangeOp]]] =
    authorizedStore
      .findDispatchingTransactionsAfter(
        timestampExclusive = watermarks.dispatched,
        limit = Some(batchSize),
      )
      .map(_.result)

  private def flush(initialize: Boolean = false)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] = {
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
          applicablePotentiallyPresent <- onlyApplicable(
            SignedTopologyTransactions(pending.map(_.transaction))
          )
          // not already present
          applicable <- notAlreadyPresent(applicablePotentiallyPresent)
        } yield (pending, applicable))
        val ret = for {
          pendingAndApplicable <- EitherT.right(pendingAndApplicableF)
          (pending, applicable) = pendingAndApplicable
          _ = lastDispatched.set(applicable.result.lastOption)
          // dispatch to domain
          responses <- dispatch(domain, handle, transactions = applicable)
          // update watermark according to responses
          _ <- EitherT.right[String](updateWatermark(cur, pending, applicable, responses))
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
      current: Watermarks,
      found: Seq[StoredTopologyTransaction[TopologyChangeOp]],
      applicable: SignedTopologyTransactions[TopologyChangeOp],
      responses: Seq[RegisterTopologyTransactionResponse.Result],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    val valid = applicable.result.zipWithIndex.zip(responses).foldLeft(true) {
      case (valid, ((item, idx), response)) =>
        if (item.uniquePath.toProtoPrimitive != response.uniquePathProtoPrimitive) {
          logger.error(
            s"Error at ${idx}: Invalid response ${response} for transaction ${item.transaction}"
          )
          false
        } else valid
    }
    if (valid) {
      val newWatermark = found.map(_.validFrom.value).foldLeft(current.dispatched) { case (a, b) =>
        CantonTimestamp.max(a, b)
      }
      watermarks.updateAndGet { c =>
        c.copy(
          // this will ensure that we have a queue count of at least 1 during catchup
          queuedApprox =
            if (c.authorized != newWatermark) Math.max(c.queuedApprox - found.length, 1) else 0,
          dispatched = newWatermark,
        )
      }.discard
      performUnlessClosingF(functionFullName)(targetStore.updateDispatchingWatermark(newWatermark))
    } else {
      FutureUnlessShutdown.unit
    }
  }
}

/** Utility class to dispatch the initial set of onboarding transactions to a domain
  *
  * Generally, when we onboard to a new domain, we only want to onboard with the minimal set of
  * topology transactions that are required to join a domain. Otherwise, if we e.g. have
  * registered one million parties and then subsequently roll a key, we'd send an enormous
  * amount of unnecessary topology transactions.
  */
private class DomainOnboardingOutbox(
    domain: DomainAlias,
    val domainId: DomainId,
    val protocolVersion: ProtocolVersion,
    participantId: ParticipantId,
    val handle: RegisterTopologyTransactionHandle,
    authorizedStore: TopologyStore[TopologyStoreId.AuthorizedStore],
    val targetStore: TopologyStore[TopologyStoreId.DomainStore],
    val timeouts: ProcessingTimeout,
    val loggerFactory: NamedLoggerFactory,
) extends DomainOutboxDispatch {

  private def run()(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, String, Unit] = (for {
    initialTransactions <- loadInitialTransactionsFromStore()
    _ = logger.debug(
      s"Sending ${initialTransactions.size} onboarding transactions to ${domain}"
    )
    _ <- dispatch(domain, handle, initialTransactions)

  } yield ()).thereafter { _ =>
    close()
  }

  private def loadInitialTransactionsFromStore()(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, String, SignedTopologyTransactions[TopologyChangeOp]] =
    for {
      candidates <- EitherT.right(
        performUnlessClosingF(functionFullName)(
          authorizedStore
            .findParticipantOnboardingTransactions(participantId, domainId)
            .map(SignedTopologyTransactions(_))
        )
      )
      applicablePossiblyPresent <- EitherT.right(
        performUnlessClosingF(functionFullName)(onlyApplicable(candidates))
      )
      _ <- EitherT.fromEither[FutureUnlessShutdown](initializedWith(applicablePossiblyPresent))
      applicable <- EitherT.right(
        performUnlessClosingF(functionFullName)(notAlreadyPresent(applicablePossiblyPresent))
      )
    } yield applicable

  private def initializedWith(
      initial: SignedTopologyTransactions[TopologyChangeOp]
  ): Either[String, Unit] = {
    val (haveEncryptionKey, haveSigningKey) =
      initial.result.map(_.transaction.element.mapping).foldLeft((false, false)) {
        case ((haveEncryptionKey, haveSigningKey), OwnerToKeyMapping(`participantId`, key)) =>
          (haveEncryptionKey || !key.isSigning, haveSigningKey || key.isSigning)
        case (acc, _) => acc
      }
    if (!haveEncryptionKey) {
      Left("Can not onboard as local participant doesn't have a valid encryption key")
    } else if (!haveSigningKey) {
      Left("Can not onboard as local participant doesn't have a valid signing key")
    } else Right(())
  }

}

object DomainOnboardingOutbox {
  def initiateOnboarding(
      domain: DomainAlias,
      domainId: DomainId,
      protocolVersion: ProtocolVersion,
      participantId: ParticipantId,
      handle: RegisterTopologyTransactionHandle,
      authorizedStore: TopologyStore[TopologyStoreId.AuthorizedStore],
      targetStore: TopologyStore[TopologyStoreId.DomainStore],
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, String, Unit] = {
    val outbox = new DomainOnboardingOutbox(
      domain,
      domainId,
      protocolVersion,
      participantId,
      handle,
      authorizedStore,
      targetStore,
      timeouts,
      loggerFactory.append("domain", domain.unwrap),
    )
    outbox.run().transform { res =>
      outbox.close()
      res
    }
  }
}

private trait DomainOutboxDispatch extends NamedLogging with FlagCloseable {

  protected def domainId: DomainId
  protected def protocolVersion: ProtocolVersion
  protected def targetStore: TopologyStore[TopologyStoreId.DomainStore]
  protected def handle: RegisterTopologyTransactionHandle

  private lazy val signedTxProtocolVersionRepresentative: RepresentativeProtocolVersion =
    SignedTopologyTransaction.protocolVersionRepresentativeFor(protocolVersion)

  // register handle close task
  // this will ensure that the handle is closed before the outbox, aborting any retries
  runOnShutdown(new RunOnShutdown {
    override def name: String = "close-handle"
    override def done: Boolean = handle.isClosing
    override def run(): Unit = Lifecycle.close(handle)(logger)
  })(TraceContext.empty)

  protected def notAlreadyPresent(
      transactions: SignedTopologyTransactions[TopologyChangeOp]
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): Future[SignedTopologyTransactions[TopologyChangeOp]] = {
    val doesNotAlreadyExistPredicate = (tx: SignedTopologyTransaction[TopologyChangeOp]) =>
      targetStore.exists(tx).map(exists => !exists)

    transactions.filter(doesNotAlreadyExistPredicate)
  }

  protected def onlyApplicable(
      transactions: SignedTopologyTransactions[TopologyChangeOp]
  ): Future[SignedTopologyTransactions[TopologyChangeOp]] = {
    def notAlien(tx: SignedTopologyTransaction[TopologyChangeOp]): Boolean = {
      val mapping = tx.transaction.element.mapping
      mapping match {
        case OwnerToKeyMapping(_: ParticipantId, _) => true
        case OwnerToKeyMapping(owner, _) => owner.uid == domainId.unwrap
        case _ => true
      }
    }

    def domainRestriction(tx: SignedTopologyTransaction[TopologyChangeOp]): Boolean =
      tx.transaction.element.mapping.restrictedToDomain.forall(_ == domainId)

    def protocolVersionRestriction(tx: SignedTopologyTransaction[TopologyChangeOp]): Boolean =
      tx.representativeProtocolVersion == signedTxProtocolVersionRepresentative

    Future.successful(
      transactions.filter(x => notAlien(x) && domainRestriction(x) && protocolVersionRestriction(x))
    )
  }

  protected def dispatch(
      domain: DomainAlias,
      handle: RegisterTopologyTransactionHandle,
      transactions: SignedTopologyTransactions[TopologyChangeOp],
  )(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, String, Seq[RegisterTopologyTransactionResponse.Result]] = if (
    transactions.isEmpty
  ) EitherT.rightT(Seq.empty)
  else {
    implicit val success = retry.Success.always
    val ret = retry
      .Backoff(
        logger,
        this,
        timeouts.unbounded.retries(1.second),
        1.second,
        10.seconds,
        "push topology transaction",
      )
      .unlessShutdown(
        {
          logger.debug(s"Attempting to push ${transactions.size} topology transactions to $domain")
          FutureUtil.logOnFailureUnlessShutdown(
            handle.submit(transactions.result),
            s"Pushing topology transactions to $domain",
          )
        },
        AllExnRetryable,
      )
      .map { responses =>
        logger.debug(
          s"$domain responded the following for the given topology transactions: $responses"
        )
        val failedResponses = responses.collect {
          case failedResponse @ RegisterTopologyTransactionResponse.Result(
                _,
                RegisterTopologyTransactionResponse.State.Failed(_),
              ) =>
            failedResponse
        }

        Either.cond(
          failedResponses.isEmpty,
          responses,
          s"The domain $domain failed the following topology transactions: $failedResponses",
        )
      }
    EitherT(ret)
  }
}
