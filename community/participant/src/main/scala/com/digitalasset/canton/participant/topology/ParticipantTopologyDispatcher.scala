// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.topology

import akka.stream.Materializer
import cats.data.{EitherT, OptionT}
import cats.syntax.either.*
import cats.syntax.functor.*
import cats.syntax.parallel.*
import com.digitalasset.canton.config.{DomainTimeTrackerConfig, ProcessingTimeout}
import com.digitalasset.canton.crypto.Crypto
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.health.admin.data.TopologyQueueStatus
import com.digitalasset.canton.lifecycle.*
import com.digitalasset.canton.logging.pretty.PrettyPrinting
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.domain.SequencerConnectClient.TopologyRequestAddressX
import com.digitalasset.canton.participant.domain.{
  DomainRegistryError,
  ParticipantInitializeTopology,
  ParticipantInitializeTopologyX,
  RegisterTopologyTransactionHandleWithProcessor,
  SequencerBasedRegisterTopologyTransactionHandle,
  SequencerBasedRegisterTopologyTransactionHandleX,
}
import com.digitalasset.canton.participant.store.{
  SyncDomainPersistentState,
  SyncDomainPersistentStateOld,
  SyncDomainPersistentStateX,
}
import com.digitalasset.canton.participant.sync.SyncDomainPersistentStateManagerImpl
import com.digitalasset.canton.protocol.StaticDomainParameters
import com.digitalasset.canton.protocol.messages.{
  DefaultOpenEnvelope,
  RegisterTopologyTransactionResponseResult,
}
import com.digitalasset.canton.sequencing.client.{SequencerClient, SequencerClientFactory}
import com.digitalasset.canton.sequencing.protocol.Batch
import com.digitalasset.canton.sequencing.{HandlerResult, SequencerConnection}
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.client.{DomainTopologyClient, DomainTopologyClientWithInit}
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.store.{
  TopologyStore,
  TopologyStoreCommon,
  TopologyStoreId,
  TopologyStoreX,
}
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import com.digitalasset.canton.topology.transaction.SignedTopologyTransactionX.GenericSignedTopologyTransactionX
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.topology.{
  DomainId,
  ParticipantId,
  TopologyManagerObserver,
  TopologyManagerX,
}
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.Thereafter.syntax.*
import com.digitalasset.canton.util.*
import com.digitalasset.canton.util.retry.RetryUtil.AllExnRetryable
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{DiscardOps, DomainAlias}
import io.functionmeta.functionFullName
import io.opentelemetry.api.trace.Tracer

import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import scala.collection.concurrent.TrieMap
import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future, Promise}

trait RegisterTopologyTransactionHandleCommon[TX] extends FlagCloseable {
  def submit(
      transactions: Seq[TX]
  ): FutureUnlessShutdown[Seq[
    // TODO(#11255): Switch to RegisterTopologyTransactionResponseResultX once non-proto version exists
    RegisterTopologyTransactionResponseResult.State
  ]]
}

trait TopologyDispatcherCommon extends NamedLogging with FlagCloseable

trait ParticipantTopologyDispatcherHandle {

  /** Signal domain connection such that we resume topology transaction dispatching
    *
    * When connecting / reconnecting to a domain, we will first attempt to push out all
    * pending topology transactions until we have caught up with the authorized store.
    *
    * This will guarantee that all parties known on this participant are active once the domain
    * is marked as ready to process transactions.
    */
  def domainConnected()(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, DomainRegistryError, Unit]

  def processor: Traced[Seq[DefaultOpenEnvelope]] => HandlerResult

}

trait ParticipantTopologyDispatcherCommon extends TopologyDispatcherCommon {

  def trustDomain(domainId: DomainId, parameters: StaticDomainParameters)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit]
  def onboardToDomain(
      domainId: DomainId,
      topologyRequestAddress: Option[TopologyRequestAddressX],
      alias: DomainAlias,
      timeTrackerConfig: DomainTimeTrackerConfig,
      sequencerConnection: SequencerConnection,
      sequencerClientFactory: SequencerClientFactory,
      protocolVersion: ProtocolVersion,
  )(implicit
      executionContext: ExecutionContextExecutor,
      materializer: Materializer,
      tracer: Tracer,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, DomainRegistryError, Boolean]

  def awaitIdle(domain: DomainAlias, timeout: Duration)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, DomainRegistryError, Boolean]

  def domainDisconnected(domain: DomainAlias)(implicit traceContext: TraceContext): Unit

  def createHandler(
      domain: DomainAlias,
      domainId: DomainId,
      topologyRequestAddress: Option[TopologyRequestAddressX],
      protocolVersion: ProtocolVersion,
      client: DomainTopologyClientWithInit,
      sequencerClient: SequencerClient,
  ): ParticipantTopologyDispatcherHandle

  def queueStatus: TopologyQueueStatus

}

abstract class ParticipantTopologyDispatcherImplCommon[S <: SyncDomainPersistentState](
    state: SyncDomainPersistentStateManagerImpl[S]
)(implicit
    val ec: ExecutionContext
) extends ParticipantTopologyDispatcherCommon {

  /** map of active domain outboxes, i.e. where we are connected and actively try to push topology state onto the domains */
  private[topology] val domains = new TrieMap[DomainAlias, DomainOutboxCommon[?, ?, ?]]()

  def queueStatus: TopologyQueueStatus = {
    val (dispatcher, clients) = domains.values.foldLeft((0, 0)) { case ((disp, clts), outbox) =>
      (disp + outbox.queueSize, clts + outbox.targetClient.numPendingChanges)
    }
    TopologyQueueStatus(
      manager = managerQueueSize,
      dispatcher = dispatcher,
      clients = clients,
    )
  }
  protected def managerQueueSize: Int

  override def domainDisconnected(
      domain: DomainAlias
  )(implicit traceContext: TraceContext): Unit = {
    domains.remove(domain) match {
      case Some(outbox) =>
        outbox.close()
      case None =>
        logger.debug(s"Topology pusher already disconnected from $domain")
    }
  }

  override def awaitIdle(domain: DomainAlias, timeout: Duration)(implicit
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

  protected def getState(domainId: DomainId)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, DomainRegistryError, S] =
    EitherT
      .fromEither[FutureUnlessShutdown](
        state
          .get(domainId)
          .toRight(
            DomainRegistryError.DomainRegistryInternalError
              .InvalidState("No persistent state for domain")
          )
      )

}

/** Identity dispatcher, registering identities with a domain
  *
  * The dispatcher observes the participant topology manager and tries to shovel
  * new topology transactions added to the manager to all connected domains.
  */
class ParticipantTopologyDispatcher(
    val manager: ParticipantTopologyManager,
    participantId: ParticipantId,
    state: SyncDomainPersistentStateManagerImpl[SyncDomainPersistentStateOld],
    crypto: Crypto,
    clock: Clock,
    override protected val timeouts: ProcessingTimeout,
    val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends ParticipantTopologyDispatcherImplCommon[SyncDomainPersistentStateOld](state) {

  override protected def managerQueueSize: Int = manager.queueSize

  // connect to manager
  manager.addObserver(new ParticipantTopologyManagerObserver {
    override def addedNewTransactions(
        timestamp: CantonTimestamp,
        transactions: Seq[SignedTopologyTransaction[TopologyChangeOp]],
    )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
      val num = transactions.size
      domains.values.toList
        .parTraverse(_.newTransactionsAddedToAuthorizedStore(timestamp, num))
        .map(_ => ())
    }
  })

  override def createHandler(
      domain: DomainAlias,
      domainId: DomainId,
      maybeTopologyRequestAddress: Option[TopologyRequestAddressX],
      protocolVersion: ProtocolVersion,
      client: DomainTopologyClientWithInit,
      sequencerClient: SequencerClient,
  ): ParticipantTopologyDispatcherHandle = new ParticipantTopologyDispatcherHandle {

    val handle = new SequencerBasedRegisterTopologyTransactionHandle(
      (traceContext, env) =>
        sequencerClient.sendAsync(
          Batch(List(env), protocolVersion)
        )(traceContext),
      domainId,
      participantId,
      participantId,
      protocolVersion,
      timeouts,
      loggerFactory,
    )

    override def domainConnected()(implicit
        traceContext: TraceContext
    ): EitherT[FutureUnlessShutdown, DomainRegistryError, Unit] =
      getState(domainId)
        .flatMap { state =>
          val outbox = new DomainOutbox(
            domain,
            domainId,
            protocolVersion,
            handle,
            client,
            manager.store,
            state.topologyStore,
            timeouts,
            loggerFactory.appendUnnamedKey("domain", domain.unwrap),
            crypto,
          )
          ErrorUtil.requireState(
            !domains.contains(domain),
            s"topology pusher for $domain already exists",
          )
          domains += domain -> outbox
          outbox.startup()
        }

    override def processor: Traced[Seq[DefaultOpenEnvelope]] => HandlerResult = handle.processor

  }

  override def trustDomain(domainId: DomainId, parameters: StaticDomainParameters)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] =
    manager.issueParticipantDomainStateCert(participantId, domainId, parameters.protocolVersion)

  override def onboardToDomain(
      domainId: DomainId,
      topologyRequestAddressUnusedInNonX: Option[TopologyRequestAddressX],
      alias: DomainAlias,
      timeTrackerConfig: DomainTimeTrackerConfig,
      sequencerConnection: SequencerConnection,
      sequencerClientFactory: SequencerClientFactory,
      protocolVersion: ProtocolVersion,
  )(implicit
      executionContext: ExecutionContextExecutor,
      materializer: Materializer,
      tracer: Tracer,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, DomainRegistryError, Boolean] = {
    require(
      topologyRequestAddressUnusedInNonX.isEmpty,
      s"Topology request address should never be set in daml 2.*, but instead found ${topologyRequestAddressUnusedInNonX}",
    )
    getState(domainId).flatMap { state =>
      (new ParticipantInitializeTopology(
        domainId,
        alias,
        participantId,
        manager.store,
        state.topologyStore,
        clock,
        timeTrackerConfig,
        timeouts,
        loggerFactory.appendUnnamedKey("onboarding", alias.unwrap),
        sequencerClientFactory,
        sequencerConnection,
        crypto,
        protocolVersion,
      )).run()
    }
  }
}

class ParticipantTopologyDispatcherX(
    val manager: TopologyManagerX,
    participantId: ParticipantId,
    state: SyncDomainPersistentStateManagerImpl[SyncDomainPersistentStateX],
    crypto: Crypto,
    clock: Clock,
    override protected val timeouts: ProcessingTimeout,
    val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends ParticipantTopologyDispatcherImplCommon[SyncDomainPersistentStateX](state) {

  override protected def managerQueueSize: Int = manager.queueSize

  // connect to manager
  manager.addObserver(new TopologyManagerObserver {
    override def addedNewTransactions(
        timestamp: CantonTimestamp,
        transactions: Seq[SignedTopologyTransactionX[TopologyChangeOpX, TopologyMappingX]],
    )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
      val num = transactions.size
      domains.values.toList
        .parTraverse(_.newTransactionsAddedToAuthorizedStore(timestamp, num))
        .map(_ => ())
    }
  })

  override def trustDomain(domainId: DomainId, parameters: StaticDomainParameters)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] = {
    def alreadyTrusted(
        state: SyncDomainPersistentStateX
    ): EitherT[FutureUnlessShutdown, String, Boolean] =
      for {
        alreadyTrusted <- EitherT
          .right[String](
            performUnlessClosingF(functionFullName)(
              state.topologyStore
                .findPositiveTransactions(
                  asOf = CantonTimestamp.MaxValue,
                  asOfInclusive = true,
                  isProposal = false,
                  types = Seq(DomainTrustCertificateX.code),
                  filterUid = Some(Seq(participantId.uid)),
                  filterNamespace = None,
                )
                .map { res =>
                  val participantUid = participantId.uid
                  val domainUid = domainId.uid
                  res.toTopologyState.exists {
                    case DomainTrustCertificateX(`participantUid`, `domainUid`) => true
                    case _ => false
                  }
                }
            )
          )
      } yield alreadyTrusted
    def trustDomain(
        state: SyncDomainPersistentStateX
    ): EitherT[FutureUnlessShutdown, String, Unit] =
      performUnlessClosingEitherUSF(functionFullName)(
        manager
          .proposeAndAuthorize(
            TopologyChangeOpX.Replace,
            DomainTrustCertificateX(
              participantId.uid,
              domainId.uid,
            )(transferOnlyToGivenTargetDomains = false, targetDomains = Seq.empty),
            serial = None,
            // TODO(#11255) auto-determine signing keys
            signingKeys = Seq(participantId.uid.namespace.fingerprint),
            protocolVersion = state.protocolVersion,
            expectFullAuthorization = true,
            force = false,
          )
          // TODO(#11255) improve error handling
          .leftMap(_.cause)
      ).map(_ => ())
    // check if cert already exists
    val ret = for {
      state <- getState(domainId).leftMap(_.cause)
      have <- alreadyTrusted(state)
      _ <-
        if (have) EitherT.rightT[FutureUnlessShutdown, String](())
        else trustDomain(state)
    } yield ()
    ret
  }

  override def onboardToDomain(
      domainId: DomainId,
      maybeTopologyRequestAddress: Option[TopologyRequestAddressX],
      alias: DomainAlias,
      timeTrackerConfig: DomainTimeTrackerConfig,
      sequencerConnection: SequencerConnection,
      sequencerClientFactory: SequencerClientFactory,
      protocolVersion: ProtocolVersion,
  )(implicit
      executionContext: ExecutionContextExecutor,
      materializer: Materializer,
      tracer: Tracer,
      traceContext: TraceContext,
  ): EitherT[FutureUnlessShutdown, DomainRegistryError, Boolean] = {
    val topologyRequestAddress = maybeTopologyRequestAddress.getOrElse(
      throw new IllegalStateException("TopologyRequestAddressX needs to be set")
    )
    getState(domainId).flatMap { state =>
      (new ParticipantInitializeTopologyX(
        domainId,
        alias,
        topologyRequestAddress,
        participantId,
        manager.store,
        state.topologyStore,
        clock,
        timeTrackerConfig,
        timeouts,
        loggerFactory.appendUnnamedKey("onboarding", alias.unwrap),
        sequencerClientFactory,
        sequencerConnection,
        crypto,
        protocolVersion,
      )).run()
    }
  }

  override def createHandler(
      domain: DomainAlias,
      domainId: DomainId,
      maybeTopologyRequestAddress: Option[TopologyRequestAddressX],
      protocolVersion: ProtocolVersion,
      client: DomainTopologyClientWithInit,
      sequencerClient: SequencerClient,
  ): ParticipantTopologyDispatcherHandle = {
    val topologyRequestAddress = maybeTopologyRequestAddress.getOrElse(
      throw new IllegalStateException("TopologyRequestAddressX needs to be set")
    )
    new ParticipantTopologyDispatcherHandle {
      val handle = new SequencerBasedRegisterTopologyTransactionHandleX(
        (traceContext, env) =>
          sequencerClient.sendAsync(
            Batch(List(env), protocolVersion)
          )(traceContext),
        domainId,
        topologyRequestAddress,
        participantId,
        participantId,
        protocolVersion,
        timeouts,
        loggerFactory,
      )

      override def domainConnected()(implicit
          traceContext: TraceContext
      ): EitherT[FutureUnlessShutdown, DomainRegistryError, Unit] =
        getState(domainId)
          .flatMap { state =>
            val outbox = new DomainOutboxX(
              domain,
              domainId,
              protocolVersion,
              handle,
              client,
              manager.store,
              state.topologyStore,
              timeouts,
              loggerFactory.appendUnnamedKey("domain", domain.unwrap),
              crypto,
            )
            ErrorUtil.requireState(
              !domains.contains(domain),
              s"topology pusher for $domain already exists",
            )
            domains += domain -> outbox
            outbox.startup()
          }

      override def processor: Traced[Seq[DefaultOpenEnvelope]] => HandlerResult = handle.processor

    }
  }

}

private class DomainOutbox(
    domain: DomainAlias,
    domainId: DomainId,
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

private class DomainOutboxX(
    domain: DomainAlias,
    domainId: DomainId,
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
}

private abstract class DomainOutboxCommon[
    TX,
    +H <: RegisterTopologyTransactionHandleCommon[TX],
    +DTS <: TopologyStoreCommon[TopologyStoreId.DomainStore, ?, ?, TX],
](
    domain: DomainAlias,
    val domainId: DomainId,
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
  ): EitherT[FutureUnlessShutdown, DomainRegistryError, Unit] = {
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
  ): EitherT[FutureUnlessShutdown, DomainRegistryError, Unit] = {
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
          _ <- EitherT.right[DomainRegistryError](
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
    val handle: RegisterTopologyTransactionHandleCommon[GenericSignedTopologyTransaction],
    val authorizedStore: TopologyStore[TopologyStoreId.AuthorizedStore],
    val targetStore: TopologyStore[TopologyStoreId.DomainStore],
    val timeouts: ProcessingTimeout,
    val loggerFactory: NamedLoggerFactory,
    override protected val crypto: Crypto,
) extends DomainOutboxDispatch[
      GenericSignedTopologyTransaction,
      RegisterTopologyTransactionHandleCommon[GenericSignedTopologyTransaction],
      TopologyStore[
        TopologyStoreId.DomainStore
      ],
    ]
    with DomainOutboxDispatchHelperOld {

  private def run()(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, DomainRegistryError, Boolean] = (for {
    initialTransactions <- loadInitialTransactionsFromStore()
    _ = logger.debug(
      s"Sending ${initialTransactions.size} onboarding transactions to ${domain}"
    )
    result <- dispatch(domain, initialTransactions)
  } yield {
    result.forall(res => RegisterTopologyTransactionResponseResult.State.isExpectedState(res))
  }).thereafter { _ =>
    close()
  }

  private def loadInitialTransactionsFromStore()(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, DomainRegistryError, Seq[GenericSignedTopologyTransaction]] =
    for {
      candidates <- EitherT.right(
        performUnlessClosingUSF(functionFullName)(
          authorizedStore
            .findParticipantOnboardingTransactions(participantId, domainId)
        )
      )
      applicablePossiblyPresent <- EitherT.right(
        performUnlessClosingF(functionFullName)(onlyApplicable(candidates))
      )
      _ <- EitherT.fromEither[FutureUnlessShutdown](initializedWith(applicablePossiblyPresent))
      applicable <- EitherT.right(
        performUnlessClosingF(functionFullName)(notAlreadyPresent(applicablePossiblyPresent))
      )
      // Try to convert if necessary the topology transactions for the required protocol version of the domain
      convertedTxs <- performUnlessClosingEitherU(functionFullName) {
        convertTransactions(applicable)
      }
    } yield convertedTxs

  private def initializedWith(
      initial: Seq[GenericSignedTopologyTransaction]
  )(implicit traceContext: TraceContext): Either[DomainRegistryError, Unit] = {
    val (haveEncryptionKey, haveSigningKey) =
      initial.map(_.transaction.element.mapping).foldLeft((false, false)) {
        case ((haveEncryptionKey, haveSigningKey), OwnerToKeyMapping(`participantId`, key)) =>
          (haveEncryptionKey || !key.isSigning, haveSigningKey || key.isSigning)
        case (acc, _) => acc
      }
    if (!haveEncryptionKey) {
      Left(
        DomainRegistryError.DomainRegistryInternalError.InitialOnboardingError(
          "Can not onboard as local participant doesn't have a valid encryption key"
        )
      )
    } else if (!haveSigningKey) {
      Left(
        DomainRegistryError.DomainRegistryInternalError.InitialOnboardingError(
          "Can not onboard as local participant doesn't have a valid signing key"
        )
      )
    } else Right(())
  }

}

object DomainOnboardingOutbox {
  def initiateOnboarding(
      domain: DomainAlias,
      domainId: DomainId,
      protocolVersion: ProtocolVersion,
      participantId: ParticipantId,
      handle: RegisterTopologyTransactionHandleCommon[GenericSignedTopologyTransaction],
      authorizedStore: TopologyStore[TopologyStoreId.AuthorizedStore],
      targetStore: TopologyStore[TopologyStoreId.DomainStore],
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
      crypto: Crypto,
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, DomainRegistryError, Boolean] = {
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
      crypto,
    )
    outbox.run().transform { res =>
      outbox.close()
      res
    }
  }
}

/** Utility class to dispatch the initial set of onboarding transactions to a domain - X version
  *
  * Generally, when we onboard to a new domain, we only want to onboard with the minimal set of
  * topology transactions that are required to join a domain. Otherwise, if we e.g. have
  * registered one million parties and then subsequently roll a key, we'd send an enormous
  * amount of unnecessary topology transactions.
  */
private class DomainOnboardingOutboxX(
    domain: DomainAlias,
    val domainId: DomainId,
    val protocolVersion: ProtocolVersion,
    participantId: ParticipantId,
    val handle: RegisterTopologyTransactionHandleWithProcessor[GenericSignedTopologyTransactionX],
    val authorizedStore: TopologyStoreX[TopologyStoreId.AuthorizedStore],
    val targetStore: TopologyStoreX[TopologyStoreId.DomainStore],
    val timeouts: ProcessingTimeout,
    val loggerFactory: NamedLoggerFactory,
    override protected val crypto: Crypto,
) extends DomainOutboxDispatch[
      GenericSignedTopologyTransactionX,
      RegisterTopologyTransactionHandleWithProcessor[GenericSignedTopologyTransactionX],
      TopologyStoreX[TopologyStoreId.DomainStore],
    ]
    with DomainOutboxDispatchHelperX {

  private def run()(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, DomainRegistryError, Boolean] = (for {
    initialTransactions <- loadInitialTransactionsFromStore()
    _ = logger.debug(
      s"Sending ${initialTransactions.size} onboarding transactions to ${domain}"
    )
    result <- dispatch(domain, initialTransactions)
  } yield {
    result.forall(res => RegisterTopologyTransactionResponseResult.State.isExpectedState(res))
  }).thereafter { _ =>
    close()
  }

  private def loadInitialTransactionsFromStore()(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, DomainRegistryError, Seq[GenericSignedTopologyTransactionX]] =
    for {
      candidates <- EitherT.right(
        performUnlessClosingUSF(functionFullName)(
          authorizedStore
            .findParticipantOnboardingTransactions(participantId, domainId)
        )
      )
      applicablePossiblyPresent <- EitherT.right(
        performUnlessClosingF(functionFullName)(onlyApplicable(candidates))
      )
      _ <- EitherT.fromEither[FutureUnlessShutdown](initializedWith(applicablePossiblyPresent))
      applicable <- EitherT.right(
        performUnlessClosingF(functionFullName)(notAlreadyPresent(applicablePossiblyPresent))
      )
      // Try to convert if necessary the topology transactions for the required protocol version of the domain
      convertedTxs <- performUnlessClosingEitherU(functionFullName) {
        convertTransactions(applicable)
      }
    } yield convertedTxs

  private def initializedWith(
      initial: Seq[GenericSignedTopologyTransactionX]
  )(implicit traceContext: TraceContext): Either[DomainRegistryError, Unit] = {
    val (haveEncryptionKey, haveSigningKey) =
      initial.map(_.transaction.mapping).foldLeft((false, false)) {
        case ((haveEncryptionKey, haveSigningKey), okm @ OwnerToKeyMappingX(`participantId`, _)) =>
          (
            haveEncryptionKey || okm.keys.exists(!_.isSigning),
            haveSigningKey || okm.keys.exists(_.isSigning),
          )
        case (acc, _) => acc
      }
    if (!haveEncryptionKey) {
      Left(
        DomainRegistryError.DomainRegistryInternalError.InitialOnboardingError(
          "Can not onboard as local participant doesn't have a valid encryption key"
        )
      )
    } else if (!haveSigningKey) {
      Left(
        DomainRegistryError.DomainRegistryInternalError.InitialOnboardingError(
          "Can not onboard as local participant doesn't have a valid signing key"
        )
      )
    } else Right(())
  }

}

object DomainOnboardingOutboxX {
  def initiateOnboarding(
      domain: DomainAlias,
      domainId: DomainId,
      protocolVersion: ProtocolVersion,
      participantId: ParticipantId,
      handle: RegisterTopologyTransactionHandleWithProcessor[GenericSignedTopologyTransactionX],
      authorizedStore: TopologyStoreX[TopologyStoreId.AuthorizedStore],
      targetStore: TopologyStoreX[TopologyStoreId.DomainStore],
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
      crypto: Crypto,
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, DomainRegistryError, Boolean] = {
    val outbox = new DomainOnboardingOutboxX(
      domain,
      domainId,
      protocolVersion,
      participantId,
      handle,
      authorizedStore,
      targetStore,
      timeouts,
      loggerFactory.append("domain", domain.unwrap),
      crypto,
    )
    outbox.run().transform { res =>
      outbox.close()
      res
    }
  }
}

private sealed trait DomainOutboxDispatchStoreSpecific[TX] extends NamedLogging {
  protected def domainId: DomainId
  protected def protocolVersion: ProtocolVersion
  protected def crypto: Crypto

  protected def topologyTransaction(tx: TX): PrettyPrinting

  protected def filterTransactions(
      transactions: Seq[TX],
      predicate: TX => Future[Boolean],
  )(implicit executionContext: ExecutionContext): Future[Seq[TX]]

  protected def onlyApplicable(transactions: Seq[TX]): Future[Seq[TX]]

  protected def convertTransactions(transactions: Seq[TX])(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[Future, DomainRegistryError, Seq[TX]]
}

private sealed trait DomainOutboxDispatchHelperOld
    extends DomainOutboxDispatchStoreSpecific[GenericSignedTopologyTransaction] {
  def authorizedStore: TopologyStore[TopologyStoreId.AuthorizedStore]

  override protected def filterTransactions(
      transactions: Seq[GenericSignedTopologyTransaction],
      predicate: GenericSignedTopologyTransaction => Future[Boolean],
  )(implicit
      executionContext: ExecutionContext
  ): Future[Seq[GenericSignedTopologyTransaction]] =
    transactions.parFilterA(tx => predicate(tx))

  override protected def topologyTransaction(
      tx: GenericSignedTopologyTransaction
  ): PrettyPrinting = tx.transaction

  protected def onlyApplicable(
      transactions: Seq[GenericSignedTopologyTransaction]
  ): Future[Seq[GenericSignedTopologyTransaction]] = {
    def notAlien(tx: GenericSignedTopologyTransaction): Boolean = {
      val mapping = tx.transaction.element.mapping
      mapping match {
        case OwnerToKeyMapping(_: ParticipantId, _) => true
        case OwnerToKeyMapping(owner, _) => owner.uid == domainId.unwrap
        case _ => true
      }
    }

    def domainRestriction(tx: GenericSignedTopologyTransaction): Boolean =
      tx.transaction.element.mapping.restrictedToDomain.forall(_ == domainId)

    Future.successful(
      transactions.filter(x => notAlien(x) && domainRestriction(x))
    )
  }

  override protected def convertTransactions(
      transactions: Seq[GenericSignedTopologyTransaction]
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[Future, DomainRegistryError, Seq[GenericSignedTopologyTransaction]] = {
    transactions
      .parTraverse { tx =>
        if (tx.transaction.hasEquivalentVersion(protocolVersion)) {
          // Transaction already in the correct version, nothing to do here
          EitherT.rightT[Future, String](tx)
        } else {
          // First try to find if the topology transaction already exists in the correct version in the topology store
          OptionT(authorizedStore.findStoredForVersion(tx.transaction, protocolVersion))
            .map(_.transaction)
            .toRight("")
            .leftFlatMap { _ =>
              // We did not find a topology transaction with the correct version, so we try to convert and resign
              SignedTopologyTransaction.asVersion(tx, protocolVersion)(crypto)
            }
        }
      }
      .leftMap(DomainRegistryError.TopologyConversionError.Error(_))
  }

}

private sealed trait DomainOutboxDispatchHelperX
    extends DomainOutboxDispatchStoreSpecific[GenericSignedTopologyTransactionX] {
  def authorizedStore: TopologyStoreX[TopologyStoreId.AuthorizedStore]

  override protected def filterTransactions(
      transactions: Seq[GenericSignedTopologyTransactionX],
      predicate: GenericSignedTopologyTransactionX => Future[Boolean],
  )(implicit
      executionContext: ExecutionContext
  ): Future[Seq[GenericSignedTopologyTransactionX]] =
    transactions.parFilterA(tx => predicate(tx))

  override protected def topologyTransaction(
      tx: GenericSignedTopologyTransactionX
  ): PrettyPrinting = tx.transaction

  override protected def onlyApplicable(
      transactions: Seq[GenericSignedTopologyTransactionX]
  ): Future[Seq[GenericSignedTopologyTransactionX]] = {
    def notAlien(tx: GenericSignedTopologyTransactionX): Boolean = {
      val mapping = tx.transaction.mapping
      mapping match {
        case OwnerToKeyMappingX(_: ParticipantId, _) => true
        case OwnerToKeyMappingX(owner, _) => owner.uid == domainId.unwrap
        case _ => true
      }
    }

    def domainRestriction(tx: GenericSignedTopologyTransactionX): Boolean =
      tx.transaction.mapping.restrictedToDomain.forall(_ == domainId)

    Future.successful(
      transactions.filter(x => notAlien(x) && domainRestriction(x))
    )
  }

  override protected def convertTransactions(
      transactions: Seq[GenericSignedTopologyTransactionX]
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[Future, DomainRegistryError, Seq[GenericSignedTopologyTransactionX]] = {
    transactions
      .parTraverse { tx =>
        if (tx.transaction.hasEquivalentVersion(protocolVersion)) {
          // Transaction already in the correct version, nothing to do here
          EitherT.rightT[Future, String](tx)
        } else {
          // First try to find if the topology transaction already exists in the correct version in the topology store
          OptionT(authorizedStore.findStoredForVersion(tx.transaction, protocolVersion))
            .map(_.transaction)
            .toRight("")
            .leftFlatMap { _ =>
              // We did not find a topology transaction with the correct version, so we try to convert and resign
              SignedTopologyTransactionX.asVersion(tx, protocolVersion)(crypto)
            }
        }
      }
      .leftMap(DomainRegistryError.TopologyConversionError.Error(_))
  }

}

private trait DomainOutboxDispatch[
    TX,
    +H <: RegisterTopologyTransactionHandleCommon[TX],
    +TS <: TopologyStoreCommon[TopologyStoreId.DomainStore, ?, ?, TX],
] extends DomainOutboxDispatchStoreSpecific[TX]
    with NamedLogging
    with FlagCloseable {

  protected def targetStore: TS
  protected def handle: H

  // register handle close task
  // this will ensure that the handle is closed before the outbox, aborting any retries
  runOnShutdown(new RunOnShutdown {
    override def name: String = "close-handle"
    override def done: Boolean = handle.isClosing
    override def run(): Unit = Lifecycle.close(handle)(logger)
  })(TraceContext.empty)

  protected def notAlreadyPresent(
      transactions: Seq[TX]
  )(implicit
      traceContext: TraceContext,
      ec: ExecutionContext,
  ): Future[Seq[TX]] = {
    val doesNotAlreadyExistPredicate = (tx: TX) => targetStore.exists(tx).map(exists => !exists)
    filterTransactions(transactions, doesNotAlreadyExistPredicate)
  }

  protected def dispatch(
      domain: DomainAlias,
      transactions: Seq[TX],
  )(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): EitherT[FutureUnlessShutdown, DomainRegistryError, Seq[
    RegisterTopologyTransactionResponseResult.State
  ]] = if (transactions.isEmpty) EitherT.rightT(Seq.empty)
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
            handle.submit(transactions),
            s"Pushing topology transactions to $domain",
          )
        },
        AllExnRetryable,
      )
      .map { responses =>
        if (responses.length != transactions.length) {
          logger.error(
            s"Topology request contained ${transactions.length} txs, but I received responses for ${responses.length}"
          )
        }
        logger.debug(
          s"$domain responded the following for the given topology transactions: $responses"
        )
        val failedResponses =
          responses.zip(transactions).collect {
            case (RegisterTopologyTransactionResponseResult.State.Failed, tx) => tx
          }

        Either.cond(
          failedResponses.isEmpty,
          responses,
          s"The domain $domain failed the following topology transactions: $failedResponses",
        )
      }
    EitherT(ret).leftMap(DomainRegistryError.DomainRegistryInternalError.InitialOnboardingError(_))
  }
}

private final case class PendingTransactions[TX](
    transactions: Seq[TX],
    newWatermark: CantonTimestamp,
)
