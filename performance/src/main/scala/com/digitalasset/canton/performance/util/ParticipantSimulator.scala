// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.performance.util

import cats.data.EitherT
import cats.syntax.functor.*
import com.daml.metrics.api.MetricsContext
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.admin.api.client.commands.{GrpcAdminCommand, TopologyAdminCommands}
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.config.{NonNegativeDuration, ProcessingTimeout}
import com.digitalasset.canton.console.{
  ConsoleEnvironment,
  LocalInstanceReference,
  SequencerReference,
}
import com.digitalasset.canton.crypto.{HashPurpose, SigningKeyUsage, SyncCryptoApi}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
import com.digitalasset.canton.lifecycle.{
  AsyncCloseable,
  FlagCloseable,
  FutureUnlessShutdown,
  LifeCycle,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging, NamedLoggingContext}
import com.digitalasset.canton.protocol.StaticSynchronizerParameters
import com.digitalasset.canton.protocol.messages.{
  AcsCommitment,
  SignedProtocolMessage,
  UnsignedProtocolMessage,
}
import com.digitalasset.canton.sequencing.SequencerAggregator.MessageAggregationConfig
import com.digitalasset.canton.sequencing.SequencerSubscriptionPool.SequencerSubscriptionPoolConfig
import com.digitalasset.canton.sequencing.client.{
  NoDelay,
  RichSequencerClientImpl,
  SequencedEventValidatorFactory,
  SequencerClientSubscriptionError,
}
import com.digitalasset.canton.sequencing.protocol.{
  Batch,
  ClosedEnvelope,
  MessageId,
  Recipients,
  SequencedEvent,
  SignedContent,
  SubmissionRequest,
}
import com.digitalasset.canton.sequencing.{
  GrpcSequencerConnectionXPoolFactory,
  PostAggregationHandlerImpl,
  SequencedSerializedEvent,
  SequencerAggregator,
  SequencerConnectionPoolDelays,
  SequencerConnectionXPool,
  SequencerConnections,
  SequencerSubscriptionPoolFactoryImpl,
  SequencerSubscriptionXFactoryImpl,
  SubmissionRequestAmplification,
  SubscriptionHandlerXFactoryImpl,
}
import com.digitalasset.canton.synchronizer.service.GrpcSequencerConnectionService
import com.digitalasset.canton.topology.admin.grpc.TopologyStoreId
import com.digitalasset.canton.topology.transaction.DelegationRestriction.CanSignAllMappings
import com.digitalasset.canton.topology.transaction.SignedTopologyTransaction.GenericSignedTopologyTransaction
import com.digitalasset.canton.topology.transaction.{
  HostingParticipant,
  NamespaceDelegation,
  OwnerToKeyMapping,
  ParticipantPermission,
  PartyToParticipant,
  SignedTopologyTransaction,
  SynchronizerTrustCertificate,
  TopologyChangeOp,
  TopologyMapping,
  TopologyTransaction,
  VettedPackage,
  VettedPackages,
}
import com.digitalasset.canton.topology.{
  ForceFlag,
  ForceFlags,
  Namespace,
  ParticipantId,
  PartyId,
  UniqueIdentifier,
}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.{
  EitherTUtil,
  FutureUnlessShutdownUtil,
  HasFlushFuture,
  MonadUtil,
  Mutex,
  SingleUseCell,
}
import com.digitalasset.canton.version.ParticipantProtocolFeatureFlags
import com.digitalasset.canton.{SynchronizerAlias, config as cfg}
import org.scalatest.OptionValues
import org.slf4j.event.Level

import java.util.concurrent.atomic.AtomicInteger
import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.concurrent.duration.*
import scala.concurrent.{Await, Future}

class ParticipantSimulatorController(
    val simulator: ParticipantSimulator,
    prefix: String,
    packagesToVet: Seq[VettedPackage],
    val loggerFactory: NamedLoggerFactory,
)(implicit env: ConsoleEnvironment)
    extends NamedLogging {

  import env.environment.*
  private val participants = mutable.ArrayBuffer[ParticipantId]()
  private val notstarted = mutable.Queue[ParticipantId]()
  private val totalParties = new AtomicInteger(0)
  private val lock = new Mutex()

  val events = new TrieMap[ParticipantId, (CantonTimestamp, Long)]()
  def running: Int = participants.size - notstarted.size
  def defined: Int = participants.size
  def parties: Int = totalParties.get()

  private def synchronize[T](idx: Int, concurrent: PositiveInt)(
      call: Option[cfg.NonNegativeDuration] => T
  ): T =
    if ((idx % concurrent.value) == concurrent.value - 1) {
      call(Some(env.commandTimeouts.unbounded))
    } else {
      call(None)
    }

  def addParticipants(add: Int, concurrent: PositiveInt = PositiveInt.tryCreate(20)): Int =
    TraceContext.withNewTraceContext("lightweight") { implicit tc =>
      {
        val start = participants.size + 1
        Await.result(
          MonadUtil
            .parTraverseWithLimit(concurrent)((start until (add + start))) { idx =>
              simulator
                .createOnboardingTransactions(
                  s"vp-$idx",
                  packagesToVet,
                )
            }
            .flatMap { pidsAndTxs =>
              val pids = pidsAndTxs.map(_._1)
              val txs = pidsAndTxs
                .flatMap(_._2)
                // group into groups divisible by 3 so that the onboarding transactions aren't split into separate batches
                .grouped((concurrent.value / 3).max(1) * 3)
                .toList
                .zipWithIndex
              MonadUtil
                .sequentialTraverse_(txs) { case (txs, idx) =>
                  synchronize(idx, PositiveInt.three) { ss =>
                    simulator.loadTransactions(txs, ss)
                  }
                }
                .map { _ =>
                  pids
                }
            }
            .map { added =>
              lock.exclusive {
                notstarted.appendAll(added).discard
                participants.appendAll(added).discard
              }
              ()
            }
            .onShutdown(Right(()))
            .value,
          scala.concurrent.duration.Duration.Inf,
        )
      } match {
        case Left(value) => logger.error(s"Failed onboarding participants: $value")
        case Right(_) => ()
      }
      participants.size
    }

  def addParties(add: Int, concurrent: PositiveInt = PositiveInt.tryCreate(10)): Unit =
    TraceContext.withNewTraceContext("lightweight-parties") { implicit tc =>
      Await.result(
        MonadUtil
          .parTraverseWithLimit(concurrent)((0 until add)) { _ =>
            val idx = totalParties.incrementAndGet()
            val pid = participants(idx % participants.size)
            simulator.createPartyTx(s"$prefix$idx", pid)
          }
          .flatMap { txs =>
            val batches = txs.grouped(concurrent.value).zipWithIndex.toSeq
            MonadUtil.sequentialTraverse(batches) { case (txs, idx) =>
              synchronize(idx, PositiveInt.three)(ss => simulator.loadTransactions(txs, ss))
            }
          }
          .void
          .onShutdown(Right(()))
          .value,
        scala.concurrent.duration.Duration.Inf,
      ) match {
        case Left(value) => logger.error(s"Failed to add parties: $value")
        case Right(_) => ()
      }
    }

  final def startParticipants(num: Int, concurrent: PositiveInt = PositiveInt.tryCreate(5)): Int =
    TraceContext.withNewTraceContext("lightweight") { implicit tc =>
      Await.result(
        MonadUtil.parTraverseWithLimit_(concurrent)((0 until num)) { _ =>
          val pidO = lock.exclusive(Option.when(notstarted.nonEmpty)(notstarted.dequeue()))
          pidO
            .map { pid =>
              Future {
                env.environment.addUserCloseable(
                  simulator.startSubscriptionForParticipant(
                    pid,
                    ts => {
                      events
                        .updateWith(pid) {
                          case Some((_, value)) => Some((ts, value + 1L))
                          case None => Some((ts, 1L))
                        }
                        .discard
                    },
                  )
                )
              }
            }
            .getOrElse(Future.unit)
        },
        scala.concurrent.duration.Duration.Inf,
      )
      notstarted.size
    }
}

@SuppressWarnings(Array("com.digitalasset.canton.EnforceVisibleForTesting"))
class ParticipantSimulator(
    val managingNode: LocalInstanceReference,
    sequencersToConnectTo: NonEmpty[Seq[SequencerReference]],
    staticSynchronizerParameters: StaticSynchronizerParameters,
    respondToAcsCommitments: Boolean,
    override val loggerFactory: NamedLoggerFactory,
    override val timeouts: ProcessingTimeout,
)(implicit env: ConsoleEnvironment)
    extends HasFlushFuture
    with OptionValues
    with FlagCloseable {

  import env.*
  import env.environment.*

  private val namespaceKey = managingNode.keys.secret
    .generate_signing_key("virtual-participants", SigningKeyUsage.NamespaceOnly)
  private val namespace = Namespace(namespaceKey.fingerprint)
  private val signingKey =
    managingNode.keys.secret.generate_signing_key(usage = SigningKeyUsage.ProtocolOnly)
  private val authKey = managingNode.keys.secret
    .generate_signing_key(usage = SigningKeyUsage.SequencerAuthenticationOnly)
  private val encryptionKey = managingNode.keys.secret
    .generate_encryption_key()

  private val synchronizerId = sequencersToConnectTo.head1.synchronizer_id
  private val psid = sequencersToConnectTo.head1.physical_synchronizer_id
  private val pv = psid.protocolVersion

  private val nodeParameters = managingNode.config
  private val crypto = managingNode.crypto

  private val syncCrypto = new FixedSyncCryptoApiForSigning(
    // the specific member doesn't actually matter, since all virtual participants share the same keys
    managingNode.id.member,
    managingNode.crypto,
    staticSynchronizerParameters,
    signingKey,
    loggerFactory,
  )

  private def value[T, L](et: Either[L, T]): T = et match {
    case Left(value) => sys.error(s"Failed request $value")
    case Right(value) => value
  }
  private def await[T](fut: Future[T]): T =
    Await.result(fut, env.environment.config.parameters.timeouts.processing.default.duration)
  private def awaitEU[T, L](fut: EitherT[FutureUnlessShutdown, L, T]): T =
    value(await(fut.value.onShutdown(sys.error("failed due to shutdown"))))

  def uploadRootCert(): SignedTopologyTransaction[TopologyChangeOp, NamespaceDelegation] =
    managingNode.topology.namespace_delegations.propose_delegation(
      namespace,
      namespaceKey,
      CanSignAllMappings,
      store = synchronizerId,
      signedBy = Seq(namespaceKey.fingerprint),
    )

  def createOnboardingTransactions(
      name: String,
      packagesToVet: Seq[VettedPackage],
  )(implicit traceContext: TraceContext): EitherT[
    FutureUnlessShutdown,
    String,
    (ParticipantId, Seq[GenericSignedTopologyTransaction]),
  ] = {
    val pid = ParticipantId(UniqueIdentifier.tryCreate(name, namespace))
    for {
      otk <-
        SignedTopologyTransaction
          .signAndCreate(
            TopologyTransaction(
              TopologyChangeOp.Replace,
              serial = PositiveInt.one,
              OwnerToKeyMapping.tryCreate(
                member = pid,
                keys = NonEmpty(Seq, signingKey, authKey, encryptionKey),
              ),
              pv,
            ),
            signingKeys =
              NonEmpty(Set, signingKey.fingerprint, authKey.fingerprint, namespaceKey.fingerprint),
            isProposal = false,
            crypto.privateCrypto,
            pv,
          )
          .leftMap(_.toString)

      stc <-
        SignedTopologyTransaction
          .signAndCreate(
            TopologyTransaction(
              TopologyChangeOp.Replace,
              serial = PositiveInt.one,
              SynchronizerTrustCertificate(
                pid,
                synchronizerId,
                ParticipantProtocolFeatureFlags.supportedFeatureFlagsByPV
                  .getOrElse(pv, Set.empty)
                  .toSeq,
              ),
              pv,
            ),
            signingKeys = NonEmpty(Set, namespaceKey.fingerprint),
            isProposal = false,
            crypto.privateCrypto,
            pv,
          )
          .leftMap(_.toString)

      vtp <-
        SignedTopologyTransaction
          .signAndCreate(
            TopologyTransaction(
              TopologyChangeOp.Replace,
              serial = PositiveInt.one,
              VettedPackages.tryCreate(pid, packagesToVet),
              pv,
            ),
            signingKeys = NonEmpty(Set, namespaceKey.fingerprint),
            isProposal = false,
            crypto.privateCrypto,
            pv,
          )
          .leftMap(_.toString)
    } yield (pid, Seq(otk, stc, vtp))
  }

  def loadTransactions(
      txs: Seq[GenericSignedTopologyTransaction],
      synchronize: Option[NonNegativeDuration] = Some(
        env.commandTimeouts.unbounded
      ),
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] = runAdminCommand(
    TopologyAdminCommands.Write
      .AddTransactions(
        txs,
        store = synchronizerId,
        ForceFlag.AlienMember,
        synchronize,
      )
  )

  def onboardNewVirtualParticipant(
      name: String,
      packagesToVet: Seq[VettedPackage],
      synchronize: Option[NonNegativeDuration] = Some(
        env.commandTimeouts.unbounded
      ),
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, String, ParticipantId] =
    for {
      pidAndTxs <- createOnboardingTransactions(name, packagesToVet)
      (pid, txs) = pidAndTxs
      _ <- loadTransactions(txs, synchronize)
    } yield pid

  private def runAdminCommand[Result](
      command: GrpcAdminCommand[?, ?, Result]
  )(implicit traceContext: TraceContext) =
    managingNode.consoleEnvironment.grpcAdminCommandRunner
      .runCommandAsync(
        managingNode.name,
        command,
        managingNode.config.clientAdminApi,
        managingNode.config.adminApi.adminTokenConfig.fixedAdminToken,
      )
      ._2
      .mapK(FutureUnlessShutdown.outcomeK)

  def createPartyTx(identifier: String, pid: ParticipantId)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, GenericSignedTopologyTransaction] =
    SignedTopologyTransaction
      .signAndCreate(
        TopologyTransaction(
          TopologyChangeOp.Replace,
          serial = PositiveInt.one,
          PartyToParticipant.tryCreate(
            partyId = PartyId.tryCreate(identifier, namespace),
            threshold = PositiveInt.one,
            participants = Seq(
              HostingParticipant(
                pid,
                ParticipantPermission.Confirmation,
                onboarding = false,
              )
            ),
            partySigningKeysWithThreshold = None,
          ),
          pv,
        ),
        signingKeys = NonEmpty(Set, namespaceKey.fingerprint),
        isProposal = false,
        crypto.privateCrypto,
        pv,
      )
      .map(_.select[TopologyChangeOp, TopologyMapping].value)
      .leftMap(_.toString)

  def allocateParty(
      identifier: String,
      pid: ParticipantId,
      synchronize: Option[cfg.NonNegativeDuration],
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, String, Unit] = {
    val command = TopologyAdminCommands.Write.Propose(
      mapping = PartyToParticipant.create(
        partyId = PartyId.tryCreate(identifier, namespace),
        threshold = PositiveInt.one,
        participants = Seq(
          HostingParticipant(
            pid,
            ParticipantPermission.Confirmation,
            onboarding = false,
          )
        ),
        partySigningKeysWithThreshold = None,
      ),
      signedBy = Seq.empty,
      serial = None,
      change = TopologyChangeOp.Replace,
      mustFullyAuthorize = true,
      store = TopologyStoreId.Synchronizer(synchronizerId),
      forceChanges = ForceFlags.none,
      waitToBecomeEffective = synchronize,
    )
    runAdminCommand(command).map(_ => ())
  }

  def startSubscriptionForParticipant(
      pid: ParticipantId,
      notifyEvent: CantonTimestamp => Unit,
  )(implicit traceContext: TraceContext): AutoCloseable = {
    val loggerFactoryForParticipant = loggerFactory.append("participant", pid.identifier.unwrap)
    val metrics = environment.metricsRegistry.forParticipant(pid.identifier.unwrap)
    val synchronizerMetrics =
      metrics.connectedSynchronizerMetrics(SynchronizerAlias.tryCreate("synchronizer"))

    val connectionPoolFactory = new GrpcSequencerConnectionXPoolFactory(
      clientProtocolVersions = NonEmpty(Seq, pv),
      minimumProtocolVersion = Some(pv),
      authConfig = nodeParameters.sequencerClient.authToken,
      member = pid,
      clock = environment.clock,
      crypto = crypto,
      seedForRandomnessO = environment.testingConfig.sequencerTransportSeed,
      metrics = synchronizerMetrics.sequencerClient.connectionPool,
      metricsContext = MetricsContext.Empty,
      futureSupervisor = environment.futureSupervisor,
      timeouts = env.environment.config.parameters.timeouts.processing,
      loggerFactory = loggerFactoryForParticipant,
    )
    implicit val namedLoggingContext: NamedLoggingContext =
      NamedLoggingContext(loggerFactoryForParticipant, traceContext)

    val sequencerTrustThreshold = PositiveInt.tryCreate(sequencersToConnectTo.size)
    val (pool, _) = awaitEU(
      GrpcSequencerConnectionService
        .waitUntilSequencerConnectionIsValidWithPool(
          connectionPoolFactory = connectionPoolFactory,
          tracingConfig = env.environment.config.monitoring.tracing,
          flagCloseable = crypto,
          loadConfig = FutureUnlessShutdown.pure(
            Some(
              SequencerConnections.tryMany(
                sequencersToConnectTo.forgetNE
                  .map(_.sequencerConnection.toInternal),
                sequencerTrustThreshold = sequencerTrustThreshold,
                sequencerLivenessMargin = NonNegativeInt.zero,
                submissionRequestAmplification = SubmissionRequestAmplification.NoAmplification,
                sequencerConnectionPoolDelays = SequencerConnectionPoolDelays.default,
              )
            )
          ),
        )
    )

    val sequencerSubscriptionFactory = new SequencerSubscriptionXFactoryImpl(
      SequencedEventValidatorFactory.noValidation(psid, warn = false),
      env.environment.config.parameters.timeouts.processing,
      loggerFactoryForParticipant,
    )

    val batchProcessor = new RichSequencerClientImpl.EventBatchProcessor {
      override def process(eventBatch: Seq[SequencedSerializedEvent]): EitherT[
        FutureUnlessShutdown,
        SequencerClientSubscriptionError.ApplicationHandlerFailure,
        Unit,
      ] = {
        eventBatch.foreach { ev =>
          notifyEvent(ev.signedEvent.content.timestamp)
          if (respondToAcsCommitments)
            respondAcsCommitment(ev.signedEvent.content, pool, syncCrypto)(ev.traceContext)
        }
        EitherTUtil.unitUS
      }
    }

    val sequencerAggregator = new SequencerAggregator(
      crypto.pureCrypto,
      nodeParameters.sequencerClient.eventInboxSize,
      loggerFactoryForParticipant,
      MessageAggregationConfig(None, sequencerTrustThreshold),
      _ => (),
      env.environment.config.parameters.timeouts.processing,
      environment.futureSupervisor,
      useNewConnectionPool = true,
    )
    val aggregationHandler = new PostAggregationHandlerImpl(
      sequencerAggregator,
      addToFlushAndLogError _,
      nodeParameters.sequencerClient.eventInboxSize,
      eventBatchProcessor = batchProcessor,
      hasSynchronizeWithClosing = this,
      loggerFactory = loggerFactoryForParticipant,
    )

    val subscriptionHandlerFactory = new SubscriptionHandlerXFactoryImpl(
      environment.clock,
      synchronizerMetrics.sequencerClient,
      new SingleUseCell[SequencerClientSubscriptionError.ApplicationHandlerFailure],
      recorderO = None,
      sequencerAggregator,
      NoDelay,
      env.environment.config.parameters.timeouts.processing,
    )

    val sequencerSubscriptionPoolFactory = new SequencerSubscriptionPoolFactoryImpl(
      sequencerSubscriptionFactory,
      subscriptionHandlerFactory,
      synchronizerMetrics.sequencerClient.connectionPool,
      metricsContext = MetricsContext.Empty,
      env.environment.config.parameters.timeouts.processing,
      loggerFactoryForParticipant,
    )

    val sequencerSubscriptionPool = sequencerSubscriptionPoolFactory.create(
      SequencerSubscriptionPoolConfig(
        livenessMargin = NonNegativeInt.zero,
        subscriptionRequestDelay = SequencerConnectionPoolDelays.default.subscriptionRequestDelay,
      ),
      pool,
      pid,
      initialSubscriptionEventO = None,
      sequencerAggregator,
    )

    sequencerSubscriptionPool.start()

    () => {
      LifeCycle.close(
        pool,
        sequencerSubscriptionPool,
        sequencerAggregator,
        AsyncCloseable(
          s"aggregation-handler-$pid",
          aggregationHandler.handlerIsIdleF,
          timeouts.closing,
        ),
      )(logger)
    }
  }
  private def respondAcsCommitment(
      event: SequencedEvent[ClosedEnvelope],
      pool: SequencerConnectionXPool,
      syncCrypto: SyncCryptoApi,
  )(implicit traceContext: TraceContext): Unit =
    event.envelopes
      .flatMap(_.openEnvelope(crypto.pureCrypto, pv).toOption.toList)
      .foreach(cc =>
        cc.protocolMessage match {
          case message: UnsignedProtocolMessage => logger.debug(s"UNSIGNED MESSAGE $message")
          case SignedProtocolMessage(typedMessage, signatures) =>
            typedMessage.content match {
              case AcsCommitment(psid, sender, counterParticipant, period, commitment) =>
                val payload = AcsCommitment.create(
                  synchronizerId = this.psid,
                  sender = counterParticipant,
                  counterParticipant = sender,
                  period = period,
                  commitment = commitment,
                  protocolVersion = pv,
                )
                val result = for {
                  msg <- SignedProtocolMessage
                    .trySignAndCreate(payload, syncCrypto, None)
                  batch = Batch.closeEnvelopes(Batch.of(pv, (msg, Recipients.cc(sender))))
                  request = SubmissionRequest.tryCreate(
                    sender = counterParticipant,
                    messageId = MessageId.randomMessageId(),
                    batch = batch,
                    maxSequencingTime = event.timestamp.plusSeconds(120),
                    topologyTimestamp = None,
                    aggregationRule = None,
                    // this works if sequencer.trafficConfig.autoFillEmptyCost = true
                    submissionCost = None,
                    pv,
                  )
                  signedRequest <- EitherTUtil.toFutureUnlessShutdown(
                    SignedContent
                      .create(
                        content = request,
                        timestampOfSigningKey = None,
                        protocolVersion = pv,
                        cryptoApi = syncCrypto.pureCrypto.pureCrypto,
                        cryptoPrivateApi = syncCrypto,
                        purpose = HashPurpose.SubmissionRequestSignature,
                        approximateTimestampOverride = None,
                      )
                      .leftMap(err => new IllegalArgumentException(err.toString))
                  )
                  _ <- EitherTUtil.toFutureUnlessShutdown(
                    pool
                      .getAllConnections()
                      .headOption
                      .map(
                        _.sendAsync(
                          request = signedRequest,
                          timeout = 10.seconds,
                        ).leftMap(err => new IllegalArgumentException(err.toString))
                      )
                      .getOrElse(
                        EitherT.leftT(
                          new IllegalArgumentException(
                            "No valid sequencer connection for " + counterParticipant
                          )
                        )
                      )
                  )
                } yield ()

                FutureUnlessShutdownUtil
                  .doNotAwaitUnlessShutdown(
                    result,
                    "responding to ACS commitment",
                    level = Level.INFO,
                  )

              case _ =>
            }
        }
      )

}
