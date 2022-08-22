// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.processing

import cats.syntax.functor._
import cats.syntax.functorFilter._
import cats.syntax.traverse._
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.SequencerCounter
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.{LocalNodeParameters, ProcessingTimeout}
import com.digitalasset.canton.crypto.{CryptoPureApi, PublicKey, SigningPublicKey}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown, Lifecycle}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.LoggingAlarmStreamer
import com.digitalasset.canton.protocol.messages.{
  DefaultOpenEnvelope,
  DomainTopologyTransactionMessage,
  ProtocolMessage,
}
import com.digitalasset.canton.sequencing._
import com.digitalasset.canton.sequencing.protocol.{Batch, Deliver, DeliverError}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.time.Clock
import com.digitalasset.canton.topology.client.{
  CachingDomainTopologyClient,
  StoreBasedDomainTopologyClient,
}
import com.digitalasset.canton.topology.processing.TopologyTransactionProcessor.subscriptionTimestamp
import com.digitalasset.canton.topology.store.{
  PositiveSignedTopologyTransactions,
  SignedTopologyTransactions,
  TopologyStore,
  TopologyStoreId,
  ValidatedTopologyTransaction,
}
import com.digitalasset.canton.topology.transaction.TopologyChangeOp.Positive
import com.digitalasset.canton.topology.transaction._
import com.digitalasset.canton.topology.{DomainId, KeyOwner}
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.{ErrorUtil, MonadUtil, SimpleExecutionQueue}
import com.google.common.annotations.VisibleForTesting
import com.google.protobuf.timestamp.{Timestamp => ProtoTimestamp}
import io.functionmeta.functionFullName

import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.mutable.ListBuffer
import scala.concurrent.{ExecutionContext, Future}

case class EffectiveTime(value: CantonTimestamp) {
  def toApproximate: ApproximateTime = ApproximateTime(value)

  def toProtoPrimitive: ProtoTimestamp = value.toProtoPrimitive

}
object EffectiveTime {
  val MinValue: EffectiveTime = EffectiveTime(CantonTimestamp.MinValue)
  def max(ts: EffectiveTime, other: EffectiveTime*): EffectiveTime =
    EffectiveTime(CantonTimestamp.max(ts.value, other.map(_.value): _*))
  implicit val orderingEffectiveTime: Ordering[EffectiveTime] =
    Ordering.by[EffectiveTime, CantonTimestamp](_.value)
  def fromProtoPrimitive(ts: ProtoTimestamp): ParsingResult[EffectiveTime] =
    CantonTimestamp.fromProtoPrimitive(ts).map(EffectiveTime(_))
}
case class ApproximateTime(value: CantonTimestamp)
case class SequencedTime(value: CantonTimestamp) {
  def toProtoPrimitive: ProtoTimestamp = value.toProtoPrimitive
}
object SequencedTime {
  def fromProtoPrimitive(ts: ProtoTimestamp): ParsingResult[SequencedTime] =
    CantonTimestamp.fromProtoPrimitive(ts).map(SequencedTime(_))
  implicit val orderingSequencedTime: Ordering[SequencedTime] =
    Ordering.by[SequencedTime, CantonTimestamp](_.value)
}

trait TopologyTransactionProcessingSubscriber {

  /** Inform the subscriber about non-idm changes (mostly about the timestamp) */
  def updateHead(
      effectiveTimestamp: EffectiveTime,
      approximateTimestamp: ApproximateTime,
      potentialTopologyChange: Boolean,
  )(implicit traceContext: TraceContext): Unit

  def observed(
      sequencedTimestamp: SequencedTime,
      effectiveTimestamp: EffectiveTime,
      sequencerCounter: SequencerCounter,
      transactions: Seq[SignedTopologyTransaction[TopologyChangeOp]],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit]

}

/** Main incoming topology transaction validation and processing
  *
  * The topology transaction processor is subscribed to the event stream and processes
  * the domain topology transactions sent via the sequencer.
  *
  * It validates and then computes the updates to the data store in order to be able
  * to represent the topology state at any point in time.
  *
  * The processor works together with the StoreBasedDomainTopologyClient
  */
class TopologyTransactionProcessor(
    domainId: DomainId,
    cryptoPureApi: CryptoPureApi,
    store: TopologyStore[TopologyStoreId.DomainStore],
    clock: Clock,
    acsCommitmentScheduleEffectiveTime: Traced[CantonTimestamp] => Unit,
    futureSupervisor: FutureSupervisor,
    override protected val timeouts: ProcessingTimeout,
    val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends NamedLogging
    with FlagCloseable {

  protected val alarmer = new LoggingAlarmStreamer(logger)
  private val authValidator =
    new IncomingTopologyTransactionAuthorizationValidator(
      cryptoPureApi,
      store,
      Some(domainId),
      loggerFactory.append("role", "incoming"),
    )
  private val listeners = ListBuffer[TopologyTransactionProcessingSubscriber]()
  private val timeAdjuster = new TopologyTimestampPlusEpsilonTracker(timeouts, loggerFactory)
  private val serializer = new SimpleExecutionQueue()
  private val initialised = new AtomicBoolean(false)

  private def listenersUpdateHead(
      effective: EffectiveTime,
      approximate: ApproximateTime,
      potentialChanges: Boolean,
  )(implicit traceContext: TraceContext): Unit = {
    listeners.foreach(_.updateHead(effective, approximate, potentialChanges))
  }

  private def initialise(
      start: SubscriptionStart
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {

    ErrorUtil.requireState(
      !initialised.getAndSet(true),
      "topology processor is already initialised",
    )

    def initClientFromSequencedTs(
        sequencedTs: SequencedTime
    ): FutureUnlessShutdown[Seq[(EffectiveTime, ApproximateTime)]] = for {
      // we need to figure out any future effective time. if we had been running, there would be a clock
      // scheduled to poke the domain client at the given time in order to adjust the approximate timestamp up to the
      // effective time at the given point in time. we need to recover these as otherwise, we might be using outdated
      // topology snapshots on startup. (wouldn't be tragic as by getting the rejects, we'd be updating the timestamps
      // anyway).
      upcoming <- performUnlessClosingF(functionFullName)(
        store.findUpcomingEffectiveChanges(sequencedTs.value)
        // find effective time of sequenced Ts (directly from store)
        // merge times
      )
      currentEpsilon <- TopologyTimestampPlusEpsilonTracker.epsilonForTimestamp(
        store,
        sequencedTs.value,
      )
    } yield {

      // we have (ts+e, ts) and quite a few te in the future, so we create list of upcoming changes and sort them
      ((
        EffectiveTime(sequencedTs.value.plus(currentEpsilon.epsilon.unwrap)),
        ApproximateTime(sequencedTs.value),
      ) +: upcoming.map(x => (x.effective, x.effective.toApproximate))).sortBy(_._1.value)
    }

    for {
      stateStoreTsO <- performUnlessClosingF(functionFullName)(
        store.timestamp(useStateStore = true)
      )
      (processorTs, clientTs) = subscriptionTimestamp(start, stateStoreTsO)
      _ <- TopologyTimestampPlusEpsilonTracker.initialize(timeAdjuster, store, processorTs)

      clientInitTimes <- clientTs match {
        case Left(sequencedTs) =>
          // approximate time is sequencedTs
          initClientFromSequencedTs(sequencedTs)
        case Right(effective) =>
          // effective and approximate time are effective time
          FutureUnlessShutdown.pure(Seq((effective, effective.toApproximate)))
      }
    } yield {
      logger.debug(
        s"Initialising topology processing for start=$start with effective ts ${clientInitTimes.map(_._1)}"
      )
      clientInitTimes.foreach { case (effective, approximate) =>
        // if the effective time is in the future, schedule a clock to update the time accordingly
        // TODO(ratko/andreas) This should be scheduled via the DomainTimeTracker or something similar
        //  rather than via the participant's local clock
        if (effective.value > clock.now) {
          // set approximate time now and schedule task to update the approximate time to the effective time in the future
          if (effective.value != approximate.value) {
            listenersUpdateHead(effective, approximate, potentialChanges = true)
          }
          clock.scheduleAt(
            _ => listenersUpdateHead(effective, effective.toApproximate, potentialChanges = true),
            effective.value,
          )
        } else {
          // if the effective time is in the past, directly advance our approximate time to the respective effective time
          listenersUpdateHead(effective, effective.toApproximate, potentialChanges = true)
        }
      }
    }
  }

  @VisibleForTesting
  private[processing] def process(
      sequencingTimestamp: SequencedTime,
      effectiveTimestamp: EffectiveTime,
      sc: SequencerCounter,
      transactions: List[SignedTopologyTransaction[TopologyChangeOp]],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {

    // start validation and change delay advancing
    val validatedF = performUnlessClosingF(functionFullName)(
      authValidator.validateAndUpdateHeadAuthState(effectiveTimestamp.value, transactions).map {
        case ret @ (_, validated) =>
          inspectAndAdvanceTopologyTransactionDelay(
            effectiveTimestamp,
            sequencingTimestamp,
            validated,
          )
          ret
      }
    )

    // string approx for output
    val epsilon =
      s"${effectiveTimestamp.value.toEpochMilli - sequencingTimestamp.value.toEpochMilli}"

    // store transactions once they are fully validated
    val storeF =
      validatedF.flatMap { case (_, validated) =>
        val ln = validated.length
        validated.zipWithIndex.foreach {
          case (ValidatedTopologyTransaction(tx, None), idx) =>
            logger.info(
              s"Storing topology transaction ${idx + 1}/$ln ${tx.transaction.op} ${tx.transaction.element.mapping} with ts=$effectiveTimestamp (epsilon=${epsilon} ms)"
            )
          case (ValidatedTopologyTransaction(tx, Some(r)), idx) =>
            logger.warn(
              s"Rejected transaction ${idx + 1}/$ln ${tx.transaction.op} ${tx.transaction.element.mapping} at ts=$effectiveTimestamp (epsilon=${epsilon} ms) due to $r"
            )
        }

        performUnlessClosingF(functionFullName)(
          store.append(sequencingTimestamp, effectiveTimestamp, validated)
        )
      }

    // collect incremental and full updates
    val collectedF = validatedF.map { case (cascadingUpdate, validated) =>
      (cascadingUpdate, collectIncrementalUpdate(cascadingUpdate, validated))
    }

    // incremental updates can be written asap
    val incrementalF = collectedF.flatMap { case (_, incremental) =>
      performIncrementalUpdates(sequencingTimestamp, effectiveTimestamp, incremental)
    }

    // cascading updates need to wait until the transactions have been stored
    val cascadingF = collectedF.flatMap { case (cascading, _) =>
      for {
        _ <- storeF
        _ <- performCascadingUpdates(sequencingTimestamp, effectiveTimestamp, cascading)
      } yield {}
    }

    // resynchronize
    for {
      validated <- validatedF
      _ <- incrementalF
      _ <- cascadingF // does synchronize storeF
      filtered = validated._2.collect {
        case transaction if transaction.rejectionReason.isEmpty => transaction.transaction
      }
      _ <- listeners.toList.traverse(
        _.observed(
          sequencingTimestamp,
          effectiveTimestamp = effectiveTimestamp,
          sc,
          filtered,
        )
      )
    } yield {}
  }

  private def inspectAndAdvanceTopologyTransactionDelay(
      effectiveTimestamp: EffectiveTime,
      sequencingTimestamp: SequencedTime,
      validated: Seq[ValidatedTopologyTransaction],
  )(implicit traceContext: TraceContext): Unit = {
    def applyEpsilon(change: DomainParametersChange) = {
      timeAdjuster
        .adjustEpsilon(
          effectiveTimestamp,
          sequencingTimestamp,
          change.domainParameters.topologyChangeDelay,
        )
        .foreach { previous =>
          logger.info(
            s"Updated topology change delay from=${previous} to ${change.domainParameters.topologyChangeDelay}"
          )
        }
      timeAdjuster.effectiveTimeProcessed(effectiveTimestamp)
    }
    val domainParamChanges = validated
      .collect {
        case validatedTx
            if validatedTx.rejectionReason.isEmpty && validatedTx.transaction.transaction.op == TopologyChangeOp.Replace =>
          validatedTx.transaction.transaction.element
      }
      .collect { case DomainGovernanceElement(change: DomainParametersChange) => change }
    NonEmpty.from(domainParamChanges) match {
      // normally, we shouldn't have any adjustment
      case None => timeAdjuster.effectiveTimeProcessed(effectiveTimestamp)
      case Some(changes) =>
        // if there is one, there should be exactly one
        // If we have several, let's panic now. however, we just pick the last and try to keep working
        if (changes.lengthCompare(1) > 0) {
          logger.error(
            s"Broken or malicious domain topology manager has sent (${changes.length}) domain parameter adjustments at $effectiveTimestamp, will ignore all of them except the last"
          )
        }
        applyEpsilon(changes.last1)
    }
  }

  private def tickleListeners(
      sequencedTimestamp: SequencedTime,
      effectiveTimestamp: EffectiveTime,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    this.performUnlessClosingF(functionFullName) {
      Future {
        val approximate = ApproximateTime(sequencedTimestamp.value)
        listenersUpdateHead(effectiveTimestamp, approximate, potentialChanges = false)
      }
    }
  }

  /** assumption: subscribers don't do heavy lifting */
  def subscribe(listener: TopologyTransactionProcessingSubscriber): Unit = {
    listeners += listener
  }

  /** pick the transactions which we can process using incremental updates */
  private def collectIncrementalUpdate(
      cascadingUpdate: UpdateAggregation,
      transactions: Seq[ValidatedTopologyTransaction],
  ): Seq[SignedTopologyTransaction[TopologyChangeOp]] = {
    def isCascading(elem: SignedTopologyTransaction[TopologyChangeOp]): Boolean = {
      elem.transaction.element.mapping.requiredAuth match {
        // namespace delegation changes are always cascading
        case RequiredAuth.Ns(_, true) => true
        // identifier delegation changes are only cascading with respect to namespace
        case RequiredAuth.Ns(namespace, false) =>
          cascadingUpdate.cascadingNamespaces.contains(namespace)
        // all others are cascading if there is at least one uid affected by the cascading update
        case RequiredAuth.Uid(uids) => uids.exists(cascadingUpdate.isCascading)
      }
    }
    transactions.filter(_.rejectionReason.isEmpty).map(_.transaction).filterNot(isCascading)
  }

  private def performIncrementalUpdates(
      sequenced: SequencedTime,
      effective: EffectiveTime,
      transactions: Seq[SignedTopologyTransaction[TopologyChangeOp]],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {
    val (deactivate, positive) = SignedTopologyTransactions(transactions).splitForStateUpdate

    performUnlessClosingF(functionFullName)(
      store.updateState(
        sequenced,
        effective,
        deactivate = deactivate,
        positive = positive,
      )
    )
  }

  private def determineUpdates(
      currents: PositiveSignedTopologyTransactions,
      targets: PositiveSignedTopologyTransactions,
  )(implicit
      traceContext: TraceContext
  ): (Seq[UniquePath], Seq[SignedTopologyTransaction[TopologyChangeOp.Positive]]) = {

    val (toRemoveForAdds, toAddForAdds) =
      determineRemovesAdds(currents.adds.result, targets.adds.result)
    val (toRemoveForReplaces, toAddForReplaces) =
      determineRemovesAdds(currents.replaces.result, targets.replaces.result)

    (toRemoveForAdds ++ toRemoveForReplaces, toAddForAdds ++ toAddForReplaces)
  }

  private def determineRemovesAdds(
      current: Seq[SignedTopologyTransaction[Positive]],
      target: Seq[SignedTopologyTransaction[Positive]],
  )(implicit
      traceContext: TraceContext
  ) = {
    def toIndex[P <: Positive](sit: SignedTopologyTransaction[P]): (
        AuthorizedTopologyTransaction[TopologyMapping],
        SignedTopologyTransaction[P],
    ) = AuthorizedTopologyTransaction(
      sit.uniquePath,
      sit.transaction.element.mapping,
      sit,
    ) -> sit

    val currentMap = current.map(toIndex).toMap
    val targetMap = target.map(toIndex).toMap

    val currentSet = currentMap.keySet
    val targetSet = targetMap.keySet
    val toRemove = currentSet -- targetSet
    val toAdd = targetSet -- currentSet

    toRemove.foreach { item =>
      logger.debug(s"Cascading remove of $item")
    }
    toAdd.foreach { item =>
      logger.debug(s"Cascading addition of $item")
    }

    (toRemove.map(_.uniquePath).toSeq, toAdd.toSeq.flatMap(key => targetMap.get(key).toList))
  }

  private def performCascadingUpdates(
      sequenced: SequencedTime,
      effective: EffectiveTime,
      cascadingUpdate: UpdateAggregation,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    if (cascadingUpdate.nothingCascading) FutureUnlessShutdown.unit
    else {
      logger.debug(
        s"Performing cascading update on namespace=${cascadingUpdate.authNamespaces} and uids=${cascadingUpdate.filteredCascadingUids}"
      )

      val uids = cascadingUpdate.filteredCascadingUids.toSeq
      val namespaces = cascadingUpdate.cascadingNamespaces.toSeq
      // filter out txs that don't fall into this namespace / uid realm, but we don't have enough
      // information on the db-level to know which tx to ignore and which one to keep
      def cascadingFilter(tx: SignedTopologyTransaction[TopologyChangeOp]): Boolean =
        tx.transaction.element.mapping.requiredAuth match {
          case RequiredAuth.Ns(namespace, _) =>
            cascadingUpdate.cascadingNamespaces.contains(namespace)
          case RequiredAuth.Uid(uids) =>
            uids.exists(uid =>
              cascadingUpdate.cascadingNamespaces.contains(uid.namespace) ||
                cascadingUpdate.filteredCascadingUids.contains(uid)
            )
        }

      for {
        target <- performUnlessClosingF(functionFullName)(
          store.findPositiveTransactions(
            asOf = effective.value,
            asOfInclusive = true,
            includeSecondary = true,
            types = DomainTopologyTransactionType.all,
            filterUid = Some(uids),
            filterNamespace = Some(namespaces),
          )
        )

        targetFiltered = target.signedTransactions.filter { tx =>
          lazy val isDomainGovernance = tx.transaction.element match {
            case _: TopologyStateUpdateElement => false
            case _: DomainGovernanceElement => true
          }

          /*
            We check that the transaction is properly authorized or is a domain governance.
            This allows not to drop domain governance transactions with cascading updates.
            In the scenario where a key authorizes a domain parameters change and is later
            revoked, the domain parameters stay valid.
           */
          val isAuthorized = authValidator.isCurrentlyAuthorized(tx) || isDomainGovernance

          cascadingFilter(tx) && isAuthorized
        }

        current <- performUnlessClosingF(functionFullName)(
          store
            .findStateTransactions(
              asOf = effective.value,
              asOfInclusive = true,
              includeSecondary = true,
              types = DomainTopologyTransactionType.all,
              filterUid = Some(uids),
              filterNamespace = Some(namespaces),
            )
        )

        currentFiltered = current.signedTransactions.filter(cascadingFilter)

        (removes, adds) = determineUpdates(currentFiltered, targetFiltered)

        _ <- performUnlessClosingF(functionFullName)(
          store.updateState(sequenced, effective, deactivate = removes, positive = adds)
        )
      } yield ()
    }

  private def extractDomainTopologyTransactionMsg(
      envelopes: List[DefaultOpenEnvelope]
  ): List[DomainTopologyTransactionMessage] =
    envelopes
      .mapFilter(ProtocolMessage.select[DomainTopologyTransactionMessage])
      .map(_.protocolMessage)

  /** Inform the topology manager where the subscription starts when using [[processEnvelopes]] rather than [[createHandler]] */
  def subscriptionStartsAt(start: SubscriptionStart)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Unit] = initialise(start)

  /** process envelopes mostly asynchronously
    *
    * Here, we return a Future[Future[Unit]]. We need to ensure the outer future finishes processing
    * before we tick the record order publisher.
    */
  def processEnvelopes(
      sc: SequencerCounter,
      ts: CantonTimestamp,
      envelopes: Traced[List[DefaultOpenEnvelope]],
  ): HandlerResult =
    envelopes.withTraceContext { implicit traceContext => env =>
      internalProcessEnvelopes(sc, ts, extractDomainTopologyTransactionMsg(env))
    }

  private def internalProcessEnvelopes(
      sc: SequencerCounter,
      ts: CantonTimestamp,
      messages: => List[DomainTopologyTransactionMessage],
  )(implicit traceContext: TraceContext): HandlerResult = {
    val sequencedTime = SequencedTime(ts)
    val updatesF = performUnlessClosingF(functionFullName)(Future { messages })
    def computeEffectiveTime(
        updates: List[DomainTopologyTransactionMessage]
    ): FutureUnlessShutdown[EffectiveTime] = {
      if (updates.nonEmpty) {
        val tmpF =
          futureSupervisor.supervisedUS(s"adjust ts=$ts for update")(
            timeAdjuster.adjustTimestampForUpdate(sequencedTime)
          )

        // we need to inform the acs commitment processor about the incoming change
        tmpF.map { eft =>
          // this is safe to do here, as the acs commitment processor `publish` method will only be
          // invoked long after the outer future here has finished processing
          acsCommitmentScheduleEffectiveTime(Traced(eft.value))
          eft
        }
      } else {
        futureSupervisor.supervisedUS(s"adjust ts=$ts for update")(
          timeAdjuster.adjustTimestampForTick(sequencedTime)
        )
      }
    }

    for {
      updates <- updatesF
      _ <- ErrorUtil.requireStateAsyncShutdown(
        initialised.get(),
        s"Topology client for $domainId is not initialized. Cannot process sequenced event with counter ${sc} at ${ts}",
      )
      // compute effective time
      effectiveTime <- computeEffectiveTime(updates)
    } yield {
      // the rest, we'll run asynchronously, but sequential
      val scheduledF = FutureUnlessShutdown(
        serializer.execute(
          {
            if (updates.nonEmpty) {
              // TODO(i4933) check signature of domain idm and don't accept any transaction other than domain uid tx until we are bootstrapped
              val txs = updates.flatMap { msg =>
                msg.transactions
              }
              process(sequencedTime, effectiveTime, sc, txs)
            } else {
              tickleListeners(sequencedTime, effectiveTime)
            }
          }.unwrap,
          "processing identity",
        )
      )
      AsyncResult(scheduledF)
    }
  }

  def createHandler(domainId: DomainId): UnsignedProtocolEventHandler =
    new UnsignedProtocolEventHandler {

      override def name: String = s"topology-processor-$domainId"

      override def apply(
          tracedBatch: BoxedEnvelope[UnsignedEnvelopeBox, DefaultOpenEnvelope]
      ): HandlerResult = {
        MonadUtil.sequentialTraverseMonoid(tracedBatch.value) {
          _.withTraceContext { implicit traceContext =>
            {
              case Deliver(sc, ts, _, _, batch) =>
                logger.debug(s"Processing sequenced event with counter $sc and timestamp $ts")
                // TODO(#8744) avoid discarded future as part of AlarmStreamer design
                @SuppressWarnings(Array("com.digitalasset.canton.DiscardedFuture"))
                def extractAndCheckMessages(
                    batch: Batch[DefaultOpenEnvelope]
                ): List[DomainTopologyTransactionMessage] = {
                  extractDomainTopologyTransactionMsg(
                    ProtocolMessage.filterDomainsEnvelopes(
                      batch,
                      domainId,
                      (wrongMsgs: List[DefaultOpenEnvelope]) => {
                        alarmer.alarm(
                          s"received messages with wrong domain ids: ${wrongMsgs.map(_.protocolMessage.domainId)}"
                        )
                        ()
                      },
                    )
                  )
                }

                internalProcessEnvelopes(
                  sc,
                  ts,
                  extractAndCheckMessages(batch),
                )
              case _: DeliverError => HandlerResult.done
            }
          }
        }
      }

      override def subscriptionStartsAt(
          start: SubscriptionStart
      )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
        TopologyTransactionProcessor.this.subscriptionStartsAt(start)
    }

  override def onClosed(): Unit = Lifecycle.close(timeAdjuster, store)(logger)

}

object TopologyTransactionProcessor {

  def createProcessorAndClientForDomain(
      topologyStore: TopologyStore[TopologyStoreId.DomainStore],
      domainId: DomainId,
      pureCrypto: CryptoPureApi,
      initKeys: Map[KeyOwner, Seq[PublicKey]],
      parameters: LocalNodeParameters,
      clock: Clock,
      futureSupervisor: FutureSupervisor,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      traceContext: TraceContext,
      executionContext: ExecutionContext,
  ): Future[(TopologyTransactionProcessor, CachingDomainTopologyClient)] = {
    val topologyProcessor = new TopologyTransactionProcessor(
      domainId,
      pureCrypto,
      topologyStore,
      clock,
      acsCommitmentScheduleEffectiveTime = _ => (),
      futureSupervisor,
      parameters.processingTimeouts,
      loggerFactory,
    )
    val topologyClientF =
      CachingDomainTopologyClient
        .create(
          clock,
          domainId,
          topologyStore,
          SigningPublicKey.collect(initKeys),
          StoreBasedDomainTopologyClient.NoPackageDependencies,
          parameters.cachingConfigs,
          parameters.processingTimeouts,
          futureSupervisor,
          loggerFactory,
        )
    topologyClientF.map { topologyClient =>
      topologyProcessor.subscribe(topologyClient)
      (topologyProcessor, topologyClient)
    }
  }

  /** Returns the timestamps for initializing the processor and client for a restarted or fresh subscription. */
  def subscriptionTimestamp(
      start: SubscriptionStart,
      storedTimestamps: Option[(SequencedTime, EffectiveTime)],
  ): (CantonTimestamp, Either[SequencedTime, EffectiveTime]) = {
    import SubscriptionStart._
    start match {
      case restart: ResubscriptionStart =>
        resubscriptionTimestamp(restart)
      case FreshSubscription =>
        storedTimestamps.fold(
          // Fresh subscription with an empty domain topology store
          // processor: init at ts = min
          // client: init at ts = min
          (CantonTimestamp.MinValue, Right(EffectiveTime(CantonTimestamp.MinValue)))
        ) { case (sequenced, effective) =>
          // Fresh subscription with a bootstrapping timestamp
          // NOTE: we assume that the bootstrapping topology snapshot does not contain the first message
          // that we are going to receive from the domain
          // processor: init at max(sequence-time) of bootstrapping transactions
          // client: init at max(effective-time) of bootstrapping transactions
          (sequenced.value, Right(effective))
        }
    }
  }

  /** Returns the timestamps for initializing the processor and client for a restarted subscription. */
  def resubscriptionTimestamp(
      start: ResubscriptionStart
  ): (CantonTimestamp, Either[SequencedTime, EffectiveTime]) = {
    import SubscriptionStart._
    start match {
      // clean-head subscription. this means that the first event we are going to get is > cleanPrehead
      // and all our stores are clean.
      // processor: initialise with ts = cleanPrehead
      // client: approximate time: cleanPrehead, knownUntil = cleanPrehead + epsilon
      //         plus, there might be "effective times" > cleanPrehead, so we need to schedule the adjustment
      //         of the approximate time to the effective time
      case CleanHeadResubscriptionStart(cleanPrehead) =>
        (cleanPrehead, Left(SequencedTime(cleanPrehead)))
      // dirty or replay subscription.
      // processor: initialise with firstReplayed.predecessor, as the next message we'll be getting is the firstReplayed
      // client: same as clean-head resubscription
      case ReplayResubscriptionStart(firstReplayed, Some(cleanPrehead)) =>
        (firstReplayed.immediatePredecessor, Left(SequencedTime(cleanPrehead)))
      // dirty re-subscription of a node that crashed before fully processing the first event
      // processor: initialise with firstReplayed.predecessor, as the next message we'll be getting is the firstReplayed
      // client: initialise client with firstReplayed (careful: firstReplayed is known, but firstReplayed.immediateSuccessor not)
      case ReplayResubscriptionStart(firstReplayed, None) =>
        (
          firstReplayed.immediatePredecessor,
          Right(EffectiveTime(firstReplayed.immediatePredecessor)),
        )
    }
  }
}
