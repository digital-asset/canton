// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.pruning

import cats.syntax.foldable._
import cats.syntax.functor._
import cats.syntax.traverse._
import com.daml.error._
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.PositiveNumeric
import com.digitalasset.canton.crypto._
import com.digitalasset.canton.data.{CantonTimestamp, CantonTimestampSecond}
import com.digitalasset.canton.error.CantonError
import com.digitalasset.canton.error.CantonErrorGroups.ParticipantErrorGroup.AcsCommitmentErrorGroup
import com.digitalasset.canton.lifecycle.{
  AsyncOrSyncCloseable,
  FlagCloseableAsync,
  FutureUnlessShutdown,
  SyncCloseable,
}
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.event.{AcsChange, AcsChangeListener, RecordTime}
import com.digitalasset.canton.participant.metrics.PruningMetrics
import com.digitalasset.canton.participant.store._
import com.digitalasset.canton.protocol.ContractIdSyntax._
import com.digitalasset.canton.protocol.messages.{
  AcsCommitment,
  CommitmentPeriod,
  ProtocolMessage,
  SignedProtocolMessage,
}
import com.digitalasset.canton.protocol.{LfContractId, LfHash, WithContractHash}
import com.digitalasset.canton.sequencing.client.{SendType, SequencerClient}
import com.digitalasset.canton.sequencing.protocol.{Batch, OpenEnvelope, Recipients}
import com.digitalasset.canton.store.SequencerCounterTrackerStore
import com.digitalasset.canton.time.PositiveSeconds
import com.digitalasset.canton.topology.{DomainId, ParticipantId}
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.EitherUtil.RichEither
import com.digitalasset.canton.util._
import com.digitalasset.canton.util.retry.Policy
import com.digitalasset.canton.version.ProtocolVersion
import com.google.common.annotations.VisibleForTesting
import io.functionmeta.functionFullName

import java.time.temporal.ChronoUnit
import java.util.concurrent.atomic.AtomicReference
import scala.annotation.tailrec
import scala.collection.concurrent.TrieMap
import scala.collection.immutable.SortedSet
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future, blocking}
import scala.math.Ordering
import scala.math.Ordering.Implicits._

/** Computes, sends, receives and compares ACS commitments
  *
  *  In more detail:
  *
  *  <ol>
  *   <li>The class computes the participant's ACS commitments (for each of the participant's "counter-participants", i.e.,
  *     participants who host a stakeholder of some contract in participant's ACS). The commitments are computed at
  *     specified (sequencer) times that are configured by the domain and are uniform for all participants connected to
  *     the domain. We refer to them as "commitment ticks". The commitments must be computed "online", i.e., after the
  *     the state of the ACS at a commitment tick becomes known.
  *
  *   <li>After the commitments for a tick are computed, they should be distributed to the counter-participants; but
  *     this is best-effort.
  *   </li>
  *
  *   <li>The class processes the ACS commitments from counter-participants (method `processBatch`):
  *
  *     <ol>
  *      <li>it checks that the commitments are properly signed
  *      </li>
  *      <li>it checks that they match the locally computed ACS commitments
  *      </li>
  *     </ol>
  *   </li>
  *
  *   <li>The class must define crash recovery points, such that the class itself combined with startup procedures of
  *      the node jointly ensure that the participant doesn't neglect to send its ACS commitments or process the remote
  *      ones. We allow the participant to send the same commitments multiple times in case of a crash, and we do allow
  *      the participant to not send some commitments in some edge cases due to crashes.
  *   </li>
  *
  *   <li>Finally, the class supports pruning: it computes the safe timestamps for participant pruning, such
  *     that, after pruning, non-repudiation still holds for any contract in the ACS
  *   </li>
  *  </ol>
  *
  *  The first four pieces of class functionality must be appropriately synchronized:
  *
  *  <ol>
  *   <li>ACS commitments for a tick cannot be completely processed before the local commitment for that tick is computed.
  *      Note that the class cannot make many assumptions on the received commitments: the counter-participants can send
  *      them in any order, and they can either precede or lag behind the local commitment computations.
  *   </li>
  *
  *   <li>The recovery points must be chosen such that the participant computes its local commitments correctly, and
  *     never misses to compute a local commitment for every tick. Otherwise, the participant will start raising false
  *     alarms when remote commitments are received (either because it computes the wrong thing, or because it doesn't
  *     compute anything at all and thus doesn't expect to receive anything).
  *   </li>
  *  </ol>
  *
  *  Additionally, the startup procedure must ensure that:
  *
  *  <ol>
  *    <li> [[processBatch]] is called for every sequencer message that contains commitment messages and whose handling
  *    hasn't yet completed sucessfully
  *    <li> [[publish]] is called for every change to the ACS after
  *    [[com.digitalasset.canton.participant.store.IncrementalCommitmentStore.watermark]]. where the request counter
  *    is to be used as a tie-breaker.
  *    </li>
  *  </ol>
  *
  *  Finally, the class requires the reconciliation interval to be a multiple of 1 second.
  *
  * The ``commitmentPeriodObserver`` is called whenever a commitment is computed for a period, except if the participant crashes.
  * If [[publish]] is called multiple times for the same timestamp (once before a crash and once after the recovery),
  * the observer may also be called twice for the same period.
  */
@SuppressWarnings(Array("org.wartremover.warts.Var"))
class AcsCommitmentProcessor(
    domainId: DomainId,
    participantId: ParticipantId,
    val sequencerClient: SequencerClient,
    domainCrypto: SyncCryptoClient[SyncCryptoApi],
    reconciliationInterval: PositiveSeconds,
    store: AcsCommitmentStore,
    commitmentPeriodObserver: (ExecutionContext, TraceContext) => FutureUnlessShutdown[Unit],
    killSwitch: => Unit,
    metrics: PruningMetrics,
    protocolVersion: ProtocolVersion,
    override protected val timeouts: ProcessingTimeout,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends AcsChangeListener
    with FlagCloseableAsync
    with NamedLogging {

  import AcsCommitmentProcessor._

  // As the commitment computation is in the worst case expected to last the same order of magnitude as the
  // reconciliation interval, wait for at least that long
  override protected def closingTimeout: FiniteDuration =
    super.closingTimeout.max(reconciliationInterval.toScala)

  /** The parallelism to use when computing commitments */
  private val threadCount: PositiveNumeric[Int] = {
    implicit val traceContext = TraceContext.empty
    val count = Threading.detectNumberOfThreads(logger)
    logger.info(s"Will use parallelism $count when computing ACs commitments")
    PositiveNumeric.tryCreate(count)
  }

  /* The sequencer timestamp for which we are ready to process remote commitments.
     Continuously updated as new local commitments are computed.
     All received remote commitments with the timestamp lower than this one will either have been processed or queued.
     Note that since access to this variable isn't synchronized, we don't guarantee that every remote commitment will
     be processed once this moves. However, such commitments will not be lost, as they will be put in the persistent
     buffer and get picked up by `processBuffered` eventually.
   */
  @volatile private var readyForRemote: Option[CantonTimestampSecond] = None

  /* End of the last period until which we have processed, sent and persisted all local and remote commitments.
     It's accessed only through chained futures, such that all accesses are synchronized  */
  @volatile private[this] var endOfLastProcessedPeriod: Option[CantonTimestampSecond] = None

  /* An in-memory, mutable running ACS snapshot, updated on every call to [[publish]]  */
  val runningCommitments: Future[RunningCommitments] = {
    store.runningCommitments.get()(TraceContext.empty).map { case (rt, snapshot) =>
      new RunningCommitments(
        rt,
        TrieMap(snapshot.toSeq.map { case (parties, h) =>
          parties -> LtHash16.tryCreate(h)
        }: _*),
      )
    }
  }

  private val timestampsWithPotentialTopologyChanges =
    new AtomicReference[List[Traced[CantonTimestamp]]](List())

  private[pruning] val queue: SimpleExecutionQueue = new SimpleExecutionQueue(logTaskTiming = true)

  // Ensure we queue the initialization as the first task in the queue. We don't care about initialization having
  // completed by the time we return - only that no other task is queued before initialization.
  private[this] val initFuture: FutureUnlessShutdown[Unit] = {
    import TraceContext.Implicits.Empty._
    val executed = queue.executeUnlessFailed(
      performUnlessClosingF("acs-commitment-processor-init") {
        for {
          lastComputed <- store.lastComputedAndSent
          _ = lastComputed.foreach { ts =>
            logger.info(s"Last computed and sent timestamp: $ts")
            endOfLastProcessedPeriod = Some(ts)
          }
          snapshot <- runningCommitments
          _ = logger.info(
            s"Initialized from stored snapshot at ${snapshot.watermark} (might be incomplete)"
          )

          _ <- lastComputed.fold(Future.unit)(processBuffered)

          _ = logger.info("Initialized the ACS commitment processor queue")
        } yield ()
      }.unwrap // TODO(#6175) Try to avoid the unwrapping and rewrapping
      ,
      "ACS commitment processor initialization",
    )
    val loggedFut = FutureUtil.logOnFailure(
      executed,
      "Failed to initialize the ACS commitment processor.",
    )
    FutureUnlessShutdown(loggedFut)
  }

  @volatile private[this] var lastPublished: Option[RecordTime] = None

  def initializeTicksOnStartup(
      timestamps: List[CantonTimestamp]
  )(implicit traceContext: TraceContext) = {
    // assuming timestamps to be ordered
    val cur = timestampsWithPotentialTopologyChanges.getAndSet(timestamps.map(Traced(_)))
    ErrorUtil.requireArgument(
      cur.isEmpty,
      s"Bad initialization attempt of timestamps with ticks, as we've already scheduled ${cur.length} ",
    )
  }

  def scheduleTopologyTick(effectiveTime: Traced[CantonTimestamp]): Unit =
    timestampsWithPotentialTopologyChanges.updateAndGet { cur =>
      // only append if this timestamp is higher than the last one (relevant during init)
      if (cur.lastOption.forall(_.value < effectiveTime.value)) cur :+ effectiveTime
      else cur
    }.discard

  override def publish(toc: RecordTime, acsChange: AcsChange)(implicit
      traceContext: TraceContext
  ): Unit = {
    @tailrec
    def go(): Unit =
      timestampsWithPotentialTopologyChanges.get().headOption match {
        // no upcoming topology change queued
        case None => publishTick(toc, acsChange)
        // pre-insert topology change queued
        case Some(effectiveTime) if effectiveTime.value <= toc.timestamp =>
          // remove the tick from our update
          timestampsWithPotentialTopologyChanges.updateAndGet(_.drop(1))
          // only update if this is a separate timestamp
          val eft = effectiveTime.value
          if (eft < toc.timestamp && lastPublished.forall(_.timestamp < eft)) {
            publishTick(
              RecordTime(timestamp = eft, tieBreaker = 0),
              AcsChange.empty,
            )(effectiveTime.traceContext)
          }
          // now, iterate (there might have been several effective time updates)
          go()
        case Some(_) =>
          publishTick(toc, acsChange)
      }
    go()
  }

  /** Event processing consists of two steps: one (primarily) for computing local commitments, and one for handling remote ones.
    * This is the "local" processing, however, it does also process remote commitments in one case: when they arrive before the corresponding
    * local ones have been computed (in which case they are buffered).
    *
    * The caller(s) must jointly ensure that:
    * 1. [[publish]] is called with a strictly lexicographically increasing combination of timestamp/tiebreaker within
    *    a "crash-epoch". I.e., the timestamp/tiebreaker combination may only decrease across participant crashes/restarts.
    *    Note that the tie-breaker can change non-monotonically between two calls to publish. The tie-breaker is introduced
    *    to handle repair requests, as these may cause several changes to have the same timestamp.
    *    Actual ACS changes (e.g., due to transactions) use their request counter as the tie-breaker, while other
    *    updates (e.g., heartbeats) that only update the current time can set the tie-breaker to 0
    * 2. after publish is first called within a participant's "crash-epoch" with timestamp `ts` and tie-breaker `tb`, all subsequent changes
    *    to the ACS are also published (no gaps), and in the record order
    * 3. on startup, [[publish]] is called for all changes that are later than the watermark returned by
    *    [[com.digitalasset.canton.participant.store.IncrementalCommitmentStore.watermark]]. It may also be called for
    *    changes that are earlier than this timestamp (these calls will be ignored).
    *
    * Processing is implemented as a [[com.digitalasset.canton.util.SimpleExecutionQueue]] and driven by publish calls
    * made by the RecordOrderPublisher.
    *
    * ACS commitments at a tick become computable once an event with a timestamp larger than the tick appears
    */
  private def publishTick(toc: RecordTime, acsChange: AcsChange)(implicit
      traceContext: TraceContext
  ): Unit = {
    if (!lastPublished.forall(_ < toc))
      throw new IllegalStateException(
        s"Publish called with non-increasing record time, $toc (old was $lastPublished)"
      )
    lastPublished = Some(toc)
    lazy val msg =
      s"Publishing ACS change at $toc activated, ${acsChange.deactivations.size} archived"
    logger.debug(msg)

    def processCompletedPeriod(
        snapshot: RunningCommitments
    )(completedPeriod: CommitmentPeriod): Future[Unit] = {
      val snapshotRes = snapshot.snapshot()
      for {
        msgs <- commitmentMessages(completedPeriod, snapshotRes.active)
        _ <- storeAndSendCommitmentMessages(completedPeriod, msgs)
        _ <- store.markOutstanding(completedPeriod, msgs.keySet)
        _ <- persistRunningCommitments(snapshotRes)
        // The ordering here is important; we shouldn't move `readyForRemote` before we mark the periods as outstanding,
        // as otherwise we can get a race where an incoming commitment doesn't "clear" the outstanding period
        _ = indicateReadyForRemote(completedPeriod.toInclusive)
        _ <- processBuffered(completedPeriod.toInclusive)
        _ <- indicateLocallyProcessed(completedPeriod)
      } yield {
        // Run the observer asynchronously so that it does not block the further generation / processing of ACS commitments
        // Since this runs after we mark the period as locally processed, there are no guarantees that the observer
        // actually runs (e.g., if the participant crashes before be spawn this future).
        FutureUtil.doNotAwait(
          commitmentPeriodObserver(ec, traceContext).onShutdown {
            logger.info("Skipping commitment period observer due to shutdown")
          },
          "commitment period observer failed",
        )
      }
    }

    def performPublish(): Future[Unit] = performUnlessClosingF(functionFullName) {
      for {
        snapshot <- runningCommitments
        _ <-
          if (snapshot.watermark >= toc) {
            logger.debug(s"ACS change at $toc is a replay, treating it as a no-op")
            // This is a replay of an already processed ACS change, ignore
            Future.unit
          } else {
            // Check whether this change pushes us to a new commitment period; if so, the previous one is completed
            val optCompletedPeriod =
              commitmentPeriodPreceding(reconciliationInterval, endOfLastProcessedPeriod)(
                toc.timestamp
              )
            for {
              // Important invariant:
              // - let t be the tick of [[com.digitalasset.canton.participant.store.AcsCommitmentStore#lastComputedAndSent]];
              //   assume that t is not None
              // - then, we must have already computed and stored the local commitments at t
              // - let t' be the next tick after t; then the watermark of the running commitments must never move beyond t';
              //   otherwise, we lose the ability to compute the commitments at t'
              // Hence, the order here is critical for correctness; if the change moves us beyond t', first compute
              // the commitments at t', and only then update the snapshot
              _ <- optCompletedPeriod.traverse_(processCompletedPeriod(snapshot))
              _ <- updateSnapshot(toc, acsChange)
            } yield ()
          }
      } yield ()
    }.onShutdown {
      // TODO(#6175) maybe stop the queue instead?
      logger.info(s"Not processing ACS change at $toc due to shutdown")
    }

    FutureUtil.doNotAwait(
      queue.executeUnlessFailed(
        Policy
          .noisyInfiniteRetry(
            performPublish(),
            this,
            timeouts.storageMaxRetryInterval.asFiniteApproximation,
            s"publish ACS change at $toc",
            s"Disconnect and reconnect to the domain $domainId if this error persists.",
          )
          .onShutdown(
            logger.info("Giving up on producing ACS commitment due to shutdown")
          ),
        s"publish ACS change at $toc",
      ),
      failureMessage = s"Producing ACS commitments failed.",
      // If this happens, then the failure is fatal or there is some bug in the queuing or retrying.
      // Unfortunately, we can't do anything anymore to reliably prevent corruption of the running snapshot in the DB,
      // as the data may already be corrupted by now.
    )
  }

  def processBatch(
      timestamp: CantonTimestamp,
      batch: Traced[List[OpenEnvelope[SignedProtocolMessage[AcsCommitment]]]],
  ): FutureUnlessShutdown[Unit] =
    batch.withTraceContext(implicit traceContext => processBatchInternal(timestamp, _))

  /** Process incoming commitments.
    *
    * The caller(s) must jointly ensure that all incoming commitments are passed to this method, in their order
    * of arrival. Upon startup, the method must be called on all incoming commitments whose processing hasn't
    * finished yet, including those whose processing has been aborted due to shutdown.
    */
  def processBatchInternal(
      timestamp: CantonTimestamp,
      batch: List[OpenEnvelope[SignedProtocolMessage[AcsCommitment]]],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {

    if (batch.lengthCompare(1) != 0) {
      Errors.InternalError.MultipleCommitmentsInBatch(domainId, timestamp, batch.length)
    }

    val future = initFuture.flatMap { case () =>
      batch.traverse_ { envelope =>
        performUnlessClosingF(functionFullName) {
          val payload = envelope.protocolMessage.message

          val errors = List(
            (
              envelope.recipients != Recipients.cc(participantId),
              s"At $timestamp, (purportedly) ${payload.sender} sent an ACS commitment to me, but addressed the message to ${envelope.recipients}",
            ),
            (
              payload.counterParticipant != participantId,
              s"At $timestamp, (purportedly) ${payload.sender} sent an ACS commitment to me, but the commitment lists ${payload.counterParticipant} as the counterparticipant",
            ),
            (
              payload.period.toInclusive > timestamp,
              s"Received an ACS commitment with a future beforeAndAt timestamp. (Purported) sender: ${payload.sender}. Timestamp: ${payload.period.toInclusive}, receive timestamp: $timestamp",
            ),
            (
              tickBeforeOrAt(
                payload.period.toInclusive.forgetSecond,
                reconciliationInterval,
              ) != payload.period.toInclusive
                || tickBeforeOrAt(
                  payload.period.fromExclusive.forgetSecond,
                  reconciliationInterval,
                ) != payload.period.fromExclusive,
              s"Received commitment period doesn't align with the domain reconciliation interval: ${payload.period}",
            ),
          ).filter(_._1)

          errors match {
            case Nil =>
              val logMsg =
                s"Checking commitment (purportedly by) ${payload.sender} for period ${payload.period}"
              logger.debug(logMsg)
              checkSignedMessage(timestamp, envelope.protocolMessage)
            case _ =>
              errors.foreach { case (_, msg) => logger.error(msg) }
              Future.unit
          }
        }
      }
    }

    FutureUtil.logOnFailureUnlessShutdown(
      future,
      failureMessage = s"Failed to process incoming commitment. Halting SyncDomain $domainId",
      onFailure = _ => {
        // First close ourselves such that we don't process any more messages
        close()
        // Then close the sync domain, do not wait for the killswitch, otherwise we block the sequencer client shutdown
        FutureUtil.doNotAwait(Future.successful(killSwitch), s"failed to kill SyncDomain $domainId")
      },
    )
  }

  private def persistRunningCommitments(
      res: CommitmentSnapshot
  )(implicit traceContext: TraceContext): Future[Unit] = {
    store.runningCommitments
      .update(res.rt, res.delta, res.deleted)
      .map(_ => logger.debug(s"Persisted ACS commitments at ${res.rt}"))
  }

  private def updateSnapshot(rt: RecordTime, acsChange: AcsChange)(implicit
      traceContext: TraceContext
  ): Future[Unit] = {
    logger.debug(
      s"Applying ACS change at $rt: ${acsChange.activations.size} activated, ${acsChange.deactivations.size} archived"
    )
    for {
      snapshot <- runningCommitments
      _ = snapshot.update(rt, acsChange)
    } yield ()
  }

  private def indicateLocallyProcessed(
      period: CommitmentPeriod
  )(implicit traceContext: TraceContext): Future[Unit] = {
    endOfLastProcessedPeriod = Some(period.toInclusive)
    for {
      // delete the processed buffered commitments (safe to do at any point after `processBuffered` completes)
      _ <- store.queue.deleteThrough(period.toInclusive.forgetSecond)
      // mark that we're done with processing this period; safe to do at any point after the commitment has been sent
      // and the outstanding commitments stored
      _ <- store.markComputedAndSent(period)
    } yield {
      logger.info(
        s"Deleted buffered commitments and set last computed and sent timestamp set to ${period.toInclusive}"
      )
    }
  }

  private def checkSignedMessage(
      timestamp: CantonTimestamp,
      message: SignedProtocolMessage[AcsCommitment],
  )(implicit traceContext: TraceContext): Future[Unit] =
    for {
      validSig <- checkCommitmentSignature(message)
      // If signature passes, store such that we can prove Byzantine behavior if necessary
      _ <- if (validSig) store.storeReceived(message) else Future.unit
      commitment = message.message

      // TODO(#8207) Properly check that bounds of commitment.period fall on commitment ticks
      correctInterval = Seq(commitment.period.fromExclusive, commitment.period.toInclusive).forall {
        ts => tickBeforeOrAt(ts.forgetSecond, reconciliationInterval) == ts
      }

      _ <- if (validSig && correctInterval) checkCommitment(commitment) else Future.unit
    } yield {
      if (!validSig) {
        // TODO(M40): Somebody is being Byzantine, but we don't know who (we don't necessarily know the sender). Raise alarm?
        logger.error(
          s"""Received wrong signature for ACS commitment at timestamp $timestamp; purported sender: ${commitment.sender}; commitment: $commitment"""
        )
      } else if (!correctInterval) {
        // TODO(#8207) This will change once we can update domain parameters dynamically.
        // TODO(M40) Also, if the interval is actually wrong, raise an alarm
        logger.error(
          s"""Wrong interval for ACS commitment at timestamp $timestamp; sender: ${commitment.sender}; commitment: $commitment, period: ${commitment.period}"""
        )
      }
    }

  private def checkCommitmentSignature(
      message: SignedProtocolMessage[AcsCommitment]
  )(implicit traceContext: TraceContext): Future[Boolean] =
    for {
      cryptoSnapshot <- domainCrypto.awaitSnapshot(message.message.period.toInclusive.forgetSecond)
      pureCrypto = domainCrypto.pureCrypto
      msgHash = pureCrypto.digest(
        HashPurpose.AcsCommitment,
        message.message.getCryptographicEvidence,
      )
      result <- cryptoSnapshot
        .verifySignature(msgHash, message.message.sender, message.signature)
        .value
    } yield result
      .tapLeft(err => logger.error(s"Commitment signature verification failed with $err"))
      .isRight

  private def checkCommitment(
      commitment: AcsCommitment
  )(implicit traceContext: TraceContext): Future[Unit] =
    queue
      .executeUnlessFailed(
        // Make sure that the ready-for-remote check is atomic with buffering the commitment
        {
          val readyToCheck = readyForRemote.exists(_ >= commitment.period.toInclusive)

          if (readyToCheck) {
            // Do not sequentialize the checking
            Future.successful(checkMatchAndMarkSafe(List(commitment)))
          } else {
            logger.debug(s"Buffering $commitment for later processing")
            store.queue.enqueue(commitment).map((_: Unit) => Future.successful(()))
          }
        },
        s"check commitment readiness at ${commitment.period} by ${commitment.sender}",
      )
      .flatten

  private def indicateReadyForRemote(timestamp: CantonTimestampSecond): Unit = {
    readyForRemote.foreach(oldTs =>
      assert(
        oldTs <= timestamp,
        s"out of order timestamps in the commitment processor: $oldTs and $timestamp",
      )
    )
    readyForRemote = Some(timestamp)
  }

  private def processBuffered(
      timestamp: CantonTimestampSecond
  )(implicit traceContext: TraceContext): Future[Unit] = {
    for {
      toProcess <- store.queue.peekThrough(timestamp.forgetSecond)
      _ <- checkMatchAndMarkSafe(toProcess)
    } yield {
      logger.debug(
        s"Checked buffered remote commitments up to $timestamp and ready to check further ones without buffering"
      )
    }
  }

  /* Logs all necessary messages and returns whether the remote commitment matches the local ones */
  private def matches(
      remote: AcsCommitment,
      local: Iterable[(CommitmentPeriod, AcsCommitment.CommitmentType)],
      lastPruningTime: Option[CantonTimestamp],
  )(implicit traceContext: TraceContext): Boolean = {
    if (local.isEmpty) {
      if (lastPruningTime.forall(_ < remote.period.toInclusive.forgetSecond)) {
        // TODO(M40): This signifies a Byzantine sender (the signature passes). Alarms should be raised.
        Errors.MismatchError.NoSharedContracts.Mismatch(domainId, remote)
      } else
        logger.info(s"Ignoring incoming commitment for a pruned period: $remote")
      false
    } else {
      local.filter(_._2 != remote.commitment) match {
        case Nil => {
          lazy val logMsg: String =
            s"Commitment correct for sender ${remote.sender} and period ${remote.period}"
          logger.debug(logMsg)
          true
        }
        case mismatches => {
          // TODO(M40): This signifies a Byzantine sender (the signature passes). Alarms should be raised.
          Errors.MismatchError.CommitmentsMismatch.Mismatch(domainId, remote, mismatches.toSeq)
          false
        }
      }
    }
  }

  private def checkMatchAndMarkSafe(
      remote: List[AcsCommitment]
  )(implicit traceContext: TraceContext): Future[Unit] = {
    remote.traverse_ { cmt =>
      for {
        commitments <- store.getComputed(cmt.period, cmt.sender)
        lastPruningTime <- store.pruningStatus.valueOr { err =>
          ErrorUtil.internalError(new RuntimeException(s"Can't get the pruning status: $err"))
        }
        _ <-
          if (matches(cmt, commitments, lastPruningTime.map(_.timestamp))) {
            store.markSafe(cmt.sender, cmt.period)
          } else Future.unit
      } yield ()
    }
  }

  private def signCommitment(
      crypto: SyncCryptoApi,
      counterParticipant: ParticipantId,
      cmt: AcsCommitment.CommitmentType,
      period: CommitmentPeriod,
  )(implicit traceContext: TraceContext): Future[SignedProtocolMessage[AcsCommitment]] = {
    val payload = AcsCommitment.create(
      domainId,
      participantId,
      counterParticipant,
      period,
      cmt,
      protocolVersion,
    )
    SignedProtocolMessage.tryCreate(payload, crypto, domainCrypto.pureCrypto)
  }

  /* Compute commitment messages to be sent for the ACS at the given timestamp. The snapshot is assumed to be ordered
   * by contract IDs (ascending or descending both work, but must be the same at all participants) */
  private def commitmentMessages(
      period: CommitmentPeriod,
      commitmentSnapshot: Map[SortedSet[LfPartyId], AcsCommitment.CommitmentType],
  )(implicit
      traceContext: TraceContext
  ): Future[Map[ParticipantId, SignedProtocolMessage[AcsCommitment]]] = {
    logger.debug(
      s"Computing commitments for $period, number of stakeholder sets: ${commitmentSnapshot.keySet.size}"
    )
    for {
      crypto <- domainCrypto.awaitSnapshot(period.toInclusive.forgetSecond)
      cmts <- commitments(
        participantId,
        commitmentSnapshot,
        domainCrypto,
        period.toInclusive,
        Some(metrics),
        threadCount,
      )
      msgs <- cmts
        .collect {
          case (counterParticipant, cmt) if LtHash16.isNonEmptyCommitment(cmt) =>
            signCommitment(crypto, counterParticipant, cmt, period).map(msg =>
              (counterParticipant, msg)
            )
        }
        .toList
        .sequence
        .map(_.toMap)
    } yield msgs
  }

  /** Send the computed commitment messages and store their computed commitments */
  private def storeAndSendCommitmentMessages(
      period: CommitmentPeriod,
      msgs: Map[ParticipantId, SignedProtocolMessage[AcsCommitment]],
  )(implicit traceContext: TraceContext): Future[Unit] =
    for {
      _ <- msgs.toList.traverse_ { case (pid, msg) =>
        store.storeComputed(msg.message.period, pid, msg.message.commitment)
      }
      _ = logger.debug(s"Computed and stored ${msgs.size} commitment messages for period $period")
      batchForm = msgs.toList.map { case (pid, msg) => (msg, Recipients.cc(pid)) }
      batch = Batch.of[ProtocolMessage](protocolVersion, batchForm: _*)
      _ = if (batch.envelopes.nonEmpty)
        performUnlessClosingEitherT(functionFullName, ()) {
          EitherTUtil
            .logOnError(
              sequencerClient.sendAsync(batch, SendType.Other, None),
              s"Failed to send commitment message batch for period $period",
            )
            .leftMap(_ => ())
        }
    } yield logger.debug(
      s"Request to sequence local commitment messages for period $period sent to sequencer"
    )

  override protected def closeAsync(): Seq[AsyncOrSyncCloseable] = {
    import TraceContext.Implicits.Empty._
    Seq(
      queue.asCloseable("acs-commitment-processor-queue", timeouts.shutdownProcessing.unwrap),
      SyncCloseable("logging", logger.info("Shut down the ACS commitment processor")),
    )
  }

}

object AcsCommitmentProcessor {

  type ProcessorType =
    (
        CantonTimestamp,
        Traced[List[OpenEnvelope[SignedProtocolMessage[AcsCommitment]]]],
    ) => FutureUnlessShutdown[Unit]

  /** A snapshot of ACS commitments per set of stakeholders
    *
    * @param rt           The timestamp and tie-breaker of the snapshot
    * @param active       Maps stakeholders to the commitment to their shared ACS, if the shared ACS is not empty
    * @param delta        A sub-map of active with those stakeholders whose commitments have changed since the last snapshot
    * @param deleted      Stakeholder sets whose ACS has gone to empty since the last snapshot (no longer active)
    */
  case class CommitmentSnapshot(
      rt: RecordTime,
      active: Map[SortedSet[LfPartyId], AcsCommitment.CommitmentType],
      delta: Map[SortedSet[LfPartyId], AcsCommitment.CommitmentType],
      deleted: Set[SortedSet[LfPartyId]],
  )

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  class RunningCommitments(
      initRt: RecordTime,
      commitments: TrieMap[SortedSet[LfPartyId], LtHash16],
  ) {

    private val lock = new Object
    @volatile private var rt: RecordTime = initRt
    private val deltaB = Map.newBuilder[SortedSet[LfPartyId], LtHash16]

    /** The latest (immutable) snapshot. Taking the snapshot also garbage collects empty commitments.
      */
    def snapshot(): CommitmentSnapshot = {

      /* Delete all hashes that have gone empty since the last snapshot and return the corresponding stakeholder sets */
      def garbageCollect(
          candidates: Map[SortedSet[LfPartyId], LtHash16]
      ): Set[SortedSet[LfPartyId]] = {
        val deletedB = Set.newBuilder[SortedSet[LfPartyId]]
        candidates.foreach { case (stkhs, h) =>
          if (h.isEmpty) {
            deletedB += stkhs
            commitments -= stkhs
          }
        }
        deletedB.result()
      }

      blocking {
        lock.synchronized {
          val delta = deltaB.result()
          deltaB.clear()
          val deleted = garbageCollect(delta)
          val activeDelta = (delta -- deleted).fmap(_.getByteString())
          // Note that it's crucial to eagerly (via fmap, as opposed to, say mapValues) snapshot the LtHash16 values,
          // since they're mutable
          val res =
            CommitmentSnapshot(
              rt,
              commitments.readOnlySnapshot().toMap.fmap(_.getByteString()),
              activeDelta,
              deleted,
            )
          res
        }
      }
    }

    def update(rt: RecordTime, change: AcsChange): Unit = {

      def concatenate(contractHash: LfHash, contractId: LfContractId): Array[Byte] =
        (contractHash.bytes.toByteString // hash always 32 bytes long per lf.crypto.Hash.underlyingLength
          concat contractId.encodeDeterministically).toByteArray

      import com.digitalasset.canton.lfPartyOrdering
      blocking {
        lock.synchronized {
          this.rt = rt
          change.activations.foreach { case (cid, WithContractHash(metadata, hash)) =>
            val sortedStakeholders = SortedSet(metadata.stakeholders.toSeq: _*)
            val h = commitments.getOrElseUpdate(sortedStakeholders, LtHash16())
            h.add(concatenate(hash, cid))

            deltaB += sortedStakeholders -> h
          }
          change.deactivations.foreach { case (cid, WithContractHash(stakeholders, hash)) =>
            val sortedStakeholders = SortedSet(stakeholders.toSeq: _*)
            val h = commitments.getOrElseUpdate(sortedStakeholders, LtHash16())
            h.remove(concatenate(hash, cid))
            deltaB += sortedStakeholders -> h
          }
        }
      }
    }

    def watermark: RecordTime = rt

  }

  def tickBeforeOrAt(
      timestamp: CantonTimestamp,
      tickInterval: PositiveSeconds,
  ): CantonTimestampSecond = {
    // Uses the assumption that the interval has a round number of seconds
    val mod = timestamp.getEpochSecond % tickInterval.unwrap.getSeconds
    val sinceTickStart = if (mod >= 0) mod else tickInterval.unwrap.getSeconds + mod
    val beforeTs = timestamp.toInstant.truncatedTo(ChronoUnit.SECONDS).minusSeconds(sinceTickStart)

    CantonTimestampSecond.assertFromInstant(beforeTs.max(CantonTimestamp.MinValue.toInstant))
  }

  def tickBefore(
      timestamp: CantonTimestamp,
      tickInterval: PositiveSeconds,
  ): CantonTimestampSecond = {
    val beforeOrAt = tickBeforeOrAt(timestamp, tickInterval)
    Ordering[CantonTimestampSecond]
      .max(
        CantonTimestampSecond.MinValue,
        if (beforeOrAt < timestamp) beforeOrAt else beforeOrAt - tickInterval,
      )
  }

  def tickAfter(
      timestamp: CantonTimestamp,
      tickInterval: PositiveSeconds,
  ): CantonTimestampSecond = tickBeforeOrAt(timestamp, tickInterval) + tickInterval

  @VisibleForTesting
  private[pruning] def commitmentPeriodPreceding(
      interval: PositiveSeconds,
      endOfPreviousPeriod: Option[CantonTimestampSecond],
  )(timestamp: CantonTimestamp): Option[CommitmentPeriod] = {
    val periodEnd = tickBefore(timestamp, interval)
    val periodStart = endOfPreviousPeriod.getOrElse(CantonTimestampSecond.MinValue)

    CommitmentPeriod(periodStart.forgetSecond, periodEnd.forgetSecond, interval).toOption
  }

  /** Compute the ACS commitments at the given timestamp.
    *
    * Extracted as a pure function to be able to test.
    */
  @VisibleForTesting
  private[pruning] def commitments(
      participantId: ParticipantId,
      runningCommitments: Map[SortedSet[LfPartyId], AcsCommitment.CommitmentType],
      domainCrypto: SyncCryptoClient[SyncCryptoApi],
      timestamp: CantonTimestampSecond,
      pruningMetrics: Option[PruningMetrics],
      parallelism: PositiveNumeric[Int],
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): Future[Map[ParticipantId, AcsCommitment.CommitmentType]] = {
    val commitmentTimer = pruningMetrics.map(_.commitments.compute.metric.time())

    for {
      ipsSnapshot <- domainCrypto.ipsSnapshot(timestamp.forgetSecond)
      // Important: use the keys of the timestamp
      isActiveParticipant <- ipsSnapshot.isParticipantActive(participantId)

      byParticipant <-
        if (isActiveParticipant) {
          val allParties = runningCommitments.keySet.flatten
          ipsSnapshot.activeParticipantsOfParties(allParties.toSeq).flatMap { participantsOf =>
            IterableUtil
              .mapReducePar[(SortedSet[LfPartyId], AcsCommitment.CommitmentType), Map[
                ParticipantId,
                Set[AcsCommitment.CommitmentType],
              ]](parallelism, runningCommitments.toSeq) { case (parties, commitment) =>
                val participants = parties.flatMap(participantsOf.getOrElse(_, Set.empty))
                // Check that we're hosting at least one stakeholder; it can happen that the stakeholder used to be
                // hosted on this participant, but is now disabled
                val pSet =
                  if (participants.contains(participantId)) participants - participantId
                  else Set.empty
                val commitmentS = Set(commitment)
                pSet.map(_ -> commitmentS).toMap
              }(MapsUtil.mergeWith(_, _)(_.union(_)))
              .map(_.getOrElse(Map.empty[ParticipantId, Set[AcsCommitment.CommitmentType]]))
          }
        } else Future.successful(Map.empty[ParticipantId, Set[AcsCommitment.CommitmentType]])
    } yield {
      val res = byParticipant.fmap { hashes =>
        val sumHash = LtHash16()
        hashes.foreach(h => sumHash.add(h.toByteArray))
        sumHash.getByteString()
      }
      commitmentTimer.foreach(_.stop())
      res
    }
  }

  /* Extracted to be able to test more easily */
  @VisibleForTesting
  private[pruning] def safeToPrune_(
      cleanReplayF: Future[CantonTimestamp],
      commitmentsPruningBound: CommitmentsPruningBound,
      earliestInFlightSubmissionF: Future[Option[CantonTimestamp]],
      reconciliationInterval: PositiveSeconds,
  )(implicit
      ec: ExecutionContext,
      loggingContext: ErrorLoggingContext,
  ): Future[Option[CantonTimestampSecond]] =
    for {
      // This logic progressively lowers the timestamp based on the following constraints:
      // 1. Pruning must not delete data needed for recovery (after the clean replay timestamp)
      cleanReplayTs <- cleanReplayF

      // 2. Pruning must not delete events from the event log for which there are still in-flight submissions.
      // We check here the `SingleDimensionEventLog` for the domain; the participant event log must be taken care of separately.
      //
      // Processing of sequenced events may concurrently move the earliest in-flight submission back in time
      // (from timeout to sequencing timestamp), but this can only happen if the corresponding request is not yet clean,
      // i.e., the sequencing timestamp is after `cleanReplayTs`. So this concurrent modification does not affect
      // the calculation below.
      inFlightSubmissionTs <- earliestInFlightSubmissionF

      getTickBeforeOrAt = tickBeforeOrAt(_, reconciliationInterval)

      // Latest potential pruning point is the ACS commitment tick before or at the "clean replay" timestamp
      // and strictly before the earliest timestamp associated with an in-flight submission.
      latestTickBeforeOrAt = getTickBeforeOrAt(
        cleanReplayTs.min(
          inFlightSubmissionTs.fold(CantonTimestamp.MaxValue)(_.immediatePredecessor)
        )
      )

      // Only acs commitment ticks whose ACS commitment fully matches all counter participant ACS commitments are safe,
      // so look for the most recent such tick before latestTickBeforeOrAt if any.
      tsSafeToPruneUpTo <- commitmentsPruningBound match {
        case CommitmentsPruningBound.Outstanding(noOutstandingCommitmentsF) =>
          noOutstandingCommitmentsF(latestTickBeforeOrAt.forgetSecond).map(_.map(getTickBeforeOrAt))
        case CommitmentsPruningBound.LastComputedAndSent(lastComputedAndSentF) =>
          lastComputedAndSentF.map(
            _.map(lastComputedAndSent =>
              getTickBeforeOrAt(lastComputedAndSent).min(latestTickBeforeOrAt)
            )
          )
      }

      // Sanity check that safe pruning timestamp has not "increased" (which would be a coding bug).
      _ = tsSafeToPruneUpTo.foreach(ts =>
        ErrorUtil.requireState(
          ts <= latestTickBeforeOrAt,
          s"limit $tsSafeToPruneUpTo after $latestTickBeforeOrAt",
        )
      )
    } yield tsSafeToPruneUpTo

  /*
    Describe how ACS commitments are taken into account for the safeToPrune computation:
   */
  sealed trait CommitmentsPruningBound extends Product with Serializable
  object CommitmentsPruningBound {
    // Not before any outstanding commitment
    final case class Outstanding(
        noOutstandingCommitmentsF: CantonTimestamp => Future[Option[CantonTimestamp]]
    ) extends CommitmentsPruningBound

    // Not before any computed and sent commitment
    final case class LastComputedAndSent(
        lastComputedAndSentF: Future[Option[CantonTimestamp]]
    ) extends CommitmentsPruningBound
  }

  /** The latest commitment tick before or at the given time at which it is safe to prune. */
  def safeToPrune(
      requestJournalStore: RequestJournalStore,
      sequencerCounterTrackerStore: SequencerCounterTrackerStore,
      reconciliationInterval: PositiveSeconds,
      acsCommitmentStore: AcsCommitmentStore,
      inFlightSubmissionStore: InFlightSubmissionStore,
      domainId: DomainId,
      checkForOutstandingCommitments: Boolean,
  )(implicit
      ec: ExecutionContext,
      loggingContext: ErrorLoggingContext,
  ): Future[Option[CantonTimestampSecond]] = {
    implicit val traceContext: TraceContext = loggingContext.traceContext
    val cleanReplayF = SyncDomainEphemeralStateFactory
      .crashRecoveryPruningBoundInclusive(requestJournalStore, sequencerCounterTrackerStore)

    val commitmentsPruningBound =
      if (checkForOutstandingCommitments)
        CommitmentsPruningBound.Outstanding(acsCommitmentStore.noOutstandingCommitments(_))
      else
        CommitmentsPruningBound.LastComputedAndSent(
          acsCommitmentStore.lastComputedAndSent.map(_.map(_.forgetSecond))
        )

    val earliestInFlightF = inFlightSubmissionStore.lookupEarliest(domainId)
    safeToPrune_(
      cleanReplayF,
      commitmentsPruningBound = commitmentsPruningBound,
      earliestInFlightF,
      reconciliationInterval,
    )
  }

  object Errors extends AcsCommitmentErrorGroup {
    @Explanation(
      """This error indicates that there was an internal error within the ACS commitment processing."""
    )
    @Resolution("Inspect error message for details.")
    object InternalError
        extends ErrorCode(
          id = "ACS_COMMITMENT_INTERNAL_ERROR",
          ErrorCategory.SystemInternalAssumptionViolated,
        ) {

      override protected def exposedViaApi: Boolean = false

      case class MultipleCommitmentsInBatch(domain: DomainId, timestamp: CantonTimestamp, num: Int)(
          implicit val loggingContext: ErrorLoggingContext
      ) extends CantonError.Impl(
            cause = "Received multiple batched ACS commitments over domain"
          )
    }

    object MismatchError extends ErrorGroup {
      @Explanation("""This error indicates that a remote participant has sent a commitment over 
          |an ACS for a period, while this participant does not think that there is a shared contract state.
          |This error occurs if a remote participant has manually changed contracts using repair,
          |or due to byzantine behavior, or due to malfunction of the system. The consequence is that 
          |the ledger is forked, and some commands that should pass will not.""")
      @Resolution(
        """Please contact the other participant in order to check the cause of the mismatch. Either repair
          |the store of this participant or of the counterparty."""
      )
      object NoSharedContracts
          extends ErrorCode(
            id = "ACS_MISMATCH_NO_SHARED_CONTRACTS",
            ErrorCategory.BackgroundProcessDegradationWarning,
          ) {
        case class Mismatch(domain: DomainId, remote: AcsCommitment)(implicit
            val loggingContext: ErrorLoggingContext
        ) extends CantonError.Impl(
              cause = "Received a commitment where we have no shared contract with the sender"
            )
      }

      @Explanation("""This error indicates that a remote participant has sent a commitment over 
          |an ACS for a period which does not match the local commitment.
          |This error occurs if a remote participant has manually changed contracts using repair,
          |or due to byzantine behavior, or due to malfunction of the system. The consequence is that the ledger is forked, 
          |and some commands that should pass will not.""")
      @Resolution(
        """Please contact the other participant in order to check the cause of the mismatch. Either repair
          |the store of this participant or of the counterparty."""
      )
      object CommitmentsMismatch
          extends ErrorCode(
            id = "ACS_COMMITMENT_MISMATCH",
            ErrorCategory.BackgroundProcessDegradationWarning,
          ) {
        case class Mismatch(
            domain: DomainId,
            remote: AcsCommitment,
            local: Seq[(CommitmentPeriod, AcsCommitment.CommitmentType)],
        )(implicit val loggingContext: ErrorLoggingContext)
            extends CantonError.Impl(
              cause = "The local commitment does not match the remote commitment"
            )
      }

    }

  }

}
