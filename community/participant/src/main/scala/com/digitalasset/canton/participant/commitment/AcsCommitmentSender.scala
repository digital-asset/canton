// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.commitment

import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.option.*
import cats.syntax.traverse.*
import cats.{Eval, Monad}
import com.daml.metrics.api.MetricsContext
import com.digitalasset.base.error.ErrorCategory
import com.digitalasset.base.error.utils.DecodedCantonError
import com.digitalasset.canton.LedgerParticipantId
import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.crypto.{SyncCryptoError, SynchronizerCryptoClient}
import com.digitalasset.canton.data.{CantonTimestamp, Offset}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
import com.digitalasset.canton.lifecycle.{
  AsyncCloseable,
  FlagCloseable,
  FutureUnlessShutdown,
  HasCloseContext,
  LifeCycle,
  SyncCloseable,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.config.AcsCommitmentSenderConfig
import com.digitalasset.canton.participant.metrics.{CommitmentMetrics, CommitmentSenderMetrics}
import com.digitalasset.canton.participant.store.AcsDigestStore.{
  AcsDigest,
  HashedDigest,
  InternedParticipantId,
}
import com.digitalasset.canton.participant.store.{
  AcsCommitmentSenderWatermarkStore,
  AcsDigestStore,
  PaginationTokenDone,
}
import com.digitalasset.canton.platform.store.interning.StringInterning
import com.digitalasset.canton.protocol.messages.{
  AcsCommitment,
  AcsCommitmentProtocolMessage,
  AcsCommitmentSummary,
  AcsCommitmentSummaryProtocolMessage,
  CommitmentPeriod,
  Digest,
  DigestForCounterparticipant,
}
import com.digitalasset.canton.sequencing.client.{
  SendAsyncClientError,
  SendCallback,
  SendResult,
  SequencerClientSend,
}
import com.digitalasset.canton.sequencing.protocol.{
  Batch,
  OpenEnvelope,
  Recipients,
  SequencerErrors,
}
import com.digitalasset.canton.time.{Clock, NonNegativeFiniteDuration, PositiveSeconds}
import com.digitalasset.canton.topology.{ParticipantId, PhysicalSynchronizerId, UniqueIdentifier}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.PekkoUtil.syntax.*
import com.digitalasset.canton.util.{ErrorUtil, LoggerUtil, MonadUtil, PekkoUtil}
import com.google.rpc.status.Status
import org.apache.pekko.stream.scaladsl.{Keep, Sink, Source}
import org.apache.pekko.stream.{KillSwitch, KillSwitches, Materializer}
import org.apache.pekko.{Done, NotUsed}

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.javaapi.DurationConverters
import scala.util.{Failure, Random, Success}

class AcsCommitmentSender(
    digestStore: AcsDigestStore,
    cryptoApi: SynchronizerCryptoClient,
    sequencerClient: SequencerClientSend,
    watermarkStore: AcsCommitmentSenderWatermarkStore,
    clock: Clock,
    stringInterningEval: Eval[StringInterning],
    metrics: CommitmentSenderMetrics,
    synchronizerId: PhysicalSynchronizerId,
    participantId: ParticipantId,
    config: AcsCommitmentSenderConfig,
    override val timeouts: ProcessingTimeout,
    override val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext, mat: Materializer)
    extends NamedLogging
    with HasCloseContext
    with FlagCloseable {
  import AcsCommitmentSender.*

  private implicit def metricsContext: MetricsContext = AcsCommitmentSender.metricsContext

  private def stringInterning = stringInterningEval.value

  private val digestJournal = digestStore.participant

  private val pipelineShutdownHandle: AtomicReference[Option[(KillSwitch, Future[Done])]] =
    new AtomicReference(None)

  private val directExecutionContext: ExecutionContext = DirectExecutionContext(noTracingLogger)

  /** First determines the latest available reconciliation interval tick checkpoint and sends ACS
    * commitments for all reconciliationtick checkpoints after the send watermark up to including
    * this crash recovery offset.
    *
    * After crash recovery, whenever the `tickSource` produces an offset, the sender attempts to
    * send ACS commitments for all offsets between the send watermark up to the emitted offset.
    */
  def startPipeline(
      tickSource: Source[Offset, NotUsed]
  )(implicit traceContext: TraceContext): Unit = {
    metrics.senderHealth.updateValue(CommitmentMetrics.HealthValues.Starting)
    // This doesn't use `futureSourceUS`, in case we want to materialize the graph multiple times,
    // which should result in properly computing the crash recovery checkpoint again. `futureSourceUS` would reuse the
    // checkpoint that was found during the first materialization.
    val crashRecoverySource = Source
      .single(())
      .mapAsyncAndDrainUS(1)(_ =>
        digestStore
          .latestCheckpointUpTo(
            Offset.MaxValue,
            AcsDigestStore.checkpointReconciliationFilter,
          )
      )
      .mapConcat(_.map(_.offset))

    val graph = crashRecoverySource
      .concat(tickSource)
      .viaMat(KillSwitches.single)(Keep.right)
      .mapAsyncAndDrainUS(1) { tickOffset =>
        // send commitments for the checkpoints after the send watermark up to `latestTickOffset`
        sendAcsCommitmentsUpTo(tickOffset)
      }
      .toMat(Sink.ignore)(Keep.both)

    synchronizeWithClosingSync("start ACS commitment send-loop") {
      val handle @ (_ks, doneF) =
        PekkoUtil.runSupervised(graph, s"AcsCommitmentSender($synchronizerId)")
      metrics.senderHealth.updateValue(CommitmentMetrics.HealthValues.Started)

      pipelineShutdownHandle.set(Some(handle))

      doneF
        .onComplete {
          case Success(_) =>
            metrics.senderHealth.updateValue(CommitmentMetrics.HealthValues.Stopped)
            if (isClosing) {
              logger.info("The send-loop terminated due to an orderly shutdown.")
            } else {
              logger.info(
                "The send-loop terminated orderly, because the reconciliation tick signaller terminated."
              )
            }
          case Failure(ex) =>
            metrics.senderHealth.updateValue(CommitmentMetrics.HealthValues.Failed)
            logger.warn(
              "The send-loop has failed with an error.",
              ex,
            )
        }(directExecutionContext)
    }.onShutdown {
      metrics.senderHealth.updateValue(CommitmentMetrics.HealthValues.Stopped)
    }

  }

  /** Sends the ACS commitments for all checkpoints after the send watermark and the provided offset
    * `upToInclusive`. If the sending for a particular checkpoint fails with an error, it makes
    * another attempt to send commitments for the same checkpoint again.
    */
  private def sendAcsCommitmentsUpTo(
      upToInclusive: Offset
  ): FutureUnlessShutdown[Unit] =
    // TODO(#33084) revisit this retry lopp
    Monad[FutureUnlessShutdown].tailRecM(()) { _ =>
      implicit val freshTraceContext = TraceContext.createNew("send-commitments")
      for {
        watermark <- watermarkStore.lookupWatermark()
        checkpointO <-
          digestStore.firstCheckpointAfter(
            watermark.map(_.offset).getOrElse(Offset.firstOffset),
            AcsDigestStore.checkpointReconciliationFilter,
          )
        checkpointUpToLatestTick = checkpointO
          .filter(_.offset <= upToInclusive)
        sendResult <- checkpointUpToLatestTick.traverse { cp =>
          for {
            snapshotCryptoApi <- cryptoApi.currentSnapshotApproximation
            dynamicSynchronizerParameters <- snapshotCryptoApi.ipsSnapshot
              .findDynamicSynchronizerParametersOrDefault(synchronizerId.protocolVersion)

            scheduleResult <- {
              val delay = randomDelay(
                dynamicSynchronizerParameters.reconciliationInterval,
                config.minSendDelayFraction,
                config.maxSendDelayFraction,
              )
              logger.debug(
                s"Delaying sending commitment by ${LoggerUtil.roundDurationForHumans(delay.duration)} (interval = ${dynamicSynchronizerParameters.reconciliationInterval}, min = ${config.minSendDelayFraction}, max = ${config.maxSendDelayFraction})"
              )
              clock
                // using scheduleAt instead of scheduleAfter, so we're not using a delay when the sender is catching up
                .scheduleAtCancelledOnShutdown(
                  action = _ => {
                    logger.info(s"Sending commitments for checkpoint $cp")
                    sendAcsCommitments(cp.timepoint).value.map((cp, _))
                  },
                  taskName = s"${getClass.getName} send ACS commitments with a randomized delay",
                  timestamp = cp.recordTime + delay,
                )
                .flatten
            }
          } yield scheduleResult
        }
      } yield sendResult match {
        case None =>
          logger.info(
            s"All commitments up to $upToInclusive have been sent successfully. Waiting for the next checkpoint signal."
          )
          // terminate the loop, because nothing had to be sent: either because there were no more checkpoints or
          // because the next checkpoint is after upToInclusive.
          Right(())
        case Some((cp, Left(error))) =>
          logger.warn(
            s"Sending commitments for checkpoint $cp resulted in the error $error. Retrying sending commitments for the same checkpoint."
          )
          Left(())
        case Some((cp, Right(()))) =>
          logger.info(
            s"Sending commitments for checkpoint $cp was successful. Moving on to the next checkpoint"
          )
          Left(())
      }
    }

  def sendAcsCommitments(
      timepoint: Timepoint
  )(implicit
      tc: TraceContext
  ): EitherT[FutureUnlessShutdown, AcsCommitmentSenderError, Unit] = {
    metrics.sendAttemptCount.inc()

    sendAcsCommitmentBatchesRecursively(
      RecursionStep[digestJournal.SnapshotPaginationToken](
        paginationToken = Right(timepoint.offset)
      ),
      timepoint.recordTime,
    ).semiflatMap { _ =>
      increaseWatermark(timepoint)
    }.leftMap { error =>
      metrics.sendFailureCount.inc()
      logger.error(s"An error occurred when sending ACS commitments: $error")

      error
    }
  }

  private def increaseWatermark(
      timepoint: Timepoint
  )(implicit tc: TraceContext): FutureUnlessShutdown[Unit] =
    watermarkStore.increaseWatermark(timepoint).map { _ =>
      metrics.watermarkOffset.updateValue(_ max timepoint.offset.unwrap)
      metrics.watermarkTimestamp.updateValue(timepoint.recordTime.toMicros)
    }

  /** The B type of the returned EitherT is either:
    *   - The next recursion step if there are more batches to send
    *   - Unit if it's the final batch
    */
  private def sendSingleBatch(
      recursionStep: RecursionStep[digestJournal.SnapshotPaginationToken],
      timestamp: CantonTimestamp,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, AcsCommitmentSenderError, Either[RecursionStep[
    digestJournal.SnapshotPaginationToken
  ], Unit]] = {

    logger.info(
      s"Preparing the ACS commitments batch [timestamp=$timestamp, batchIndex=${recursionStep.batchIndex}, attemptNumber=${recursionStep.attemptNumber}]"
    )

    val batchSendingResult
        : EitherT[FutureUnlessShutdown, AcsCommitmentSenderError, BatchSendingResult[
          digestJournal.SnapshotPaginationToken
        ]] = for {
      // Destructuring below, because of
      // https://contributors.scala-lang.org/t/for-comprehension-requires-withfilter-to-destructure-tuples/5953
      snapshotResult <- EitherT.right[AcsCommitmentSenderError](
        digestJournal.snapshot(recursionStep.paginationToken, config.maxBatchSize.value)
      )

      (digestUpdates, pagination) = snapshotResult
      digests = digestUpdates.map(_.digestUpdate).toSeq

      digestPartitionResult <- partitionDigestsForActiveAndInactiveCounterparticipants(digests)
      (internalDigestsForActive, internalDigestsForInactive) = digestPartitionResult

      unsentDigests = internalDigestsForInactive.map { digest =>
        DigestForCounterparticipant(
          digest = digest.digest,
          counterparticipant = digest.participantId.toLf,
        )
      }

      nextBatchIndex <-
        if (internalDigestsForActive.nonEmpty || unsentDigests.nonEmpty) {
          val commitments = internalDigestsForActive.map(commitmentFromDigest(_, timestamp))

          for {
            protocolMessages <- MonadUtil
              .parTraverseWithLimit(config.parallelism)(commitments)(
                AcsCommitmentProtocolMessage.signAndCreate(cryptoApi, _)
              )
              .leftMap(AcsCommitmentSenderError.SigningError.apply)

            commitmentEnvelopes = protocolMessages.map { protocolMessage =>
              OpenEnvelope(
                protocolMessage,
                Recipients.cc(
                  ParticipantId(
                    // The original counterparticipant id comes from the store,
                    // so using tryFromProtoPrimitive should be safe
                    UniqueIdentifier.tryFromProtoPrimitive(
                      protocolMessage.acsCommitment.counterparticipant
                    )
                  )
                ),
              )(synchronizerId.protocolVersion)
            }

            acsCommitmentSummaryMessage <- AcsCommitmentSummaryProtocolMessage
              .signAndCreate(
                cryptoApi,
                makeAcsCommitmentSummary(
                  counterparticipants = commitments.map(_.counterparticipant),
                  unsentDigests = unsentDigests,
                  commitmentTick = timestamp,
                  batchIndex = recursionStep.batchIndex,
                  lastBatch = pagination.isLeft,
                ),
              )
              .leftMap(AcsCommitmentSenderError.SigningError.apply)

            commitmentSummaryEnvelope = OpenEnvelope(
              acsCommitmentSummaryMessage,
              Recipients.cc(participantId),
            )(synchronizerId.protocolVersion)

            batch = Batch(
              commitmentEnvelopes.toList :+ commitmentSummaryEnvelope,
              synchronizerId.protocolVersion,
            )

            sendCallback = SendCallback.future

            _ <- sequencerClient
              .send(
                batch,
                callback = sendCallback,
              )
              .leftMap[AcsCommitmentSenderError](
                AcsCommitmentSenderError.SequencerClientError.apply
              )

            sendResult <- EitherT.right[AcsCommitmentSenderError](sendCallback.future)

            nextBatchIndex <- sendResult match {
              case _: SendResult.Success =>
                EitherT.pure[FutureUnlessShutdown, AcsCommitmentSenderError](
                  recursionStep.batchIndex.increment
                    .valueOr(err =>
                      ErrorUtil.invalidState(s"Batch index reached max value: ${err.message}")
                    )
                    .toNonNegative
                )
              case timeout: SendResult.Timeout =>
                EitherT.leftT[FutureUnlessShutdown, NonNegativeInt](
                  AcsCommitmentSenderError.SendResultTimeout(timeout): AcsCommitmentSenderError
                )
              case error: SendResult.Error =>
                EitherT.leftT[FutureUnlessShutdown, NonNegativeInt](
                  AcsCommitmentSenderError.SendResultError(error): AcsCommitmentSenderError
                )
            }
          } yield nextBatchIndex
        } else {
          EitherT.pure[FutureUnlessShutdown, AcsCommitmentSenderError](recursionStep.batchIndex)
        }
    } yield BatchSendingResult(pagination, nextBatchIndex, internalDigestsForActive.length)

    val nextStepResult = batchSendingResult
      .map[Either[RecursionStep[digestJournal.SnapshotPaginationToken], Unit]] { result =>
        if (result.nextBatchIndex > recursionStep.batchIndex) {
          metrics.sentBatchCount.inc()
        }
        metrics.sentCommitmentCount.inc(result.commitmentCount.toLong)

        result.paginationResult match {
          case Left(PaginationTokenDone) =>
            Right(())
          case Right(snapshotToken) =>
            Left(
              RecursionStep(
                paginationToken = Left(snapshotToken),
                batchIndex = result.nextBatchIndex,
              )
            )
        }
      }
      .recover {
        case error if error.retryStrategy.retryDelay.isDefined =>
          val finalDelay = calculateFinalRetryDelay(
            error.retryStrategy,
            recursionStep.attemptNumber,
            config.maxRetryDelay.underlying,
          )

          logger.info(
            s"Received error: $error, attemptCount: ${recursionStep.attemptNumber} setting up a retry with a delay of $finalDelay"
          )

          metrics.batchSendingErrorCount.inc()

          Left(
            recursionStep.copy(
              delay = finalDelay,
              attemptNumber = recursionStep.attemptNumber.increment
                .valueOr(err =>
                  ErrorUtil.invalidState(s"Attempt number reached max value: ${err.message}")
                )
                .toNonNegative,
            )
          )
      }
      .leftMap { error =>
        metrics.batchSendingErrorCount.inc()

        error
      }

    EitherT(
      nextStepResult.value.transform(
        identity,
        exception => {
          logger.error(s"An exception has been thrown during sending a batch: $exception.")
          metrics.batchSendingErrorCount.inc()

          exception
        },
      )
    )
  }

  private def partitionDigestsForActiveAndInactiveCounterparticipants(
      digests: Seq[AcsDigest[InternedParticipantId]]
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, Nothing, (Seq[InternalAcsDigest], Seq[InternalAcsDigest])] = {
    val internalDigests = makeInternalDigests(digests)
    val counterparticipants = internalDigests.map(_.participantId)

    for {
      activeCounterparticipants <- EitherT.right(
        cryptoApi.ips.currentSnapshotApproximation.flatMap(
          _.areMembersKnown(counterparticipants.toSet)
        )
      )
    } yield internalDigests.partition { digest =>
      activeCounterparticipants.contains(digest.participantId)
    }
  }

  private def sendAcsCommitmentBatchesRecursively(
      initialRecursionStep: RecursionStep[digestJournal.SnapshotPaginationToken],
      timestamp: CantonTimestamp,
  )(implicit
      tc: TraceContext
  ): EitherT[FutureUnlessShutdown, AcsCommitmentSenderError, Unit] =
    Monad[EitherT[FutureUnlessShutdown, AcsCommitmentSenderError, *]]
      .tailRecM[RecursionStep[digestJournal.SnapshotPaginationToken], Unit](
        initialRecursionStep
      ) { recursionStep =>
        recursionStep.delay match {
          case Some(delay) =>
            EitherT(
              clock
                .scheduleAfterCancelledOnShutdown(
                  action = _ => sendSingleBatch(recursionStep, timestamp).value,
                  taskName = s"${getClass.getName}: send ACS commitments batch with a delay",
                  delta = DurationConverters.toJava(delay),
                )
                .flatten
            )
          case None => sendSingleBatch(recursionStep, timestamp)
        }
      }

  private def makeInternalDigests(
      digests: Seq[AcsDigest[InternedParticipantId]]
  ): Seq[InternalAcsDigest] =
    digests.view.collect { digest =>
      digest.digestO match {
        case Some(value) =>
          InternalAcsDigest(
            participantId = ParticipantId(
              // The original counterparticipant id comes from the store,
              // so using tryFromProtoPrimitive should be safe
              UniqueIdentifier.tryFromProtoPrimitive(
                stringInterning.participantId.externalize(digest.key)
              )
            ),
            offset = digest.offset,
            timestamp = digest.timestamp,
            digest = Digest.hashDigest(value).getCryptographicEvidence,
          )
      }
    }.toSeq

  private def commitmentFromDigest(
      digest: InternalAcsDigest,
      timestamp: CantonTimestamp,
  ): AcsCommitment =
    AcsCommitment.create(
      synchronizerId = synchronizerId,
      sender = participantId.toLf,
      counterparticipant = digest.participantId.toLf,
      period = CommitmentPeriod
        .tryCreate(
          fromExclusive = digest.timestamp.immediatePredecessor,
          toInclusive = timestamp,
        ),
      digest = digest.digest,
      protocolVersion = synchronizerId.protocolVersion,
    )

  private def makeAcsCommitmentSummary(
      counterparticipants: Seq[LedgerParticipantId],
      unsentDigests: Seq[DigestForCounterparticipant],
      commitmentTick: CantonTimestamp,
      batchIndex: NonNegativeInt,
      lastBatch: Boolean,
  ) = AcsCommitmentSummary.create(
    psid = synchronizerId,
    commitmentTick = commitmentTick,
    addressedCounterparticipants = counterparticipants,
    unsentDigests = unsentDigests,
    batchIndex = batchIndex,
    lastBatch = lastBatch,
    protocolVersion = synchronizerId.protocolVersion,
  )

  override protected def onClosed(): Unit = {
    import TraceContext.Implicits.Empty.*
    val handleO = pipelineShutdownHandle.getAndSet(None)
    val closeables = handleO.toList.flatMap { case (ks, doneF) =>
      Seq(
        SyncCloseable("killSwitch shutdown", ks.shutdown()),
        AsyncCloseable(
          "pipeline completion future",
          doneF,
          timeouts.shutdownProcessing,
        ),
      )
    }
    LifeCycle.close(closeables)(logger)

  }
}

object AcsCommitmentSender {
  private[commitment] val metricsContext: MetricsContext = MetricsContext(
    "type" -> "acs-commitment-sender"
  )

  private final case class RecursionStep[T](
      paginationToken: Either[
        T,
        Offset,
      ],
      batchIndex: NonNegativeInt = NonNegativeInt.zero,
      delay: Option[FiniteDuration] = None,
      attemptNumber: NonNegativeInt = NonNegativeInt.zero,
  )

  private final case class BatchSendingResult[T](
      paginationResult: Either[
        PaginationTokenDone,
        T,
      ],
      nextBatchIndex: NonNegativeInt,
      commitmentCount: Int, // Not using NonNegativeInt for simplicity, because we pass the value of .length here
  )

  private val immediately = FiniteDuration(0, TimeUnit.SECONDS)
  private val defaultRetryDelay = FiniteDuration(1, TimeUnit.SECONDS)

  /** An internal case class representing a digest from
    * [[com.digitalasset.canton.participant.store.AcsDigestStore.AcsDigest]] after filtering empty
    * digests. Changes made to the original class:
    *   - digest is the actual value, not Option
    *   - trace has been removed (not needed in this context)
    *   - Got rid of the generic types
    *   - The key (participant id) is not interned
    */
  private final case class InternalAcsDigest(
      participantId: ParticipantId,
      offset: Offset,
      timestamp: CantonTimestamp,
      digest: HashedDigest,
  )

  /** @param retryDelay
    *   duration till the retry, if None, the attempt should not be retried.
    * @param useExponentialBackoff
    *   flag indicating whether or not the `retryDelay` should be potentially increased using the
    *   exponential backoff logic.
    */
  final case class RetryStrategy(
      retryDelay: Option[FiniteDuration],
      useExponentialBackoff: Boolean = true,
  )

  private[commitment] def calculateFinalRetryDelay(
      retryStrategy: RetryStrategy,
      attemptCount: NonNegativeInt,
      maxDelay: FiniteDuration,
  ): Option[FiniteDuration] =
    if (retryStrategy.useExponentialBackoff) {
      retryStrategy.retryDelay.map { retryDelay =>
        (0 until attemptCount.value).foldLeft(retryDelay) { case (currentDelay, _) =>
          (currentDelay * 2).min(maxDelay)
        }
      }
    } else retryStrategy.retryDelay

  private[commitment] def randomDelay(
      reconciliationInterval: PositiveSeconds,
      min: Double,
      max: Double,
  ): NonNegativeFiniteDuration =
    if (min < max)
      NonNegativeFiniteDuration.tryOfMicros(
        (Random
          .between(min, max) * reconciliationInterval.toFiniteDuration.toMicros.toDouble).toLong
      )
    else NonNegativeFiniteDuration.Zero

  sealed trait AcsCommitmentSenderError extends Product with Serializable {
    def retryStrategy: RetryStrategy
  }

  private object AcsCommitmentSenderError {
    final case class SigningError(error: SyncCryptoError) extends AcsCommitmentSenderError {
      override val retryStrategy: RetryStrategy = RetryStrategy(retryDelay = None)
    }

    /** The error coming from [[com.digitalasset.canton.sequencing.client.SequencerClientSend#send]]
      * directly
      */
    final case class SequencerClientError(clientError: SendAsyncClientError)
        extends AcsCommitmentSenderError {
      override val retryStrategy: RetryStrategy = clientError match {
        case _: SendAsyncClientError.RequestInvalid => RetryStrategy(retryDelay = None)
        case _ => RetryStrategy(retryDelay = defaultRetryDelay.some)
      }
    }

    /** The timeout coming from
      * [[com.digitalasset.canton.sequencing.client.SequencerClientSend#send]] being set in the
      * `callback` argument.
      */
    final case class SendResultTimeout(timeout: SendResult.Timeout)
        extends AcsCommitmentSenderError {
      override val retryStrategy: RetryStrategy = RetryStrategy(retryDelay = immediately.some)
    }

    /** The error coming from [[com.digitalasset.canton.sequencing.client.SequencerClientSend#send]]
      * being set in the `callback` argument.
      */
    final case class SendResultError(error: SendResult.Error) extends AcsCommitmentSenderError {
      override val retryStrategy: RetryStrategy = {
        categoryFromErrorStatus(error.error.reason).map { category =>
          val retryDelay = category.retryable.map(_.duration)
          val useExponentialBackoff = category match {
            case ErrorCategory.ContentionOnSharedResources => true
            case _ => false
          }

          RetryStrategy(retryDelay, useExponentialBackoff)
        }
      }.getOrElse(RetryStrategy(retryDelay = None))
    }
  }

  private def categoryFromErrorStatus(errorStatus: Status): Option[ErrorCategory] =
    errorStatus match {
      // UnknownRecipients should actually be retried since we filter out inactive recipients before sending
      case SequencerErrors.UnknownRecipients(_) => ErrorCategory.TransientServerFailure.some

      case _ =>
        DecodedCantonError.fromGrpcStatus(errorStatus).toOption.map { decodedCantonError =>
          decodedCantonError.code.category
        }
    }

}
