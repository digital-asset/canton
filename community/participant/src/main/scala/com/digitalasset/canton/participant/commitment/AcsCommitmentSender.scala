// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.commitment

import cats.data.EitherT
import cats.{Eval, Monad}
import com.daml.metrics.api.MetricsContext
import com.digitalasset.canton.LedgerParticipantId
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.crypto.{SyncCryptoError, SynchronizerCryptoClient}
import com.digitalasset.canton.data.{CantonTimestamp, Offset}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
import com.digitalasset.canton.participant.config.AcsCommitmentSenderConfig
import com.digitalasset.canton.participant.store.AcsDigestStore.{
  AcsDigestUpdate,
  HashedDigest,
  InternedParticipantId,
  RawDigest,
}
import com.digitalasset.canton.participant.store.{AcsDigestStore, PaginationTokenDone}
import com.digitalasset.canton.platform.store.interning.StringInterning
import com.digitalasset.canton.protocol.messages.{
  AcsCommitment,
  AcsCommitmentProtocolMessage,
  AcsCommitmentSummary,
  AcsCommitmentSummaryProtocolMessage,
  CommitmentPeriod,
}
import com.digitalasset.canton.sequencing.client.{
  SendAsyncClientError,
  SendCallback,
  SendResult,
  SequencerClientSend,
}
import com.digitalasset.canton.sequencing.protocol.{Batch, OpenEnvelope, Recipients}
import com.digitalasset.canton.topology.{ParticipantId, PhysicalSynchronizerId, UniqueIdentifier}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.MonadUtil

import scala.concurrent.ExecutionContext

class AcsCommitmentSender(
    digestStore: AcsDigestStore,
    cryptoApi: SynchronizerCryptoClient,
    sequencerClient: SequencerClientSend,
    stringInterningEval: Eval[StringInterning],
    synchronizerId: PhysicalSynchronizerId,
    participantId: ParticipantId,
    config: AcsCommitmentSenderConfig,
)(implicit ec: ExecutionContext) {
  import AcsCommitmentSender.*

  private implicit val metricsContext: MetricsContext = MetricsContext(
    "type" -> "acs-commitment-sender"
  )

  private def stringInterning = stringInterningEval.value

  private val digestJournal = digestStore.participant

  def sendAcsCommitments(
      offset: Offset,
      timestamp: CantonTimestamp,
  )(implicit
      tc: TraceContext
  ): EitherT[FutureUnlessShutdown, AcsCommitmentSenderError, Unit] =
    sendAcsCommitmentBatchesRecursively(
      RecursionStep[digestJournal.SnapshotPaginationToken](
        paginationToken = Right(offset),
        batchIndex = NonNegativeInt.zero,
      ),
      timestamp,
    ).map(_ => ())

  private def sendAcsCommitmentBatchesRecursively(
      initialRecursionStep: RecursionStep[digestJournal.SnapshotPaginationToken],
      timestamp: CantonTimestamp,
  )(implicit
      tc: TraceContext
  ): EitherT[FutureUnlessShutdown, AcsCommitmentSenderError, PaginationTokenDone] =
    Monad[EitherT[FutureUnlessShutdown, AcsCommitmentSenderError, *]]
      .tailRecM[RecursionStep[digestJournal.SnapshotPaginationToken], PaginationTokenDone](
        initialRecursionStep
      ) { recursionStep =>
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
          internalDigests = makeInternalDigests(digestUpdates.toSeq)

          nextBatchIndex <-
            if (internalDigests.nonEmpty) {
              val commitments = internalDigests.map(commitmentFromDigest(_, timestamp))

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
                  .leftMap[AcsCommitmentSenderError](AcsCommitmentSenderError.SendingError.apply)

                sendResult <- EitherT.right[AcsCommitmentSenderError](sendCallback.future)

                nextBatchIndex <- sendResult match {
                  case _: SendResult.Success =>
                    EitherT.pure[FutureUnlessShutdown, AcsCommitmentSenderError](
                      recursionStep.batchIndex.increment.toNonNegative
                    )
                  case _ =>
                    // At this stage, we treat every non-successful result as generic error
                    // This will be improved later
                    // TODO(#34070) Improve the error handling of the ACS Commitment Sender
                    EitherT.leftT[FutureUnlessShutdown, NonNegativeInt](
                      AcsCommitmentSenderError.UnexpectedSendResultError: AcsCommitmentSenderError
                    )
                }
              } yield nextBatchIndex
            } else {
              EitherT.pure[FutureUnlessShutdown, AcsCommitmentSenderError](recursionStep.batchIndex)
            }
        } yield BatchSendingResult(pagination, nextBatchIndex)

        batchSendingResult
          .map[Either[RecursionStep[digestJournal.SnapshotPaginationToken], PaginationTokenDone]] {
            result =>
              result.paginationResult match {
                case Left(PaginationTokenDone) => Right(PaginationTokenDone)
                case Right(snapshotToken) =>
                  Left(
                    RecursionStep(
                      paginationToken = Left(snapshotToken),
                      batchIndex = result.nextBatchIndex,
                    )
                  )
              }

          }
      }

  private def makeInternalDigests(
      digestUpdates: Seq[AcsDigestUpdate[InternedParticipantId, (RawDigest, HashedDigest)]]
  ): Seq[InternalAcsDigest] =
    digestUpdates.view
      .map(_.digestUpdate)
      .collect { digest =>
        digest.digestO match {
          case Some(value) =>
            InternalAcsDigest(
              key = digest.key,
              offset = digest.offset,
              timestamp = digest.timestamp,
              digest = value._2,
            )
        }
      }
      .toSeq

  private def commitmentFromDigest(
      digest: InternalAcsDigest,
      timestamp: CantonTimestamp,
  ): AcsCommitment =
    AcsCommitment.create(
      synchronizerId = synchronizerId,
      sender = participantId.toLf,
      counterparticipant = stringInterning.participantId.externalize(digest.key),
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
      commitmentTick: CantonTimestamp,
      batchIndex: NonNegativeInt,
      lastBatch: Boolean,
  ) = AcsCommitmentSummary.create(
    psid = synchronizerId,
    commitmentTick = commitmentTick,
    addressedCounterparticipants = counterparticipants,
    unsentDigests = Seq.empty,
    batchIndex = batchIndex,
    lastBatch = lastBatch,
    protocolVersion = synchronizerId.protocolVersion,
  )
}

object AcsCommitmentSender {

  /** An internal case class representing a digest from
    * [[com.digitalasset.canton.participant.store.AcsDigestStore.AcsDigest]] after filtering empty
    * digests. Changes made to the original class:
    *   - digest is the actual value, not Option
    *   - trace has been removed (not needed in this context)
    *   - Got rid of the generic types
    */
  private final case class InternalAcsDigest(
      key: InternedParticipantId,
      offset: Offset,
      timestamp: CantonTimestamp,
      digest: HashedDigest,
  )

  private final case class RecursionStep[T](
      paginationToken: Either[T, Offset],
      batchIndex: NonNegativeInt,
  )

  private final case class BatchSendingResult[T](
      paginationResult: Either[
        PaginationTokenDone,
        T,
      ],
      nextBatchIndex: NonNegativeInt,
  )

  sealed trait AcsCommitmentSenderError extends Product with Serializable

  private object AcsCommitmentSenderError {
    final case class SigningError(error: SyncCryptoError) extends AcsCommitmentSenderError
    final case class SendingError(error: SendAsyncClientError) extends AcsCommitmentSenderError
    final case object UnexpectedSendResultError extends AcsCommitmentSenderError
  }
}
