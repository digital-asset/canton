// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol

import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.{
  AsyncOrSyncCloseable,
  FlagCloseableAsync,
  FutureUnlessShutdown,
  PromiseUnlessShutdown,
  SyncCloseable,
}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.protocol.SubmissionTracker.SubmissionData
import com.digitalasset.canton.participant.store.SubmissionTrackerStore
import com.digitalasset.canton.protocol.{RequestId, RootHash}
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ErrorUtil
import com.digitalasset.canton.version.ProtocolVersion

import scala.collection.concurrent.TrieMap
import scala.concurrent.ExecutionContext

/** Tracker for submission, backed by a `SubmissionTrackerStore`.
  *
  * The purpose of this tracker is to detect replayed requests, and allow the participant to emit
  * a command completion only for genuine requests that it has submitted.
  * A request R2 is considered a replay of a request R1 if they both contain the same transaction (root hash),
  * and R2 has been sequenced after R1 (its requestId is later).
  *
  * The protocol to use this tracker is:
  *
  * - At the beginning of Phase 3, the participant must first call the tracker's `register()` method for every incoming
  *   confirmation request, in sequencing order. This returns a `Future`, which must be kept until the completion of the
  *   request.
  *
  * - Further during processing of Phase 3, when further information about the request is obtained, the participant
  *   must either call `cancelRegistration()` when a request is deemed invalid and rejected immediately, or call
  *   `provideSubmissionData()` with information about the submission when a request proceeds normally.
  *
  * - During phase 7, when finalizing the request, the participant must use the result of the `Future` returned during
  *   registration to determine whether the request requires a command completion.
  *
  * Calling the methods in a different order than described above will result in undefined behavior.
  */
trait SubmissionTracker extends AutoCloseable {

  /** Register an ongoing transaction in the tracker.
    * @return a `Future` that represents the conditions:
    *         * the transaction is fresh, i.e. it is not a replay;
    *         * the transaction was submitted by this participant;
    *         * the transaction is timely, i.e. it was sequenced within its max sequencing time.
    */
  def register(rootHash: RootHash, requestId: RequestId)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Boolean]

  /** Cancel a previously registered submission and perform the necessary cleanup.
    * In particular, the associated `Future` returned by `register()` will be completed with `false`.
    */
  def cancelRegistration(rootHash: RootHash, requestId: RequestId)(implicit
      traceContext: TraceContext
  ): Unit

  /** Provide the submission data necessary to decide on transaction validity.
    */
  def provideSubmissionData(
      rootHash: RootHash,
      requestId: RequestId,
      submissionData: SubmissionData,
  )(implicit traceContext: TraceContext): Unit
}

object SubmissionTracker {
  final case class SubmissionData(
      submitterParticipant: ParticipantId,
      maxSequencingTimeO: Option[CantonTimestamp],
  )

  def apply(protocolVersion: ProtocolVersion)(
      participantId: ParticipantId,
      store: SubmissionTrackerStore,
      futureSupervisor: FutureSupervisor,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit ec: ExecutionContext): SubmissionTracker =
    new SubmissionTrackerImpl(protocolVersion)(
      participantId,
      store,
      futureSupervisor,
      timeouts,
      loggerFactory,
    )
}

class SubmissionTrackerImpl private[protocol] (protocolVersion: ProtocolVersion)(
    participantId: ParticipantId,
    store: SubmissionTrackerStore,
    futureSupervisor: FutureSupervisor,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit val ec: ExecutionContext)
    extends SubmissionTracker
    with FlagCloseableAsync
    with NamedLogging {
  // Used to order the observed requests. Only the first occurrence is considered for further processing,
  // the following ones are already known non-fresh.
  private val ongoingRequests = TrieMap[RootHash, (RequestId, PromiseUnlessShutdown[Boolean])]()

  override def register(rootHash: RootHash, requestId: RequestId)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[Boolean] =
    performUnlessClosing(functionFullName) {
      val resultPUS = new PromiseUnlessShutdown[Boolean](
        s"submission-tracker-result-$rootHash-$requestId",
        futureSupervisor,
      )

      ongoingRequests.putIfAbsent(rootHash, (requestId, resultPUS)) match {
        case Some((_requestId, _resultP)) =>
          // A previous request has already been observed for this transaction
          resultPUS.outcome(false)

        case None =>
        // No previous request has been observed for this transaction
      }

      resultPUS.futureUS
    }.onShutdown(FutureUnlessShutdown.abortedDueToShutdown)

  override def cancelRegistration(rootHash: RootHash, requestId: RequestId)(implicit
      traceContext: TraceContext
  ): Unit =
    ongoingRequests
      .updateWith(rootHash) {
        case Some((`requestId`, resultPUS)) =>
          // It is an ongoing request, complete it negatively and remove it
          resultPUS.completeWith(FutureUnlessShutdown.pure(false))
          None

        case v => v // Ignore
      }
      .discard

  override def provideSubmissionData(
      rootHash: RootHash,
      requestId: RequestId,
      submissionData: SubmissionData,
  )(implicit traceContext: TraceContext): Unit =
    ongoingRequests
      .updateWith(rootHash) {
        case Some((`requestId`, resultPUS)) =>
          val amParticipant = submissionData.submitterParticipant == participantId

          val requestIsValidFUS = if (protocolVersion <= ProtocolVersion.v4) {
            // Replay mitigation was introduced in PV=5; before that, we fall back on the previous behavior
            FutureUnlessShutdown.pure(amParticipant)
          } else {
            val maxSequencingTime = submissionData.maxSequencingTimeO.getOrElse(
              ErrorUtil.internalError(
                new InternalError(
                  s"maxSequencingTime in SubmissionData for PV > 4 must be defined"
                )
              )
            )
            if (amParticipant && requestId.unwrap <= maxSequencingTime) {
              store.registerFreshRequest(rootHash, requestId, maxSequencingTime)
            } else {
              FutureUnlessShutdown.pure(false)
            }
          }

          resultPUS.completeWith(requestIsValidFUS)

          // The entry can be removed
          None

        case value @ Some(_) =>
          // The root hash is associated to a different request, which is still ongoing.
          // The caller's request was not inserted during registration, and its future has
          // already been resolved with `false`.
          // We keep the original entry.
          value

        case None =>
          // The root hash is not associated to any request, the one that was associated to this
          // root hash has already been completed.
          // The caller's request was not inserted during registration, and its future has
          // already been resolved with `false`.
          // We keep the entry unassigned.
          None
      }
      .discard

  private def shutdown(): Unit =
    ongoingRequests.values.foreach { case (_requestId, resultPUS) => resultPUS.shutdown() }

  override protected def closeAsync(): Seq[AsyncOrSyncCloseable] =
    Seq(SyncCloseable("submission-tracker-result-promises", shutdown()))
}
