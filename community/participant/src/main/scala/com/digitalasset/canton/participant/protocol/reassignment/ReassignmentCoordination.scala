// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.reassignment

import cats.data.EitherT
import cats.instances.future.catsStdInstancesForFuture
import cats.syntax.functor.*
import cats.syntax.traverse.*
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.crypto.{DomainSnapshotSyncCryptoApi, SyncCryptoApiProvider}
import com.digitalasset.canton.data.{CantonTimestamp, ReassignmentSubmitterMetadata}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.protocol.reassignment.ReassignmentProcessingSteps.{
  ApplicationShutdown,
  ReassignmentProcessorError,
  ReassignmentStoreFailed,
  UnknownDomain,
}
import com.digitalasset.canton.participant.store.ReassignmentStore
import com.digitalasset.canton.participant.sync.SyncDomainPersistentStateManager
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.messages.DeliveredUnassignmentResult
import com.digitalasset.canton.sequencing.protocol.TimeProof
import com.digitalasset.canton.time.DomainTimeTracker
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.digitalasset.canton.util.SingletonTraverse.syntax.*
import com.digitalasset.canton.util.{ReassignmentTag, SameReassignmentType, SingletonTraverse}
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.{ExecutionContext, Future}

class ReassignmentCoordination(
    reassignmentStoreFor: Target[SynchronizerId] => Either[
      ReassignmentProcessorError,
      ReassignmentStore,
    ],
    recentTimeProofFor: RecentTimeProofProvider,
    reassignmentSubmissionFor: SynchronizerId => Option[ReassignmentSubmissionHandle],
    val staticDomainParameterFor: Traced[SynchronizerId] => Option[StaticDomainParameters],
    syncCryptoApi: SyncCryptoApiProvider,
    override val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends NamedLogging {

  private[reassignment] def awaitDomainTime(
      domain: ReassignmentTag[SynchronizerId],
      timestamp: CantonTimestamp,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, UnknownDomain, Unit] =
    reassignmentSubmissionFor(domain.unwrap) match {
      case Some(handle) =>
        handle.timeTracker.requestTick(timestamp, immediately = true)
        EitherT.right(handle.timeTracker.awaitTick(timestamp).map(_.void).getOrElse(Future.unit))
      case None =>
        EitherT.leftT(
          UnknownDomain(
            domain.unwrap,
            s"Unable to find domain when awaiting domain time $timestamp.",
          )
        )
    }

  /** Returns a future that completes when a snapshot can be taken on the given domain for the given timestamp.
    *
    * This is used when an assignment blocks for the identity state at the unassignment. For more general uses,
    * `awaitTimestamp` should be preferred as it triggers the progression of time on `domain` by requesting a tick.
    */
  private[reassignment] def awaitUnassignmentTimestamp(
      domain: Source[SynchronizerId],
      staticDomainParameters: Source[StaticDomainParameters],
      timestamp: CantonTimestamp,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, UnknownDomain, Unit] =
    EitherT(
      syncCryptoApi
        .forDomain(domain.unwrap, staticDomainParameters.unwrap)
        .toRight(UnknownDomain(domain.unwrap, "When assignment waits for unassignment timestamp"))
        .traverse(_.awaitTimestamp(timestamp).getOrElse(Future.unit))
    )

  /** Returns a future that completes when it is safe to take an identity snapshot for the given `timestamp` on the given `domain`.
    * [[scala.None$]] indicates that this point has already been reached before the call.
    * [[scala.Left$]] if the `domain` is unknown or the participant is not connected to the domain.
    */
  private[reassignment] def awaitTimestamp[T[X] <: ReassignmentTag[X]: SameReassignmentType](
      domain: T[SynchronizerId],
      staticDomainParameters: T[StaticDomainParameters],
      timestamp: CantonTimestamp,
  )(implicit
      traceContext: TraceContext
  ): Either[ReassignmentProcessorError, Option[Future[Unit]]] =
    (for {
      cryptoApi <- syncCryptoApi.forDomain(domain.unwrap, staticDomainParameters.unwrap)
      handle <- reassignmentSubmissionFor(domain.unwrap)
    } yield {
      handle.timeTracker.requestTick(timestamp, immediately = true)
      cryptoApi.awaitTimestamp(timestamp)
    }).toRight(UnknownDomain(domain.unwrap, "When waiting for timestamp"))

  /** Similar to [[awaitTimestamp]] but lifted into an [[EitherT]]
    *
    * @param onImmediate A callback that will be invoked if no wait was actually needed
    */
  private[reassignment] def awaitTimestamp[T[X] <: ReassignmentTag[X]: SameReassignmentType](
      domain: T[SynchronizerId],
      staticDomainParameters: T[StaticDomainParameters],
      timestamp: CantonTimestamp,
      onImmediate: => Future[Unit],
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, ReassignmentProcessorError, Unit] =
    for {
      timeout <- EitherT.fromEither[Future](
        awaitTimestamp(domain, staticDomainParameters, timestamp)
      )
      _ <- EitherT.right[ReassignmentProcessorError](timeout.getOrElse(onImmediate))
    } yield ()

  /** Submits an assignment. Used by the [[UnassignmentProcessingSteps]] to automatically trigger the submission of
    * an assignment after the exclusivity timeout.
    */
  private[reassignment] def assign(
      targetDomain: Target[SynchronizerId],
      submitterMetadata: ReassignmentSubmitterMetadata,
      reassignmentId: ReassignmentId,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, ReassignmentProcessorError, AssignmentProcessingSteps.SubmissionResult] = {
    logger.debug(s"Triggering automatic assignment of reassignment `$reassignmentId`")

    for {
      inSubmission <- EitherT.fromEither[Future](
        reassignmentSubmissionFor(targetDomain.unwrap).toRight(
          UnknownDomain(targetDomain.unwrap, "When submitting assignment")
        )
      )
      submissionResult <- inSubmission
        .submitAssignment(
          submitterMetadata,
          reassignmentId,
        )
        .mapK(FutureUnlessShutdown.outcomeK)
        .semiflatMap(Predef.identity)
        .onShutdown(Left(ApplicationShutdown))
    } yield submissionResult
  }

  private[reassignment] def getStaticDomainParameter[T[_]: SingletonTraverse](
      synchronizerId: T[SynchronizerId]
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, UnknownDomain, T[StaticDomainParameters]] =
    synchronizerId.traverseSingleton { (_, synchronizerId) =>
      EitherT.fromOption[Future](
        staticDomainParameterFor(Traced(synchronizerId)),
        UnknownDomain(synchronizerId, "getting static domain parameters"),
      )
    }

  /** Returns a [[crypto.DomainSnapshotSyncCryptoApi]] for the given `domain` at the given timestamp.
    * The returned future fails with [[java.lang.IllegalArgumentException]] if the `domain` has not progressed far enough
    * such that it can compute the snapshot. Use [[awaitTimestamp]] to ensure progression to `timestamp`.
    */
  private[reassignment] def cryptoSnapshot[T[X] <: ReassignmentTag[
    X
  ]: SameReassignmentType: SingletonTraverse](
      synchronizerId: T[SynchronizerId],
      staticDomainParameters: T[StaticDomainParameters],
      timestamp: CantonTimestamp,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, ReassignmentProcessorError, T[DomainSnapshotSyncCryptoApi]] =
    EitherT
      .fromEither[Future](
        // we use traverseSingleton to avoid wartremover warning about FutureTraverse
        synchronizerId.traverseSingleton { (_, synchronizerId) =>
          syncCryptoApi
            .forDomain(synchronizerId, staticDomainParameters.unwrap)
            .toRight(
              UnknownDomain(
                synchronizerId,
                "When getting crypto snapshot",
              ): ReassignmentProcessorError
            )
        }
      )
      .semiflatMap(_.traverseSingleton((_, syncCrypto) => syncCrypto.snapshot(timestamp)))

  private[reassignment] def awaitTimestampAndGetTaggedCryptoSnapshot(
      domain: Target[SynchronizerId],
      staticDomainParameters: Target[StaticDomainParameters],
      timestamp: CantonTimestamp,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ReassignmentProcessorError, Target[
    DomainSnapshotSyncCryptoApi
  ]] =
    for {
      _ <- awaitTimestamp(
        domain,
        staticDomainParameters,
        timestamp,
        Future.unit,
      ).mapK(FutureUnlessShutdown.outcomeK)
      snapshot <- cryptoSnapshot(domain, staticDomainParameters, timestamp).mapK(
        FutureUnlessShutdown.outcomeK
      )
    } yield snapshot

  private[reassignment] def getTimeProofAndSnapshot(
      targetDomain: Target[SynchronizerId],
      staticDomainParameters: Target[StaticDomainParameters],
  )(implicit
      traceContext: TraceContext
  ): EitherT[
    FutureUnlessShutdown,
    ReassignmentProcessorError,
    (TimeProof, Target[DomainSnapshotSyncCryptoApi]),
  ] =
    for {
      timeProof <- recentTimeProofFor.get(targetDomain, staticDomainParameters)
      // Since events are stored before they are processed, we wait just to be sure.
      targetCrypto <- awaitTimestampAndGetTaggedCryptoSnapshot(
        targetDomain,
        staticDomainParameters,
        timeProof.timestamp,
      )
    } yield (timeProof, targetCrypto)

  /** Stores the given reassignment data on the target domain. */
  private[reassignment] def addUnassignmentRequest(
      reassignmentData: ReassignmentData
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ReassignmentProcessorError, Unit] =
    for {
      reassignmentStore <- EitherT.fromEither[FutureUnlessShutdown](
        reassignmentStoreFor(reassignmentData.targetDomain)
      )
      _ <- reassignmentStore
        .addReassignment(reassignmentData)
        .leftMap[ReassignmentProcessorError](
          ReassignmentStoreFailed(reassignmentData.reassignmentId, _)
        )
    } yield ()

  /** Adds the unassignment result to the reassignment stored on the given domain. */
  private[reassignment] def addUnassignmentResult(
      domain: Target[SynchronizerId],
      unassignmentResult: DeliveredUnassignmentResult,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ReassignmentProcessorError, Unit] =
    for {
      reassignmentStore <- EitherT.fromEither[FutureUnlessShutdown](reassignmentStoreFor(domain))
      _ <- reassignmentStore
        .addUnassignmentResult(unassignmentResult)
        .leftMap[ReassignmentProcessorError](
          ReassignmentStoreFailed(unassignmentResult.reassignmentId, _)
        )
    } yield ()

  /** Removes the given [[com.digitalasset.canton.protocol.ReassignmentId]] from the given [[com.digitalasset.canton.topology.SynchronizerId]]'s [[store.ReassignmentStore]]. */
  private[reassignment] def deleteReassignment(
      targetDomain: Target[SynchronizerId],
      reassignmentId: ReassignmentId,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, ReassignmentProcessorError, Unit] =
    for {
      reassignmentStore <- EitherT.fromEither[FutureUnlessShutdown](
        reassignmentStoreFor(targetDomain)
      )
      _ <- EitherT.right[ReassignmentProcessorError](
        reassignmentStore.deleteReassignment(reassignmentId)
      )
    } yield ()

}

object ReassignmentCoordination {
  def apply(
      reassignmentTimeProofFreshnessProportion: NonNegativeInt,
      syncDomainPersistentStateManager: SyncDomainPersistentStateManager,
      submissionHandles: SynchronizerId => Option[ReassignmentSubmissionHandle],
      syncCryptoApi: SyncCryptoApiProvider,
      loggerFactory: NamedLoggerFactory,
  )(implicit ec: ExecutionContext): ReassignmentCoordination = {
    def domainDataFor(domain: Target[SynchronizerId]): Either[UnknownDomain, ReassignmentStore] =
      syncDomainPersistentStateManager
        .get(domain.unwrap)
        .map(_.reassignmentStore)
        .toRight(UnknownDomain(domain.unwrap, "looking for persistent state"))

    val staticDomainParametersGetter: Traced[SynchronizerId] => Option[StaticDomainParameters] =
      (tracedSynchronizerId: Traced[SynchronizerId]) =>
        syncDomainPersistentStateManager.staticDomainParameters(tracedSynchronizerId.value)

    val recentTimeProofProvider = new RecentTimeProofProvider(
      submissionHandles,
      syncCryptoApi,
      loggerFactory,
      reassignmentTimeProofFreshnessProportion,
    )

    new ReassignmentCoordination(
      reassignmentStoreFor = domainDataFor,
      recentTimeProofFor = recentTimeProofProvider,
      reassignmentSubmissionFor = submissionHandles,
      staticDomainParameterFor = staticDomainParametersGetter,
      syncCryptoApi = syncCryptoApi,
      loggerFactory = loggerFactory,
    )
  }
}

trait ReassignmentSubmissionHandle {
  def timeTracker: DomainTimeTracker

  def submitUnassignment(
      submitterMetadata: ReassignmentSubmitterMetadata,
      contractId: LfContractId,
      targetDomain: Target[SynchronizerId],
      targetProtocolVersion: Target[ProtocolVersion],
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, ReassignmentProcessorError, FutureUnlessShutdown[
    UnassignmentProcessingSteps.SubmissionResult
  ]]

  def submitAssignment(
      submitterMetadata: ReassignmentSubmitterMetadata,
      reassignmentId: ReassignmentId,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, ReassignmentProcessorError, FutureUnlessShutdown[
    AssignmentProcessingSteps.SubmissionResult
  ]]
}
