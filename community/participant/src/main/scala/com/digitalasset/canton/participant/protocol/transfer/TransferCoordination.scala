// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.transfer

import cats.data.EitherT
import com.digitalasset.canton.LfWorkflowId
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.crypto.{DomainSnapshotSyncCryptoApi, SyncCryptoApiProvider}
import com.digitalasset.canton.data.{CantonTimestamp, TransferSubmitterMetadata}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.protocol.transfer.TransferCoordination.DomainData
import com.digitalasset.canton.participant.protocol.transfer.TransferProcessingSteps.{
  ApplicationShutdown,
  NoTimeProofFromDomain,
  TransferProcessorError,
  TransferStoreFailed,
  UnknownDomain,
}
import com.digitalasset.canton.participant.store.TransferStore
import com.digitalasset.canton.participant.sync.SyncDomainPersistentStateManager
import com.digitalasset.canton.protocol.messages.DeliveredTransferOutResult
import com.digitalasset.canton.protocol.{LfContractId, TransferId}
import com.digitalasset.canton.time.{DomainTimeTracker, NonNegativeFiniteDuration, TimeProof}
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.OptionUtil
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.version.Transfer.{SourceProtocolVersion, TargetProtocolVersion}

import scala.concurrent.{ExecutionContext, Future}

class TransferCoordination(
    domainDataById: DomainId => Option[DomainData],
    inSubmissionById: DomainId => Option[TransferSubmissionHandle],
    val protocolVersion: Traced[DomainId] => Future[Option[ProtocolVersion]],
    syncCryptoApi: SyncCryptoApiProvider,
    override val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends NamedLogging {

  /** Returns a future that completes when a snapshot can be taken on the given domain for the given timestamp.
    *
    * This is used when a transfer-in blocks for the identity state at the transfer-out. For more general uses,
    * `awaitTimestamp` should be preferred as it triggers the progression of time on `domain` by requesting a tick.
    */
  private[transfer] def awaitTransferOutTimestamp(
      domain: DomainId,
      timestamp: CantonTimestamp,
  )(implicit
      traceContext: TraceContext
  ): Either[UnknownDomain, Future[Unit]] = {
    syncCryptoApi
      .forDomain(domain)
      .toRight(UnknownDomain(domain, "When transfer-in waits for transfer-out timestamp"))
      .map(_.awaitTimestamp(timestamp, waitForEffectiveTime = true).getOrElse(Future.unit))
  }

  /** Returns a future that completes when it is safe to take an identity snapshot for the given `timestamp` on the given `domain`.
    * [[scala.None$]] indicates that this point has already been reached before the call.
    * [[scala.Left$]] if the `domain` is unknown or the participant is not connected to the domain.
    *
    * @param waitForEffectiveTime if set to true, we'll wait for t+epsilon, which means we'll wait until we have observed the sequencing time t
    */
  private[transfer] def awaitTimestamp(
      domain: DomainId,
      timestamp: CantonTimestamp,
      waitForEffectiveTime: Boolean,
  )(implicit
      traceContext: TraceContext
  ): Either[TransferProcessorError, Option[Future[Unit]]] = {
    OptionUtil
      .zipWith(syncCryptoApi.forDomain(domain), inSubmissionById(domain)) { (cryptoApi, handle) =>
        handle.timeTracker.requestTick(timestamp)
        cryptoApi.awaitTimestamp(timestamp, waitForEffectiveTime)
      }
      .toRight(UnknownDomain(domain, "When waiting for timestamp"))
  }

  /** Submits a transfer-in. Used by the [[TransferOutProcessingSteps]] to automatically trigger the submission of a
    * transfer-in after the exclusivity timeout.
    */
  private[transfer] def transferIn(
      domainId: DomainId,
      submitterMetadata: TransferSubmitterMetadata,
      workflowId: Option[LfWorkflowId],
      transferId: TransferId,
      sourceProtocolVersion: SourceProtocolVersion,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, TransferProcessorError, TransferInProcessingSteps.SubmissionResult] = {
    logger.debug(s"Triggering automatic transfer-in of transfer `$transferId`")

    for {
      inSubmission <- EitherT.fromEither[Future](
        inSubmissionById(domainId).toRight(UnknownDomain(domainId, "When transferring in"))
      )
      submissionResult <- inSubmission
        .submitTransferIn(
          submitterMetadata,
          workflowId,
          transferId,
          sourceProtocolVersion,
        )
        .mapK(FutureUnlessShutdown.outcomeK)
        .semiflatMap(Predef.identity)
        .onShutdown(Left(ApplicationShutdown))
    } yield submissionResult
  }

  /** Returns a [[crypto.DomainSnapshotSyncCryptoApi]] for the given `domain` at the given timestamp.
    * The returned future fails with [[java.lang.IllegalArgumentException]] if the `domain` has not progressed far enough
    * such that it can compute the snapshot. Use [[awaitTimestamp]] to ensure progression to `timestamp`.
    */
  private[transfer] def cryptoSnapshot(domain: DomainId, timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): EitherT[Future, TransferProcessorError, DomainSnapshotSyncCryptoApi] =
    EitherT
      .fromEither[Future](
        syncCryptoApi
          .forDomain(domain)
          .toRight(UnknownDomain(domain, "When getting crypto snapshot"): TransferProcessorError)
      )
      .semiflatMap(_.snapshot(timestamp))

  /** Returns a recent time proof received from the given domain. */
  private def recentTimeProof(
      domain: DomainId
  ): EitherT[FutureUnlessShutdown, TransferProcessorError, TimeProof] =
    for {
      domainData <- EitherT.fromEither[FutureUnlessShutdown](lookupDomain(domain))
      timeProof <- domainData
        .recentTimeProofSource()
        .leftMap[TransferProcessorError](_ => NoTimeProofFromDomain(domain))
    } yield timeProof

  private[transfer] def getTimeProofAndSnapshot(targetDomain: DomainId)(implicit
      traceContext: TraceContext
  ): EitherT[
    FutureUnlessShutdown,
    TransferProcessorError,
    (TimeProof, DomainSnapshotSyncCryptoApi),
  ] =
    for {
      timeProof <- recentTimeProof(targetDomain)
      timestamp = timeProof.timestamp

      // Since events are stored before they are processed, we wait just to be sure.
      waitFuture <- EitherT.fromEither[FutureUnlessShutdown](
        awaitTimestamp(targetDomain, timestamp, waitForEffectiveTime = true)
      )
      _ <- EitherT.right(FutureUnlessShutdown.outcomeF(waitFuture.getOrElse(Future.unit)))
      targetCrypto <- cryptoSnapshot(targetDomain, timestamp)
        .mapK(FutureUnlessShutdown.outcomeK)
    } yield (timeProof, targetCrypto)

  /** Stores the given transfer data on the target domain. */
  private[transfer] def addTransferOutRequest(
      transferData: TransferData
  )(implicit traceContext: TraceContext): EitherT[Future, TransferProcessorError, Unit] = {
    for {
      domainData <- EitherT.fromEither[Future](lookupDomain(transferData.targetDomain))
      _ <- domainData.transferStore
        .addTransfer(transferData)
        .leftMap[TransferProcessorError](TransferStoreFailed(transferData.transferId, _))
    } yield ()
  }

  /** Adds the transfer-out result to the transfer stored on the given domain. */
  private[transfer] def addTransferOutResult(
      domain: DomainId,
      transferOutResult: DeliveredTransferOutResult,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, TransferProcessorError, Unit] = {
    for {
      domainData <- EitherT.fromEither[Future](lookupDomain(domain))
      _ <- domainData.transferStore
        .addTransferOutResult(transferOutResult)
        .leftMap[TransferProcessorError](TransferStoreFailed(transferOutResult.transferId, _))
    } yield ()
  }

  /** Removes the given [[com.digitalasset.canton.protocol.TransferId]] from the given [[com.digitalasset.canton.topology.DomainId]]'s [[store.TransferStore]]. */
  private[transfer] def deleteTransfer(targetDomain: DomainId, transferId: TransferId)(implicit
      traceContext: TraceContext
  ): EitherT[Future, TransferProcessorError, Unit] =
    for {
      domainData <- EitherT.fromEither[Future](lookupDomain(targetDomain))
      _ <- EitherT.right[TransferProcessorError](
        domainData.transferStore.deleteTransfer(transferId)
      )
    } yield ()

  private[this] def lookupDomain(domainId: DomainId): Either[TransferProcessorError, DomainData] =
    domainDataById(domainId).toRight(UnknownDomain(domainId, "When looking up domain"))
}

object TransferCoordination {

  sealed trait TimeProofSourceError

  /** It is likely not possible for the domain parameters to be missing from our store after successfully connecting */
  case object DomainParametersNotAvailable extends TimeProofSourceError

  case class DomainData(
      transferStore: TransferStore,
      recentTimeProofSource: () => EitherT[FutureUnlessShutdown, TimeProofSourceError, TimeProof],
  )

  def apply(
      transferTimeProofFreshnessProportion: NonNegativeInt,
      syncDomainPersistentStateManager: SyncDomainPersistentStateManager,
      submissionHandles: DomainId => Option[TransferSubmissionHandle],
      syncCryptoApi: SyncCryptoApiProvider,
      loggerFactory: NamedLoggerFactory,
  )(implicit ec: ExecutionContext, traceContext: TraceContext): TransferCoordination = {
    def calculateFreshness(
        exclusivityTimeout: NonNegativeFiniteDuration
    ): NonNegativeFiniteDuration =
      if (transferTimeProofFreshnessProportion.unwrap == 0)
        NonNegativeFiniteDuration.Zero // always fetch time proof
      else {
        // divide the exclusivity timeout by the given proportion
        NonNegativeFiniteDuration(
          exclusivityTimeout.duration.dividedBy(transferTimeProofFreshnessProportion.unwrap.toLong)
        )
      }

    def domainDataFor(domain: DomainId): Option[DomainData] = {
      OptionUtil.zipWith(syncDomainPersistentStateManager.get(domain), submissionHandles(domain)) {
        (state, handle) =>
          def recentTimeProofSource()
              : EitherT[FutureUnlessShutdown, TimeProofSourceError, TimeProof] = for {
            crypto <- EitherT.fromEither[FutureUnlessShutdown](
              syncCryptoApi.forDomain(domain).toRight(DomainParametersNotAvailable)
            )

            /*
              We use `findDynamicDomainParameters` rather than `findDynamicDomainParametersOrDefault`
              because it makes no sense to progress if we don't manage to fetch domain parameters.
              Also, the `findDynamicDomainParametersOrDefault` method expected protocol version
              that we don't have here.
             */
            parameters <- EitherT(
              FutureUnlessShutdown
                .outcomeF(
                  crypto.ips.currentSnapshotApproximation.findDynamicDomainParameters()
                )
                .map(_.toRight(DomainParametersNotAvailable))
            )

            exclusivityTimeout = parameters.transferExclusivityTimeout
            desiredTimeProofFreshness = calculateFreshness(exclusivityTimeout)
            timeProof <- EitherT.right[TimeProofSourceError](
              handle.timeTracker.fetchTimeProof(desiredTimeProofFreshness)
            )
          } yield timeProof

          DomainData(
            state.transferStore,
            () => recentTimeProofSource(),
          )
      }
    }

    val domainProtocolVersionGetter: Traced[DomainId] => Future[Option[ProtocolVersion]] =
      (tracedDomainId: Traced[DomainId]) =>
        tracedDomainId.withTraceContext { implicit traceContext => domainId =>
          syncDomainPersistentStateManager.protocolVersionFor(domainId)
        }

    new TransferCoordination(
      domainDataFor,
      submissionHandles,
      domainProtocolVersionGetter,
      syncCryptoApi,
      loggerFactory,
    )
  }
}

trait TransferSubmissionHandle {
  def timeTracker: DomainTimeTracker

  def submitTransferOut(
      submitterMetadata: TransferSubmitterMetadata,
      workflowId: Option[LfWorkflowId],
      contractId: LfContractId,
      targetDomain: DomainId,
      targetProtocolVersion: TargetProtocolVersion,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, TransferProcessorError, FutureUnlessShutdown[
    TransferOutProcessingSteps.SubmissionResult
  ]]

  def submitTransferIn(
      submitterMetadata: TransferSubmitterMetadata,
      workflowId: Option[LfWorkflowId],
      transferId: TransferId,
      sourceProtocolVersion: SourceProtocolVersion,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, TransferProcessorError, FutureUnlessShutdown[
    TransferInProcessingSteps.SubmissionResult
  ]]
}
