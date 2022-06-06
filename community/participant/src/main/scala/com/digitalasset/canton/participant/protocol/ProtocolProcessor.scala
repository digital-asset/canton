// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol

import cats.data.{EitherT, NonEmptyChain}
import cats.syntax.either._
import cats.syntax.functor._
import cats.syntax.functorFilter._
import cats.syntax.traverse._
import com.daml.ledger.api.DeduplicationPeriod
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.crypto.{DomainSnapshotSyncCryptoApi, DomainSyncCryptoClient}
import com.digitalasset.canton.data.{CantonTimestamp, ViewTree, ViewType}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown.syntax._
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.participant.event.AcsChange
import com.digitalasset.canton.participant.protocol.ProcessingSteps.{
  PendingRequestData,
  WrapsProcessorError,
}
import com.digitalasset.canton.participant.protocol.RequestJournal.RequestState
import com.digitalasset.canton.participant.protocol.conflictdetection.RequestTracker.{
  RequestNotFound,
  ResultAlreadyExists,
  TimeoutResult,
}
import com.digitalasset.canton.participant.protocol.conflictdetection.{
  ActivenessSet,
  CommitSet,
  RequestTracker,
}
import com.digitalasset.canton.participant.protocol.submission.CommandDeduplicator.DeduplicationFailed
import com.digitalasset.canton.participant.protocol.submission.{
  InFlightSubmission,
  InFlightSubmissionTracker,
  UnsequencedSubmission,
}
import com.digitalasset.canton.participant.store.{StoredContract, SyncDomainEphemeralState}
import com.digitalasset.canton.participant.sync.TimestampedEvent
import com.digitalasset.canton.participant.{RequestCounter, store}
import com.digitalasset.canton.protocol._
import com.digitalasset.canton.protocol.messages.EncryptedViewMessageDecryptionError
import com.digitalasset.canton.protocol.messages.SignedProtocolMessageContent.SignedMessageContentCast
import com.digitalasset.canton.protocol.messages.Verdict.Approve
import com.digitalasset.canton.protocol.messages._
import com.digitalasset.canton.sequencing.client._
import com.digitalasset.canton.sequencing.protocol._
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.topology.{MediatorId, ParticipantId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.EitherTUtil.{condUnitET, ifThenET}
import com.digitalasset.canton.util.{EitherTUtil, ErrorUtil, FutureUtil}
import com.digitalasset.canton.{DiscardOps, SequencerCounter, checked}
import com.google.common.annotations.VisibleForTesting
import io.functionmeta.functionFullName

import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

/** The [[ProtocolProcessor]] combines [[ProcessingSteps]] specific to a particular kind of request
  * with the common processing steps and wires them up with the state updates and synchronization.
  *
  * @param steps The specific processing steps
  * @tparam SubmissionParam  The bundled submission parameters
  * @tparam SubmissionResult The bundled submission results
  * @tparam RequestViewType     The type of view trees used by the request
  * @tparam Result           The specific type of the result message
  * @tparam SubmissionError  The type of errors that occur during submission processing
  */
abstract class ProtocolProcessor[
    SubmissionParam,
    SubmissionResult,
    RequestViewType <: ViewType,
    Result <: MediatorResult with SignedProtocolMessageContent,
    SubmissionError <: WrapsProcessorError,
](
    private[protocol] val steps: ProcessingSteps[
      SubmissionParam,
      SubmissionResult,
      RequestViewType,
      Result,
      SubmissionError,
    ],
    inFlightSubmissionTracker: InFlightSubmissionTracker,
    ephemeral: SyncDomainEphemeralState,
    crypto: DomainSyncCryptoClient,
    sequencerClient: SequencerClient,
    participantId: ParticipantId,
    futureSupervisor: FutureSupervisor,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext, resultCast: SignedMessageContentCast[Result])
    extends AbstractMessageProcessor(
      ephemeral,
      crypto,
      sequencerClient,
    )
    with RequestProcessor[RequestViewType] {

  import ProtocolProcessor._
  import com.digitalasset.canton.util.ShowUtil._

  private type ResultError = steps.ResultError

  private val alarmer = new LoggingAlarmStreamer(logger)

  private def alarm(message: String)(implicit traceContext: TraceContext): Future[Unit] =
    alarmer.alarm(message)

  private[this] def withKind(message: String): String = s"${steps.requestKind}: $message"

  /** Stores a counter for the submissions.
    * Incremented whenever we pick a mediator for a submission
    * so that we use mediators round-robin.
    *
    * Every processor picks the mediators independently,
    * so it may be that the participant picks the same mediator several times in a row,
    * but for different kinds of requests.
    */
  private val submissionCounter: AtomicInteger = new AtomicInteger(0)

  /** Submits the request to the sequencer, using a recent topology snapshot and the current persisted state
    * as an approximation to the future state at the assigned request timestamp.
    *
    * @param submissionParam The bundled submission parameters
    * @return The submission error or a future with the submission result.
    *         With submission tracking, the outer future completes after the submission is registered as in-flight,
    *         and the inner future after the submission has been sequenced or if it will never be sequenced.
    *         Without submission tracking, both futures complete after the submission has been sequenced
    *         or if it will not be sequenced.
    */
  def submit(submissionParam: SubmissionParam)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SubmissionError, Future[SubmissionResult]] = {
    logger.debug(withKind(s"Preparing request ${steps.submissionDescription(submissionParam)}"))

    val recentSnapshot = crypto.currentSnapshotApproximation
    for {
      mediatorId <- chooseMediator(recentSnapshot.ipsSnapshot)
        .leftMap(steps.embedNoMediatorError)
        .mapK(FutureUnlessShutdown.outcomeK)
      submission <- steps.prepareSubmission(submissionParam, mediatorId, ephemeral, recentSnapshot)
      result <- {
        submission match {
          case untracked: steps.UntrackedSubmission =>
            submitWithoutTracking(submissionParam, untracked)
          case tracked: steps.TrackedSubmission => submitWithTracking(submissionParam, tracked)
        }
      }.mapK(FutureUnlessShutdown.outcomeK)
    } yield result
  }

  private def chooseMediator(
      recentSnapshot: TopologySnapshot
  ): EitherT[Future, NoMediatorError, MediatorId] = {
    val fut = for {
      allActiveMediators <- recentSnapshot.mediators()
    } yield {
      val mediatorCount = allActiveMediators.size
      if (mediatorCount == 0) {
        Left(NoMediatorError(recentSnapshot.timestamp))
      } else {
        // Pick the next by incrementing the counter and selecting the mediator modulo the number of all mediators.
        // When the number of mediators changes, this strategy may result in the same mediator being picked twice in a row.
        // This is acceptable as mediator changes are rare.
        //
        // This selection strategy assumes that the `mediators` method in the `MediatorDomainStateClient`
        // returns the mediators in a consistent order. This assumption holds mostly because the cache
        // usually returns the fixed `Seq` in the cache.
        val newSubmissionCounter = submissionCounter.incrementAndGet()
        val chosenIndex = {
          val mod = newSubmissionCounter % mediatorCount
          // The submissionCounter overflows after Int.MAX_VALUE submissions
          // and then the modulo is negative. We must ensure that it's positive!
          if (mod < 0) mod + mediatorCount else mod
        }
        val mediator = checked(allActiveMediators(chosenIndex))
        Right(mediator)
      }
    }
    EitherT(fut)
  }

  /** Submits the batch without registering as in-flight and reports send errors as [[scala.Left$]] */
  private def submitWithoutTracking(
      submissionParam: SubmissionParam,
      untracked: steps.UntrackedSubmission,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, SubmissionError, Future[SubmissionResult]] = {
    val result = for {

      maxSequencingTime <- EitherT.right(
        untracked.maxSequencingTimeO.getOrElse(sequencerClient.generateMaxSequencingTime)
      )

      sendResultAndResultArgs <- submitInternal(
        submissionParam,
        untracked.pendingSubmissionId,
        MessageId.randomMessageId(),
        untracked.batch,
        maxSequencingTime,
        untracked.embedSubmissionError,
      )
      (sendResult, resultArgs) = sendResultAndResultArgs
      result <- EitherT.fromEither[Future](sendResult match {
        case SendResult.Success(deliver) => Right(steps.createSubmissionResult(deliver, resultArgs))
        case SendResult.Error(error) =>
          Left(untracked.embedSubmissionError(SequencerDeliverError(error)))
        case SendResult.Timeout(sequencerTime) =>
          Left(untracked.embedSubmissionError(SequencerTimeoutError(sequencerTime)))
      })
    } yield result
    result.bimap(untracked.toSubmissionError, Future.successful)
  }

  /** Register the submission as in-flight, deduplicate it, and submit it.
    * Errors after the registration are reported asynchronously only and return a [[scala.Right$]].
    * This ensures that every submission generates at most one rejection reason, namely through the
    * timely rejection mechanism. In-flight tracking may concurrently remove the submission at any time
    * and publish the timely rejection event instead of the actual error.
    */
  def submitWithTracking(submissionParam: SubmissionParam, tracked: steps.TrackedSubmission)(
      implicit traceContext: TraceContext
  ): EitherT[Future, SubmissionError, Future[SubmissionResult]] = {
    val maxSequencingTimeF =
      tracked.maxSequencingTimeO.getOrElse(sequencerClient.generateMaxSequencingTime)

    EitherT.right(maxSequencingTimeF).flatMap(submitWithTracking(submissionParam, tracked, _))
  }

  private def submitWithTracking(
      submissionParam: SubmissionParam,
      tracked: steps.TrackedSubmission,
      maxSequencingTime: CantonTimestamp,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, SubmissionError, Future[SubmissionResult]] = {
    val messageUuid = UUID.randomUUID()
    val inFlightSubmission = InFlightSubmission(
      changeIdHash = tracked.changeIdHash,
      submissionId = tracked.submissionId,
      submissionDomain = sequencerClient.domainId,
      messageUuid = messageUuid,
      sequencingInfo =
        UnsequencedSubmission(maxSequencingTime, tracked.submissionTimeoutTrackingData),
      submissionTraceContext = traceContext,
    )
    val messageId = inFlightSubmission.messageId
    val specifiedDeduplicationPeriod = tracked.specifiedDeduplicationPeriod
    logger.debug(s"Registering the submission as in-flight")

    val registeredF = inFlightSubmissionTracker
      .register(inFlightSubmission, specifiedDeduplicationPeriod)
      .leftMap(tracked.embedInFlightSubmissionTrackerError)
      .onShutdown {
        // If we abort due to a shutdown, we don't know whether the submission was registered.
        // The SyncDomain should guard this method call with a performUnlessClosing,
        // so we should see a shutdown here only if the SyncDomain close timeout was exceeded.
        // Therefore, WARN makes sense as a logging level.
        logger.warn(s"Shutdown while registering the submission as in-flight.")
        Left(tracked.shutdownDuringInFlightRegistration)
      }

    def observeSubmissionError(
        newUnsequencedSubmission: UnsequencedSubmission
    ): Future[SubmissionResult] = {
      // cap the new timeout by the max sequencing time
      // so that the timeout field can move only backwards
      val newUnsequencedSubmissionWithCappedTimeout =
        if (newUnsequencedSubmission.timeout > maxSequencingTime)
          newUnsequencedSubmission.copy(timeout = maxSequencingTime)
        else newUnsequencedSubmission
      for {
        _unit <- inFlightSubmissionTracker.observeSubmissionError(
          tracked.changeIdHash,
          sequencerClient.domainId,
          messageId,
          newUnsequencedSubmissionWithCappedTimeout,
        )
      } yield tracked.onFailure
    }

    // After in-flight registration, Make sure that all errors get a chance to update the tracking data and
    // instead return a `SubmissionResult` so that the submission will be acknowledged over the ledger API.
    def unlessError[A](eitherT: EitherT[Future, UnsequencedSubmission, A])(
        continuation: A => Future[SubmissionResult]
    ): Future[SubmissionResult] = eitherT.value.transformWith {
      case Success(Right(a)) => continuation(a)
      case Success(Left(newUnsequencedSubmission)) =>
        observeSubmissionError(newUnsequencedSubmission)
      case Failure(exception) =>
        // We merely log an error and rely on the maxSequencingTimeout to produce a rejection event eventually.
        // It is not clear whether we managed to send the submission.
        logger.error(s"Failed to submit submission", exception)
        Future.successful(tracked.onFailure)
    }

    def afterRegistration(
        deduplicationResult: Either[DeduplicationFailed, DeduplicationPeriod.DeduplicationOffset]
    ): Future[SubmissionResult] = deduplicationResult match {
      case Left(failed) =>
        observeSubmissionError(tracked.commandDeduplicationFailure(failed))
      case Right(actualDeduplicationOffset) =>
        def sendBatch(preparedBatch: steps.PreparedBatch): Future[SubmissionResult] = {
          val submittedEF = submitInternal(
            submissionParam,
            preparedBatch.pendingSubmissionId,
            messageId,
            preparedBatch.batch,
            maxSequencingTime,
            preparedBatch.embedSequencerRequestError,
          ).leftMap { submissionError =>
            logger.warn(s"Failed to submit submission due to $submissionError")
            preparedBatch.submissionErrorTrackingData(submissionError)
          }
          // As the `SendTracker` does not persist its state,
          // we would observe the sequencing here only if the participant has not crashed.
          // We therefore delegate observing the sequencing to the MessageDispatcher,
          // which can rely on the SequencedEventStore for persistence.
          unlessError(submittedEF) { case (sendResult, resultArgs) =>
            val submissionResult = sendResult match {
              case SendResult.Success(deliver) => steps.createSubmissionResult(deliver, resultArgs)
              case _: SendResult.NotSequenced => tracked.onFailure
            }
            Future.successful(submissionResult)
          }
        }

        unlessError(tracked.prepareBatch(actualDeduplicationOffset))(sendBatch)
    }

    registeredF.map(afterRegistration)
  }

  /** Submit the batch and return the [[com.digitalasset.canton.sequencing.client.SendResult]]
    * and the [[com.digitalasset.canton.participant.protocol.ProcessingSteps#SubmissionResultArgs]].
    */
  private def submitInternal(
      submissionParam: SubmissionParam,
      submissionId: steps.PendingSubmissionId,
      messageId: MessageId,
      batch: Batch[DefaultOpenEnvelope],
      maxSequencingTime: CantonTimestamp,
      embedSubmissionError: SequencerRequestError => steps.SubmissionSendError,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, steps.SubmissionSendError, (SendResult, steps.SubmissionResultArgs)] = {
    def removePendingSubmission(): Unit = {
      steps
        .removePendingSubmission(steps.pendingSubmissions(ephemeral), submissionId)
        .discard[Option[steps.PendingSubmissionData]]
    }

    for {
      // The pending submission must be registered before the request is sent, to avoid races
      resultArgs <- steps.updatePendingSubmissions(
        steps.pendingSubmissions(ephemeral),
        submissionParam,
        submissionId,
      )

      _ = logger.debug(
        withKind(
          s"Sending batch with id $submissionId for request ${steps.submissionDescription(submissionParam)}"
        )
      )

      // use the send callback and a promise to capture the eventual sequenced event read by the submitter
      sendResultP = Promise[SendResult]()

      _ <- sequencerClient
        .sendAsync(
          batch,
          SendType.ConfirmationRequest,
          callback = sendResultP.success,
          maxSequencingTime = maxSequencingTime,
          messageId = messageId,
        )
        .leftMap { err =>
          removePendingSubmission()
          embedSubmissionError(SequencerRequestError(err))
        }

      sendResult <- EitherT.right(sendResultP.future)
    } yield {
      SendResult.log("Submission", logger)(sendResult)

      sendResult match {
        case SendResult.Success(deliver) =>
          schedulePendingSubmissionRemoval(deliver.timestamp, submissionId)
        case SendResult.Error(_) | SendResult.Timeout(_) => removePendingSubmission()
      }

      sendResult -> resultArgs
    }
  }

  /** Removes the pending submission once the request tracker has advanced to the decision time.
    * This happens if the request times out (w.r.t. the submission timestamp) or the sequencer never sent a request.
    */
  private def schedulePendingSubmissionRemoval(
      submissionTimestamp: CantonTimestamp,
      submissionId: steps.PendingSubmissionId,
  )(implicit traceContext: TraceContext): Unit = {

    val removeF = for {
      domainParameters <- crypto.ips
        .awaitSnapshot(submissionTimestamp)
        .flatMap(_.findDynamicDomainParametersOrDefault())
      decisionTime = domainParameters.decisionTimeFor(submissionTimestamp)
      _ = ephemeral.timeTracker.requestTick(decisionTime)
      _ <- ephemeral.requestTracker.awaitTimestamp(decisionTime).getOrElse(Future.unit).map { _ =>
        steps.removePendingSubmission(steps.pendingSubmissions(ephemeral), submissionId).foreach {
          submissionData =>
            logger.debug(s"Removing sent submission $submissionId without a result.")
            steps.postProcessResult(Verdict.Timeout, submissionData)
        }
      }
    } yield ()

    FutureUtil.doNotAwait(removeF, s"Failed to remove the pending submission $submissionId")
  }

  override def processRequest(
      ts: CantonTimestamp,
      rc: RequestCounter,
      sc: SequencerCounter,
      batch: steps.RequestBatch,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {

    val RequestAndRootHashMessage(viewMessages, rootHashMessage, mediatorId) = batch
    val requestId = RequestId(ts)

    def checkRootHash(
        decryptedViews: steps.DecryptedViews
    ): (Seq[MalformedPayload], Seq[WithRecipients[steps.DecryptedView]]) = {

      val correctRootHash = rootHashMessage.rootHash
      val (correctRootHashes, wrongRootHashes) =
        decryptedViews.views.partition(_.unwrap.rootHash == correctRootHash)
      val malformedPayloads: Seq[MalformedPayload] =
        decryptedViews.decryptionErrors.map(ProtocolProcessor.ViewMessageDecryptionError(_)) ++
          wrongRootHashes.map(viewTree =>
            ProtocolProcessor.WrongRootHash(viewTree.unwrap, correctRootHash)
          )

      (malformedPayloads, correctRootHashes)
    }

    def trackAndSendResponses(
        snapshot: DomainSnapshotSyncCryptoApi,
        contractsAndContinue: steps.CheckActivenessAndWritePendingContracts,
    ): EitherT[Future, steps.RequestError, Unit] = {
      val steps.CheckActivenessAndWritePendingContracts(
        activenessSet,
        pendingContracts,
        constructPendingDataAndResponseArgs,
      ) = contractsAndContinue

      val cleanReplay = isCleanReplay(rc)
      for {
        domainParameters <- EitherT.right(
          snapshot.ipsSnapshot.findDynamicDomainParametersOrDefault()
        )

        requestFuturesF <- EitherT
          .fromEither[Future](
            ephemeral.requestTracker
              .addRequest(rc, sc, ts, ts, domainParameters.decisionTimeFor(ts), activenessSet)
          )
          .leftMap(err => steps.embedRequestError(RequestTrackerError(err)))

        conflictingContracts <- EitherT.right(
          ephemeral.storedContractManager.addPendingContracts(rc, pendingContracts)
        )
        // TODO(M40): This check may evaluate differently during a replay. Should not cause a hard failure
        //  E.g., if the contract has been written to the store in between with different contract data or metadata.
        _ <- condUnitET[Future](
          conflictingContracts.isEmpty,
          steps.embedRequestError(ConflictingContractData(conflictingContracts, pendingContracts)),
        )

        pendingContractIds = pendingContracts.map(_.unwrap.contractId).toSet

        pendingDataAndResponsesAndTimeoutEvent <-
          if (cleanReplay) {
            val pendingData = CleanReplayData(rc, sc, pendingContractIds)
            val responses = Seq.empty[(MediatorResponse, Recipients)]
            val causalityMessages = Seq.empty[(CausalityMessage, Recipients)]
            val timeoutEvent = Either.right(Option.empty[TimestampedEvent])
            EitherT.pure[Future, steps.RequestError](
              (pendingData, responses, causalityMessages, () => timeoutEvent)
            )
          } else {
            for {
              pendingCursor <- EitherT.right(ephemeral.requestJournal.insert(rc, ts))

              pendingDataAndResponses <- steps.constructPendingDataAndResponse(
                constructPendingDataAndResponseArgs,
                ephemeral.transferCache,
                ephemeral.storedContractManager,
                ephemeral.causalityLookup,
                requestFuturesF.flatMap(_.activenessResult),
                pendingCursor,
                mediatorId,
              )

              steps.StorePendingDataAndSendResponseAndCreateTimeout(
                pendingData,
                responses,
                causalityMessages,
                rejectionArgs,
              ) = pendingDataAndResponses
              PendingRequestData(
                pendingRequestCounter,
                pendingSequencerCounter,
                pendingContractIds2,
              ) = pendingData
              _ = if (
                pendingRequestCounter != rc
                || pendingSequencerCounter != sc
                || pendingContractIds2 != pendingContractIds
              )
                throw new RuntimeException("Pending result data inconsistent with request")

            } yield (
              WrappedPendingRequestData(pendingData),
              responses,
              causalityMessages,
              () => steps.createRejectionEvent(rejectionArgs),
            )
          }
        (pendingData, responsesTo, causalityMsgs, timeoutEvent) =
          pendingDataAndResponsesAndTimeoutEvent

        // Make sure activeness result finished
        requestFutures <- EitherT.right[steps.RequestError](requestFuturesF)
        _activenessResult <- EitherT.right[steps.RequestError](requestFutures.activenessResult)

        _existingData = steps.pendingRequestMap(ephemeral).putIfAbsent(requestId, pendingData)
        // TODO(Andreas): Handle existing request data (validation here)

        _ <- EitherT.right[steps.RequestError](
          unlessCleanReplay(rc)(
            ephemeral.requestJournal.transit(rc, ts, RequestState.Pending, RequestState.Confirmed)
          )
        )

        _ = ephemeral.phase37Synchronizer.markConfirmed(rc, requestId)

        timeoutET = EitherT
          .right(requestFutures.timeoutResult)
          .flatMap(
            handleTimeout(
              requestId,
              rc,
              sc,
              pendingData.pendingContracts,
              domainParameters,
              timeoutEvent(),
            )
          )
        _ = EitherTUtil.doNotAwait(timeoutET, "Handling timeout failed")

        signedResponsesTo <- EitherT.right(responsesTo.traverse { case (response, recipients) =>
          signResponse(snapshot, response).map(_ -> recipients)
        })
        messages: Seq[(ProtocolMessage, Recipients)] = signedResponsesTo ++ causalityMsgs
        _ <- sendResponses(requestId, rc, messages)
          .leftMap(err => steps.embedRequestError(SequencerRequestError(err)))
      } yield ()

    }

    if (precedesCleanReplay(requestId)) {
      // The `MessageDispatcher` should not call this method for requests before the clean replay starting point
      ErrorUtil.internalErrorAsyncShutdown(
        new IllegalArgumentException(
          s"Request with timestamp $ts precedes the clean replay starting point"
        )
      )
    } else {
      logger.info(show"Processing ${steps.requestKind.unquoted} request at $requestId.")
      performUnlessClosingF(functionFullName) {
        val resultF = for {
          snapshot <- EitherT.right(
            futureSupervisor.supervised(s"await crypto snapshot $ts")(crypto.awaitSnapshot(ts))
          )
          decryptedViews <- steps.decryptViews(viewMessages, snapshot)

          (malformedPayloads, correctRootHashes) = checkRootHash(decryptedViews)
          _ = malformedPayloads.foreach { mp =>
            logger.warn(s"Request $rc: Found malformed payload: $mp")
          }

          _ <- NonEmpty.from(correctRootHashes) match {
            case None =>
              val pendingDataAndResponseArgsE =
                steps.pendingDataAndResponseArgsForMalformedPayloads(
                  ts,
                  rc,
                  sc,
                  malformedPayloads,
                  snapshot,
                )
              EitherT.fromEither[Future](pendingDataAndResponseArgsE).flatMap {
                pendingDataAndResponseArgs =>
                  val contractsAndContinue = steps.CheckActivenessAndWritePendingContracts(
                    activenessSet = ActivenessSet.empty,
                    pendingContracts = Seq.empty,
                    pendingDataAndResponseArgs = pendingDataAndResponseArgs,
                  )
                  trackAndSendResponses(snapshot, contractsAndContinue)
              }

            case Some(goodViews) =>
              // All views with the same correct root hash declare the same mediator, so it's enough to look at the head
              val declaredMediator = goodViews.head1.unwrap.mediatorId
              if (declaredMediator == mediatorId) {
                // Check whether the declared mediator is still an active mediator.
                EitherT.right(snapshot.ipsSnapshot.isMediatorActive(mediatorId)).flatMap {
                  case true =>
                    steps
                      .computeActivenessSetAndPendingContracts(
                        ts,
                        rc,
                        sc,
                        goodViews,
                        malformedPayloads,
                        snapshot,
                      )
                      .flatMap(trackAndSendResponses(snapshot, _))
                  case false =>
                    logger.info(
                      s"Request $rc: Chosen mediator $mediatorId is inactive at $ts. Skipping this request."
                    )
                    // The chosen mediator may have become inactive between submission and sequencing.
                    // All honest participants and the mediator will ignore the request,
                    // but the submitting participant still must produce a completion event.
                    val (eventO, submissionIdO) =
                      steps.eventAndSubmissionIdForInactiveMediator(ts, rc, sc, goodViews)
                    for {
                      _ <- EitherT.right(
                        unlessCleanReplay(rc)(
                          ephemeral.recordOrderPublisher
                            .schedulePublication(sc, rc, ts, eventO, None)
                        )
                      )
                      submissionDataO = submissionIdO.flatMap(submissionId =>
                        // This removal does not interleave with `schedulePendingSubmissionRemoval`
                        // as the sequencer respects the max sequencing time of the request.
                        // TODO(M99) Gracefully handle the case that the sequencer does not respect the max sequencing time.
                        steps.removePendingSubmission(
                          steps.pendingSubmissions(ephemeral),
                          submissionId,
                        )
                      )
                      _ = submissionDataO.foreach(
                        steps.postProcessSubmissionForInactiveMediator(declaredMediator, ts, _)
                      )
                      _ <- EitherT.right[steps.RequestError](invalidRequest(rc, sc, ts))
                    } yield ()
                }
              } else {
                // When the mediator `mediatorId` receives the root hash message,
                // it will either lack the informee tree or find the wrong mediator ID in it.
                // The submitting participant is malicious (unless the sequencer is), so it is not this participant
                // and therefore we don't have to output a completion event
                logger.error(
                  s"Mediator $declaredMediator declared in views is not the recipient $mediatorId of the root hash message"
                )
                EitherT.right[steps.RequestError](prepareForMediatorResultOfBadRequest(rc, sc, ts))
              }
          }
        } yield ()

        EitherTUtil
          .logOnError(resultF, s"${steps.requestKind} $requestId: Failed to process request")
          .value
          .void
      }
    }
  }

  override def processMalformedMediatorRequestResult(
      timestamp: CantonTimestamp,
      sequencerCounter: SequencerCounter,
      signedResultBatch: SignedContent[Deliver[DefaultOpenEnvelope]],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    performUnlessClosingF(functionFullName) {
      val malformedMediatorRequestEnvelopes = signedResultBatch.content.batch.envelopes
        .mapFilter(ProtocolMessage.select[SignedProtocolMessage[MalformedMediatorRequestResult]])
      require(
        malformedMediatorRequestEnvelopes.sizeCompare(1) == 0,
        steps.requestKind + " result contains multiple malformed mediator request envelopes",
      )
      val malformedMediatorRequest = malformedMediatorRequestEnvelopes(0).protocolMessage.message
      val requestId = malformedMediatorRequest.requestId
      val ts = signedResultBatch.content.timestamp
      val sc = signedResultBatch.content.counter

      logger.info(
        show"Got malformed mediator result for ${steps.requestKind.unquoted} request at $requestId."
      )
      val processedE = performResultProcessing(
        signedResultBatch,
        Left(malformedMediatorRequest),
        requestId,
        ts,
        sc,
      )
      logResultWarnings(timestamp, processedE)
    }

  override def processResult(
      signedResultBatch: SignedContent[Deliver[DefaultOpenEnvelope]]
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] = {

    performUnlessClosingF(functionFullName) {
      val resultEnvelopes =
        signedResultBatch.content.batch.envelopes
          .mapFilter(ProtocolMessage.select[SignedProtocolMessage[Result]])
      ErrorUtil.requireArgument(
        resultEnvelopes.sizeCompare(1) == 0,
        steps.requestKind + " result contains multiple such messages",
      )

      val result = resultEnvelopes(0).protocolMessage.message
      val requestId = result.requestId
      val ts = signedResultBatch.content.timestamp
      val sc = signedResultBatch.content.counter

      logger.debug(
        show"Got result for ${steps.requestKind.unquoted} request at $requestId: $resultEnvelopes"
      )
      val processedE = performResultProcessing(signedResultBatch, Right(result), requestId, ts, sc)
      logResultWarnings(ts, processedE)
    }
  }

  @VisibleForTesting
  // TODO(#8744) avoid discarded future as part of AlarmStreamer design
  @SuppressWarnings(Array("com.digitalasset.canton.DiscardedFuture"))
  private[protocol] def performResultProcessing(
      signedResultBatch: SignedContent[Deliver[DefaultOpenEnvelope]],
      result: Either[MalformedMediatorRequestResult, Result],
      requestId: RequestId,
      ts: CantonTimestamp,
      sc: SequencerCounter,
  )(implicit traceContext: TraceContext): EitherT[Future, steps.ResultError, Unit] = {
    ephemeral.recordOrderPublisher.tick(sc, ts)
    for {
      snapshot <- EitherT.right(
        futureSupervisor.supervised(s"await crypto snapshot $ts")(crypto.ips.awaitSnapshot(ts))
      )

      domainParameters <- EitherT.right(snapshot.findDynamicDomainParametersOrDefault())

      _ <- condUnitET[Future](
        ts <= domainParameters.decisionTimeFor(requestId.unwrap), {
          ephemeral.requestTracker.tick(sc, ts)
          steps.embedResultError(DecisionTimeElapsed(requestId, ts))
          /* We must not evict the request from `pendingRequestData` or `pendingSubmissionMap`
           * because this will have been taken care of by `handleTimeout`
           * when the request tracker progresses to the decision time.
           */
        },
      )
      _ <- ifThenET(
        result.merge.verdict == Verdict.Timeout &&
          ts <= domainParameters.participantResponseDeadlineFor(requestId.unwrap)
      ) {
        alarmer.alarm(
          s"Received mediator timeout message at $ts before response deadline for request ${requestId}"
        )
        ephemeral.requestTracker.tick(sc, ts)
        EitherT.leftT[Future, Unit](steps.embedResultError(TimeoutResultTooEarly(requestId)))
      }

      _ <- EitherTUtil.ifThenET(!precedesCleanReplay(requestId)) {
        performResultProcessing2(signedResultBatch, result, requestId, ts, sc, domainParameters)
      }
    } yield ()
  }

  private[this] def performResultProcessing2(
      signedResultBatch: SignedContent[Deliver[DefaultOpenEnvelope]],
      result: Either[MalformedMediatorRequestResult, Result],
      requestId: RequestId,
      ts: CantonTimestamp,
      sc: SequencerCounter,
      domainParameters: DynamicDomainParameters,
  )(implicit traceContext: TraceContext): EitherT[Future, steps.ResultError, Unit] = {
    val verdict = result.merge.verdict
    for {
      // Wait until we have processed the corresponding request
      //
      // This may deadlock if we haven't received the `requestId` as a request.
      // For example, if there never was a request with the given timestamp,
      // then the phase 3-7 synchronizer waits until the all requests until `requestId`'s timestamp
      // and the next request have reached `Confirmed`.
      // However, if there was no request between `requestId` and `ts`,
      // then the next request will not reach `Confirmed`
      // because the request tracker will not progress beyond `ts` as the `tick` for `ts` comes only after this point.
      // Accordingly, time proofs will not trigger a timeout either.
      //
      // We don't know whether any protocol processor has ever seen the request with `requestId`;
      // it might be that the message dispatcher already decided that the request is malformed and should not be processed.
      // In this case, the message dispatcher has assigned a request counter to the request if it expects to get a mediator result
      // and the BadRootHashMessagesRequestProcessor moved the request counter to `Confirmed`.
      // So the deadlock should happen only if the mediator or sequencer are dishonest.
      //
      // TODO(M99) This argument relies on the mediator sending a MalformedMediatorRequest only to participants
      //  that have also received a message with the request.
      //  A dishonest sequencer or mediator could break this assumption.
      _ <- EitherT.right(ephemeral.phase37Synchronizer.awaitConfirmed(requestId))

      pendingRequestDataOrReplayData <- EitherT.fromEither[Future](
        steps.pendingRequestMap(ephemeral).remove(requestId).toRight {
          ephemeral.requestTracker.tick(sc, ts)
          steps.embedResultError(UnknownPendingRequest(requestId))
        }
      )
      PendingRequestData(requestCounter, requestSequencerCounter, pendingContracts) =
        pendingRequestDataOrReplayData

      cleanReplay = isCleanReplay(requestCounter, pendingRequestDataOrReplayData)
      pendingSubmissionDataO = pendingSubmissionDataForRequest(pendingRequestDataOrReplayData)

      commitAndEvent <- pendingRequestDataOrReplayData match {
        case WrappedPendingRequestData(pendingRequestData) =>
          for {
            commitSetAndContractsAndEvent <- steps
              .getCommitSetAndContractsToBeStoredAndEvent(
                signedResultBatch,
                result,
                pendingRequestData,
                steps.pendingSubmissions(ephemeral),
                ephemeral.causalityLookup,
                crypto.pureCrypto,
              )
          } yield {
            val steps.CommitAndStoreContractsAndPublishEvent(
              commitSetOF,
              contractsToBeStored,
              maybeEvent,
              update,
            ) =
              commitSetAndContractsAndEvent
            if (verdict != Approve && commitSetOF.isDefined)
              throw new RuntimeException("Negative verdicts entail an empty commit set")
            if (!contractsToBeStored.subsetOf(pendingContracts))
              throw new RuntimeException("All contracts to be stored should be pending")
            (commitSetOF, contractsToBeStored, maybeEvent, update)
          }
        case _: CleanReplayData =>
          val commitSet = if (verdict == Approve) Some(Future.successful(CommitSet.empty)) else None
          val contractsToBeStored = Set.empty[LfContractId]
          val maybeEvent = None
          EitherT.pure[Future, steps.ResultError](
            (commitSet, contractsToBeStored, maybeEvent, None)
          )
      }
      (commitSetOF, contractsToBeStored, maybeEvent, updateO) = commitAndEvent

      commitTime = ts
      commitSetF <- signalResultToRequestTracker(
        requestCounter,
        sc,
        requestId,
        ts,
        commitTime,
        commitSetOF,
        domainParameters,
      ).leftMap(err => steps.embedResultError(RequestTrackerError(err)))

      contractStoreUpdate = pendingContracts
        .map(contractId => (contractId, contractsToBeStored.contains(contractId)))
        .toMap

      _ <- EitherT.right(
        ephemeral.storedContractManager.commitIfPending(requestCounter, contractStoreUpdate)
      )

      _ <- ifThenET(!cleanReplay) {
        for {
          _unit <- {
            logger.info(
              show"Finalizing ${steps.requestKind.unquoted} request at $requestId with event ${maybeEvent}."
            )
            // Schedule publication of the event with the associated causality update.
            // Note that both fields are optional.
            // Some events (such as rejection events) are not associated with causality updates.
            // Additionally, we may process a causality update without an associated event (this happens on transfer-in)
            EitherT.right[steps.ResultError](
              ephemeral.recordOrderPublisher
                .schedulePublication(
                  requestSequencerCounter,
                  requestCounter,
                  requestId.unwrap,
                  maybeEvent,
                  updateO,
                )
            )
          }

          commitSet <- EitherT.right[steps.ResultError](commitSetF)
          _ = ephemeral.recordOrderPublisher.scheduleAcsChangePublication(
            requestSequencerCounter,
            requestId.unwrap,
            requestCounter,
            AcsChange.fromCommitSet(commitSet),
          )
          requestTimestamp = requestId.unwrap
          _unit <- EitherT.right[steps.ResultError](
            terminateRequest(requestCounter, requestSequencerCounter, requestTimestamp, commitTime)
          )
        } yield pendingSubmissionDataO.foreach(steps.postProcessResult(verdict, _))
      }
    } yield ()
  }

  private[this] def logResultWarnings(
      resultTimestamp: CantonTimestamp,
      result: EitherT[Future, steps.ResultError, Unit],
  )(implicit traceContext: TraceContext): Future[Unit] = {

    val warningsLogged = EitherTUtil.leftSubflatMap(result) {
      _.underlyingProcessorError() match {
        case Some(DecisionTimeElapsed(requestId, _)) => {
          logger.warn(
            show"${steps.requestKind.unquoted} request at $requestId: Result arrived after the decision time (arrived at $resultTimestamp)"
          )
          Right(())
        }
        case Some(UnknownPendingRequest(requestId)) => {
          // the mediator can send duplicate transaction results during crash recovery and fail over, triggering this error
          logger.info(
            show"${steps.requestKind.unquoted} request at $requestId: Received event at $resultTimestamp for request that is not pending"
          )
          Right(())
        }
        case err => Left(err)
      }
    }

    val loggedE =
      EitherTUtil.logOnError(warningsLogged, s"${steps.requestKind}: Failed to process result")
    loggedE.value.void
  }

  private[this] def pendingSubmissionDataForRequest(
      pendingRequestDataOrReplayData: PendingRequestDataOrReplayData[steps.PendingRequestData]
  ): Option[steps.PendingSubmissionData] =
    for {
      pendingRequestData <- maybePendingRequestData(pendingRequestDataOrReplayData)
      submissionId = steps.submissionIdOfPendingRequest(pendingRequestData)
      submissionData <- steps.removePendingSubmission(
        steps.pendingSubmissions(ephemeral),
        submissionId,
      )
    } yield submissionData

  private def maybePendingRequestData[A <: PendingRequestData](
      pendingRequestDataOrReplayData: PendingRequestDataOrReplayData[A]
  ): Option[A] =
    pendingRequestDataOrReplayData match {
      case WrappedPendingRequestData(pendingRequestData) => Some(pendingRequestData)
      case _: CleanReplayData => None
    }

  private def signalResultToRequestTracker(
      rc: RequestCounter,
      sc: SequencerCounter,
      requestId: RequestId,
      resultTimestamp: CantonTimestamp,
      commitTime: CantonTimestamp,
      commitSetOF: Option[Future[CommitSet]],
      domainParameters: DynamicDomainParameters,
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[Future, RequestTracker.RequestTrackerError, Future[CommitSet]] = {

    def withRc(rc: RequestCounter, msg: String): String = s"Request $rc: $msg"

    val requestTimestamp = requestId.unwrap

    ErrorUtil.requireArgument(
      resultTimestamp <= domainParameters.decisionTimeFor(requestTimestamp),
      withRc(rc, "Result message after decision time"),
    )

    for {
      _ <- EitherT
        .fromEither[Future](ephemeral.requestTracker.addResult(rc, sc, resultTimestamp, commitTime))
        .leftMap(handleRequestTrackerResultError(requestId))
      commitSetF = commitSetOF.getOrElse(Future.successful(CommitSet.empty))
      commitSetT <- EitherT.right(commitSetF.transform(Success(_)))
      commitFuture <- EitherT
        .fromEither[Future](ephemeral.requestTracker.addCommitSet(rc, commitSetT))
        .leftMap(handleCommitSetError(requestId))
    } yield commitFuture.foldF(
      irregularities =>
        alarm(withRc(rc, s"Result message causes ACS irregularities: $irregularities")).map(_ =>
          commitSetT.fold(throw _, identity)
        ),
      _ => Future.fromTry(commitSetT),
    )
  }

  // TODO(#8744) avoid discarded future as part of AlarmStreamer design
  @SuppressWarnings(Array("com.digitalasset.canton.DiscardedFuture"))
  private def handleCommitSetError(requestId: RequestId)(
      e: RequestTracker.CommitSetError
  )(implicit traceContext: TraceContext): RequestTracker.RequestTrackerError = {
    /* The request tracker must have evicted the request between the previous call to `addTransactionResult`
     * and this call. Since `addTransactionResult` marks the request as not having timed out,
     * eviction can only happen if another transaction result processing has been interleaved,
     * so we must have received two transaction result messages.
     */
    alarm(s"Received a second result message for request $requestId")
    e
  }

  // TODO(#8744) avoid discarded future as part of AlarmStreamer design
  @SuppressWarnings(Array("com.digitalasset.canton.DiscardedFuture"))
  private def handleRequestTrackerResultError(requestId: RequestId)(
      error: RequestTracker.ResultError
  )(implicit traceContext: TraceContext): RequestTracker.RequestTrackerError = {
    error match {
      case _: RequestNotFound =>
        /* The request tracker evicts a request only when it times out or a commit set has been provided.
         * So if the result timestamp is before the decision time, a commit set must have already been added with an
         * earlier timestamp. So there must have been an earlier transaction result for the request.
         */
        alarm(s"Received a second result message for request $requestId.")

      case _: ResultAlreadyExists =>
        alarm(s"Received a second result message for request $requestId")
    }
    error
  }

  private def handleTimeout(
      requestId: RequestId,
      requestCounter: RequestCounter,
      sequencerCounter: SequencerCounter,
      pendingContracts: Set[LfContractId],
      domainParameters: DynamicDomainParameters,
      timeoutEvent: => Either[steps.ResultError, Option[TimestampedEvent]],
  )(
      result: TimeoutResult
  )(implicit traceContext: TraceContext): EitherT[Future, steps.ResultError, Unit] =
    if (result.timedOut) {
      logger.info(
        show"${steps.requestKind.unquoted} request at $requestId timed out without a transaction result message."
      )

      val pendingRequestDataOrReplayDataO = steps.pendingRequestMap(ephemeral).remove(requestId)
      val pendingRequestDataOrReplayData = pendingRequestDataOrReplayDataO.getOrElse(
        throw new IllegalStateException(s"Unknown pending request $requestId at timeout.")
      )
      // No need to clean up the pending submissions because this is handled (concurrently) by schedulePendingSubmissionRemoval

      val cleanReplay = isCleanReplay(requestCounter, pendingRequestDataOrReplayData)

      def publishEvent(): EitherT[Future, steps.ResultError, Unit] = {
        for {
          maybeEvent <- EitherT.fromEither[Future](timeoutEvent)
          update = None
          _ <- EitherT.liftF(
            ephemeral.recordOrderPublisher
              .schedulePublication(
                sequencerCounter,
                requestCounter,
                requestId.unwrap,
                maybeEvent,
                update,
              )
          )
          requestTimestamp = requestId.unwrap
          commitTime = domainParameters.decisionTimeFor(requestTimestamp)
          _unit <- EitherT.right[steps.ResultError](
            terminateRequest(requestCounter, sequencerCounter, requestTimestamp, commitTime)
          )
        } yield ()
      }

      for {
        _ <- EitherT.right[steps.ResultError](
          ephemeral.storedContractManager.deleteIfPending(requestCounter, pendingContracts)
        )

        _ <- ifThenET(!cleanReplay)(publishEvent())
      } yield ()
    } else EitherT.pure[Future, steps.ResultError](())

  private[this] def isCleanReplay(
      requestCounter: RequestCounter,
      pendingData: PendingRequestDataOrReplayData[_ <: PendingRequestData],
  ): Boolean = {
    val cleanReplay = isCleanReplay(requestCounter)
    if (cleanReplay != pendingData.isCleanReplay)
      throw new IllegalStateException(
        s"Request $requestCounter is before the starting point at ${ephemeral.startingPoints.processing.nextRequestCounter}, but not a replay"
      )
    cleanReplay
  }

  /** A request precedes the clean replay if it came before the
    * [[com.digitalasset.canton.participant.store.SyncDomainEphemeralState.startingPoints]]'s
    * [[com.digitalasset.canton.participant.store.SyncDomainEphemeralStateFactory.StartingPoints.cleanReplay]].
    */
  private[this] def precedesCleanReplay(requestId: RequestId): Boolean =
    requestId.unwrap <= ephemeral.startingPoints.cleanReplay.prenextTimestamp
}

object ProtocolProcessor {

  sealed trait PendingRequestDataOrReplayData[+A <: PendingRequestData]
      extends PendingRequestData
      with Product
      with Serializable {
    def isCleanReplay: Boolean
  }

  case class WrappedPendingRequestData[+A <: PendingRequestData](unwrap: A)
      extends PendingRequestDataOrReplayData[A] {
    override def requestCounter: RequestCounter = unwrap.requestCounter
    override def requestSequencerCounter: SequencerCounter = unwrap.requestSequencerCounter
    override def pendingContracts: Set[LfContractId] = unwrap.pendingContracts
    override def isCleanReplay: Boolean = false
  }

  case class CleanReplayData(
      override val requestCounter: RequestCounter,
      override val requestSequencerCounter: SequencerCounter,
      override val pendingContracts: Set[LfContractId],
  ) extends PendingRequestDataOrReplayData[Nothing] {
    override def isCleanReplay: Boolean = true
  }

  sealed trait ProcessorError extends Product with Serializable with PrettyPrinting

  sealed trait SubmissionProcessingError extends ProcessorError

  sealed trait RequestProcessingError extends ProcessorError

  sealed trait ResultProcessingError extends ProcessorError

  /** We were unable to send the request to the sequencer */
  case class SequencerRequestError(sendError: SendAsyncClientError)
      extends SubmissionProcessingError
      with RequestProcessingError {
    override def pretty: Pretty[SequencerRequestError] = prettyOfParam(_.sendError)
  }

  /** The sequencer refused to sequence the batch for delivery */
  case class SequencerDeliverError(deliverError: DeliverError)
      extends SubmissionProcessingError
      with RequestProcessingError {
    override def pretty: Pretty[SequencerDeliverError] = prettyOfParam(_.deliverError)
  }

  /** The identity snapshot does not list a mediator, so we cannot pick one. */
  case class NoMediatorError(topologySnapshotTimestamp: CantonTimestamp)
      extends SubmissionProcessingError {
    override def pretty: Pretty[NoMediatorError] = prettyOfClass(
      param("topology snapshot timestamp", _.topologySnapshotTimestamp)
    )
  }

  /** The sequencer did not sequence our event within the allotted time
    * @param timestamp sequencer time when the timeout occurred
    */
  case class SequencerTimeoutError(timestamp: CantonTimestamp)
      extends SubmissionProcessingError
      with RequestProcessingError {
    override def pretty: Pretty[SequencerTimeoutError] = prettyOfClass(unnamedParam(_.timestamp))
  }

  case class RequestTrackerError(error: RequestTracker.RequestTrackerError)
      extends RequestProcessingError
      with ResultProcessingError {
    override def pretty: Pretty[RequestTrackerError] = prettyOfParam(_.error)
  }

  case class ConflictingContractData(
      existing: Set[StoredContract],
      newContracts: Seq[WithTransactionId[SerializableContract]],
  ) extends RequestProcessingError {
    override def pretty: Pretty[ConflictingContractData] = prettyOfClass(
      param("existing", _.existing),
      param("new contracts", _.newContracts),
    )
  }

  case class ContractStoreError(error: NonEmptyChain[store.ContractStoreError])
      extends ResultProcessingError {
    override def pretty: Pretty[ContractStoreError] = prettyOfParam(_.error.toChain.toList)
  }

  case class DecisionTimeElapsed(requestId: RequestId, timestamp: CantonTimestamp)
      extends ResultProcessingError {
    override def pretty: Pretty[DecisionTimeElapsed] = prettyOfClass(
      param("request id", _.requestId),
      param("timestamp", _.timestamp),
    )
  }

  case class UnknownPendingRequest(requestId: RequestId) extends ResultProcessingError {
    override def pretty: Pretty[UnknownPendingRequest] = prettyOfClass(unnamedParam(_.requestId))
  }

  case class TimeoutResultTooEarly(requestId: RequestId) extends ResultProcessingError {
    override def pretty: Pretty[TimeoutResultTooEarly] = prettyOfClass(unnamedParam(_.requestId))
  }

  sealed trait MalformedPayload extends Product with Serializable with PrettyPrinting {
    def viewHash: ViewHash
  }

  case class ViewMessageDecryptionError[VT <: ViewType](
      error: EncryptedViewMessageDecryptionError[VT]
  ) extends MalformedPayload {
    override def viewHash: ViewHash = error.message.viewHash

    override def pretty: Pretty[ViewMessageDecryptionError.this.type] = prettyOfParam(_.error)
  }

  case class WrongRootHash(viewTree: ViewTree, expectedRootHash: RootHash)
      extends MalformedPayload {
    override def viewHash: ViewHash = viewTree.viewHash

    override def pretty: Pretty[WrongRootHash] = prettyOfClass(
      param("view tree", _.viewTree),
      param("expected root hash", _.expectedRootHash),
    )
  }
}
