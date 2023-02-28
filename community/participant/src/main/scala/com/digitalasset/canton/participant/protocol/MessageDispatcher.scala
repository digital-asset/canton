// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol

import cats.data.Chain
import cats.syntax.alternative.*
import cats.syntax.functorFilter.*
import cats.{Foldable, Monoid}
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.data.ViewType.{
  TransactionViewType,
  TransferInViewType,
  TransferOutViewType,
}
import com.digitalasset.canton.data.{CantonTimestamp, ViewType}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.event.RecordOrderPublisher
import com.digitalasset.canton.participant.protocol.conflictdetection.RequestTracker
import com.digitalasset.canton.participant.protocol.submission.{
  InFlightSubmissionTracker,
  SequencedSubmission,
}
import com.digitalasset.canton.participant.protocol.transfer.{
  TransferInProcessor,
  TransferOutProcessor,
}
import com.digitalasset.canton.participant.pruning.AcsCommitmentProcessor
import com.digitalasset.canton.participant.sync.SyncServiceError.SyncServiceAlarm
import com.digitalasset.canton.protocol.messages.ProtocolMessage.select
import com.digitalasset.canton.protocol.messages.*
import com.digitalasset.canton.protocol.{RequestAndRootHashMessage, RequestProcessor, RootHash}
import com.digitalasset.canton.sequencing.*
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.topology.processing.TopologyTransactionProcessor
import com.digitalasset.canton.topology.{DomainId, MediatorId, Member, ParticipantId}
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.{Checked, ErrorUtil}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{RequestCounter, SequencerCounter}
import com.google.common.annotations.VisibleForTesting
import io.opentelemetry.api.trace.Tracer

import scala.concurrent.{ExecutionContext, Future}

/** Dispatches the incoming messages of the [[com.digitalasset.canton.sequencing.client.SequencerClient]]
  * to the different processors. It also informs the [[conflictdetection.RequestTracker]] about the passing of time for messages
  * that are not processed by the [[TransactionProcessor]].
  */
trait MessageDispatcher { this: NamedLogging =>
  import MessageDispatcher.*

  protected def protocolVersion: ProtocolVersion

  protected def domainId: DomainId

  protected def participantId: ParticipantId

  protected type ProcessingResult
  protected def doProcess[A](
      kind: MessageKind[A],
      run: => FutureUnlessShutdown[A],
  ): FutureUnlessShutdown[ProcessingResult]
  protected implicit def processingResultMonoid: Monoid[ProcessingResult]

  protected def requestTracker: RequestTracker

  protected def requestProcessors: RequestProcessors

  protected def tracker: SingleDomainCausalTracker

  protected def topologyProcessor
      : (SequencerCounter, CantonTimestamp, Traced[List[DefaultOpenEnvelope]]) => HandlerResult
  protected def acsCommitmentProcessor: AcsCommitmentProcessor.ProcessorType
  protected def requestCounterAllocator: RequestCounterAllocator
  protected def recordOrderPublisher: RecordOrderPublisher
  protected def badRootHashMessagesRequestProcessor: BadRootHashMessagesRequestProcessor
  protected def repairProcessor: RepairProcessor
  protected def inFlightSubmissionTracker: InFlightSubmissionTracker

  implicit protected val ec: ExecutionContext

  def handleAll(
      events: Traced[Seq[Either[Traced[
        EventWithErrors[SequencedEvent[DefaultOpenEnvelope]]
      ], PossiblyIgnoredProtocolEvent]]]
  ): HandlerResult

  /** Returns a future that completes when all calls to [[handleAll]]
    * whose returned [[scala.concurrent.Future]] has completed prior to this call have completed processing.
    */
  @VisibleForTesting
  def flush(): Future[Unit]

  private def processAcsCommitmentEnvelope(
      envelopes: List[DefaultOpenEnvelope],
      sc: SequencerCounter,
      ts: CantonTimestamp,
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[ProcessingResult] = {
    val acsCommitments = envelopes.mapFilter(select[SignedProtocolMessage[AcsCommitment]])
    if (acsCommitments.nonEmpty) {
      // When a participant receives an ACS commitment from a counter-participant, the counter-participant
      // expects to receive the corresponding commitment from the local participant.
      // However, the local participant may not have seen neither an ACS change nor a time proof
      // since the commitment's interval end. So we signal an empty ACS change to the ACS commitment processor
      // at the commitment sequencing time (which is after the interval end for an honest counter-participant)
      // so that this triggers an ACS commitment computation on the local participant if necessary.
      //
      // This ACS commitment may be bundled with a request that may lead to a non-empty ACS change at this timestamp.
      // It is nevertheless OK to schedule the empty ACS change
      // because we use a different tie breaker for the empty ACS commitment.
      // This is also why we must not tick the record order publisher here.
      recordOrderPublisher.scheduleEmptyAcsChangePublication(sc, ts)
      doProcess(
        AcsCommitment, {
          logger.debug(s"Processing ACS commitments for timestamp $ts")
          acsCommitmentProcessor(ts, Traced(acsCommitments))
        },
      )
    } else FutureUnlessShutdown.pure(processingResultMonoid.empty)
  }

  private def tryProtocolProcessor(
      viewType: ViewType
  )(implicit traceContext: TraceContext): RequestProcessor[viewType.type] =
    requestProcessors
      .get(viewType)
      .getOrElse(
        ErrorUtil.internalError(
          new IllegalArgumentException(show"No processor for view type $viewType")
        )
      )

  /** Rules for processing batches of envelopes:
    * <ul>
    *   <li>Identity transactions can be included in any batch of envelopes. They must be processed first.
    *     <br/>
    *     The identity processor ignores replayed or invalid transactions and merely logs an error.
    *   </li>
    *   <li>Acs commitments can be included in any batch of envelopes.
    *     They must be processed before the requests and results to
    *     meet the precondition of [[com.digitalasset.canton.participant.pruning.AcsCommitmentProcessor]]'s `processBatch`
    *     method.
    *   </li>
    *   <li>A [[com.digitalasset.canton.protocol.messages.MediatorResult]] message should be sent only by the trusted mediator of the domain.
    *     The mediator should never include further messages with a [[com.digitalasset.canton.protocol.messages.MediatorResult]].
    *     So a participant accepts a [[com.digitalasset.canton.protocol.messages.MediatorResult]]
    *     only if there are no other messages (except topology transactions and ACS commitments) in the batch.
    *     Otherwise, the participant ignores the [[com.digitalasset.canton.protocol.messages.MediatorResult]] and raises an alarm.
    *     <br/>
    *     The same applies to a [[com.digitalasset.canton.protocol.messages.MalformedMediatorRequestResult]] message
    *     that is triggered by root hash messages.
    *     The mediator uses the [[com.digitalasset.canton.data.ViewType]] from the [[com.digitalasset.canton.protocol.messages.RootHashMessage]],
    *     which the participants also used to choose the processor for the request.
    *     So it suffices to forward the [[com.digitalasset.canton.protocol.messages.MalformedMediatorRequestResult]]
    *     to the appropriate processor.
    *   </li>
    *   <li>
    *     Request messages originate from untrusted participants.
    *     If the batch contains exactly one [[com.digitalasset.canton.protocol.messages.RootHashMessage]]
    *     that is sent to the participant and the mediator only,
    *     the participant processes only request messages with the same root hash.
    *     If there are no such root hash message or multiple thereof,
    *     the participant does not process the request at all
    *     because the mediator will reject the request as a whole.
    *   </li>
    *   <li>
    *     We do not know the submitting member of a particular submission because such a submission may be sequenced through
    *     an untrusted individual sequencer node (e.g., on a BFT domain). Such a sequencer node could lie about
    *     the actual submitting member. These lies work even with signed submission requests
    *     when an earlier submission request is replayed.
    *     So we cannot rely on honest domain nodes sending their messages only once and instead must
    *     deduplicate replays on the recipient side.
    *   </li>
    * </ul>
    */
  /* TODO(M40): If the participant does not process the request at all,
   *  this participant's conflict detection state may get desynchronized from other participants' conflict detection state
   *  until the mediator's rejection arrives.
   *  This can lead to requests being accepted that other participants would have rejected during Phase 3.
   *  For example, suppose that party `A` is hosted on participants `P1` and `P2`
   *  and an informee of a consuming exercise action `act` on a contract `c`.
   *  The dishonest submitter of a transaction `tx1` involving `act` sends a root hash message only for `P1` but not for `P2`.
   *  `P2` thus does not process `tx1` at all, whereas `P1` does process `tx1` and locks `c` for deactivation.
   *  Then, another transaction `tx2`, which uses `c`, arrives before the mediator has rejected `tx1`.
   *  `P2` approves `tx2` on behalf of `A` as `c` is not locked on `P2`.
   *  In contrast, `P1` rejects `tx2` on behalf of `A` as `c` is locked on `P1`.
   *  If `P2`'s approval arrives before `P1`'s rejection at the mediator, the mediator approves `tx2`.
   *  When `P1` receives the mediator approval for `tx2`, it crashes because a request that failed the activeness check is to be committed.
   *  Conversely, if `P1`'s rejection arrives before `P2`'s approval, the mediator may raise an alarm when it receives the approval.
   */
  protected def processBatch(
      eventE: Either[
        EventWithErrors[Deliver[DefaultOpenEnvelope]],
        SignedContent[Deliver[DefaultOpenEnvelope]],
      ]
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[ProcessingResult] = {
    val content = eventE.fold(_.content, _.content)
    val Deliver(sc, ts, _, _, batch) = content

    val envelopesWithCorrectDomainId = filterBatchForDomainId(batch, sc, ts)

    for {
      // Sanity check the batch
      // we can receive an empty batch if it was for a deliver we sent but were not a recipient
      sanityCheck <-
        if (content.isReceipt) {
          logger.debug(show"Received the receipt for a previously sent batch:\n${content}")
          FutureUnlessShutdown.pure(processingResultMonoid.empty)
        } else if (batch.envelopes.isEmpty) {
          doProcess(
            MalformedMessage,
            FutureUnlessShutdown.pure(alarm(sc, ts, "Received an empty batch.")),
          )
        } else FutureUnlessShutdown.pure(processingResultMonoid.empty)

      identityResult <- processTopologyTransactions(sc, ts, envelopesWithCorrectDomainId)
      causalityProcessed <- processCausalityMessages(envelopesWithCorrectDomainId)
      acsCommitmentResult <- processAcsCommitmentEnvelope(envelopesWithCorrectDomainId, sc, ts)
      transactionTransferResult <- processTransactionAndTransferMessages(
        eventE,
        sc,
        ts,
        envelopesWithCorrectDomainId,
      )
      repairProcessorResult <- repairProcessorWedging(ts)
    } yield Foldable[List].fold(
      List(
        sanityCheck,
        identityResult,
        causalityProcessed,
        acsCommitmentResult,
        transactionTransferResult,
        repairProcessorResult,
      )
    )
  }

  protected def processTopologyTransactions(
      sc: SequencerCounter,
      ts: CantonTimestamp,
      envelopes: List[DefaultOpenEnvelope],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[ProcessingResult] =
    doProcess(
      TopologyTransaction, {
        topologyProcessor(sc, ts, Traced(envelopes))
      },
    )

  protected def processCausalityMessages(
      envelopes: List[DefaultOpenEnvelope]
  )(implicit tc: TraceContext): FutureUnlessShutdown[ProcessingResult] = {
    val causalityMessages = envelopes.mapFilter(select[CausalityMessage])
    if (causalityMessages.nonEmpty)
      doProcess(
        CausalityMessageKind,
        FutureUnlessShutdown.outcomeF {
          tracker.registerCausalityMessages(causalityMessages.map(e => e.protocolMessage))
        },
      )
    else FutureUnlessShutdown.pure(processingResultMonoid.empty)

  }

  private def processTransactionAndTransferMessages(
      eventE: Either[
        EventWithErrors[Deliver[DefaultOpenEnvelope]],
        SignedContent[Deliver[DefaultOpenEnvelope]],
      ],
      sc: SequencerCounter,
      ts: CantonTimestamp,
      envelopes: List[DefaultOpenEnvelope],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[ProcessingResult] = {
    def alarmIfNonEmptySigned(
        kind: MessageKind[_],
        envelopes: Seq[
          OpenEnvelope[SignedProtocolMessage[HasRequestId with SignedProtocolMessageContent]]
        ],
    ): Unit =
      if (envelopes.nonEmpty) {
        val requestIds = envelopes.map(_.protocolMessage.message.requestId)
        val _ = alarm(sc, ts, show"Received unexpected $kind for $requestIds")
      }

    def withNewRequestCounter(
        body: RequestCounter => FutureUnlessShutdown[ProcessingResult]
    ): FutureUnlessShutdown[ProcessingResult] = {
      requestCounterAllocator.allocateFor(sc) match {
        case Some(rc) => body(rc)
        case None => FutureUnlessShutdown.pure(processingResultMonoid.empty)
      }
    }

    // Extract the participant relevant messages from the batch. All other messages are ignored.
    val encryptedViews = envelopes.mapFilter(select[EncryptedViewMessage[ViewType]])
    val regularMediatorResults =
      envelopes.mapFilter(select[SignedProtocolMessage[RegularMediatorResult]])
    val malformedMediatorRequestResults =
      envelopes.mapFilter(select[SignedProtocolMessage[MalformedMediatorRequestResult]])
    val rootHashMessages =
      envelopes.mapFilter(select[RootHashMessage[SerializedRootHashMessagePayload]])

    val isValidMediatorBatch =
      encryptedViews.isEmpty &&
        rootHashMessages.isEmpty &&
        regularMediatorResults.size + malformedMediatorRequestResults.size == 1
    if (isValidMediatorBatch) {
      if (regularMediatorResults.nonEmpty) {
        val regularResult = regularMediatorResults(0)
        val viewType = regularResult.protocolMessage.message.viewType
        val processor = tryProtocolProcessor(viewType)
        doProcess(ResultKind(viewType), processor.processResult(eventE))
      } else {
        val msg = malformedMediatorRequestResults(0)
        val viewType = msg.protocolMessage.message.viewType
        val processor = tryProtocolProcessor(viewType)
        doProcess(
          MalformedMediatorRequestMessage,
          processor.processMalformedMediatorRequestResult(ts, sc, eventE),
        )
      }
    } else {
      regularMediatorResults.groupBy(_.protocolMessage.message.viewType).foreach {
        case (viewType, messages) => alarmIfNonEmptySigned(ResultKind(viewType), messages)
      }
      alarmIfNonEmptySigned(MalformedMediatorRequestMessage, malformedMediatorRequestResults)

      val checkedRootHashMessages = correctRootHashMessageAndViews(rootHashMessages, encryptedViews)
      checkedRootHashMessages.nonaborts.iterator.foreach { alarmMsg =>
        val _ = alarm(sc, ts, alarmMsg)
      }
      checkedRootHashMessages.toEither match {
        case Right(goodRequest) =>
          withNewRequestCounter { rc =>
            val rootHashMessage: goodRequest.rootHashMessage.type = goodRequest.rootHashMessage
            val viewType: rootHashMessage.viewType.type = rootHashMessage.viewType
            val processor = tryProtocolProcessor(viewType)
            val batch = RequestAndRootHashMessage(
              goodRequest.requestEnvelopes,
              rootHashMessage,
              goodRequest.mediatorId,
            )
            doProcess(
              RequestKind(goodRequest.rootHashMessage.viewType),
              processor.processRequest(ts, rc, sc, batch),
            )
          }
        case Left(DoNotExpectMediatorResult) => tickRecordOrderPublisher(sc, ts)
        case Left(ExpectMalformedMediatorRequestResult(mediatorId)) =>
          // The request is malformed from this participant's and the mediator's point of view if the sequencer is honest.
          // An honest mediator will therefore try to send a `MalformedMediatorRequestResult`.
          withNewRequestCounter { rc =>
            doProcess(
              UnspecifiedMessageKind,
              badRootHashMessagesRequestProcessor
                .handleBadRequestWithExpectedMalformedMediatorRequest(rc, sc, ts, mediatorId),
            )
          }
        case Left(SendMalformedAndExpectMediatorResult(rootHash, mediatorId, reason)) =>
          // The request is malformed from this participant's point of view, but not necessarily from the mediator's.
          // TODO(M40): This needs to be addressed properly as part of the transparency guarantees.
          //  It is not clear that the mediator will process the rejection.
          withNewRequestCounter { rc =>
            doProcess(
              UnspecifiedMessageKind,
              badRootHashMessagesRequestProcessor.sendRejectionAndExpectMediatorResult(
                rc,
                sc,
                ts,
                rootHash,
                mediatorId,
                LocalReject.MalformedRejects.BadRootHashMessages
                  .Reject(reason, protocolVersion),
              ),
            )
          }
      }
    }
  }

  /** Checks the root hash messages and extracts the views with the correct view type.
    * @return [[com.digitalasset.canton.util.Checked.Abort]] indicates a really malformed request and the appropriate reaction
    */
  private def correctRootHashMessageAndViews(
      rootHashMessages: List[OpenEnvelope[RootHashMessage[SerializedRootHashMessagePayload]]],
      encryptedViews: List[OpenEnvelope[EncryptedViewMessage[ViewType]]],
  )(implicit
      traceContext: TraceContext
  ): Checked[FailedRootHashMessageCheck, String, GoodRequest] = {
    def isMediatorId(member: Member): Boolean = member match {
      case _: MediatorId => true
      case _ => false
    }

    val (rootHashMessagesSentToAMediator, rootHashMessagesNotSentToAMediator) =
      rootHashMessages.partition(_.recipients.allRecipients.exists(isMediatorId))
    val rootHashMessagesNotSentToAMediatorC =
      if (rootHashMessagesNotSentToAMediator.nonEmpty)
        Checked.continue(
          show"Received root hash messages that were not sent to a mediator: $rootHashMessagesNotSentToAMediator."
        )
      else Checked.result(())

    // The sequencer checks that participants can send only batches that target at most one mediator.
    // Only participants send batches with encrypted views.
    // So we should find at most one mediator among the recipients of the root hash messages if there are encrypted views.
    val allMediators = rootHashMessagesSentToAMediator.foldLeft(Set.empty[MediatorId]) {
      (acc, rhm) =>
        val mediators =
          rhm.recipients.allRecipients.collect { case mediatorId: MediatorId => mediatorId }
        acc.union(mediators)
    }
    if (allMediators.sizeCompare(1) > 0 && encryptedViews.nonEmpty) {
      // TODO(M99) The sequencer should have checked that no participant can send such a request.
      //  Honest nodes of the domain should not send such a request either
      //  (though they do send other batches to several mediators, e.g., topology updates).
      //  So the domain nodes or the sequencer are malicious.
      //  Handle this case of dishonest domain nodes more gracefully.
      ErrorUtil.internalError(
        new IllegalArgumentException(
          s"Received batch with encrypted views and root hash messages addressed to multiple mediators: $allMediators"
        )
      )
    }

    def goodRootHashMessage(
        rootHashMessage: RootHashMessage[SerializedRootHashMessagePayload],
        mediatorId: MediatorId,
    ): Checked[SendMalformedAndExpectMediatorResult, String, GoodRequest] = {
      val viewType: rootHashMessage.viewType.type = rootHashMessage.viewType
      val (badEncryptedViewTypes, goodEncryptedViews) = encryptedViews
        .map(_.traverse { encryptedViewMessage =>
          encryptedViewMessage
            .traverse(_.select(viewType))
            .toRight(encryptedViewMessage.viewType)
        })
        .separate
      val badEncryptedViewsC =
        if (badEncryptedViewTypes.nonEmpty)
          Checked.continue(
            show"Expected view type $viewType, but received view types ${badEncryptedViewTypes.distinct}"
          )
        else Checked.result(())
      val goodEncryptedViewsC = NonEmpty.from(goodEncryptedViews) match {
        case None =>
          // We received a batch with at least one root hash message,
          // but no view with the same view type.
          // Send a best-effort Malformed response.
          // This ensures that the mediator doesn't wait until the participant timeout
          // if the mediator expects a confirmation from this participant.
          Checked.Abort(
            SendMalformedAndExpectMediatorResult(
              rootHashMessage.rootHash,
              mediatorId,
              show"Received no encrypted view message of type $viewType",
            ),
            Chain(show"Received no encrypted view message of type $viewType"),
          )
        case Some(nonemptyEncryptedViews) =>
          Checked.result(GoodRequest(rootHashMessage, mediatorId)(nonemptyEncryptedViews))
      }
      badEncryptedViewsC.flatMap((_: Unit) => goodEncryptedViewsC)
    }

    rootHashMessagesNotSentToAMediatorC.flatMap { (_: Unit) =>
      (rootHashMessagesSentToAMediator: @unchecked) match {
        case Seq(rootHashMessage, furtherRHMs @ _*) =>
          val mediatorId = allMediators.headOption.getOrElse {
            val err = new RuntimeException(
              "The previous checks ensure that there is exactly one mediator ID"
            )
            ErrorUtil.internalError(err)
          }

          if (furtherRHMs.isEmpty) {
            val validRecipients = rootHashMessage.recipients.asSingleGroup.contains(
              NonEmpty.mk(Set, participantId, mediatorId)
            )
            if (validRecipients) {
              goodRootHashMessage(rootHashMessage.protocolMessage, mediatorId)
            } else {
              // We assume that the participant receives only envelopes of which it is a recipient
              Checked.Abort(
                ExpectMalformedMediatorRequestResult(mediatorId),
                Chain(
                  show"Received root hash message with invalid recipients: ${rootHashMessage.recipients}"
                ),
              )
            }
          } else {
            // Since all messages in `rootHashMessagesSentToAMediator` are addressed to a mediator
            // and there is at most one mediator recipient for all the envelopes in the batch,
            // all these messages must have been sent to the same mediator.
            // This mediator will therefore reject the request as malformed.
            Checked.Abort(
              ExpectMalformedMediatorRequestResult(mediatorId),
              Chain(show"Multiple root hash messages in batch: $rootHashMessagesSentToAMediator"),
            )
          }
        case Seq() =>
          // The batch may have contained a message that doesn't require a root hash message, e.g., an ACS commitment
          // So raise an alarm only if there are views
          val alarms =
            if (encryptedViews.nonEmpty) Chain(show"No valid root hash message in batch")
            else Chain.empty
          // The participant hasn't received a root hash message that was sent to the mediator.
          // The mediator sends a MalformedMediatorRequest only to the recipients of the root hash messages which it has received.
          // It sends a RegularMediatorResult only to the informee participants
          // and it checks that all the informee participants have received a root hash message.
          Checked.Abort(DoNotExpectMediatorResult, alarms)
      }
    }
  }

  protected def repairProcessorWedging(
      timestamp: CantonTimestamp
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[ProcessingResult] = {
    lazy val future = FutureUnlessShutdown.pure {
      repairProcessor.wedgeRepairRequests(timestamp)
    }
    doProcess(UnspecifiedMessageKind, future)
  }

  protected def observeSequencing(
      events: Seq[RawProtocolEvent]
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[ProcessingResult] = {
    val receipts = events.mapFilter {
      case Deliver(counter, timestamp, _domainId, messageIdO, _batch) =>
        // The event was submitted by the current participant iff the message ID is set.
        messageIdO.map(_ -> SequencedSubmission(counter, timestamp))
      case DeliverError(_counter, _timestamp, _domainId, _messageId, _reason) =>
        // `observeDeliverError` takes care of generating a rejection reason if necessary
        None
    }
    lazy val future =
      FutureUnlessShutdown.outcomeF(
        inFlightSubmissionTracker.observeSequencing(domainId, receipts.toMap)
      )
    doProcess(DeliveryMessageKind, future)
  }

  protected def observeDeliverError(
      error: DeliverError
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[ProcessingResult] = {
    doProcess(
      DeliveryMessageKind,
      FutureUnlessShutdown.outcomeF(inFlightSubmissionTracker.observeDeliverError(error)),
    )
  }

  private def tickRecordOrderPublisher(sc: SequencerCounter, ts: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): FutureUnlessShutdown[ProcessingResult] = {
    lazy val future = FutureUnlessShutdown.pure {
      recordOrderPublisher.tick(sc, ts)
    }
    doProcess(UnspecifiedMessageKind, future)
  }

  protected def filterBatchForDomainId(
      batch: Batch[DefaultOpenEnvelope],
      sc: SequencerCounter,
      ts: CantonTimestamp,
  )(implicit traceContext: TraceContext): List[DefaultOpenEnvelope] =
    ProtocolMessage.filterDomainsEnvelopes(
      batch,
      domainId,
      (wrongMsgs: List[DefaultOpenEnvelope]) => {
        val _ = alarm(
          sc,
          ts,
          s"Received messages with wrong domain IDs ${wrongMsgs.map(_.protocolMessage.domainId)}. Discarding them.",
        )
        ()
      },
    )

  protected def alarm(sc: SequencerCounter, ts: CantonTimestamp, msg: String)(implicit
      traceContext: TraceContext
  ): Unit = SyncServiceAlarm.Warn(s"(sequencer counter: $sc, timestamp: $ts): $msg").report()
}

private[participant] object MessageDispatcher {

  trait RequestProcessors {
    /* A bit of a round-about way to make the Scala compiler recognize that pattern matches on `viewType` refine
     * the type Processor.
     */
    protected def getInternal[P](viewType: ViewType { type Processor = P }): Option[P]
    def get(viewType: ViewType): Option[viewType.Processor] =
      getInternal[viewType.Processor](viewType)
  }

  /** Sigma type to tie the envelope's view type to the root hash message's. */
  private sealed trait GoodRequest {
    val rootHashMessage: RootHashMessage[SerializedRootHashMessagePayload]
    val mediatorId: MediatorId
    val requestEnvelopes: NonEmpty[Seq[
      OpenEnvelope[EncryptedViewMessage[rootHashMessage.viewType.type]]
    ]]
  }
  private object GoodRequest {
    def apply(rhm: RootHashMessage[SerializedRootHashMessagePayload], mediator: MediatorId)(
        envelopes: NonEmpty[Seq[OpenEnvelope[EncryptedViewMessage[rhm.viewType.type]]]]
    ): GoodRequest = new GoodRequest {
      override val rootHashMessage: rhm.type = rhm
      override val mediatorId: MediatorId = mediator
      override val requestEnvelopes
          : NonEmpty[Seq[OpenEnvelope[EncryptedViewMessage[rootHashMessage.viewType.type]]]] =
        envelopes
    }
  }

  /** @tparam A The type returned by processors for the given kind
    */
  sealed trait MessageKind[A] extends Product with Serializable with PrettyPrinting {
    override def pretty: Pretty[MessageKind.this.type] = prettyOfObject[MessageKind.this.type]
  }

  case object TopologyTransaction extends MessageKind[AsyncResult]
  case class RequestKind(viewType: ViewType) extends MessageKind[Unit] {
    override def pretty: Pretty[RequestKind] = prettyOfParam(unnamedParam(_.viewType))
  }
  case class ResultKind(viewType: ViewType) extends MessageKind[AsyncResult] {
    override def pretty: Pretty[ResultKind] = prettyOfParam(unnamedParam(_.viewType))
  }
  case object AcsCommitment extends MessageKind[Unit]
  case object MalformedMediatorRequestMessage extends MessageKind[AsyncResult]
  case object MalformedMessage extends MessageKind[Unit]
  case object UnspecifiedMessageKind extends MessageKind[Unit]
  case object CausalityMessageKind extends MessageKind[Unit]
  case object DeliveryMessageKind extends MessageKind[Unit]

  @VisibleForTesting
  private[protocol] sealed trait FailedRootHashMessageCheck extends Product with Serializable
  @VisibleForTesting
  private[protocol] case class ExpectMalformedMediatorRequestResult(mediatorId: MediatorId)
      extends FailedRootHashMessageCheck
  @VisibleForTesting
  private[protocol] case class SendMalformedAndExpectMediatorResult(
      rootHash: RootHash,
      mediatorId: MediatorId,
      rejectionReason: String,
  ) extends FailedRootHashMessageCheck
  @VisibleForTesting
  private[protocol] case object DoNotExpectMediatorResult extends FailedRootHashMessageCheck

  trait Factory[+T <: MessageDispatcher] {
    def create(
        protocolVersion: ProtocolVersion,
        domainId: DomainId,
        participantId: ParticipantId,
        requestTracker: RequestTracker,
        requestProcessors: RequestProcessors,
        tracker: SingleDomainCausalTracker,
        topologyProcessor: (
            SequencerCounter,
            CantonTimestamp,
            Traced[List[DefaultOpenEnvelope]],
        ) => HandlerResult,
        acsCommitmentProcessor: AcsCommitmentProcessor.ProcessorType,
        requestCounterAllocator: RequestCounterAllocator,
        recordOrderPublisher: RecordOrderPublisher,
        badRootHashMessagesRequestProcessor: BadRootHashMessagesRequestProcessor,
        repairProcessor: RepairProcessor,
        inFlightSubmissionTracker: InFlightSubmissionTracker,
        loggerFactory: NamedLoggerFactory,
    )(implicit ec: ExecutionContext, tracer: Tracer): T

    def create(
        protocolVersion: ProtocolVersion,
        domainId: DomainId,
        participantId: ParticipantId,
        requestTracker: RequestTracker,
        transactionProcessor: TransactionProcessor,
        transferOutProcessor: TransferOutProcessor,
        transferInProcessor: TransferInProcessor,
        registerTopologyTransactionResponseProcessor: Traced[
          List[DefaultOpenEnvelope]
        ] => HandlerResult,
        tracker: SingleDomainCausalTracker,
        topologyProcessor: TopologyTransactionProcessor,
        acsCommitmentProcessor: AcsCommitmentProcessor.ProcessorType,
        requestCounterAllocator: RequestCounterAllocator,
        recordOrderPublisher: RecordOrderPublisher,
        badRootHashMessagesRequestProcessor: BadRootHashMessagesRequestProcessor,
        repairProcessor: RepairProcessor,
        inFlightSubmissionTracker: InFlightSubmissionTracker,
        loggerFactory: NamedLoggerFactory,
    )(implicit ec: ExecutionContext, tracer: Tracer): T = {
      val requestProcessors = new RequestProcessors {
        override def getInternal[P](viewType: ViewType { type Processor = P }): Option[P] =
          viewType match {
            case TransferInViewType => Some(transferInProcessor)
            case TransferOutViewType => Some(transferOutProcessor)
            case TransactionViewType => Some(transactionProcessor)
            case _ => None
          }
      }

      val identityProcessor: (
          SequencerCounter,
          CantonTimestamp,
          Traced[List[DefaultOpenEnvelope]],
      ) => HandlerResult =
        (counter, timestamp, envelopes) => {
          val registerF = registerTopologyTransactionResponseProcessor(envelopes)
          val processingF = topologyProcessor.processEnvelopes(counter, timestamp, envelopes)
          for {
            r1 <- registerF
            r2 <- processingF
          } yield AsyncResult.monoidAsyncResult.combine(r1, r2)
        }

      create(
        protocolVersion,
        domainId,
        participantId,
        requestTracker,
        requestProcessors,
        tracker,
        identityProcessor,
        acsCommitmentProcessor,
        requestCounterAllocator,
        recordOrderPublisher,
        badRootHashMessagesRequestProcessor,
        repairProcessor,
        inFlightSubmissionTracker,
        loggerFactory,
      )
    }
  }

  object DefaultFactory extends Factory[MessageDispatcher] {
    override def create(
        protocolVersion: ProtocolVersion,
        domainId: DomainId,
        participantId: ParticipantId,
        requestTracker: RequestTracker,
        requestProcessors: RequestProcessors,
        tracker: SingleDomainCausalTracker,
        topologyProcessor: (
            SequencerCounter,
            CantonTimestamp,
            Traced[List[DefaultOpenEnvelope]],
        ) => HandlerResult,
        acsCommitmentProcessor: AcsCommitmentProcessor.ProcessorType,
        requestCounterAllocator: RequestCounterAllocator,
        recordOrderPublisher: RecordOrderPublisher,
        badRootHashMessagesRequestProcessor: BadRootHashMessagesRequestProcessor,
        repairProcessor: RepairProcessor,
        inFlightSubmissionTracker: InFlightSubmissionTracker,
        loggerFactory: NamedLoggerFactory,
    )(implicit ec: ExecutionContext, tracer: Tracer): MessageDispatcher = {
      new DefaultMessageDispatcher(
        protocolVersion,
        domainId,
        participantId,
        requestTracker,
        requestProcessors,
        tracker,
        topologyProcessor,
        acsCommitmentProcessor,
        requestCounterAllocator,
        recordOrderPublisher,
        badRootHashMessagesRequestProcessor,
        repairProcessor,
        inFlightSubmissionTracker,
        loggerFactory,
      )
    }
  }
}
