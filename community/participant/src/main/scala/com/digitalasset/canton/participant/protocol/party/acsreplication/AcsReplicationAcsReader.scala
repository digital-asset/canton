// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.party.acsreplication

import cats.syntax.either.*
import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.data.{CantonTimestamp, Offset}
import com.digitalasset.canton.ledger.participant.state.InternalIndexService
import com.digitalasset.canton.lifecycle.FlagCloseable
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.admin.data.ActiveContract
import com.digitalasset.canton.participant.admin.party.LapiAcsHelper
import com.digitalasset.canton.participant.protocol.party.SourceParticipantStore
import com.digitalasset.canton.participant.protocol.party.acsreplication.AcsReplicationAcsReader.flowControlBackoffMillis
import com.digitalasset.canton.participant.protocol.party.acsreplication.AcsReplicationSourceParticipantMessage.GetAcsArguments
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.topology.{ParticipantId, PartyId, PhysicalSynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{ErrorUtil, Mutex}
import com.digitalasset.canton.{ReassignmentCounter, checked}
import com.google.protobuf.ByteString
import org.apache.pekko.stream.scaladsl.{Keep, Sink}
import org.apache.pekko.stream.{KillSwitches, Materializer}

import java.util.concurrent.atomic.AtomicBoolean
import scala.annotation.tailrec
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future, blocking}
import scala.util.chaining.scalaUtilChainingOps

/** The ACS reader encapsulates the ledger API ACS pekko source and a size-bound in-memory queue
  * with associated flow-control to ensure that the SP does not read too far ahead of the contracts
  * that the TP has requested.
  *
  * It also provides a helper method to read and dequeue contracts from the queue in a safe manner.
  *
  * @param partyId
  *   The party whose ACS is being replicated.
  * @param psid
  *   The physical synchronizer within which the ACS is being replicated.
  * @param effectiveAtLapiOffset
  *   The Ledger API offset at which the party is being onboarded, needed to read the correct ACS
  *   snapshot via the LAPI.
  * @param excludedStakeholders
  *   Shared contract stakeholder parties to exclude from the read ACS, for example as in the case
  *   of party replication, exclude parties already hosted by the target participants.
  * @param acsBatchSize
  *   The expected batch size read from the reader. Used to limit the size of a queue buffer fed by
  *   the Pekko flow until the queue is drained by reads from the caller.
  * @param maxQueueSize
  *   The size of the queue to allow prefetching of ACS entries for reading. Must be at least as
  *   large as acsBatchSize to prevent hanging.
  * @param lapiIndexService
  *   The Ledger API index service used to read the ACS.
  * @param spStore
  *   The source participant store interface used to determine which range of contracts to retrieve.
  */
private[party] final class AcsReplicationAcsReader(
    partyId: PartyId,
    psid: PhysicalSynchronizerId,
    asOf: CantonTimestamp,
    effectiveAtLapiOffset: Offset,
    excludedStakeholders: Set[PartyId],
    sourceParticipantId: ParticipantId,
    agreedAt: CantonTimestamp,
    acsBatchSize: PositiveInt,
    maxQueueSize: PositiveInt,
    lapiIndexService: InternalIndexService,
    spStore: SourceParticipantStore,
    protected val loggerFactory: NamedLoggerFactory,
    protected val timeouts: ProcessingTimeout,
)(implicit
    ec: ExecutionContext,
    traceContext: TraceContext,
    mat: Materializer,
) extends NamedLogging
    with FlagCloseable {

  // Queue and acsDigest certification need to be synchronized for thread-safety
  private val queue = mutable.Queue.empty[ActiveContract]
  private val acsDigestHelper = new PartyReplicationAcsDigestHelper(
    GetAcsArguments(
      partyId,
      psid.logical,
      asOf,
      excludedStakeholders,
    ),
    sourceParticipantId,
    agreedAt,
    initialAcsHashO = None, // start hash empty since we read from the beginning
    psid,
  )

  private val lock = new Mutex()

  // Completed flag set only once/if the ACS reader stream has completed successfully.
  private val hasAcsReaderCompletedSuccessfully = new AtomicBoolean(false)

  private val (killSwitch, doneF) =
    LapiAcsHelper
      .ledgerApiAcsSource(
        lapiIndexService,
        Set(partyId),
        effectiveAtLapiOffset,
        excludedStakeholders,
        Some(psid.logical),
      )(traceContext)
      .viaMat(KillSwitches.single)(Keep.right)
      .zipWithIndex
      // Use mapAsync(parallelism=1) for flow-control rather than map
      // to ensure that flow-control runs in the execution context instead
      // of blocking activity on the entire pekko stream.
      .mapAsync(parallelism = 1) { case (activeContract, ordinal) =>
        @tailrec
        def processActiveContract(): Unit = {
          val canProceed =
            if (isClosing) {
              true // if closing, skip processing further active contracts to allow completing the flow
            } else if (spStore.initialContractOrdinalInclusiveO.isEmpty) {
              false // flow-control until initialized source participant processor is initialized
            } else {
              lock.exclusive {
                if (queue.sizeIs >= maxQueueSize.unwrap) {
                  false // flow-control until queue has space
                } else if (ordinal < spStore.contractOrdinalToSendUpToExclusive.unwrap) {
                  val lfContractId =
                    LfContractId
                      .fromString(activeContract.contract.getCreatedEvent.contractId)
                      .valueOr(ErrorUtil.invalidState(_))
                  val reassignmentCounter =
                    ReassignmentCounter.apply(activeContract.contract.reassignmentCounter)
                  // Skip any active contracts before the initial contract ordinal (if any).
                  if (spStore.initialContractOrdinalInclusiveO.exists(ordinal >= _.unwrap)) {
                    logger.debug(
                      s"Queue.appending active contract with ordinal $ordinal: $lfContractId"
                    )
                    queue.append(activeContract)
                  }
                  acsDigestHelper.addContract(lfContractId, reassignmentCounter)
                  true // proceed to next active contract
                } else {
                  false // flow-control until TP requests more contracts
                }
              }
            }

          if (!canProceed) {
            blocking(Threading.sleep(flowControlBackoffMillis))
            processActiveContract()
          }
        }

        Future.successful(processActiveContract())
      }
      .watchTermination()(Keep.left)
      .toMat(Sink.ignore)(Keep.both)
      .run()
      .tap { case (_, df) =>
        logger.info(s"Started ACS reader flow")
        df.onComplete { maybeDoneT =>
          logger.info(s"ACS source stream completed with: $maybeDoneT")
          // For the reader to have completed successfully, the stream must complete successfully
          // and not as a result of closing.
          if (maybeDoneT.isSuccess && !isClosing) {
            hasAcsReaderCompletedSuccessfully.set(true)
          }
        }
      }

  /** Helper to read batches from the ACS queue and dequeue them.
    *
    * @param numContractsRequested
    *   The number of contracts requested by the target participant.
    * @return
    *   A tuple of true iff we have read and sent the entire ACS and a potentially empty list of ACS
    *   batches.
    */
  def readContracts(numContractsRequested: PositiveInt): (Boolean, Seq[Seq[ActiveContract]]) =
    lock.exclusive {
      AcsReplicationAcsReader.readContracts(
        numContractsRequested,
        acsBatchSize,
        queue,
        hasAcsReaderCompletedSuccessfully.get(),
      )
    }

  /** Return the contracts digest at the end of the ACS replication.
    */
  def extractAcsDigest(): AcsReplicationSourceParticipantMessage.AcsDigest = lock.exclusive {
    require(
      hasAcsReaderCompletedSuccessfully.get() && queue.isEmpty,
      "contracts digest read before the end of the stream",
    )
    acsDigestHelper.extractAcsDigest()
  }

  /** Return the ACS digest hash of the contracts read so far.
    */
  def getAcsDigestHash: ByteString = lock.exclusive(acsDigestHelper.getAcsHash)

  override protected def onClosed(): Unit = {
    logger.info("Shutting down ACS source stream kill switch")(traceContext)
    killSwitch.shutdown()
    timeouts.closing.await_("Completing ACS source stream shutdown")(doneF)
  }
}

private[acsreplication] object AcsReplicationAcsReader {
  lazy val flowControlBackoffMillis = 1000L

  // Separate function for unit testing.
  private[acsreplication] def readContracts[T](
      numContractsRequested: PositiveInt,
      batchSize: PositiveInt,
      queue: mutable.Queue[T],
      isAcsReaderFinished: Boolean,
  ): (Boolean, Seq[Seq[T]]) = {
    @tailrec
    def go(
        numRequested: PositiveInt,
        acsBatchesAccum: Seq[Seq[T]],
    ): Seq[Seq[T]] = {
      // Return contract batch if at least a batch or the number of request contracts are in the
      // queue or if the ACS replication has completed successfully indicating that there might be
      // fewer or no entries in the queue.
      val numContractsRequestedNext = batchSize.unwrap.min(numRequested.unwrap)
      val canDequeueContractBatch =
        queue.sizeIs >= numContractsRequestedNext || (queue.nonEmpty && isAcsReaderFinished)

      // Case 1: Return if we don't have enough elements for a batch.
      if (!canDequeueContractBatch) {
        acsBatchesAccum
      } else {
        val nextBatch =
          (0 until numContractsRequestedNext).flatMap(_ => queue.dequeueFirst(_ => true))
        val numAccum = nextBatch +: acsBatchesAccum

        // Case 2: Return if we have requested all the elements.
        if (numContractsRequestedNext >= numRequested.unwrap) {
          numAccum
        } else {
          // Case 3: Check if we can return another batch.
          go(
            checked(PositiveInt.tryCreate(numRequested.unwrap - numContractsRequestedNext)),
            numAccum,
          )
        }
      }
    }

    val batchesReversed = go(numContractsRequested, Seq.empty)
    val isDone = isAcsReaderFinished && queue.isEmpty
    (isDone, batchesReversed.reverse)
  }
}
