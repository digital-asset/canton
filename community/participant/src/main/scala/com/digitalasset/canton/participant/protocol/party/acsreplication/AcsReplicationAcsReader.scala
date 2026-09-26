// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.party.acsreplication

import cats.syntax.either.*
import com.digitalasset.canton.ReassignmentCounter
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
import com.digitalasset.canton.participant.protocol.party.acsreplication.AcsReplicationAcsReader.{
  flowControlBackoffMillis,
  maxQueueSize,
}
import com.digitalasset.canton.participant.protocol.party.acsreplication.AcsReplicationSourceParticipantMessage.GetAcsArguments
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.topology.{ParticipantId, PartyId, PhysicalSynchronizerId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{ErrorUtil, Mutex}
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

  /** Helper to read up to `numContractsToRead` contracts from the ACS queue and dequeue them.
    * @return
    *   A tuple of true iff we have read and sent the entire ACS and a potentially empty batch of
    *   contracts.
    */
  def readContracts(
      numContractsToRead: PositiveInt
  ): (Boolean, Seq[ActiveContract]) =
    lock.exclusive {
      // Return contract batch if numContractsToRead are in the queue or if the ACS replication has
      // completed successfully indicating that there might be fewer or no entries in the queue.
      val isAcsReaderFinished = hasAcsReaderCompletedSuccessfully.get()
      val canReturnContractBatch =
        queue.sizeIs >= numContractsToRead.unwrap || isAcsReaderFinished
      val batch = if (canReturnContractBatch) {
        (0 until numContractsToRead.unwrap).flatMap(_ => queue.dequeueFirst(_ => true))
      } else Seq.empty
      val isDone = isAcsReaderFinished && queue.isEmpty
      (isDone, batch)
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

private object AcsReplicationAcsReader {
  // TODO(#22251): Make this configurable. The maxQueueSize cannot be below the TP request-batch-size
  //  AcsReplicationTargetParticipantProcessor.contractsToRequestEachTime to avoid a deadlock.
  lazy val maxQueueSize: PositiveInt = PositiveInt.tryCreate(1000)
  lazy val flowControlBackoffMillis = 1000L
}
