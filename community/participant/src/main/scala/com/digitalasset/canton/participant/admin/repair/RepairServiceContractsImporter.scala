// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.repair

import cats.Eval
import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.traverse.*
import cats.syntax.traverseFilter.*
import com.digitalasset.canton.*
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.SyncCryptoApiParticipantProvider
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.ledger.participant.state.Update.RepairParticipantEventUpdate
import com.digitalasset.canton.ledger.participant.state.Update.TransactionAccepted.RepresentativePackageId.DedicatedRepresentativePackageId
import com.digitalasset.canton.ledger.participant.state.{ParticipantEvent, RepairUpdate}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown, HasCloseContext}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.ParticipantNodeParameters
import com.digitalasset.canton.participant.admin.data.{
  ContractImportMode,
  RepairContract,
  RepresentativePackageIdOverride,
}
import com.digitalasset.canton.participant.admin.repair.RepairServiceContractsImporter.ContractToAdd
import com.digitalasset.canton.participant.admin.repair.RepairServiceError.ImportAcsError
import com.digitalasset.canton.participant.store.*
import com.digitalasset.canton.participant.store.memory.PackageMetadataView
import com.digitalasset.canton.participant.sync.SyncPersistentStateLookup
import com.digitalasset.canton.participant.util.TimeOfChange
import com.digitalasset.canton.platform.indexer.IndexerState.ShutdownInProgress
import com.digitalasset.canton.platform.store.backend.PersistentParticipantEventUpdateCategory.{
  AcsImport,
  AcsImportTxStyle,
}
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.store.packagemeta.PackageMetadata
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.*
import com.digitalasset.canton.util.PekkoUtil.FutureQueue
import com.digitalasset.daml.lf.data.Bytes
import org.apache.pekko.NotUsed
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.{Keep, Sink, Source as PekkoSource}

import java.util.concurrent.atomic.AtomicLong
import scala.concurrent.{ExecutionContext, Future}

/** Implements the ACS import repair comments
  */
final class RepairServiceContractsImporter(
    syncCrypto: SyncCryptoApiParticipantProvider,
    syncPersistentStateLookup: SyncPersistentStateLookup,
    packageMetadataView: PackageMetadataView,
    contractStore: Eval[ContractStore],
    nodeParameters: ParticipantNodeParameters,
    helpers: RepairServiceHelpers,
    contractValidator: ContractValidator,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext, mat: Materializer)
    extends NamedLogging
    with FlagCloseable
    with HasCloseContext {

  private type MissingContract = ContractInstance
  private type MissingAssignment =
    (LfContractId, ReassignmentTag.Source[SynchronizerId], ReassignmentCounter, TimeOfChange)
  private type MissingAdd = (LfContractId, ReassignmentCounter, TimeOfChange)

  override protected def timeouts: ProcessingTimeout = nodeParameters.processingTimeouts

  private val eventMode: RepairLedgerApiEventMode =
    RepairLedgerApiEventMode.fromParameters(nodeParameters)

  /** Decide whether contract `repairContract` requires persistence based on its current storage
    * state.
    *
    * Guarantees idempotency by skipping contracts that are already active. DB calls: None
    *
    * @param repairContract
    *   Candidate contract payload and metadata being imported.
    * @param acsState
    *   Current status of the contract in the `ActiveContractStore`.
    * @param storedContract
    *   Current raw contract instance cached from the `ContractStore`.
    * @param skippedAlreadyActiveCount
    *   Thread-safe accumulator tracking bypassed pre-existing active contracts.
    * @return
    *   `Right(Some(...))` if the contract must be added, `Right(None)` if safely bypassed, or
    *   `Left` on validation failure.
    */
  private def contractToAdd(
      repairContract: RepairContract,
      acsState: Option[ActiveContractStore.Status],
      storedContract: Option[ContractInstance],
      skippedAlreadyActiveCount: AtomicLong,
  )(implicit
      traceContext: TraceContext
  ): Either[String, Option[ContractToAdd]] = {
    val contractId = repairContract.contract.contractId

    def addContract(
        reassigningFrom: Option[ReassignmentTag.Source[SynchronizerId]]
    ): Either[String, Option[ContractToAdd]] =
      ContractInstance
        .create(repairContract.contract)
        .map(contractInstance =>
          Option(
            ContractToAdd(
              contract = contractInstance,
              reassignmentCounter = repairContract.reassignmentCounter,
              reassigningFrom = reassigningFrom,
              representativePackageId = repairContract.representativePackageId,
            )
          )
        )

    acsState match {
      case None => addContract(reassigningFrom = None)

      case Some(ActiveContractStore.Active(_)) =>
        logger.debug(s"Skipping contract $contractId because it is already active")
        skippedAlreadyActiveCount.incrementAndGet().discard

        for {
          contractAlreadyThere <-
            storedContract.toRight {
              s"Contract ${repairContract.contract.contractId} is active but is not found in the stores"
            }
          _ <- EitherUtil.condUnit(
            contractAlreadyThere.inst == repairContract.contract,
            s"Contract $contractId exists in synchronizer, but does not match with contract being added. "
              + s"Existing contract is $contractAlreadyThere while contract supposed to be added ${repairContract.contract}",
          )
        } yield None
      case Some(ActiveContractStore.Archived) =>
        Left(
          s"Cannot add previously archived contract ${repairContract.contract.contractId} as archived contracts cannot become active."
        )
      case Some(ActiveContractStore.Purged) => addContract(reassigningFrom = None)
      case Some(ActiveContractStore.ReassignedAway(targetSynchronizer, reassignmentCounter)) =>
        logger.info(
          s"Marking contract ${repairContract.contract.contractId} previously unassigned targeting $targetSynchronizer as " +
            s"assigned from $targetSynchronizer (even though contract may have been reassigned to yet another synchronizer since)."
        )

        val isReassignmentCounterIncreasing =
          repairContract.reassignmentCounter > reassignmentCounter

        if (isReassignmentCounterIncreasing) {
          addContract(reassigningFrom = Option(ReassignmentTag.Source(targetSynchronizer.unwrap)))
        } else {
          Left(
            s"The reassignment counter ${repairContract.reassignmentCounter} of the contract " +
              s"${repairContract.contract.contractId} needs to be strictly larger than the reassignment counter " +
              s"$reassignmentCounter at the time of the unassignment."
          )
        }
    }
  }

  // This function requires that all contracts in contractsToAdd are already present in the
  // contract store and therefore their internal contract ids can be looked up.
  private def tryAddInternalContractIds(
      contractsToAdd: Seq[(TimeOfRepair, Seq[ContractToAdd])],
      internalContractIds: Map[LfContractId, Long],
  )(implicit
      traceContext: TraceContext
  ): Seq[(TimeOfRepair, Seq[(ContractToAdd, Long)])] =
    contractsToAdd.map { case (timeOfRepair, batch) =>
      val batchWithInternalIds = batch.map { contractToAdd =>
        val internalContractId =
          internalContractIds.getOrElse(
            contractToAdd.cid,
            ErrorUtil
              .invalidState(
                s"Not found internal contract id for contract ${contractToAdd.cid}"
              ),
          )
        (contractToAdd, internalContractId)
      }
      (timeOfRepair, batchWithInternalIds)
    }

  /** Participant repair utility for manually adding contracts to a synchronizer in an offline
    * fashion.
    *
    * For argument description:
    * @see
    *   [[com.digitalasset.canton.participant.admin.repair.RepairService#addContracts]]
    *
    * Note: Assigning the internal contract ids to the contracts requires that all the contracts are
    * already persisted in the contract store.
    */
  private[repair] def addContracts(
      synchronizerId: SynchronizerId,
      contracts: PekkoSource[RepairContract, NotUsed],
      contractImportMode: ContractImportMode,
      packageMetadataSnapshot: PackageMetadata,
      representativePackageIdOverride: RepresentativePackageIdOverride,
      workflowIdPrefix: Option[String] = None,
      forceRepairWhenTopologyTransactionAtLedgerEnd: Boolean,
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, String, Unit] = {
    val parameters = Map(
      "workflowIdPrefix" -> workflowIdPrefix.toString
    )

    logger.info(s"Adding contracts to synchronizer $synchronizerId with parameters: $parameters")

    def toFutureForIndexer[T](resET: EitherT[FutureUnlessShutdown, String, T]): Future[T] =
      EitherTUtil.toFuture(
        resET
          .failOnShutdownTo(ShutdownInProgress)
          .leftMap(err => ImportAcsError.Error(err).asGrpcError)
      )

    val workflowProvider =
      Iterator
        .from(1)
        .map(i => workflowIdPrefix.map(prefix => LfWorkflowId.assertFromString(s"$prefix-$i")))

    val selectRepresentativePackageIds = new SelectRepresentativePackageIds(
      representativePackageIdOverride = representativePackageIdOverride,
      knownPackages = packageMetadataSnapshot.packages.keySet,
      packageNameMap = packageMetadataSnapshot.packageNameMap,
      contractImportMode = contractImportMode,
      loggerFactory = loggerFactory,
    )

    val batchSize = nodeParameters.batchingConfig.maxAcsImportBatchSize.unwrap
    val parallelism = nodeParameters.batchingConfig.parallelism.unwrap

    // Shared visibility metrics accumulators to be INFO logged
    val droppedContractsCount = new AtomicLong(0L)
    val skippedAlreadyActiveCount = new AtomicLong(0L)
    val persistedContractsCount = new AtomicLong(0L)

    helpers.withRepairIndexer { repairIndexer =>
      val indexedContractBatches: PekkoSource[(Seq[RepairContract], Long), NotUsed] =
        contracts.grouped(batchSize).zipWithIndex

      val doneF = for {
        synchronizer <- toFutureForIndexer(
          helpers.readSynchronizerData(synchronizerId, repairIndexer)
        )
        // 1. Explicitly trigger crash recovery cleanup and verify ACS commitment watermarks
        _ <- toFutureForIndexer(
          helpers.verifyRepairPreconditions(
            synchronizer,
            forceRepairWhenTopologyTransactionAtLedgerEnd,
          )
        )
        // 2. Execute the stream only after the Active Contract Store is clean
        _ <- indexedContractBatches
          .mapAsync(parallelism) { data =>
            toFutureForIndexer(
              validateAndPersistContracts(
                synchronizer = synchronizer,
                contractImportMode = contractImportMode,
                selectRepresentativePackageIds = selectRepresentativePackageIds,
                batchSize = batchSize,
                droppedContractsCount = droppedContractsCount,
                skippedAlreadyActiveCount = skippedAlreadyActiveCount,
                persistedContractsCount = persistedContractsCount,
              )(data)
            )
          }
          // Publish events to the indexer
          .mapAsync(1) { contractsToAddWithInternalContractIds =>
            publishAddedEvents(
              synchronizerId,
              contractsToAddWithInternalContractIds,
              workflowProvider,
              repairIndexer,
            )
          }
          .toMat(Sink.ignore)(Keep.right)
          .run()
      } yield ()

      EitherT.liftF(doneF.map { _ =>
        val totalPersisted = persistedContractsCount.get()
        val totalSkipped = skippedAlreadyActiveCount.get()
        val totalDropped = droppedContractsCount.get()

        logger.info(
          s"Contract import pipeline for synchronizer $synchronizerId finished. " +
            s"Persisted: $totalPersisted new active contracts written. " +
            s"Skipped: $totalSkipped pre-existing contracts safely bypassed (idempotent). " +
            s"Dropped: $totalDropped foreign payloads ignored (synchronizer mismatch)."
        )
      })
    }
  }

  /** Validates a batch of repair contracts and synchronously persists new ones to Canton internal
    * storage.
    *
    *   1. Filters contracts to the expected synchronizer and verifies reassignment counter
    *      constraints.
    *   1. Authenticates payloads and signatures.
    *   1. Drops internally active contracts to guarantee idempotency.
    *   1. Writes missing contracts to the internal `ContractStore` and `ActiveContractStore` with
    *      sequential repair counters.
    *
    * @param synchronizer
    *   Target synchronizer context containing essential persistence handles (`persistentState`),
    *   the current logical record time, and the baseline repair counter.
    * @param contractImportMode
    *   Processing strictness level. In
    *   [[com.digitalasset.canton.participant.admin.data.ContractImportMode.Accept]] mode,
    *   validation is bypassed; in
    *   [[com.digitalasset.canton.participant.admin.data.ContractImportMode.Validation]] mode,
    *   contracts undergo full signature/version authentication and allow representative package ID
    *   overrides.
    * @param selectRepresentativePackageIds
    *   Resolves optimal representative package IDs using an explicit chain of precedence rules
    *   (scanning from granular contract ID overrides down to the highest versioned package in the
    *   local store).
    * @param batchSize
    *   Expected source stream chunk size, used to space sequential repair counters cleanly.
    * @param droppedContractsCount
    *   Thread-safe accumulator tracking contracts ignored due to logical synchronizer mismatch.
    * @param skippedAlreadyActiveCount
    *   Thread-safe accumulator tracking pre-existing contracts bypassed to guarantee idempotency.
    * @param persistedContractsCount
    *   Thread-safe accumulator tracking the total number of newly persisted active contracts
    *   written.
    * @param data
    *   A tuple containing the unfiltered incoming repair contracts and their absolute chunk index.
    * @return
    *   Successfully persisted contracts grouped by creation time and assigned `TimeOfRepair`.
    */
  private def validateAndPersistContracts(
      synchronizer: RepairRequest.SynchronizerData,
      contractImportMode: ContractImportMode,
      selectRepresentativePackageIds: SelectRepresentativePackageIds,
      batchSize: Int,
      droppedContractsCount: AtomicLong,
      skippedAlreadyActiveCount: AtomicLong,
      persistedContractsCount: AtomicLong,
  )(
      data: (Seq[RepairContract], Long)
  )(implicit traceContext: TraceContext): EitherT[
    FutureUnlessShutdown,
    String,
    Seq[(TimeOfRepair, Seq[(ContractToAdd, Long)])],
  ] = {
    val (contractsUnchecked, idx) = data

    for {
      contractsUnfiltered <- selectRepresentativePackageIds(contractsUnchecked)
        .toEitherT[FutureUnlessShutdown]

      contracts = contractsUnfiltered.filter(
        _.synchronizerId == synchronizer.psid.logical
      )

      localDroppedContractsCount = contractsUnfiltered.size - contracts.size

      _ = if (localDroppedContractsCount > 0) {
        droppedContractsCount.addAndGet(localDroppedContractsCount.toLong).discard
      }

      _ <- ContractAuthenticationImportProcessor.validate(
        loggerFactory,
        syncPersistentStateLookup,
        contractValidator,
        contractImportMode,
      )(contracts)

      contractIds = contracts.map(_.contract.contractId)

      contractStates <- EitherT.right[String](
        helpers.readContractAcsStates(
          synchronizer.persistentState,
          contractIds,
        )
      )

      contractInstances <- helpers
        .logOnFailureWithInfoLevel(
          contractStore.value.lookupManyUncached(contractIds),
          "Unable to lookup contracts in contract store",
        )
        .map(_.flatten)

      storedContracts = contractInstances.map(c => c.contractId -> c).toMap

      // Fail-fast before zipping unequal collections (future refactorings)
      _ = ErrorUtil.requireState(
        contracts.sizeCompare(contractStates) == 0,
        s"Contract states list size (${contractStates.size}) must align exactly with the targeted contracts list size (${contracts.size})",
      )

      filteredContracts <- EitherT.fromEither[FutureUnlessShutdown](
        contracts.zip(contractStates).traverseFilter { case (contract, acsState) =>
          contractToAdd(
            repairContract = contract,
            acsState = acsState,
            storedContract = storedContracts.get(contract.contract.contractId),
            skippedAlreadyActiveCount = skippedAlreadyActiveCount,
          )
        }
      )

      _ <- EitherT.fromEither[FutureUnlessShutdown](addContractsCheck(filteredContracts))

      // Group per create time and per Ledger API representation (Created vs Assigned events),
      // so that every group results in exactly one homogeneous update.
      contractsSortedByCreationTime = filteredContracts
        .groupBy(c =>
          (c.contract.inst.createdAt, eventMode.useReassignmentEvent(c.reassignmentCounter))
        )
        .toList
        .sortBy { case ((ledgerCreateTime, useReassignmentEvent), _) =>
          (ledgerCreateTime.time, useReassignmentEvent)
        }
        .map { case ((ledgerCreateTime, _), contractsOfGroup) =>
          contractsOfGroup
        }

      /*
      Repair counters need to be strictly increasing but gaps are allowed.
      - monotonicity is guaranteed by pekko ordering guarantee
      - gaps can happen because of the grouping per create time and representation
       */
      timesOfRepair = Iterator
        .from(idx.toInt * batchSize, 1)
        .map(i => TimeOfRepair(synchronizer.currentRecordTime, synchronizer.nextRepairCounter + i))

      contractsToAdd = timesOfRepair.zip(contractsSortedByCreationTime).toSeq

      // Logging exactly once per non-empty chunk provides a non-spammy, reliable progress heartbeat
      // while atomically accumulating the exact volume for a final summary.
      _ = if (filteredContracts.nonEmpty) {
        persistedContractsCount.addAndGet(filteredContracts.size.toLong).discard
        logger.info(
          s"Importing chunk index $idx: synchronously persisting ${filteredContracts.size} active contracts"
        )
      }

      contractsWithTimeOfChange = contractsToAdd.flatMap { case (tor, cs) =>
        cs.map(_ -> tor.toToc)
      }

      _ <- persistAddContracts(
        synchronizer,
        contractsToAdd = contractsWithTimeOfChange,
        storedContracts = storedContracts,
      )

      internalContractIdsForContractsAdded <-
        helpers.logOnFailureWithInfoLevel(
          contractStore.value.lookupBatchedInternalIdsNonReadThrough(
            contractsWithTimeOfChange.map(_._1.contract.contractId)
          ),
          "Unable to lookup internal contract ids in contract store",
        )

    } yield checked(tryAddInternalContractIds(contractsToAdd, internalContractIdsForContractsAdded))
  }

  /** Checks that the contracts can be added (packages known, contract keys have maintainers)
    */
  private def addContractsCheck(
      contracts: Seq[ContractToAdd]
  )(implicit traceContext: TraceContext): Either[String, Unit] =
    for {
      // Check that the representative package-id is known
      _ <- contracts.map(_.representativePackageId).distinct.traverse(packageKnown)

      _ <- contracts.traverse { contractToAdd =>
        val contract = contractToAdd.contract
        val contractId = contractToAdd.cid
        EitherUtil.condUnit(
          !contract.contractKeyWithMaintainers.exists(_.maintainers.isEmpty),
          s"Contract $contractId has key without maintainers.",
        )
      }
    } yield ()

  /** Actual persistence work
    * @param synchronizer
    *   Synchronizer data
    * @param contractsToAdd
    *   Contracts to be added
    * @param storedContracts
    *   Contracts that already exists in the store
    */
  private def persistAddContracts(
      synchronizer: RepairRequest.SynchronizerData,
      contractsToAdd: Seq[(ContractToAdd, TimeOfChange)],
      storedContracts: Map[LfContractId, ContractInstance],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, String, Unit] = {
    // We compute first which changes we need to persist
    val missingContractsE: Either[String, Seq[MissingContract]] =
      contractsToAdd.traverseFilter[Either[String, *], MissingContract] { case (contractToAdd, _) =>
        storedContracts.get(contractToAdd.cid) match {
          case None => Right(Some(contractToAdd.contract))
          case Some(storedContract) =>
            Either.cond(
              storedContract == contractToAdd.contract,
              Option.empty[MissingContract],
              s"Contract ${contractToAdd.cid} already exists in the contract store, but differs from contract to be created. Contract to be created $contractToAdd versus existing contract $storedContract.",
            )
        }
      }

    for {
      // We compute first which changes we need to persist
      missingContracts <- EitherT.fromEither[FutureUnlessShutdown](missingContractsE)

      (missingAssignments, missingAdds) = contractsToAdd.foldLeft(
        (Seq.empty[MissingAssignment], Seq.empty[MissingAdd])
      ) { case ((missingAssignments, missingAdds), (contract, toc)) =>
        contract.reassigningFrom match {
          case Some(sourceSynchronizerId) =>
            val newAssignment =
              (contract.cid, sourceSynchronizerId, contract.reassignmentCounter, toc)
            (newAssignment +: missingAssignments, missingAdds)

          case None =>
            val newAdd = (contract.cid, contract.reassignmentCounter, toc)
            (missingAssignments, newAdd +: missingAdds)
        }
      }

      // Now, we update the stores
      _ <- helpers.logOnFailureWithInfoLevel(
        contractStore.value.storeContracts(missingContracts),
        "Unable to store missing contracts",
      )

      _ <- synchronizer.persistentState.activeContractStore
        .markContractsAdded(missingAdds)
        .toEitherTWithNonaborts
        .leftMap(e =>
          s"Failed to add contracts ${missingAdds.map { case (cid, _, _) => cid }} in ActiveContractStore: $e"
        )

      _ <- synchronizer.persistentState.activeContractStore
        .assignContracts(missingAssignments)
        .toEitherTWithNonaborts
        .leftMap(e =>
          s"Failed to assign ${missingAssignments.map { case (cid, _, _, _) => cid }} in ActiveContractStore: $e"
        )
    } yield ()
  }
  private def prepareAddedUpdate(
      synchronizerId: SynchronizerId,
      recordTime: CantonTimestamp,
      repairCounter: RepairCounter,
      contractsAdded: Seq[(ContractToAdd, Long)],
      workflowIdProvider: () => Option[LfWorkflowId],
      eventMode: RepairLedgerApiEventMode,
  )(implicit traceContext: TraceContext): Option[RepairParticipantEventUpdate] = {
    val category =
      if (
        contractsAdded.exists { case (contract, _) =>
          eventMode.useReassignmentEvent(contract.reassignmentCounter)
        }
      ) AcsImport
      else AcsImportTxStyle
    Option.when(contractsAdded.nonEmpty)(
      RepairParticipantEventUpdate(
        updateId = randomUpdateId(syncCrypto),
        workflowId = workflowIdProvider(),
        synchronizerId = synchronizerId,
        recordTime = recordTime,
        events = contractsAdded.zipWithIndex.map { case ((c, internalContractId), nodeId) =>
          ParticipantEvent.Added(
            nodeId = nodeId,
            contractInstance = PersistedContractInstance(
              internalContractId = internalContractId,
              inst = c.contract.inst,
            ),
            reassignmentCounter = c.reassignmentCounter.unwrap,
            representativePackageId = DedicatedRepresentativePackageId(c.representativePackageId),
          )
        },
        category = category,
        traceContext = traceContext,
        repairCounter = repairCounter,
      )
    )
  }

  private def publishAddedEvents(
      synchronizerId: SynchronizerId,
      contractsAdded: Seq[(TimeOfRepair, Seq[(ContractToAdd, Long)])],
      workflowIds: Iterator[Option[LfWorkflowId]],
      repairIndexer: FutureQueue[RepairUpdate],
  )(implicit traceContext: TraceContext): Future[Unit] =
    // not waiting for Update.persisted, since CommitRepair anyway will be waited for at the end
    MonadUtil
      .sequentialTraverse_(
        contractsAdded.flatMap { case (timeOfChange, contractsToAdd) =>
          prepareAddedUpdate(
            synchronizerId = synchronizerId,
            recordTime = timeOfChange.timestamp,
            repairCounter = timeOfChange.repairCounter,
            contractsAdded = contractsToAdd,
            workflowIdProvider = () => workflowIds.next(),
            eventMode,
          )
        }
      )(repairIndexer.offer)

  private def packageKnown(
      lfPackageId: LfPackageId
  )(implicit traceContext: TraceContext): Either[String, Unit] =
    Either.cond(
      packageMetadataView.getSnapshot.packages.contains(lfPackageId),
      (),
      s"Failed to locate package $lfPackageId",
    )
}

object RepairServiceContractsImporter {

  private final case class ContractToAdd(
      contract: ContractInstance,
      reassignmentCounter: ReassignmentCounter,
      reassigningFrom: Option[ReassignmentTag.Source[SynchronizerId]],
      representativePackageId: LfPackageId,
  ) {
    def cid: LfContractId = contract.contractId

    def authenticationData: Bytes =
      contract.inst.authenticationData
  }

}
