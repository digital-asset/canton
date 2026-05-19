// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.repair

import cats.Eval
import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.traverse.*
import cats.syntax.traverseFilter.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.*
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.SyncCryptoApiParticipantProvider
import com.digitalasset.canton.data.{CantonTimestamp, LedgerTimeBoundaries}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.ledger.participant.state.Update.TransactionAccepted.RepresentativePackageId.DedicatedRepresentativePackageId
import com.digitalasset.canton.ledger.participant.state.Update.{
  ContractInfo,
  RepairReassignmentAccepted,
}
import com.digitalasset.canton.ledger.participant.state.{
  Reassignment,
  ReassignmentInfo,
  RepairUpdate,
  TransactionMeta,
  Update,
}
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
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.store.packagemeta.PackageMetadata
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.*
import com.digitalasset.canton.util.PekkoUtil.FutureQueue
import com.digitalasset.canton.util.ReassignmentTag.{Source, Target}
import com.digitalasset.daml.lf.CantonOnly
import com.digitalasset.daml.lf.data.{Bytes, ImmArray}
import com.digitalasset.daml.lf.transaction.CreationTime
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
      contractsToAdd: Seq[(TimeOfRepair, (CreationTime.CreatedAt, Seq[ContractToAdd]))],
      internalContractIds: Map[LfContractId, Long],
  )(implicit
      traceContext: TraceContext
  ): Seq[(TimeOfRepair, (CreationTime.CreatedAt, Seq[(ContractToAdd, Long)]))] =
    contractsToAdd.map { case (timeOfRepair, (createdAt, batch)) =>
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
      (timeOfRepair, (createdAt, batchWithInternalIds))
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
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, String, Unit] = {
    val parameters = Map(
      "workflowIdPrefix" -> workflowIdPrefix.toString
    )

    logger.info(s"Adding contracts to synchronizer $synchronizerId with parameters: $parameters")

    def toFuture[T](resET: EitherT[FutureUnlessShutdown, String, T]): Future[T] = resET.value
      .flatMap(
        _.fold(
          err => FutureUnlessShutdown.failed[T](ImportAcsError.Error(err).asGrpcError),
          res => FutureUnlessShutdown.pure(res),
        )
      )
      .failOnShutdownToAbortException("addContracts")

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
        synchronizer <- toFuture(helpers.readSynchronizerData(synchronizerId, repairIndexer))
        // 1. Explicitly trigger crash recovery cleanup and verify ACS commitment watermarks
        _ <- toFuture(helpers.verifyRepairPreconditions(synchronizer))
        // 2. Execute the stream only after the Active Contract Store is clean
        _ <- indexedContractBatches
          .mapAsync(parallelism) { data =>
            toFuture(
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
            if (nodeParameters.alphaMultiSynchronizerSupport) {
              publishAssignedEvents(
                synchronizerId,
                synchronizer.currentRecordTime,
                contractsToAddWithInternalContractIds,
                workflowProvider,
                repairIndexer,
              )
            } else {
              writeContractsAddedEvents(
                synchronizerId,
                contractsToAddWithInternalContractIds,
                workflowProvider,
                repairIndexer,
              ).failOnShutdownToAbortException("addContracts")
            }
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
    Seq[(TimeOfRepair, (CreationTime.CreatedAt, Seq[(ContractToAdd, Long)]))],
  ] = {
    val (contractsUnchecked, idx) = data

    for {
      contractsUnfiltered <- selectRepresentativePackageIds(contractsUnchecked)
        .toEitherT[FutureUnlessShutdown]

      contractsWithUnexpectedReassignmentCounter =
        if (nodeParameters.alphaMultiSynchronizerSupport) {
          Nil
        } else {
          contractsUnfiltered.filter(_.reassignmentCounter != ReassignmentCounter.Genesis)
        }

      _ <- EitherTUtil.condUnitET[FutureUnlessShutdown](
        contractsWithUnexpectedReassignmentCounter.isEmpty,
        s"Contracts with a non-zero reassignment counter found with disabled multi-synchronizer support: ${contractsWithUnexpectedReassignmentCounter
            .map(c => (c.contractId, c.reassignmentCounter))}",
      )

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

      contractsByCreationTime = filteredContracts
        .groupBy(_.contract.inst.createdAt)
        .toList
        .sortBy { case (ledgerCreateTime, _) => ledgerCreateTime.time }

      /*
      Repair counters need to be strictly increasing but gaps are allowed.
      - monotonicity is guaranteed by pekko ordering guarantee
      - gaps can happen because of the grouping per create time
       */
      timesOfRepair = Iterator
        .from(idx.toInt * batchSize, 1)
        .map(i => TimeOfRepair(synchronizer.currentRecordTime, synchronizer.nextRepairCounter + i))

      contractsToAdd = timesOfRepair.zip(contractsByCreationTime).toSeq

      // Logging exactly once per non-empty chunk provides a non-spammy, reliable progress heartbeat
      // while atomically accumulating the exact volume for a final summary.
      _ = if (filteredContracts.nonEmpty) {
        persistedContractsCount.addAndGet(filteredContracts.size.toLong).discard
        logger.info(
          s"Importing chunk index $idx: synchronously persisting ${filteredContracts.size} active contracts"
        )
      }

      contractsWithTimeOfChange = contractsToAdd.flatMap { case (tor, (_, cs)) =>
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

  private def prepareAddedEvents(
      synchronizerId: SynchronizerId,
      recordTime: CantonTimestamp,
      repairCounter: RepairCounter,
      ledgerCreateTime: CreationTime.CreatedAt,
      contractsAdded: Seq[(ContractToAdd, Long)],
      workflowIdProvider: () => Option[LfWorkflowId],
  )(implicit traceContext: TraceContext): RepairUpdate = {
    val contractInfos = contractsAdded.view.map { case (c, internalContractId) =>
      val cid = c.contract.contractId
      cid -> ContractInfo(
        persistedContractInstance = PersistedContractInstance(
          internalContractId = internalContractId,
          inst = c.contract.inst,
        ),
        representativePackageId = DedicatedRepresentativePackageId(c.representativePackageId),
      )
    }.toMap
    val nodeIds = LazyList.from(0).map(LfNodeId.apply)
    val txNodes = nodeIds.zip(contractsAdded.map(_._1.contract.toLf)).toMap
    Update.RepairTransactionAccepted(
      transactionMeta = TransactionMeta(
        ledgerEffectiveTime = ledgerCreateTime.time,
        workflowId = workflowIdProvider(),
        preparationTime = recordTime.toLf,
        submissionSeed = Update.noOpSeed,
        timeBoundaries = LedgerTimeBoundaries.unconstrained,
        optUsedPackages = None,
        optNodeSeeds = None,
        optByKeyNodes = None,
      ),
      transactionInfo = Update.TransactionAccepted.TransactionInfo(
        LfCommittedTransaction(
          CantonOnly.lfVersionedTransaction(
            nodes = txNodes,
            roots = ImmArray.from(nodeIds.take(txNodes.size)),
          )
        )
      ),
      updateId = randomUpdateId(syncCrypto),
      synchronizerId = synchronizerId,
      repairCounter = repairCounter,
      recordTime = recordTime,
      contractInfos = contractInfos,
    )
  }

  private def writeContractsAddedEvents(
      synchronizerId: SynchronizerId,
      contractsAdded: Seq[(TimeOfRepair, (CreationTime.CreatedAt, Seq[(ContractToAdd, Long)]))],
      workflowIds: Iterator[Option[LfWorkflowId]],
      repairIndexer: FutureQueue[RepairUpdate],
  )(implicit traceContext: TraceContext): FutureUnlessShutdown[Unit] =
    FutureUnlessShutdown.outcomeF(MonadUtil.sequentialTraverse_(contractsAdded) {
      case (timeOfChange, (timestamp, contractsToAdd)) =>
        // not waiting for Update.persisted, since CommitRepair anyway will be waited for at the end
        repairIndexer
          .offer(
            prepareAddedEvents(
              synchronizerId = synchronizerId,
              recordTime = timeOfChange.timestamp,
              repairCounter = timeOfChange.repairCounter,
              ledgerCreateTime = timestamp,
              contractsAdded = contractsToAdd,
              workflowIdProvider = () => workflowIds.next(),
            )
          )
          .map(_ => ())
    })

  /** Build assignment events to be offered to the indexer to signal imported contracts. Assigned
    * event allows to preserve reassignment counters and shall be used until we have a dedicated
    * `add` event in the indexer.
    * @return
    *   - None if `contractsAdded` is empty
    *   - Some(Left) if contract authentication data cannot be computed
    *   - Some(Right) otherwise
    */
  private def prepareAssignedEvent(
      synchronizerId: SynchronizerId,
      recordTime: CantonTimestamp,
      repairCounter: RepairCounter,
      contractsAdded: Seq[(ContractToAdd, Long)],
      workflowIdProvider: () => Option[LfWorkflowId],
  )(implicit traceContext: TraceContext): Option[RepairReassignmentAccepted] = {

    // Assignments set the same source and target synchronizerIds since they are artificial
    // assigns without an actual target synchronizer (this is used for adding a contract)
    val reassignmentId = ReassignmentId(
      Source(synchronizerId),
      Target(synchronizerId),
      unassignmentTs = recordTime,
      contractIdCounters = contractsAdded.view.map { case (c, _internalContractId) =>
        (c.cid, c.reassignmentCounter)
      },
    )

    val assigns = NonEmpty.from[Seq[Reassignment]](
      contractsAdded.zipWithIndex
        .map { case ((c, internalContractId), nodeId) =>
          Reassignment.Assign(
            reassignmentCounter = c.reassignmentCounter.unwrap,
            nodeId = nodeId,
            persistedContractInstance = PersistedContractInstance(
              internalContractId = internalContractId,
              inst = c.contract.inst,
            ),
          )
        }
    )

    assigns.map { assignsNE =>
      RepairReassignmentAccepted(
        workflowId = workflowIdProvider(),
        updateId = randomUpdateId(syncCrypto),
        reassignmentInfo = ReassignmentInfo(
          sourceSynchronizer = Source(synchronizerId),
          targetSynchronizer = Target(synchronizerId),
          submitter = None,
          reassignmentId = reassignmentId,
          isReassigningParticipant = false,
        ),
        reassignment = Reassignment.Batch(assignsNE),
        repairCounter = repairCounter,
        recordTime = recordTime,
        synchronizerId = synchronizerId,
      )
    }
  }

  private def publishAssignedEvents(
      synchronizerId: SynchronizerId,
      recordTime: CantonTimestamp,
      contractsAdded: Seq[(TimeOfRepair, (CreationTime.CreatedAt, Seq[(ContractToAdd, Long)]))],
      workflowIds: Iterator[Option[LfWorkflowId]],
      repairIndexer: FutureQueue[RepairUpdate],
  )(implicit traceContext: TraceContext): Future[Unit] =
    MonadUtil
      .sequentialTraverse_(
        contractsAdded.flatMap { case (timeOfChange, (timestamp, contractsToAdd)) =>
          prepareAssignedEvent(
            synchronizerId,
            recordTime,
            timeOfChange.repairCounter,
            contractsToAdd,
            () => workflowIds.next(),
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
