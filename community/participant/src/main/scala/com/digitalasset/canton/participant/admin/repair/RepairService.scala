// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.repair

import cats.Eval
import cats.data.{EitherT, OptionT}
import cats.syntax.either.*
import cats.syntax.foldable.*
import cats.syntax.functorFilter.*
import cats.syntax.parallel.*
import cats.syntax.traverse.*
import com.daml.ledger.api.v1.value.{Identifier, Record}
import com.daml.lf.CantonOnly
import com.daml.lf.data.{Bytes, ImmArray, Ref}
import com.daml.lf.value.Value
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.*
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.{Salt, SyncCryptoApiProvider}
import com.digitalasset.canton.data.{CantonTimestamp, RepairContract}
import com.digitalasset.canton.ledger.api.validation.{
  FieldValidator as LedgerApiFieldValidations,
  StricterValueValidator as LedgerApiValueValidator,
}
import com.digitalasset.canton.ledger.participant.state.v2.TransactionMeta
import com.digitalasset.canton.lifecycle.{
  FlagCloseable,
  FutureUnlessShutdown,
  HasCloseContext,
  Lifecycle,
}
import com.digitalasset.canton.logging.{
  HasLoggerName,
  NamedLoggerFactory,
  NamedLogging,
  NamedLoggingContext,
}
import com.digitalasset.canton.participant.admin.PackageDependencyResolver
import com.digitalasset.canton.participant.domain.DomainAliasManager
import com.digitalasset.canton.participant.event.RecordTime
import com.digitalasset.canton.participant.protocol.RequestJournal.RequestState
import com.digitalasset.canton.participant.store.*
import com.digitalasset.canton.participant.sync.{
  LedgerSyncEvent,
  SyncDomainPersistentStateManager,
  TimestampedEvent,
}
import com.digitalasset.canton.participant.util.DAMLe.ContractWithMetadata
import com.digitalasset.canton.participant.util.{DAMLe, TimeOfChange}
import com.digitalasset.canton.participant.{ParticipantNodeParameters, RichRequestCounter}
import com.digitalasset.canton.platform.participant.util.LfEngineToApi
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.resource.TransactionalStoreUpdate
import com.digitalasset.canton.store.{
  CursorPrehead,
  IndexedDomain,
  IndexedStringStore,
  SequencedEventStore,
}
import com.digitalasset.canton.topology.{DomainId, ParticipantId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.ShowUtil.*
import com.digitalasset.canton.util.*
import com.digitalasset.canton.util.retry.RetryUtil.AllExnRetryable

import java.time.Instant
import scala.Ordered.orderingToOrdered
import scala.collection.immutable.HashMap
import scala.concurrent.duration.DurationInt
import scala.concurrent.{ExecutionContext, Future}

/** Implements the repair commands.
  * Repair commands work only if the participant has disconnected from the affected domains.
  * Every individual repair commands is executed transactionally, i.e., either all its effects are applied or none.
  * This is achieved in the same way as for request processing:
  * <ol>
  *   <li>A request counter is allocated for the repair request (namely the clean request head) and
  *     marked as [[com.digitalasset.canton.participant.protocol.RequestJournal.RequestState.Pending]].
  *     The repair request's timestamp is the timestamp where processing starts again upon reconnection to the domain.</li>
  *   <li>All repair effects are persisted to the stores using the repair request counter.</li>
  *   <li>The clean request prehead is advanced to the repair request counter. This commits the changes.
  *     If multiple domains are involved, transactionality is ensured
  *     via the [[com.digitalasset.canton.resource.TransactionalStoreUpdate]] mechanism.</li>
  * </ol>
  * If anything goes wrong before advancing the clean request prehead,
  * the already persisted data will be cleaned up upon the next repair request or reconnection to the domain.
  */
final class RepairService(
    participantId: ParticipantId,
    syncCrypto: SyncCryptoApiProvider,
    packageDependencyResolver: PackageDependencyResolver,
    damle: DAMLe,
    multiDomainEventLog: Eval[MultiDomainEventLog],
    syncDomainPersistentStateManager: SyncDomainPersistentStateManager,
    aliasManager: DomainAliasManager,
    parameters: ParticipantNodeParameters,
    indexedStringStore: IndexedStringStore,
    isConnected: DomainId => Boolean,
    futureSupervisor: FutureSupervisor,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends NamedLogging
    with FlagCloseable
    with HasCloseContext {

  override protected def timeouts: ProcessingTimeout = parameters.processingTimeouts

  // Ensure only one repair runs at a time. This ensures concurrent activity among repair operations does
  // not corrupt state.
  private val executionQueue = new SimpleExecutionQueue(
    "repair-service-queue",
    futureSupervisor,
    timeouts,
    loggerFactory,
  )

  private def aliasToUnconnectedDomainId(alias: DomainAlias): EitherT[Future, String, DomainId] =
    for {
      domainId <- EitherT.fromEither[Future](
        aliasManager.domainIdForAlias(alias).toRight(s"Could not find $alias")
      )
      _ <- domainNotConnected(domainId)
    } yield domainId
  private def domainNotConnected(domainId: DomainId): EitherT[Future, String, Unit] = EitherT.cond(
    !isConnected(domainId),
    (),
    s"Participant is still connected to domain $domainId",
  )

  /** Participant repair utility for manually adding contracts to a domain in an offline fashion.
    *
    * @param domain             alias of domain to add contracts to. The domain needs to be configured, but disconnected
    *                           to prevent race conditions.
    * @param contractsToAdd     contracts to add. Relevant pieces of each contract: create-arguments (LfContractInst),
    *                           template-id (LfContractInst), contractId, ledgerCreateTime, salt (to be added to
    *                           SerializableContract), and witnesses, SerializableContract.metadata is only validated,
    *                           but otherwise ignored as stakeholder and signatories can be recomputed from contracts.
    * @param ignoreAlreadyAdded whether to ignore and skip over contracts already added/present in the domain. Setting
    *                           this to true (at least on retries) enables writing idempotent repair scripts.
    * @param ignoreStakeholderCheck do not check for stakeholder presence for the given parties
    */
  def addContracts(
      domain: DomainAlias,
      contractsToAdd: Seq[RepairContract],
      ignoreAlreadyAdded: Boolean,
      ignoreStakeholderCheck: Boolean,
  )(implicit traceContext: TraceContext): Either[String, Unit] = {
    logger.info(
      s"Adding ${contractsToAdd.length} contracts to domain ${domain} with ignoreAlreadyAdded=${ignoreAlreadyAdded} and ignoreStakeholderCheck=${ignoreStakeholderCheck}"
    )
    lockAndAwaitEitherT(
      "repair.add", {
        for {
          // Ensure domain is configured but not connected to avoid race conditions.
          domainId <- aliasToUnconnectedDomainId(domain)
          _ <- EitherT.cond[Future](contractsToAdd.nonEmpty, (), "No contracts to add specified")

          repair <- initRepairRequestAndVerifyPreconditions(domainId)

          // All referenced templates known and vetted
          _packagesVetted <- contractsToAdd
            .map(_.contract.rawContractInstance.contractInstance.unversioned.template.packageId)
            .toSet
            .toList
            .parTraverse_(packageVetted)

          _uniqueKeysWithHostedMaintainerInContractsToAdd <- EitherTUtil.ifThenET(
            repair.domainParameters.uniqueContractKeys
          ) {
            val keysWithContractIdsF = contractsToAdd
              .parTraverseFilter {
                case RepairContract(
                      contract,
                      _witnesses,
                      _transferCounter,
                    ) =>
                  // Only check for duplicates where the participant hosts a maintainer
                  getKeyIfOneMaintainerIsLocal(
                    repair.topologySnapshot,
                    contract.metadata.maybeKeyWithMaintainers,
                    participantId,
                  ).map { lfKeyO =>
                    lfKeyO.flatMap(_ => contract.metadata.maybeKeyWithMaintainers).map {
                      keyWithMaintainers =>
                        keyWithMaintainers.globalKey -> contract.contractId
                    }
                  }
              }
              .map(x => x.groupBy { case (globalKey, _) => globalKey })
            EitherT(keysWithContractIdsF.map { keysWithContractIds =>
              val duplicates = keysWithContractIds.mapFilter { keyCoids =>
                if (keyCoids.lengthCompare(1) > 0) Some(keyCoids.map(_._2)) else None
              }
              Either.cond(
                duplicates.isEmpty,
                (),
                log(show"Cannot add multiple contracts for the same key(s): $duplicates"),
              )
            })
          }

          // Note the following purposely fails if any contract fails which results in not all contracts being processed.
          contractsToPublishUpstreamO <- MonadUtil.sequentialTraverse(contractsToAdd)(
            addContract(repair, ignoreAlreadyAdded, ignoreStakeholderCheck)(
              _,
              repair.tryOneTimeOfChange,
            )
          )
          contractsAndSaltsToPublishUpstream = contractsToPublishUpstreamO.view.flatten

          (contractsToPublishUpstream, contractIdSalts) =
            contractsAndSaltsToPublishUpstream.map { case (cId, (metadata, saltO)) =>
              (cId -> metadata) -> (cId -> saltO)
            }.unzip

          _ = logger.debug(s"Publishing ${contractsToPublishUpstream.size} added contracts")

          contractMetadata = contractIdSalts.collect { case (cId, Some(salt)) =>
            val driverContractMetadataBytes =
              DriverContractMetadata(salt).toLfBytes(repair.domainParameters.protocolVersion)
            cId -> driverContractMetadataBytes
          }.toMap

          // Publish added contracts upstream as created via the ledger api.
          _ <- EitherT.right(
            scheduleUpstreamEventPublishUponDomainStart(
              contractsToPublishUpstream.toList,
              contractMetadata,
              repair,
              repair.tryOneRequestCounter,
            ) { case (contractId, contractWithMetadata) =>
              LfNodeCreate(
                coid = contractId,
                templateId = contractWithMetadata.instance.unversioned.template,
                arg = contractWithMetadata.instance.unversioned.arg,
                agreementText = "",
                signatories = contractWithMetadata.signatories,
                stakeholders = contractWithMetadata.stakeholders,
                keyOpt = contractWithMetadata.keyWithMaintainers,
                version = contractWithMetadata.instance.version,
              )
            }
          )

          // If all has gone well, bump the clean head, effectively committing the changes to the domain.
          _ <- commitRepairs(repair)
        } yield ()
      },
    )
  }

  /** Participant repair utility for manually purging (archiving) contracts in an offline fashion.
    *
    * @param domain              alias of domain to purge contracts from. The domain needs to be configured, but
    *                            disconnected to prevent race conditions.
    * @param contractIds         lf contract ids of contracts to purge
    * @param ignoreAlreadyPurged whether to ignore already purged contracts.
    */
  def purgeContracts(
      domain: DomainAlias,
      contractIds: NonEmpty[Seq[LfContractId]],
      ignoreAlreadyPurged: Boolean,
  )(implicit traceContext: TraceContext): Either[String, Unit] = {
    logger.info(
      s"Purging ${contractIds.length} contracts from $domain with ignoreAlreadyPurged=$ignoreAlreadyPurged"
    )
    lockAndAwaitEitherT(
      "repair.purge", {
        for {
          domainId <- aliasToUnconnectedDomainId(domain)
          repair <- initRepairRequestAndVerifyPreconditions(domainId)

          // Note the following purposely fails if any contract fails which results in not all contracts being processed.
          contractsToPublishUpstream <- MonadUtil
            .sequentialTraverse(contractIds)(purgeContract(repair, ignoreAlreadyPurged))
            .map(_.collect { case Some(x) => x })

          // Publish purged contracts upstream as archived via the ledger api.
          _ <- EitherT.right(
            scheduleUpstreamEventPublishUponDomainStart(
              contracts = contractsToPublishUpstream,
              // Only exercise nodes are created here, so no need to forward contract metadata
              driverContractMetadata = Map.empty,
              repair = repair,
              requestCounter = repair.tryOneRequestCounter,
            )(contract =>
              LfNodeExercises(
                targetCoid = contract.contractId,
                templateId = contract.rawContractInstance.contractInstance.unversioned.template,
                interfaceId = None,
                choiceId = Ref.ChoiceName.assertFromString(s"Archive"),
                consuming = true,
                actingParties = contract.metadata.signatories,
                chosenValue = contract.rawContractInstance.contractInstance.unversioned.arg,
                stakeholders = contract.metadata.stakeholders,
                signatories = contract.metadata.signatories,
                choiceObservers =
                  Set.empty[LfPartyId], // default archive choice has no choice observers
                choiceAuthorizers = None, // default (signatories + actingParties)
                children = ImmArray.empty[LfNodeId],
                exerciseResult = Some(Value.ValueNone),
                // Not setting the contract key as the indexer deletes contract keys along with contracts.
                // If the contract keys were needed, we'd have to reinterpret the contract to look up the key.
                keyOpt = None,
                byKey = false,
                version = contract.rawContractInstance.contractInstance.version,
              )
            )
          )

          // If all has gone well, bump the clean head, effectively committing the changes to the domain.
          _ <- commitRepairs(repair)

        } yield ()
      },
    )
  }

  /** Participant repair utility for manually moving contracts from a source domain to a target domain in an offline
    * fashion.
    *
    * @param contractIds    ids of contracts to move that reside in the sourceDomain
    * @param sourceDomain alias of source domain from which to move contracts
    * @param targetDomain alias of target domain to which to move contracts
    * @param skipInactive   whether to only move contracts that are active in the source domain
    * @param batchSize      how big the batches should be used during the migration
    */
  def changeDomainAwait(
      contractIds: Seq[LfContractId],
      sourceDomain: DomainAlias,
      targetDomain: DomainAlias,
      skipInactive: Boolean,
      batchSize: PositiveInt,
  )(implicit traceContext: TraceContext): Either[String, Unit] = {
    logger.info(
      s"Change domain request for ${contractIds.length} contracts from ${sourceDomain} to ${targetDomain} with skipInactive=${skipInactive}"
    )
    lockAndAwaitEitherT(
      "repair.change_domain", {
        for {
          sourceDomainId <- aliasToUnconnectedDomainId(sourceDomain)
          targetDomainId <- aliasToUnconnectedDomainId(targetDomain)
          _ <- changeDomain(
            contractIds,
            sourceDomainId,
            targetDomainId,
            skipInactive,
            batchSize,
          )
        } yield ()
      },
    )
  }

  /** Change the association of a contract from one domain to another
    *
    * This function here allows us to manually insert a transfer out / in into the respective
    * journals in order to move a contract from one domain to another. The procedure will result in
    * a consistent state if and only if all the counter parties run the same command. Failure to do so,
    * will results in participants reporting errors and possibly break.
    *
    * @param skipInactive if true, then the migration will skip contracts in the contractId list that are inactive
    */
  def changeDomain(
      contractIds: Seq[LfContractId],
      sourceDomainId: DomainId,
      targetDomainId: DomainId,
      skipInactive: Boolean,
      batchSize: PositiveInt,
  )(implicit traceContext: TraceContext): EitherT[Future, String, Unit] =
    PositiveInt
      .create(contractIds.size)
      .map { numberOfContracts =>
        for {
          _ <- EitherTUtil.condUnitET[Future](
            sourceDomainId != targetDomainId,
            "Source must differ from target domain!",
          )
          repairSource <- initRepairRequestAndVerifyPreconditions(sourceDomainId, numberOfContracts)
          repairTarget <- initRepairRequestAndVerifyPreconditions(targetDomainId, numberOfContracts)

          migration =
            contractIds
              .zip(repairSource.timesOfChange)
              .zip(repairTarget.timesOfChange)
              .map { case (((contractId, sourceToc), targetToc)) =>
                MigrateContracts.Data(contractId, sourceToc, targetToc)
              }

          // Note the following purposely fails if any contract fails which results in not all contracts being processed.
          _ <- migrateContractsBatched(
            migration,
            repairSource,
            repairTarget,
            skipInactive,
            batchSize,
          )

          // If all has gone well, bump the clean head to both domains transactionally
          _ <- commitRepairs(repairTarget, repairSource)
        } yield ()
      }
      .getOrElse(EitherT.rightT(()))

  def ignoreEvents(domain: DomainId, from: SequencerCounter, to: SequencerCounter, force: Boolean)(
      implicit traceContext: TraceContext
  ): Either[String, Unit] = {
    logger.info(s"Ignoring sequenced events from $from to $to (force = $force).")
    lockAndAwaitEitherT(
      "repair.skip_messages",
      for {
        _ <- domainNotConnected(domain)
        _ <- performIfRangeSuitableForIgnoreOperations(domain, from, force)(
          _.ignoreEvents(from, to).leftMap(_.toString)
        )
      } yield (),
    )
  }

  private def performIfRangeSuitableForIgnoreOperations[T](
      domain: DomainId,
      from: SequencerCounter,
      force: Boolean,
  )(
      action: SequencedEventStore => EitherT[Future, String, T]
  )(implicit traceContext: TraceContext): EitherT[Future, String, T] =
    for {
      persistentState <- EitherT.fromEither[Future](lookUpDomainPersistence(domain, domain.show))
      indexedDomain <- EitherT.right(IndexedDomain.indexed(indexedStringStore)(domain))
      startingPoints <- EitherT.right(
        SyncDomainEphemeralStateFactory.startingPoints(
          indexedDomain,
          persistentState.requestJournalStore,
          persistentState.sequencerCounterTrackerStore,
          persistentState.sequencedEventStore,
          multiDomainEventLog.value,
        )
      )

      _ <- EitherTUtil
        .condUnitET[Future](
          force || startingPoints.rewoundSequencerCounterPrehead.forall(_.counter <= from),
          show"Unable to modify events between $from (inclusive) and ${startingPoints.rewoundSequencerCounterPrehead.showValue} (exclusive), " +
            """as they won't be read from the sequencer client. Enable "force" to modify them nevertheless.""",
        )

      _ <- EitherTUtil
        .condUnitET[Future](
          force || startingPoints.processing.nextSequencerCounter <= from,
          show"Unable to modify events between $from (inclusive) and ${startingPoints.processing.nextSequencerCounter} (exclusive), " +
            """as they have already been processed. Enable "force" to modify them nevertheless.""",
        )
      res <- action(persistentState.sequencedEventStore)
    } yield res

  def unignoreEvents(
      domain: DomainId,
      from: SequencerCounter,
      to: SequencerCounter,
      force: Boolean,
  )(implicit traceContext: TraceContext): Either[String, Unit] = {
    logger.info(s"Unignoring sequenced events from $from to $to (force = $force).")
    lockAndAwaitEitherT(
      "repair.unskip_messages",
      for {
        _ <- domainNotConnected(domain)
        _ <- performIfRangeSuitableForIgnoreOperations(domain, from, force)(sequencedEventStore =>
          sequencedEventStore.unignoreEvents(from, to).leftMap(_.toString)
        )
      } yield (),
    )
  }

  private def addContract(
      repair: RepairRequest,
      ignoreAlreadyAdded: Boolean,
      ignoreStakeholderCheck: Boolean,
  )(
      contractToAdd: RepairContract,
      timeOfChange: TimeOfChange,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, String, Option[(LfContractId, (ContractWithMetadata, Option[Salt]))]] = {
    val topologySnapshot = repair.topologySnapshot

    def runStakeholderCheck[L](check: => EitherT[Future, L, Unit]): EitherT[Future, L, Unit] =
      if (ignoreStakeholderCheck) EitherT.rightT(()) else check

    // TODO(#4001) - performance of point-db lookups will be slow, add batching (also to purgeContract and moveContract)
    def persistCreation(
        cid: LfContractId,
        transferCounter: TransferCounterO,
    ): EitherT[Future, String, Unit] = {
      repair.domainPersistence.activeContractStore
        .markContractActive(
          cid -> transferCounter,
          timeOfChange,
        )
        .toEitherTWithNonaborts
        .leftMap(e => log(s"Failed to create contract $cid in ActiveContractStore: $e"))
    }

    def useComputedContractAndMetadata(
        inputContract: SerializableContract,
        computed: ContractWithMetadata,
    ): EitherT[Future, String, SerializableContract] =
      EitherT.fromEither[Future](
        for {
          rawContractInstance <- SerializableRawContractInstance
            .create(computed.instance, computed.agreementText)
            .leftMap(err =>
              log(s"Failed to serialize contract ${inputContract.contractId}: ${err.errorMessage}")
            )
        } yield inputContract.copy(
          metadata = computed.metadataWithGlobalKey,
          rawContractInstance = rawContractInstance,
        )
      )

    val contract = contractToAdd.contract
    val witnesses = contractToAdd.witnesses.map(_.toLf)
    val lfContractInstance = contract.rawContractInstance.contractInstance
    for {
      // Able to recompute contract signatories and stakeholders (and sanity check contractToAdd metadata otherwise
      // ignored matches real metadata)
      computedContractWithMetadata <- damle
        .contractWithMetadata(
          lfContractInstance,
          contract.metadata.signatories,
        )
        .leftMap(e => log(s"Failed to compute contract ${contract.contractId} metadata: ${e}"))
      _warnedOnMetadataMismatch = {
        if (contract.metadata.signatories != computedContractWithMetadata.signatories) {
          log(
            s"Contract ${contract.contractId} metadata signatories ${contract.metadata.signatories} differ from actual signatories ${computedContractWithMetadata.signatories}"
          ).discard
        }
        if (contract.metadata.stakeholders != computedContractWithMetadata.stakeholders) {
          log(
            s"Contract ${contract.contractId} metadata stakeholders ${contract.metadata.stakeholders} differ from actual stakeholders ${computedContractWithMetadata.stakeholders}"
          ).discard
        }
      }
      _warnOnEmptyMaintainers <- EitherT.cond[Future](
        !computedContractWithMetadata.keyWithMaintainers.exists(_.maintainers.isEmpty),
        (),
        log(s"Contract ${contract.contractId} has key without maintainers."),
      )

      // Witnesses all known locally.
      _witnessesKnownLocally <- witnesses.toList.parTraverse_ { p =>
        EitherT(hostsParty(topologySnapshot, participantId)(p).map { hosted =>
          EitherUtil.condUnitE(
            hosted,
            log(s"Witness ${p} not active on domain ${repair.domainAlias} and local participant"),
          )
        })
      }

      // At least one stakeholder is hosted locally if no witnesses are defined
      _localStakeholderOrWitnesses <-
        runStakeholderCheck(
          EitherT(
            computedContractWithMetadata.stakeholders.toList
              .findM(hostsParty(topologySnapshot, participantId))
              .map(_.isDefined)
              .map { oneStakeholderIsLocal =>
                EitherUtil.condUnitE(
                  witnesses.nonEmpty || oneStakeholderIsLocal,
                  log(
                    s"Contract ${contract.contractId} has no local stakeholders ${computedContractWithMetadata.stakeholders} and no witnesses defined"
                  ),
                )
              }
          )
        )

      // All stakeholders exist on the domain
      _ <- runStakeholderCheck(
        topologySnapshot
          .allHaveActiveParticipants(computedContractWithMetadata.stakeholders)
          .leftMap { missingStakeholders =>
            log(
              s"Domain ${repair.domainAlias} missing stakeholders ${missingStakeholders} of contract ${contract.contractId}"
            )
          }
      )

      // All witnesses exist on the domain
      _ <- topologySnapshot.allHaveActiveParticipants(witnesses).leftMap { missingWitnesses =>
        log(
          s"Domain ${repair.domainAlias} missing witnesses ${missingWitnesses} of contract ${contract.contractId}"
        )
      }

      // See if contract already exists according to ACS(journal)
      acsStatus <- readContractAcsState(repair.domainPersistence, contract.contractId)
      res <- EitherT.fromEither[Future](acsStatus match {
        case None =>
          Right(
            (true, None)
          ) // TODO(#13573): we should pass the transfer counter in as part of the RepairRequest
        case Some(ActiveContractStore.Active(_)) =>
          Either.cond(
            ignoreAlreadyAdded, {
              logger.debug(s"Skipping contract ${contract.contractId} because it is already active")
              (false, None)
            },
            log(
              s"A contract with ${contract.contractId} is already active. Set ignoreAlreadyAdded = true to skip active contracts."
            ),
          )
        case Some(ActiveContractStore.Archived) =>
          Left(
            log(
              s"Cannot add previously archived contract ${contract.contractId} as archived contracts cannot become active."
            )
          )
        case Some(ActiveContractStore.TransferredAway(targetDomain, transferCounter)) =>
          log(
            s"Marking contract ${contract.contractId} previously transferred-out to ${targetDomain} as " +
              s"transferred-in from ${targetDomain} (even though contract may have been transferred to yet another domain since)."
          ).discard
          Right(
            (
              true,
              Some(
                SourceDomainId(targetDomain.unwrap) -> transferCounter.flatMap(_.increment.toOption)
              ),
            )
          ) // TODO(#13573): we should pass the transfer counter in as part of the RepairRequest
      })
      (needToAddContract, transferringFromAndTransferCounter) = res

      keyOfHostedMaintainerO <- {
        EitherT.right(
          getKeyIfOneMaintainerIsLocal(
            topologySnapshot,
            contract.metadata.maybeKeyWithMaintainers,
            participantId,
          )
        )
      }

      keyToAssignO: EitherT[Future, String, Option[LfGlobalKey]] =
        if (repair.domainParameters.uniqueContractKeys) {
          keyOfHostedMaintainerO
            .traverse[EitherT[Future, String, *], Option[LfGlobalKey]] { key =>
              EitherT(repair.domainPersistence.contractKeyJournal.fetchState(key).map {
                case None => Either.right(Some(key))
                case Some(state) =>
                  state.status match {
                    case ContractKeyJournal.Unassigned => Either.right(Some(key))
                    case ContractKeyJournal.Assigned =>
                      // The key being already assigned is OK if the key is already assigned to the contract.
                      // Then we know that ignoreAlreadyAdded is true.
                      val ok = acsStatus.exists(_.isActive)
                      Either.cond(
                        ok,
                        None,
                        log(show"Key $key is already assigned to a different contract"),
                      )
                  }
              })
            }
            .map(_.flattenOption)
        } else EitherT.pure[Future, String](None)
      keyToAssignO <- keyToAssignO

      contractToPersistAndPublishUpstream <- useComputedContractAndMetadata(
        contract,
        computedContractWithMetadata,
      )

      // Persist contract if it does not already exist. Make sure to use computed rather than input metadata.
      _createdContract <-
        if (needToAddContract)
          persistContract(repair, contractToPersistAndPublishUpstream)
        else {
          for {
            // Ensure that contracts in source and target domains agree.
            contractAlreadyThere <- readContract(
              repair,
              contractToPersistAndPublishUpstream.contractId,
            ).leftMap(log)
            _ <- EitherTUtil.condUnitET[Future](
              contractAlreadyThere == contractToPersistAndPublishUpstream,
              log(
                s"Contract ${contract.contractId} exists in domain, but does not match with contract being added. "
                  + s"Existing contract is ${contractAlreadyThere} while contract supposed to be added ${contractToPersistAndPublishUpstream}"
              ),
            )
          } yield ()
        }

      _persistedInCkj <- keyToAssignO.traverse_ { key =>
        repair.domainPersistence.contractKeyJournal
          .addKeyStateUpdates(Map(key -> (ContractKeyJournal.Assigned, timeOfChange)))
          .leftMap(err => log(s"Error while persisting key state updates: $err"))
      }

      _persistedInAcs <- EitherTUtil.ifThenET(needToAddContract)(
        transferringFromAndTransferCounter.fold(
          persistCreation(
            contractToPersistAndPublishUpstream.contractId,
            contractToAdd.transferCounter,
          )
        ) { case (sourceDomainId, transferCounter) =>
          persistTransferIn(
            repair,
            sourceDomainId,
            contractToPersistAndPublishUpstream.contractId,
            transferCounter,
            timeOfChange,
          )
        }
      )
    } yield
      if (needToAddContract)
        Some(contract.contractId -> (computedContractWithMetadata -> contract.contractSalt))
      else None
  }

  private def purgeContract(repair: RepairRequest, ignoreAlreadyPurged: Boolean)(
      cid: LfContractId
  )(implicit traceContext: TraceContext): EitherT[Future, String, Option[SerializableContract]] = {
    val timeOfChange = repair.tryOneTimeOfChange
    for {
      acsStatus <- readContractAcsState(repair.domainPersistence, cid)

      contractO <- EitherTUtil.fromFuture(
        repair.domainPersistence.contractStore.lookupContract(cid).value,
        t => log(s"Failed to look up contract ${cid} in domain ${repair.domainAlias}", t),
      )
      // Not checking that the participant hosts a stakeholder as we might be cleaning up contracts
      // on behalf of stakeholders no longer around.
      contractToArchiveInEvent <- acsStatus match {
        case None =>
          EitherT.cond[Future](
            ignoreAlreadyPurged,
            None,
            log(
              s"Contract $cid does not exist in domain ${repair.domainAlias} and cannot be purged. Set ignoreAlreadyPurged = true to skip non-existing contracts."
            ),
          )
        case Some(ActiveContractStore.Active(_)) =>
          for {
            contract <- EitherT
              .fromOption[Future](
                contractO,
                log(show"Active contract $cid not found in contract store"),
              )

            keyOfHostedMaintainerO <- EitherT.liftF(
              if (repair.domainParameters.uniqueContractKeys)
                getKeyIfOneMaintainerIsLocal(
                  repair.topologySnapshot,
                  contract.metadata.maybeKeyWithMaintainers,
                  participantId,
                )
              else Future.successful(None)
            )

            _ <- keyOfHostedMaintainerO.traverse_ { key =>
              repair.domainPersistence.contractKeyJournal
                .addKeyStateUpdates(Map(key -> (ContractKeyJournal.Unassigned, timeOfChange)))
                .leftMap(err => log(s"Error while persisting key state updates: $err"))
            }
            _ <- persistArchival(repair, timeOfChange)(cid)
          } yield {
            logger.info(
              s"purged contract $cid at repair request ${repair.tryOneRequestCounter} at ${repair.timestamp}"
            )
            contractO
          }
        case Some(ActiveContractStore.Archived) =>
          EitherT.cond[Future](
            ignoreAlreadyPurged,
            None,
            log(
              s"Contract $cid is already archived in domain ${repair.domainAlias} and cannot be purged. Set ignoreAlreadyPurged = true to skip archived contracts."
            ),
          )
        case Some(ActiveContractStore.TransferredAway(targetDomain, transferCounter)) =>
          log(
            s"Purging contract ${cid} previously marked as transferred away to ${targetDomain}. " +
              s"Marking contract as transferred-in from ${targetDomain} (even though contract may have since been transferred to yet another domain) and subsequently as archived."
          ).discard
          for {
            newTransferCounter <- EitherT.fromEither[Future](transferCounter.traverse(_.increment))
            _unit <- persistTransferInAndArchival(repair, SourceDomainId(targetDomain.unwrap))(
              cid,
              newTransferCounter,
              timeOfChange,
            )
          } yield contractO
      }
    } yield contractToArchiveInEvent
  }

  private def migrateContractsBatched(
      contractIds: Seq[MigrateContracts.Data[LfContractId]],
      repairSource: RepairRequest,
      repairTarget: RepairRequest,
      skipInactive: Boolean,
      batchSize: PositiveInt,
  )(implicit traceContext: TraceContext): EitherT[Future, String, Unit] =
    MonadUtil
      .batchedSequentialTraverse(parameters.maxDbConnections * PositiveInt.two, batchSize)(
        contractIds
      )(
        MigrateContracts(
          _,
          repairSource,
          repairTarget,
          skipInactive,
          participantId,
          syncCrypto,
          loggerFactory,
        )
          .map(_ => Seq[Unit]())
      )
      .map(_ => ())

  private def persistContract(repair: RepairRequest, contract: SerializableContract)(implicit
      traceContext: TraceContext
  ): EitherT[Future, String, Unit] = {

    def fromFuture[T](f: Future[T], errorMessage: => String): EitherT[Future, String, T] =
      EitherTUtil.fromFuture(
        f,
        t => log(s"$errorMessage: ${ErrorUtil.messageWithStacktrace(t)}"),
      )

    for {
      // Write contract if it does not already exist.
      alreadyExistingContract <- fromFuture(
        repair.domainPersistence.contractStore.lookupContract(contract.contractId).value,
        s"Failed to check if contract ${contract.contractId} already exists in ContractStore",
      )

      _createdContract <- alreadyExistingContract.fold(
        fromFuture(
          repair.domainPersistence.contractStore
            .storeCreatedContract(repair.tryOneRequestCounter, repair.transactionId, contract),
          s"Failed to store contract ${contract.contractId} in ContractStore",
        )
      )(storedContract =>
        EitherTUtil.condUnitET[Future](
          storedContract == contract,
          s"Contract ${contract.contractId} already exists in the contract store, but differs from contract to be created. Contract to be created ${contract} versus existing contract ${storedContract}.",
        )
      )

    } yield ()
  }

  private def persistTransferIn(
      repair: RepairRequest,
      sourceDomain: SourceDomainId,
      cid: LfContractId,
      transferCounter: TransferCounterO,
      timeOfChange: TimeOfChange,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, String, Unit] =
    repair.domainPersistence.activeContractStore
      .transferInContract(cid, timeOfChange, sourceDomain, transferCounter)
      .toEitherTWithNonaborts
      .leftMap(e => log(s"Failed to transfer in contract ${cid} in ActiveContractStore: ${e}"))

  /*
  We do not store transfer counters for archivals in the repair service,
  given that we do not store them as part of normal transaction processing either.
  The latter is due to the fact that we do not know the correct transfer counters
  at the time we persist the deactivation in the ACS.
   */
  private def persistArchival(
      repair: RepairRequest,
      timeOfChange: TimeOfChange,
  )(cid: LfContractId)(implicit traceContext: TraceContext): EitherT[Future, String, Unit] =
    repair.domainPersistence.activeContractStore
      .archiveContract(cid, timeOfChange)
      .toEitherT // not turning warnings to errors on behalf of archived contracts, in contract to created contracts
      .leftMap(e => log(s"Failed to mark contract ${cid} as archived: ${e}"))

  private def persistTransferInAndArchival(repair: RepairRequest, sourceDomain: SourceDomainId)(
      cid: LfContractId,
      transferCounter: TransferCounterO,
      timeOfChange: TimeOfChange,
  )(implicit traceContext: TraceContext): EitherT[Future, String, Unit] =
    for {
      _ <- persistTransferIn(repair, sourceDomain, cid, transferCounter, timeOfChange)
      _ <- persistArchival(repair, timeOfChange)(cid)
    } yield ()

  private def scheduleUpstreamEventPublishUponDomainStart[C](
      contracts: Seq[C],
      driverContractMetadata: Map[LfContractId, Bytes],
      repair: RepairRequest,
      requestCounter: RequestCounter,
  )(buildLfNode: C => LfNode)(implicit traceContext: TraceContext): Future[Unit] =
    contracts match {
      case Seq() => Future.unit
      case allContracts =>
        val createNodes = HashMap(
          LazyList
            .from(0)
            .map(LfNodeId(_))
            .zip(allContracts.map(buildLfNode)): _*
        )
        val tx = LfCommittedTransaction(
          CantonOnly.lfVersionedTransaction(
            nodes = createNodes,
            roots = createNodes.keys.to(ImmArray),
          )
        )
        val transactionId = repair.transactionId.tryAsLedgerTransactionId
        def eventFor(hostedWitnesses: List[LfPartyId]) = TimestampedEvent(
          LedgerSyncEvent.TransactionAccepted(
            completionInfoO = None,
            transactionMeta = TransactionMeta(
              ledgerEffectiveTime = repair.timestamp.toLf,
              workflowId = None,
              submissionTime = repair.timestamp.toLf,
              submissionSeed =
                LedgerSyncEvent.noOpSeed, // "fake" transaction - None no longer allowed
              optUsedPackages = None,
              optNodeSeeds = None,
              optByKeyNodes = None,
              optDomainId = Some(repair.domainId),
            ),
            transaction = tx,
            transactionId = transactionId,
            recordTime = repair.timestamp.toLf,
            divulgedContracts = List.empty, // create and plain archive don't involve divulgence
            blindingInfoO = None,
            hostedWitnesses = hostedWitnesses,
            contractMetadata = driverContractMetadata,
          ),
          requestCounter.asLocalOffset,
          None,
        )
        val eventLog = repair.domainPersistence.eventLog
        for {
          hostedWitnesses <- tx.informees.toList.parTraverseFilter(party =>
            hostsParty(repair.topologySnapshot, participantId)(party).map(Option.when(_)(party))
          )
          event = eventFor(hostedWitnesses)
          existingEventO <- eventLog.eventByTransactionId(transactionId).value
          _ <- existingEventO match {
            case None => eventLog.insert(event)
            case Some(existingEvent) if existingEvent.normalized.event == event.normalized.event =>
              logger.info(
                show"Skipping duplicate publication of event at offset ${event.localOffset} with transaction id $transactionId."
              )
              Future.unit
            case Some(existingEvent) =>
              ErrorUtil.internalError(
                new IllegalArgumentException(
                  show"Unable to publish event at offset ${event.localOffset}, " +
                    show"as it has the same transaction id $transactionId as the previous event with offset ${existingEvent.localOffset}."
                )
              )
          }
        } yield ()
    }

  private def packageVetted(
      lfPackageId: LfPackageId
  )(implicit traceContext: TraceContext): EitherT[Future, String, Unit] = {
    for {
      packageDescription <- EitherTUtil.fromFuture(
        packageDependencyResolver.getPackageDescription(lfPackageId),
        t => log(s"Failed to look up package ${lfPackageId}", t),
      )
      _packageVetted <- EitherTUtil
        .condUnitET[Future](
          packageDescription.nonEmpty,
          log(s"Failed to locate package ${lfPackageId}"),
        )
    } yield ()
  }

  /** Allows to wait until clean head has progressed up to a certain timestamp */
  def awaitCleanHeadForTimestamp(
      domainId: DomainId,
      timestamp: CantonTimestamp,
  )(implicit traceContext: TraceContext): EitherT[FutureUnlessShutdown, String, Unit] = {
    def check(
        persistentState: SyncDomainPersistentState,
        indexedDomain: IndexedDomain,
    ): Future[Either[String, Unit]] = {
      SyncDomainEphemeralStateFactory
        .startingPoints(
          indexedDomain,
          persistentState.requestJournalStore,
          persistentState.sequencerCounterTrackerStore,
          persistentState.sequencedEventStore,
          multiDomainEventLog.value,
        )
        .map { startingPoints =>
          if (startingPoints.processing.prenextTimestamp >= timestamp) {
            logger.debug(
              s"Clean head reached ${startingPoints.processing.prenextTimestamp}, clearing ${timestamp}"
            )
            Right(())
          } else {
            logger.debug(
              s"Clean head is still at ${startingPoints.processing.prenextTimestamp} which is not yet ${timestamp}"
            )
            Left(
              s"Clean head is still at ${startingPoints.processing.prenextTimestamp} which is not yet ${timestamp}"
            )
          }
        }
    }
    getPersistentState(domainId)
      .mapK(FutureUnlessShutdown.outcomeK)
      .flatMap { case (persistentState, _, indexedDomain) =>
        EitherT(
          retry
            .Pause(
              logger,
              this,
              retry.Forever,
              50.milliseconds,
              s"awaiting clean-head for=${domainId} at ts=${timestamp}",
            )
            .unlessShutdown(
              FutureUnlessShutdown.outcomeF(check(persistentState, indexedDomain)),
              AllExnRetryable,
            )
        )
      }
  }

  private def requestCounterSequence(
      fromInclusive: RequestCounter,
      length: PositiveInt,
  ): Either[String, NonEmpty[Seq[RequestCounter]]] =
    for {
      rcs <- Seq
        .iterate(fromInclusive.asRight[String], length.value)(_.flatMap(_.increment))
        .sequence
      ne <- NonEmpty.from(rcs).toRight("generated an empty collection with PositiveInt length")
    } yield ne

  private def getPersistentState(
      domainId: DomainId
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, String, (SyncDomainPersistentState, String, IndexedDomain)] = {
    val domainAlias = aliasManager.aliasForDomainId(domainId).fold(domainId.filterString)(_.unwrap)
    for {
      persistentState <- EitherT.fromEither[Future](
        lookUpDomainPersistence(domainId, s"domain ${domainAlias}")
      )
      indexedDomain <- EitherT.right(IndexedDomain.indexed(indexedStringStore)(domainId))
    } yield (persistentState, domainAlias, indexedDomain)
  }

  /** Repair commands are inserted where processing starts again upon reconnection.
    *
    * @param domainId The ID of the domain for which the request is valid
    * @param requestCounters The number of request counters to allocate in order to fulfill the request
    */
  private def initRepairRequestAndVerifyPreconditions(
      domainId: DomainId,
      requestCounters: PositiveInt = PositiveInt.one,
  )(implicit traceContext: TraceContext): EitherT[Future, String, RepairRequest] = {
    for {
      obtainedPersistentState <- getPersistentState(domainId)
      (persistentState, domainAlias, indexedDomain) = obtainedPersistentState
      startingPoints <- EitherTUtil.fromFuture(
        SyncDomainEphemeralStateFactory.startingPoints(
          indexedDomain,
          persistentState.requestJournalStore,
          persistentState.sequencerCounterTrackerStore,
          persistentState.sequencedEventStore,
          multiDomainEventLog.value,
        ),
        t => log(s"Failed to compute starting points", t),
      )
      // The repair request gets inserted at the reprocessing starting point.
      // We use the prenextTimestamp such that a regular request is always the first request for a given timestamp.
      // This is needed for causality tracking, which cannot use a tie breaker on timestamps.
      //
      // If this repair request succeeds, it will advance the clean request prehead to this time of change.
      // That's why it is important that there are no dirty requests before the repair request.
      rcRepair = startingPoints.processing.nextRequestCounter
      tsRepair = startingPoints.processing.prenextTimestamp
      rtRepair = RecordTime.fromTimeOfChange(TimeOfChange(rcRepair, tsRepair))
      _ = logger.debug(s"Starting repair request on $domainId at $rtRepair.")
      _ <- EitherT.cond[Future](
        startingPoints.processingAfterPublished,
        (),
        log(
          s"""Cannot apply a repair command as events have been published up to
             |${startingPoints.eventPublishingNextLocalOffset} offset exclusive
             |and the repair command would be assigned the offset $rcRepair.
             |Reconnect to the domain to reprocess the dirty requests and retry repair afterwards.""".stripMargin
        ),
      )
      _ <- EitherTUtil.fromFuture(
        SyncDomainEphemeralStateFactory.cleanupPersistentState(persistentState, startingPoints),
        t => log(s"Failed to clean up the persistent state", t),
      )
      incrementalAcsSnapshotWatermark <- EitherTUtil.fromFuture(
        persistentState.acsCommitmentStore.runningCommitments.watermark,
        _ => log(s"Failed to obtain watermark from incremental ACS snapshot"),
      )
      _ <- EitherT.cond[Future](
        rtRepair > incrementalAcsSnapshotWatermark,
        (),
        log(
          s"""Cannot apply a repair command as the incremental acs snapshot is already at $incrementalAcsSnapshotWatermark
             |and the repair command would be assigned a record time of $rtRepair.
             |Reconnect to the domain to reprocess dirty requests and retry repair afterwards.""".stripMargin
        ),
      )
      // Make sure that the topology state for the repair timestamp is available.
      // The topology manager does not maintain its own watermark of processed events,
      // so we conservatively approximate this via the clean sequencer counter prehead.
      _ <- startingPoints.rewoundSequencerCounterPrehead match {
        case None =>
          EitherT.cond[Future](
            // Check that this is an empty domain.
            // We don't check the request counter because there may already have been earlier repairs on this domain.
            tsRepair == RepairService.RepairTimestampOnEmptyDomain,
            (),
            log(
              s"""Cannot apply a repair command to $domainId as no events have been completely processed on this non-empty domain.
                   |Reconnect to the domain to initialize the identity management and retry repair afterwards.""".stripMargin
            ),
          )
        case Some(CursorPrehead(_, tsClean)) =>
          EitherT.cond[Future](
            tsClean <= tsRepair,
            (),
            log(
              s"""Cannot apply a repair command at $tsRepair to $domainId
                   |as event processing has completed only up to $tsClean.
                   |Reconnect to the domain to process the locally stored events and retry repair afterwards.""".stripMargin
            ),
          )
      }
      topologyFactory <- syncDomainPersistentStateManager
        .topologyFactoryFor(domainId)
        .toRight("No topology factory for domain")
        .toEitherT[Future]

      topologySnapshot = topologyFactory.createTopologySnapshot(
        tsRepair,
        packageId => packageDependencyResolver.packageDependencies(List(packageId)),
        preferCaching = true,
      )
      domainParameters <- OptionT(persistentState.parameterStore.lastParameters)
        .toRight(log(s"No static domains parameters found for $domainAlias"))
      requestCounters <- EitherT.fromEither[Future](
        requestCounterSequence(rcRepair, requestCounters)
      )
      repair = RepairRequest(
        domainId,
        domainAlias,
        domainParameters,
        randomTransactionId(syncCrypto),
        requestCounters,
        tsRepair,
        RepairContext.tryFromTraceContext,
      )(persistentState, topologySnapshot)

      // Mark the repair request as pending in the request journal store
      _ <- EitherTUtil.fromFuture(
        repair.requestData.parTraverse_(persistentState.requestJournalStore.insert),
        t => log("Failed to insert repair request", t),
      )

    } yield repair
  }

  private def markClean(
      repair: RepairRequest
  )(implicit traceContext: TraceContext): EitherT[Future, String, Unit] = {
    repair.requestCounters.forgetNE
      .parTraverse_(
        repair.domainPersistence.requestJournalStore.replace(
          _,
          repair.timestamp,
          RequestState.Pending,
          RequestState.Clean,
          Some(repair.timestamp),
        )
      )
      .leftMap(t => log(s"Failed to update request journal store on ${repair.domainAlias}: $t"))
  }

  private def commitRepairs(
      repairs: RepairRequest*
  )(implicit traceContext: TraceContext): EitherT[Future, String, Unit] = {
    for {
      _ <- repairs.parTraverse_(markClean)
      _ <- EitherTUtil.fromFuture(
        TransactionalStoreUpdate.execute {
          repairs.map { repair =>
            repair.domainPersistence.requestJournalStore.advancePreheadCleanToTransactionalUpdate(
              CursorPrehead(repair.requestCounters.last1, repair.timestamp)
            )
          }
        },
        t => log("Failed to advance clean request preheads", t),
      )
    } yield ()
  }

  private def readContractAcsState(persistentState: SyncDomainPersistentState, cid: LfContractId)(
      implicit traceContext: TraceContext
  ): EitherT[Future, String, Option[ActiveContractStore.Status]] =
    EitherTUtil.fromFuture(
      persistentState.activeContractStore.fetchState(cid).map(_.map(_.status)),
      e => log(s"Failed to look up contract ${cid} status in ActiveContractStore: ${e}"),
    )

  // Looks up domain persistence erroring if domain is based on in-memory persistence for which repair is not supported.
  private def lookUpDomainPersistence(domainId: DomainId, domainDescription: String)(implicit
      traceContext: TraceContext
  ): Either[String, SyncDomainPersistentState] =
    for {
      dp <- syncDomainPersistentStateManager
        .get(domainId)
        .toRight(log(s"Could not find ${domainDescription}"))
      _ <- Either.cond(
        !dp.isMemory(),
        (),
        log(
          s"${domainDescription} is in memory which is not supported by repair. Use db persistence."
        ),
      )
    } yield dp

  private def lockAndAwaitEitherT[B](description: String, code: => EitherT[Future, String, B])(
      implicit traceContext: TraceContext
  ): Either[String, B] = {
    logger.info(s"Queuing ${description}")
    // repair commands can take an unbounded amount of time
    parameters.processingTimeouts.unbounded.await(description)(
      executionQueue
        .executeE(code, description)
        .value
        .onShutdown(Left(s"$description aborted due to shutdown"))
    )
  }

  private def log(message: String, cause: Throwable)(implicit traceContext: TraceContext): String =
    log(s"$message: ${ErrorUtil.messageWithStacktrace(cause)}")

  private def log(message: String)(implicit traceContext: TraceContext): String = {
    // consider errors user errors and log them on the server side as info:
    logger.info(message)
    message
  }

  override protected def onClosed(): Unit = Lifecycle.close(executionQueue)(logger)

}

object RepairService {

  /** The timestamp to be used for a repair request on a domain without requests */
  val RepairTimestampOnEmptyDomain: CantonTimestamp = CantonTimestamp.MinValue

  object ContractConverter extends HasLoggerName {

    def contractDataToInstance(
        templateId: Identifier,
        createArguments: Record,
        signatories: Set[String],
        observers: Set[String],
        lfContractId: LfContractId,
        ledgerTime: Instant,
        contractSalt: Option[Salt],
    )(implicit namedLoggingContext: NamedLoggingContext): Either[String, SerializableContract] = {
      for {
        template <- LedgerApiFieldValidations.validateIdentifier(templateId).leftMap(_.getMessage)

        argsValue <- LedgerApiValueValidator
          .validateRecord(createArguments)
          .leftMap(e => s"Failed to validate arguments: ${e}")

        argsVersionedValue = LfVersioned(
          protocol.DummyTransactionVersion, // Version is ignored by daml engine upon RepairService.addContract
          argsValue,
        )

        lfContractInst = LfContractInst(template = template, arg = argsVersionedValue)

        /*
         It is fine to set the agreement text to empty because method `addContract` recomputes the agreement text
         and will discard this value.
         */
        serializableRawContractInst <- SerializableRawContractInstance
          .create(lfContractInst, AgreementText.empty)
          .leftMap(_.errorMessage)

        signatoriesAsParties <- signatories.toList.traverse(LfPartyId.fromString).map(_.toSet)
        observersAsParties <- observers.toList.traverse(LfPartyId.fromString).map(_.toSet)

        time <- CantonTimestamp.fromInstant(ledgerTime)
      } yield SerializableContract(
        contractId = lfContractId,
        rawContractInstance = serializableRawContractInst,
        // TODO(#13870): Calculate contract keys from the serializable contract
        metadata = checked(
          ContractMetadata
            .tryCreate(signatoriesAsParties, signatoriesAsParties ++ observersAsParties, None)
        ),
        ledgerCreateTime = time,
        contractSalt = contractSalt,
      )
    }

    def contractInstanceToData(
        contract: SerializableContract
    ): Either[
      String,
      (Identifier, Record, Set[String], Set[String], LfContractId, Option[Salt], CantonTimestamp),
    ] = {
      val contractInstance = contract.rawContractInstance.contractInstance
      LfEngineToApi
        .lfValueToApiRecord(verbose = true, contractInstance.unversioned.arg)
        .bimap(
          e =>
            s"Failed to convert contract instance to data due to issue with create-arguments: ${e}",
          record => {
            val signatories = contract.metadata.signatories.map(_.toString)
            val stakeholders = contract.metadata.stakeholders.map(_.toString)
            (
              LfEngineToApi.toApiIdentifier(contractInstance.unversioned.template),
              record,
              signatories,
              stakeholders -- signatories,
              contract.contractId,
              contract.contractSalt,
              contract.ledgerCreateTime,
            )
          },
        )
    }
  }
}
