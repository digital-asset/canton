// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin

import cats.data.{EitherT, OptionT}
import cats.syntax.either._
import cats.syntax.foldable._
import cats.syntax.functorFilter._
import cats.syntax.traverse._
import cats.syntax.traverseFilter._
import com.daml.ledger.api.v1.value.{Identifier, Record}
import com.daml.ledger.api.validation.{ValueValidator => LedgerApiValueValidator}
import com.daml.ledger.participant.state.v2.TransactionMeta
import com.daml.lf.CantonOnly
import com.daml.lf.data.{ImmArray, Ref}
import com.daml.lf.value.Value
import com.daml.platform.participant.util.LfEngineToApi
import com.daml.platform.server.api.validation.{FieldValidations => LedgerApiFieldValidations}
import com.digitalasset.canton._
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.{
  LengthLimitedStringWrapper,
  LengthLimitedStringWrapperCompanion,
  PositiveInt,
  String255,
}
import com.digitalasset.canton.crypto.{HashPurpose, SyncCryptoApiProvider}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.{AsyncOrSyncCloseable, FlagCloseableAsync, HasCloseContext}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{
  HasLoggerName,
  NamedLoggerFactory,
  NamedLogging,
  NamedLoggingContext,
}
import com.digitalasset.canton.participant.LedgerSyncEvent
import com.digitalasset.canton.participant.config.ParticipantNodeParameters
import com.digitalasset.canton.participant.domain.DomainAliasManager
import com.digitalasset.canton.participant.event.RecordTime
import com.digitalasset.canton.participant.protocol.RequestJournal.{RequestData, RequestState}
import com.digitalasset.canton.participant.store.ActiveContractStore.ContractState
import com.digitalasset.canton.participant.store._
import com.digitalasset.canton.participant.store.db.DbSyncDomainPersistentState
import com.digitalasset.canton.participant.store.memory.InMemorySyncDomainPersistentState
import com.digitalasset.canton.participant.sync.{
  LedgerEvent,
  SyncDomainPersistentStateManager,
  TimestampedEvent,
  TimestampedEventAndCausalChange,
}
import com.digitalasset.canton.participant.util.DAMLe.ContractWithMetadata
import com.digitalasset.canton.participant.util.{DAMLe, TimeOfChange}
import com.digitalasset.canton.protocol._
import com.digitalasset.canton.resource.TransactionalStoreUpdate
import com.digitalasset.canton.store.{
  CursorPrehead,
  IndexedDomain,
  IndexedStringStore,
  SequencedEventStore,
}
import com.digitalasset.canton.topology.client.{
  CachingTopologySnapshot,
  StoreBasedTopologySnapshot,
  TopologySnapshot,
}
import com.digitalasset.canton.topology.store.TopologyStoreFactory
import com.digitalasset.canton.topology.store.TopologyStoreId.DomainStore
import com.digitalasset.canton.topology.{DomainId, ParticipantId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ShowUtil._
import com.digitalasset.canton.util._

import java.time.Instant
import scala.Ordered.orderingToOrdered
import scala.collection.immutable.HashMap
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
class RepairService(
    participantId: ParticipantId,
    topologyStoreFactory: TopologyStoreFactory,
    syncCrypto: SyncCryptoApiProvider,
    packagesDarsService: PackageService,
    damle: DAMLe,
    multiDomainEventLog: MultiDomainEventLog,
    syncDomainPersistentStateManager: SyncDomainPersistentStateManager,
    aliasManager: DomainAliasManager,
    parameters: ParticipantNodeParameters,
    indexedStringStore: IndexedStringStore,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends NamedLogging
    with FlagCloseableAsync
    with HasCloseContext {
  import RepairService._

  override protected def timeouts: ProcessingTimeout = parameters.processingTimeouts

  // Ensure only one repair runs at a time. This ensures concurrent activity among repair operations does
  // not corrupt state.
  private val executionQueue = new SimpleExecutionQueue()

  /** Participant repair utility for manually adding contracts to a domain in an offline fashion.
    *
    * @param domainId           id of domain to add contracts to. The domain needs to be configured, but disconnected
    *                           to prevent race conditions.
    * @param contractsToAdd     contracts to add. Relevant pieces of each contract: create-arguments (LfContractInst),
    *                           template-id (LfContractInst), contractId, ledgerCreateTime, salt (to be added to
    *                           SerializableContract), and witnesses, SerializableContract.metadata is only validated,
    *                           but otherwise ignored as stakeholders, signatories, maintainers and key can be recomputed from contracts.
    * @param ignoreAlreadyAdded whether to ignore and skip over contracts already added/present in the domain.
    */
  def addContracts(
      domainId: DomainId,
      contractsToAdd: Seq[SerializableContractWithWitnesses],
      ignoreAlreadyAdded: Boolean,
  )(implicit traceContext: TraceContext): Either[String, Unit] =
    lockAndAwaitEitherT(
      "repair.add", {
        for {
          _ <- EitherT.cond[Future](contractsToAdd.nonEmpty, (), "No contracts to add specified")

          repair <- initRepairRequestAndVerifyPreconditions(domainId)

          // All referenced templates known and vetted
          _packagesVetted <- contractsToAdd
            .map(_.contract.rawContractInstance.contractInstance.unversioned.template.packageId)
            .toSet
            .toList
            .traverse_(packageVetted)

          _uniqueKeysWithHostedMaintainerInContractsToAdd <- EitherTUtil.ifThenET(
            repair.domainParameters.uniqueContractKeys
          ) {
            val keysWithContractIdsF = contractsToAdd
              .traverseFilter { case SerializableContractWithWitnesses(contract, _witnesses) =>
                // Only check for duplicates where the participant hosts a maintainer
                getKeyIfOneMaintainerIsLocal(
                  repair.topologySnapshot,
                  contract.metadata.maybeKeyWithMaintainers,
                ).map { lfKeyO =>
                  lfKeyO.flatMap(_ => contract.metadata.maybeKeyWithMaintainers).map {
                    keyWithMaintainers =>
                      keyWithMaintainers.globalKey -> contract.contractId
                  }
                }
              }
              .map(x => x.groupBy(_._1))
            EitherT(keysWithContractIdsF.map { keysWithContractIds =>
              val duplicates = keysWithContractIds.mapFilter { keyCoids =>
                if (keyCoids.size > 1) Some(keyCoids.map(_._2)) else None
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
            addContract(repair, ignoreAlreadyAdded)
          )
          contractsToPublishUpstream = contractsToPublishUpstreamO.toList.flattenOption

          // Publish added contracts upstream as created via the ledger api.
          _ <- EitherT.right(
            scheduleUpstreamEventPublishUponDomainStart(contractsToPublishUpstream, repair) {
              case (contractId, contractWithMetadata) =>
                LfNodeCreate(
                  coid = contractId,
                  templateId = contractWithMetadata.instance.unversioned.template,
                  arg = contractWithMetadata.instance.unversioned.arg,
                  agreementText = "",
                  signatories = contractWithMetadata.signatories,
                  stakeholders = contractWithMetadata.stakeholders,
                  key = contractWithMetadata.keyWithMaintainers,
                  version = contractWithMetadata.instance.version,
                )
            }
          )

          // If all has gone well, bump the clean head, effectively committing the changes to the domain.
          _ <- commitRepairs(repair)

        } yield ()
      },
    )

  /** Participant repair utility for manually purging contracts from a domain in an offline fashion.
    *
    * @param domainId            id of domain to purge contracts from. The domain needs to be configured, but
    *                            disconnected to prevent race conditions.
    * @param contractIds         contract ids of contracts to purge.
    * @param ignoreAlreadyPurged whether to ignore and skip over contracts already purged/absent from the domain.
    */
  def purgeContracts(
      domainId: DomainId,
      contractIds: Seq[LfContractId],
      ignoreAlreadyPurged: Boolean,
  )(implicit traceContext: TraceContext): Either[String, Unit] =
    lockAndAwaitEitherT(
      "repair.purge", {
        for {
          _ <- EitherTUtil.condUnitET[Future](contractIds.nonEmpty, "Missing contract ids to purge")

          repair <- initRepairRequestAndVerifyPreconditions(domainId)

          // Note the following purposely fails if any contract fails which results in not all contracts being processed.
          contractsToPublishUpstream <- MonadUtil
            .sequentialTraverse(contractIds)(purgeContract(repair, ignoreAlreadyPurged))
            .map(_.collect { case Some(x) => x })

          // Publish purged contracts upstream as archived via the ledger api.
          _ <- EitherT.right(
            scheduleUpstreamEventPublishUponDomainStart(contractsToPublishUpstream, repair)(
              contract =>
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
                  children = ImmArray.empty[LfNodeId],
                  exerciseResult = Some(Value.ValueNone),
                  // Not setting the contract key as the indexer deletes contract keys along with contracts.
                  // If the contract keys were needed, we'd have to reinterpret the contract to look up the key.
                  key = None,
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

  /** Participant repair utility for manually moving contracts from a source domain to a target domain in an offline
    * fashion.
    *
    * @param contractIds    ids of contracts to move that reside in the sourceDomain
    * @param sourceDomainId the id of the source domain from which to move contracts
    * @param targetDomainId the id of the target domain to which to move contracts
    * @param skipInactive   whether to only move contracts that are active in the source domain
    * @param batchSize      how big the batches should be used during the migration
    */
  def changeDomainAwait(
      contractIds: Seq[LfContractId],
      sourceDomainId: DomainId,
      targetDomainId: DomainId,
      skipInactive: Boolean,
      batchSize: PositiveInt,
  )(implicit traceContext: TraceContext): Either[String, Unit] = {

    lockAndAwaitEitherT(
      "repair.change_domain", {
        changeDomain(
          contractIds,
          sourceDomainId,
          targetDomainId,
          skipInactive,
          batchSize,
        )
      },
    )
  }

  /** Change the association of a contract from one domain to another
    *
    * This function here allows us to manually insert a transfer out / in into the respective
    * journals in order to move a contract from one domain to another. It obviously depends on
    * all the counter parties running the same command. Otherwise, the participants will start
    * to complain and possibly break.
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
    if (contractIds.isEmpty) EitherT.rightT(())
    else
      for {

        repairSource <- initRepairRequestAndVerifyPreconditions(sourceDomainId)
        repairTarget <- initRepairRequestAndVerifyPreconditions(targetDomainId)

        // Note the following purposely fails if any contract fails which results in not all contracts being processed.
        _ <- moveContractsBatched(
          contractIds,
          repairSource,
          repairTarget,
          skipInactive,
          batchSize,
        )

        // If all has gone well, bump the clean head to both domains transactionally
        _ <- commitRepairs(repairTarget, repairSource)
      } yield ()

  def ignoreEvents(domain: DomainId, from: SequencerCounter, to: SequencerCounter, force: Boolean)(
      implicit traceContext: TraceContext
  ): Either[String, Unit] =
    lockAndAwaitEitherT(
      "repair.skip_messages", {
        performIfRangeSuitableForIgnoreOperations(domain, from, force)(
          _.ignoreEvents(from, to).leftMap(_.toString)
        )
      },
    )

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
          multiDomainEventLog,
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
  )(implicit traceContext: TraceContext): Either[String, Unit] =
    lockAndAwaitEitherT(
      "repair.unskip_messages", {
        performIfRangeSuitableForIgnoreOperations(domain, from, force)(sequencedEventStore =>
          sequencedEventStore.unignoreEvents(from, to).leftMap(_.toString)
        )
      },
    )

  private def hostsParty(snapshot: TopologySnapshot)(party: LfPartyId): Future[Boolean] =
    snapshot.hostedOn(party, participantId).map(_.exists(_.permission.isActive))

  private def getKeyIfOneMaintainerIsLocal(
      snapshot: TopologySnapshot,
      keyO: Option[LfGlobalKeyWithMaintainers],
  ): Future[Option[LfGlobalKey]] = {
    keyO.collect { case LfGlobalKeyWithMaintainers(key, maintainers) =>
      (maintainers, key)
    } match {
      case None => Future.successful(None)
      case Some((maintainers, key)) =>
        maintainers.toList.findM(hostsParty(snapshot)).map(_.map(_ => key))
    }
  }

  private def addContract(repair: RepairRequest, ignoreAlreadyAdded: Boolean)(
      contractToAdd: SerializableContractWithWitnesses
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, String, Option[(LfContractId, ContractWithMetadata)]] = {
    val topologySnapshot = repair.topologySnapshot

    // TODO(#4001) - performance of point-db lookups will be slow, add batching (also to purgeContract and moveContract)
    def persistCreation(cid: LfContractId): EitherT[Future, String, Unit] =
      repair.domainPersistence.activeContractStore
        .createContract(cid, repair.timeOfChange)
        .toEitherTWithNonaborts
        .leftMap(e => log(s"Failed to create contract ${cid} in ActiveContractStore: ${e}"))

    def useComputedContractAndMetadata(
        inputContract: SerializableContract,
        computed: ContractWithMetadata,
    ): EitherT[Future, String, SerializableContract] =
      EitherT.fromEither[Future](for {
        metadata <- computed.metadataWithGlobalKey.leftMap(err =>
          log(s"Failed to convert contract key ${err.message}")
        )
        rawContractInstance <- SerializableRawContractInstance
          .create(computed.instance)
          .leftMap(err =>
            log(s"Failed to serialize contract ${inputContract.contractId}: ${err.errorMessage}")
          )
      } yield inputContract.copy(metadata = metadata, rawContractInstance = rawContractInstance))

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
          )
        }
        if (contract.metadata.stakeholders != computedContractWithMetadata.stakeholders) {
          log(
            s"Contract ${contract.contractId} metadata stakeholders ${contract.metadata.stakeholders} differ from actual stakeholders ${computedContractWithMetadata.stakeholders}"
          )
        }
        ()
      }
      _warnOnEmptyMaintainers <- EitherT.cond[Future](
        !computedContractWithMetadata.keyWithMaintainers.exists(_.maintainers.isEmpty),
        (),
        log(s"Contract ${contract.contractId} has key without maintainers."),
      )

      // Witnesses all known locally.
      _witnessesKnownLocally <- witnesses.toList.traverse_ { p =>
        EitherT(hostsParty(topologySnapshot)(p).map { hosted =>
          EitherUtil.condUnitE(
            hosted,
            log(s"Witness ${p} not active on domain ${repair.domainAlias} and local participant"),
          )
        })
      }

      // At least one stakeholder is hosted locally if no witnesses are defined
      _localStakeholderOrWitnesses <- EitherT(
        computedContractWithMetadata.stakeholders.toList
          .findM(hostsParty(topologySnapshot))
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

      // All stakeholders exist on the domain
      _ <- topologySnapshot
        .allHaveActiveParticipants(computedContractWithMetadata.stakeholders)
        .leftMap { missingStakeholders =>
          log(
            s"Domain ${repair.domainAlias} missing stakeholders ${missingStakeholders} of contract ${contract.contractId}"
          )
        }

      // All witnesses exist on the domain
      _ <- topologySnapshot.allHaveActiveParticipants(witnesses).leftMap { missingWitnesses =>
        log(
          s"Domain ${repair.domainAlias} missing witnesses ${missingWitnesses} of contract ${contract.contractId}"
        )
      }

      // See if contract already exists according to ACS(journal)
      acsStatus <- readContractAcsState(repair.domainPersistence, contract.contractId)
      res <- EitherT.fromEither[Future](acsStatus match {
        case None => Right((true, None))
        case Some(ActiveContractStore.Active) =>
          Either.cond(
            ignoreAlreadyAdded,
            (false, None),
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
        case Some(ActiveContractStore.TransferredAway(domainId)) =>
          val _ = log(
            s"Marking contract ${contract.contractId} previously transferred away to ${domainId} as " +
              s"transferred in from ${domainId} (even though contract may have been transferred to yet another domain since)."
          )
          Right((true, Some(domainId)))
      })
      (needToAddContract, transferringFrom) = res

      keyOfHostedMaintainerO <- {
        EitherT.right(
          getKeyIfOneMaintainerIsLocal(topologySnapshot, contract.metadata.maybeKeyWithMaintainers)
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
                      val ok = acsStatus.contains(ActiveContractStore.Active)
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
            )
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
          .addKeyStateUpdates(Map(key -> ContractKeyJournal.Assigned), repair.timeOfChange)
          .leftMap(err => log(s"Error while persisting key state updates: $err"))
      }

      _persistedInAcs <- EitherTUtil.ifThenET(needToAddContract)(
        transferringFrom.fold(persistCreation(contractToPersistAndPublishUpstream.contractId))(
          persistTransferIn(repair, _, contractToPersistAndPublishUpstream.contractId)
        )
      )
    } yield
      if (needToAddContract) Some(contract.contractId -> computedContractWithMetadata) else None
  }

  private def purgeContract(repair: RepairRequest, ignoreAlreadyPurged: Boolean)(
      cid: LfContractId
  )(implicit traceContext: TraceContext): EitherT[Future, String, Option[SerializableContract]] = {
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
              s"Contract ${cid} does not exist in domain ${repair.domainAlias} and cannot be purged. Set ignoreAlreadyPurged = true to skip non-existing contracts."
            ),
          )
        case Some(ActiveContractStore.Active) =>
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
                )
              else Future.successful(None)
            )

            _ <- keyOfHostedMaintainerO.traverse_ { key =>
              repair.domainPersistence.contractKeyJournal
                .addKeyStateUpdates(Map(key -> ContractKeyJournal.Unassigned), repair.timeOfChange)
                .leftMap(err => log(s"Error while persisting key state updates: $err"))
            }
            _ <- persistArchival(repair)(cid)
          } yield {
            logger.info(s"purged contract ${cid} at repair request ${repair.rc} at ${repair.ts}")
            contractO
          }
        case Some(ActiveContractStore.Archived) =>
          EitherT.cond[Future](
            ignoreAlreadyPurged,
            None,
            log(
              s"Contract ${cid} is already archived in domain ${repair.domainAlias} and cannot be purged. Set ignoreAlreadyPurged = true to skip archived contracts."
            ),
          )
        case Some(ActiveContractStore.TransferredAway(domainId)) =>
          val _ = log(
            s"Purging contract ${cid} previously marked as transferred away to ${domainId}. " +
              s"Marking contract as transferred in from ${domainId} (even though contract may have since been transferred to yet another domain) and subsequently as archived."
          )
          for {
            _unit <- persistTransferInAndArchival(repair, domainId)(cid)
          } yield contractO
      }
    } yield contractToArchiveInEvent
  }

  private def moveContractsBatched(
      cids: Seq[LfContractId],
      repairSource: RepairRequest,
      repairTarget: RepairRequest,
      skipInactive: Boolean,
      batchSize: PositiveInt,
  )(implicit traceContext: TraceContext): EitherT[Future, String, Unit] =
    MonadUtil
      .batchedSequentialTraverse(parameters.maxDbConnections * 2, batchSize.value)(cids)(
        moveContracts(_, repairSource, repairTarget, skipInactive).map(_ => Seq[Unit]())
      )
      .map(_ => ())

  /** Move contract from `repairSource` to `repairTarget`. */
  private def moveContracts(
      cids: Iterable[LfContractId],
      repairSource: RepairRequest,
      repairTarget: RepairRequest,
      skipInactive: Boolean,
  )(implicit traceContext: TraceContext): EitherT[Future, String, Unit] = {
    def determineSourceContractsToMigrate(
        source: Map[LfContractId, ContractState]
    ): EitherT[Future, String, List[LfContractId]] = {
      EitherT.fromEither(
        cids
          .map(cid => (cid, source.get(cid).map(_.status)))
          .toList
          .traverse {
            case (cid, None) =>
              Either.cond(
                skipInactive,
                None,
                log(s"Contract ${cid} does not exist in source domain and cannot be moved."),
              )
            case (cid, Some(ActiveContractStore.Active)) => Right(Some(cid))
            case (cid, Some(ActiveContractStore.Archived)) =>
              Either.cond(
                skipInactive,
                None,
                log(s"Contract $cid has been archived and cannot be moved."),
              )
            case (cid, Some(ActiveContractStore.TransferredAway(target))) =>
              Either
                .cond(
                  skipInactive,
                  None,
                  log(s"Contract $cid has been transferred to $target and cannot be moved."),
                )
          }
          .map(_.flatten)
      )
    }

    def determineTargetContractsToMigrate(
        sourceContracts: List[LfContractId],
        targetStatus: Map[LfContractId, ContractState],
    ): EitherT[Future, String, List[LfContractId]] = {
      val filteredE =
        sourceContracts.map(cid => (cid, targetStatus.get(cid).map(_.status))).traverse {
          case (cid, None | Some(ActiveContractStore.TransferredAway(_))) => Right(cid)
          case (cid, Some(targetState)) =>
            Left(
              log(
                s"Active contract ${cid} in source domain exists in target domain with status $targetState. Use 'repair.add' or 'repair.purge' instead."
              )
            )
        }
      def checkStakeholders(
          cid: LfContractId,
          stakeholders: Set[LfPartyId],
      ): EitherT[Future, String, Unit] = {
        // At least one stakeholder is hosted locally in the target domain
        EitherT(
          stakeholders.toSeq
            .findM(hostsParty(repairTarget.topologySnapshot))
            .map { oneLocalStakeholderO =>
              oneLocalStakeholderO
                .toRight(
                  log(
                    show"Not allowed to move contract ${cid} without at least one stakeholder of ${stakeholders} existing locally on the target domain asOf=${repairTarget.topologySnapshot.timestamp}"
                  )
                )
                .map(_ => ())
            }
        )
      }
      for {
        filtered <- EitherT.fromEither[Future](filteredE)
        stakeholders <- readStakeholders(repairSource, filtered.toSet)
        _ <- filtered.traverse { cid =>
          checkStakeholders(cid, stakeholders.getOrElse(cid, Set.empty))
        }
      } yield filtered
    }

    def adjustContractKeys(
        request: RepairRequest,
        contracts: List[SerializableContract],
        newStatus: ContractKeyJournal.Status,
    ): EitherT[Future, String, List[LfGlobalKey]] =
      if (!request.domainParameters.uniqueContractKeys)
        EitherT.rightT(List.empty)
      else {
        for {
          keys <- EitherT.right(
            contracts.traverseFilter(contract =>
              getKeyIfOneMaintainerIsLocal(
                request.topologySnapshot,
                contract.metadata.maybeKeyWithMaintainers,
              )
            )
          )
          _ <- request.domainPersistence.contractKeyJournal
            .addKeyStateUpdates(keys.map(_ -> newStatus).toMap, request.timeOfChange)
            .leftMap(x => log(x.toString))
        } yield keys
      }

    def persistContracts(cids: List[LfContractId]): EitherT[Future, String, Unit] = {
      val loadedET = cids
        .traverse { cid =>
          for {
            // first, read the contract from the store such that we can store it in the target domain
            serializedSource <- readContract(repairSource, cid)
            serializedTargetO <- EitherT(
              repairTarget.domainPersistence.contractStore
                .lookupContract(cid)
                .value
                .map(Right(_): Either[String, Option[SerializableContract]])
            )
            _ <- serializedTargetO.fold(EitherT.rightT[Future, String](())) { serializedTarget =>
              EitherTUtil.condUnitET[Future](
                serializedTargetO.forall(_ == serializedSource),
                s"Contract ${cid} already exists in the contract store, but differs from contract to be created. Contract to be created ${serializedSource} versus existing contract ${serializedTarget}.",
              )
            }
          } yield (serializedTargetO.isEmpty, serializedSource)
        }

      for {
        loaded <- loadedET
        allContracts = loaded.map(_._2)
        filtered = loaded.collect {
          case (notInTarget, contract) if notInTarget => contract
        }
        _ <- adjustContractKeys(repairSource, allContracts, ContractKeyJournal.Unassigned)
        _ <- adjustContractKeys(repairTarget, allContracts, ContractKeyJournal.Assigned)
        // finally, persist contracts
        _ <- EitherT.right(
          repairTarget.domainPersistence.contractStore.storeCreatedContracts(
            repairTarget.rc,
            repairTarget.transactionId,
            filtered,
          )
        )
      } yield ()

    }

    def persistTransferOutAndIn(
        cids: List[LfContractId]
    ): EitherT[Future, String, Unit] = {

      // Note: this supports crash recovery, as we only activate these changes once we commit using clean-head logic
      val outF = repairSource.domainPersistence.activeContractStore
        .transferOutContracts(
          cids.map(cid => (cid -> repairTarget.domainId)),
          repairSource.timeOfChange,
        )
        .toEitherT
        .leftMap(e => log(s"Failed to mark contracts as transferred out: ${e}"))

      val inF = repairTarget.domainPersistence.activeContractStore
        .transferInContracts(
          cids.map(cid => (cid -> repairSource.domainId)),
          repairTarget.timeOfChange,
        )
        .toEitherT
        .leftMap(e => log(s"Failed to mark contracts as transferred in: ${e}"))

      for {
        _ <- outF
        _ <- inF
      } yield ()

    }

    for {
      contractStatusAtSource <- EitherT.right(
        repairSource.domainPersistence.activeContractStore.fetchStates(cids)
      )
      sourceContractsToMigrate <- determineSourceContractsToMigrate(contractStatusAtSource)
      contractStatusAtTarget <- EitherT.right(
        repairTarget.domainPersistence.activeContractStore.fetchStates(sourceContractsToMigrate)
      )
      contractsToMigrate <- determineTargetContractsToMigrate(
        sourceContractsToMigrate,
        contractStatusAtTarget,
      )
      _ <- persistContracts(contractsToMigrate)
      _ <- persistTransferOutAndIn(contractsToMigrate)
    } yield ()

  }

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
            .storeCreatedContract(repair.rc, repair.transactionId, contract),
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

  private def persistTransferIn(repair: RepairRequest, from: DomainId, cid: LfContractId)(implicit
      traceContext: TraceContext
  ): EitherT[Future, String, Unit] =
    repair.domainPersistence.activeContractStore
      .transferInContract(cid, repair.timeOfChange, from)
      .toEitherTWithNonaborts
      .leftMap(e => log(s"Failed to transfer in contract ${cid} in ActiveContractStore: ${e}"))

  private def persistArchival(
      repair: RepairRequest
  )(cid: LfContractId)(implicit traceContext: TraceContext): EitherT[Future, String, Unit] =
    repair.domainPersistence.activeContractStore
      .archiveContract(cid, repair.timeOfChange)
      .toEitherT // not turning warnings to errors on behalf of archived contracts, in contract to created contracts
      .leftMap(e => log(s"Failed to mark contract ${cid} as archived: ${e}"))

  private def persistTransferInAndArchival(repair: RepairRequest, from: DomainId)(
      cid: LfContractId
  )(implicit traceContext: TraceContext): EitherT[Future, String, Unit] =
    for {
      _ <- persistTransferIn(repair, from, cid)
      _ <- persistArchival(repair)(cid)
    } yield ()

  private def readContract(repair: RepairRequest, cid: LfContractId)(implicit
      traceContext: TraceContext
  ): EitherT[Future, String, SerializableContract] =
    repair.domainPersistence.contractStore
      .lookupContractE(cid)
      .leftMap(e => log(s"Failed to look up contract ${cid} in domain ${repair.domainAlias}: ${e}"))

  private def readStakeholders(repair: RepairRequest, cids: Set[LfContractId])(implicit
      traceContext: TraceContext
  ): EitherT[Future, String, Map[LfContractId, Set[LfPartyId]]] =
    repair.domainPersistence.contractStore
      .lookupStakeholders(cids)
      .leftMap(e =>
        log(
          s"Failed to look up stakeholder of contracts in domain ${repair.domainAlias}: ${e}"
        )
      )

  private def scheduleUpstreamEventPublishUponDomainStart[C](
      contracts: Seq[C],
      repair: RepairRequest,
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
        val event = TimestampedEvent(
          LedgerSyncEvent.TransactionAccepted(
            optCompletionInfo = None,
            transactionMeta = TransactionMeta(
              ledgerEffectiveTime = repair.ts.toLf,
              workflowId = None,
              submissionTime = repair.ts.toLf,
              submissionSeed = LedgerEvent.noOpSeed, // "fake" transaction - None no longer allowed
              optUsedPackages = None,
              optNodeSeeds = None,
              optByKeyNodes = None,
            ),
            transaction = tx,
            transactionId = transactionId,
            recordTime = repair.ts.toLf,
            divulgedContracts = List.empty, // create and plain archive don't involve divulgence
            blindingInfo = None,
            contractMetadata = Map(), // TODO(#9795) wire proper value
          ),
          repair.rc.asLocalOffset,
          None,
        )
        val eventLog = repair.domainPersistence.eventLog
        for {
          existingEventO <- eventLog.eventByTransactionId(transactionId).value
          _ <- existingEventO match {
            case None => eventLog.insert(event, None)
            case Some(TimestampedEventAndCausalChange(existingEvent, updateO))
                if existingEvent.normalized.event == event.normalized.event =>
              logger.info(
                show"Skipping duplicate publication of event at offset ${event.localOffset} with transaction id $transactionId."
              )
              Future.unit
            case Some(TimestampedEventAndCausalChange(existingEvent, updateO)) =>
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
        packagesDarsService.packagesDarsStore.getPackageDescription(lfPackageId),
        t => log(s"Failed to look up package ${lfPackageId}", t),
      )
      _packageVetted <- EitherTUtil
        .condUnitET[Future](
          packageDescription.nonEmpty,
          log(s"Failed to locate package ${lfPackageId}"),
        )
    } yield ()
  }

  /** Repair commands are inserted where processing starts again upon reconnection. */
  private def initRepairRequestAndVerifyPreconditions(
      domainId: DomainId
  )(implicit traceContext: TraceContext): EitherT[Future, String, RepairRequest] = {
    val domainAlias = aliasManager.aliasForDomainId(domainId).fold(domainId.filterString)(_.unwrap)
    for {
      persistentState <- EitherT.fromEither[Future](
        lookUpDomainPersistence(domainId, s"domain ${domainAlias}")
      )
      indexedDomain <- EitherT.right(IndexedDomain.indexed(indexedStringStore)(domainId))
      startingPoints <- EitherTUtil.fromFuture(
        SyncDomainEphemeralStateFactory.startingPoints(
          indexedDomain,
          persistentState.requestJournalStore,
          persistentState.sequencerCounterTrackerStore,
          persistentState.sequencedEventStore,
          multiDomainEventLog,
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
      topologyStore = topologyStoreFactory.forId(DomainStore(domainId))
      topologySnapshot = new CachingTopologySnapshot(
        new StoreBasedTopologySnapshot(
          tsRepair,
          topologyStore,
          initKeys = Map.empty,
          useStateTxs = true,
          packageId => packagesDarsService.packageDependencies(List(packageId)),
          loggerFactory,
        ),
        parameters.cachingConfigs,
        loggerFactory,
      )
      domainParameters <- OptionT(persistentState.parameterStore.lastParameters)
        .toRight(log(s"No static domains parameters found for $domainAlias"))
      repair = RepairRequest(
        domainId,
        domainAlias,
        domainParameters,
        makeUpDummyTransactionId,
        rcRepair,
        tsRepair,
        RepairContext.fromTraceContext,
      )(persistentState, topologySnapshot)

      // Mark the repair request as pending in the request journal store
      _ <- EitherTUtil.fromFuture(
        persistentState.requestJournalStore.insert(repair.requestJournalData),
        t => log("Failed to insert repair request", t),
      )
    } yield repair
  }

  private def commitRepairs(
      repairs: RepairRequest*
  )(implicit traceContext: TraceContext): EitherT[Future, String, Unit] = {
    def markClean(repair: RepairRequest): EitherT[Future, String, Unit] =
      repair.domainPersistence.requestJournalStore
        .replace(repair.rc, repair.ts, RequestState.Pending, RequestState.Clean, Some(repair.ts))
        .leftMap(t => log(s"Failed to update request journal store on ${repair.domainAlias}: $t"))

    for {
      _ <- repairs.traverse_(markClean)
      advancePreheads = repairs.map { repair =>
        val prehead = CursorPrehead(repair.rc, repair.ts)
        repair.domainPersistence.requestJournalStore.advancePreheadCleanToTransactionalUpdate(
          prehead
        )
      }
      _ <- EitherTUtil.fromFuture(
        TransactionalStoreUpdate.execute(advancePreheads),
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

  /** Cooks up a random dummy transaction id.
    *
    * With single-participant repair commands, we have little hope of coming up with a transactionId that matches up with
    * other participants. We can get away with differing transaction ids across participants because the
    * AcsCommitmentProcessor does not compare transaction ids.
    */
  private def makeUpDummyTransactionId = {
    // We take as much entropy as for a random UUID.
    // This should be enough to guard against clashes between the repair requests executed on a single participant.
    // We don't have to worry about clashes with ordinary transaction IDs as the hash purpose is different.
    val randomness = syncCrypto.pureCrypto.generateRandomByteString(16)
    val hash = syncCrypto.pureCrypto.digest(HashPurpose.RepairTransactionId, randomness)
    TransactionId(hash)
  }

  // Looks up domain persistence erroring if domain is based on in-memory persistence for which repair is not supported.
  private def lookUpDomainPersistence(domainId: DomainId, domainDescription: String)(implicit
      traceContext: TraceContext
  ): Either[String, SyncDomainPersistentState] =
    for {
      dp <- syncDomainPersistentStateManager
        .get(domainId)
        .toRight(log(s"Could not find ${domainDescription}"))
      _ <- dp match {
        case _: InMemorySyncDomainPersistentState =>
          Left(
            log(
              s"${domainDescription} is in memory which is not supported by repair. Use db persistence."
            )
          )
        case _: DbSyncDomainPersistentState =>
          Right(())
        case _ =>
          Left(log(s"${domainDescription} does not support repair. Use db persistence."))
      }
    } yield dp

  private def lockAndAwaitEitherT[A, B](description: String, code: => EitherT[Future, A, B])(
      implicit traceContext: TraceContext
  ): Either[A, B] = {
    logger.info(s"Queuing ${description}")
    // repair commands can take an unbounded amount of time
    parameters.processingTimeouts.unbounded.await(description)(
      executionQueue.executeE(code, description).value
    )
  }

  private def log(message: String, cause: Throwable)(implicit traceContext: TraceContext): String =
    log(s"$message: ${ErrorUtil.messageWithStacktrace(cause)}")

  private def log(message: String)(implicit traceContext: TraceContext): String = {
    // consider errors user errors and log them on the server side as info:
    logger.info(message)
    message
  }

  override protected def closeAsync(): Seq[AsyncOrSyncCloseable] = {
    import TraceContext.Implicits.Empty._
    Seq(
      executionQueue.asCloseable(
        "repair-service-queue",
        parameters.processingTimeouts.shutdownProcessing.unwrap,
      )
    )
  }

}

object RepairService {

  /** The timestamp to be used for a repair request on a domain without requests */
  val RepairTimestampOnEmptyDomain: CantonTimestamp = CantonTimestamp.MinValue

  private[RepairService] case class RepairRequest(
      domainId: DomainId,
      domainAlias: String, // for logging
      domainParameters: StaticDomainParameters,
      transactionId: TransactionId,
      rc: RequestCounter,
      ts: CantonTimestamp,
      context: RepairContext,
  )(val domainPersistence: SyncDomainPersistentState, val topologySnapshot: TopologySnapshot) {
    def timeOfChange = TimeOfChange(rc, ts)
    def requestJournalData =
      // Trace context persisted explicitly doubling as a marker for repair requests in the request journal
      RequestData(rc, RequestState.Pending, ts, Some(context))
  }

  /** Make tracecontext mandatory throughout repair operations.
    *
    * @param str the w3c serialized tracing information of the trace parent
    *                    The W3C standard specifies that the traceparent is length-limited -> thus it is safe to limit it to 255 characters
    *                    However, Tracestates aren't limited, so the repair context should be saved as a blob (like [[com.digitalasset.canton.tracing.TraceContext]])
    *                    if we want to use it for repair contexts
    */
  case class RepairContext(override protected val str: String255)
      extends LengthLimitedStringWrapper
      with PrettyPrinting {
    def toLengthLimitedString: String255 = str
    override def pretty: Pretty[RepairContext] = prettyOfClass(unnamedParam(_.str.unwrap.unquoted))
  }

  object RepairContext extends LengthLimitedStringWrapperCompanion[String255, RepairContext] {

    def fromTraceContext(implicit traceContext: TraceContext): RepairContext =
      RepairContext(
        // take the serialized parent value which should contain
        traceContext.asW3CTraceContext
          .map(tc => String255.tryCreate(tc.parent, Some("RepairContext")))
          .getOrElse(
            throw new IllegalArgumentException(
              "RepairService cannot be invoked with an empty trace context"
            )
          )
      )

    override def instanceName: String = "RepairContext"

    override protected def companion: String255.type = String255

    override protected def factoryMethodWrapper(str: String255): RepairContext = RepairContext(str)
  }

  object ContractConverter extends HasLoggerName {

    def contractDataToInstance(
        templateId: Identifier,
        createArguments: Record,
        signatories: Set[String],
        lfContractId: LfContractId,
        ledgerTime: Instant,
    )(implicit namedLoggingContext: NamedLoggingContext): Either[String, SerializableContract] = {
      for {
        template <- LedgerApiFieldValidations.validateIdentifier(templateId).leftMap(_.getMessage)

        argsValue <- LedgerApiValueValidator
          .validateRecord(createArguments)
          .leftMap(e => s"Failed to validate arguments: ${e}")

        argsVersionedValue = CantonOnly.asVersionedValue(
          argsValue,
          CantonOnly.DummyTransactionVersion, // Version is ignored by daml engine upon RepairService.addContract
        )

        lfContractInst = LfContractInst(
          template = template,
          arg = argsVersionedValue,
          agreementText = "",
        )

        serializableRawContractInst <- SerializableRawContractInstance
          .create(lfContractInst)
          .leftMap(_.errorMessage)

        signatoriesAsParties <- signatories.toList.traverse(LfPartyId.fromString).map(_.toSet)

        time <- CantonTimestamp.fromInstant(ledgerTime)
      } yield SerializableContract(
        contractId = lfContractId,
        rawContractInstance = serializableRawContractInst,
        metadata =
          checked(ContractMetadata.tryCreate(signatoriesAsParties, signatoriesAsParties, None)),
        ledgerCreateTime = time,
      )
    }

    def contractInstanceToData(
        contract: SerializableContract
    ): Either[String, (Identifier, Record, Set[String], LfContractId)] = {
      val contractInstance = contract.rawContractInstance.contractInstance
      LfEngineToApi
        .lfValueToApiRecord(verbose = true, contractInstance.unversioned.arg)
        .bimap(
          e =>
            s"Failed to convert contract instance to data due to issue with create-arguments: ${e}",
          record =>
            (
              LfEngineToApi.toApiIdentifier(contractInstance.unversioned.template),
              record,
              contract.metadata.signatories.map(_.toString),
              contract.contractId,
            ),
        )
    }

  }
}
