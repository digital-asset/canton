// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.db

import cats.data.{EitherT, OptionT}
import cats.syntax.parallel.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.RequireTypes.{PositiveNumeric, String2066}
import com.digitalasset.canton.config.{BatchAggregatorConfig, CacheConfig, ProcessingTimeout}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.pretty.Pretty
import com.digitalasset.canton.logging.{NamedLoggerFactory, TracedLogger}
import com.digitalasset.canton.metrics.TimedLoadGauge
import com.digitalasset.canton.participant.store.*
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.resource.DbStorage.{DbAction, SQLActionBuilderChain}
import com.digitalasset.canton.resource.{DbStorage, DbStore}
import com.digitalasset.canton.store.IndexedDomain
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.util.EitherUtil.RichEitherIterable
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.Thereafter.syntax.ThereafterOps
import com.digitalasset.canton.util.{BatchAggregator, MonadUtil}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{LfPartyId, RequestCounter, checked}
import com.github.blemale.scaffeine.AsyncCache
import io.functionmeta.functionFullName
import slick.jdbc.{GetResult, SetParameter}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

class DbContractStore(
    override protected val storage: DbStorage,
    domainIdIndexed: IndexedDomain,
    protocolVersion: ProtocolVersion,
    maxContractIdSqlInListSize: PositiveNumeric[Int],
    maxDbConnections: Int, // used to throttle query batching
    cacheConfig: CacheConfig,
    dbQueryBatcherConfig: BatchAggregatorConfig,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(protected implicit val ec: ExecutionContext)
    extends ContractStore
    with DbStore {

  import DbStorage.Implicits.*
  import storage.api.*
  import storage.converters.*

  private val profile = storage.profile
  private val domainId = domainIdIndexed.index

  private val processingTime: TimedLoadGauge =
    storage.metrics.loadGaugeM("contract-store")

  override protected[store] def logger: TracedLogger = super.logger

  private implicit val storedContractGetResult: GetResult[StoredContract] = GetResult { r =>
    val contractId = r.<<[LfContractId]
    val contractInstance = r.<<[SerializableRawContractInstance]
    val metadata = r.<<[ContractMetadata]
    val ledgerCreateTime = r.<<[CantonTimestamp]
    val requestCounter = r.<<[RequestCounter]
    val creatingTransactionIdO = r.<<[Option[TransactionId]]

    val contract = SerializableContract(contractId, contractInstance, metadata, ledgerCreateTime)
    StoredContract(contract, requestCounter, creatingTransactionIdO)
  }

  private implicit val setParameterContractMetadata: SetParameter[ContractMetadata] =
    ContractMetadata.getVersionedSetParameter(protocolVersion)

  private val cache: AsyncCache[LfContractId, Option[StoredContract]] =
    cacheConfig.buildScaffeine().buildAsync[LfContractId, Option[StoredContract]]()

  // batch aggregator used for single point queries: damle will run many "lookups"
  // during interpretation. they will hit the db like a nail gun. the batch
  // aggregator will limit the number of parallel queries to the db and "batch them"
  // together. so if there is high load with a lot of interpretation happening in parallel
  // batching will kick in.
  private val batchAggregator = {
    val processor: BatchAggregator.Processor[LfContractId, Option[StoredContract]] =
      new BatchAggregator.Processor[LfContractId, Option[StoredContract]] {
        override val kind: String = "request"
        override def logger: TracedLogger = DbContractStore.this.logger

        override def executeBatch(ids: NonEmpty[Seq[Traced[LfContractId]]])(implicit
            traceContext: TraceContext
        ): Future[Iterable[Option[StoredContract]]] =
          lookupManyUncachedInternal(ids.map(_.value))

        override def prettyItem: Pretty[LfContractId] = implicitly
      }
    BatchAggregator(
      processor,
      dbQueryBatcherConfig,
      Some(storage.metrics.loadGaugeM("contract-store-query-batcher")),
    )
  }

  private def lookupQueries(
      ids: NonEmpty[Seq[LfContractId]]
  ): Iterable[DbAction.ReadOnly[Seq[Option[StoredContract]]]] = {
    import DbStorage.Implicits.BuilderChain.*

    DbStorage.toInClauses("contract_id", ids, maxContractIdSqlInListSize).map {
      case (idGroup, inClause) =>
        (sql"""select contract_id, instance, metadata, ledger_create_time, request_counter, creating_transaction_id
          from contracts
          where domain_id = $domainId and """ ++ inClause)
          .as[StoredContract]
          .map { storedContracts =>
            val foundContracts =
              storedContracts
                .map(storedContract => (storedContract.contractId, storedContract))
                .toMap
            idGroup.map(foundContracts.get)
          }
    }
  }

  def lookup(
      id: LfContractId
  )(implicit traceContext: TraceContext): OptionT[Future, StoredContract] = {
    def get(): Future[Option[StoredContract]] =
      performUnlessClosingF(functionFullName)(batchAggregator.run(id))
        .onShutdown(
          throw DbContractStore.AbortedDueToShutdownException(
            s"Shutdown in progress, unable to fetch contract $id"
          )
        )

    OptionT(cache.getFuture(id, _ => get()).recover {
      case e: DbContractStore.AbortedDueToShutdownException =>
        logger.info(e.getMessage)
        None // TODO(Error handling) Consider propagating the shutdown info further instead of converting to None
    })
  }

  override def lookupManyUncached(
      ids: Seq[LfContractId]
  )(implicit traceContext: TraceContext): Future[List[Option[StoredContract]]] = {
    NonEmpty
      .from(ids)
      .fold(Future.successful(List.empty[Option[StoredContract]])) { items =>
        lookupManyUncachedInternal(items).map(_.toList)
      }
  }

  private def lookupManyUncachedInternal(
      ids: NonEmpty[Seq[LfContractId]]
  )(implicit traceContext: TraceContext) = {
    processingTime.event {
      storage.sequentialQueryAndCombine(lookupQueries(ids), functionFullName)(
        traceContext,
        closeContext,
      )
    }
  }

  override def find(
      filterId: Option[String],
      filterPackage: Option[String],
      filterTemplate: Option[String],
      limit: Int,
  )(implicit traceContext: TraceContext): Future[List[SerializableContract]] =
    processingTime.event {

      import DbStorage.Implicits.BuilderChain.*

      // If filter is set returns a conjunctive (`and` prepended) constraint on attribute `name`.
      // Otherwise empty sql action.
      @SuppressWarnings(Array("com.digitalasset.canton.SlickString"))
      def createConjunctiveFilter(name: String, filter: Option[String]): SQLActionBuilderChain =
        filter
          .map { f =>
            sql" and #$name " ++ (f match {
              case rs if rs.startsWith("!") => sql"= ${rs.drop(1)}" // Must be equal
              case rs if rs.startsWith("^") => sql"""like ${rs.drop(1) + "%"}""" // Starts with
              case rs => sql"""like ${"%" + rs + "%"}""" // Contains
            })
          }
          .getOrElse(sql" ")

      val contractsBaseQuery =
        sql"select contract_id, instance, metadata, ledger_create_time, request_counter, creating_transaction_id from contracts"
      val where = sql" where "
      val domainConstraint = sql" domain_id = $domainId "
      val pkgFilter = createConjunctiveFilter("package_id", filterPackage)
      val templateFilter = createConjunctiveFilter("template_id", filterTemplate)
      val coidFilter = createConjunctiveFilter("contract_id", filterId)
      val limitFilter = sql" #${storage.limit(limit)}"

      val contractsQuery = contractsBaseQuery ++ where ++
        domainConstraint ++ pkgFilter ++ templateFilter ++ coidFilter ++ limitFilter

      storage
        .query(contractsQuery.as[StoredContract], functionFullName)
        .map(_.map(_.contract).toList)
    }

  override def storeCreatedContracts(
      requestCounter: RequestCounter,
      transactionId: TransactionId,
      creations: Seq[SerializableContract],
  )(implicit traceContext: TraceContext): Future[Unit] =
    processingTime.event {
      storeElements(
        creations,
        StoredContract.fromCreatedContract(_, requestCounter, transactionId),
      )
    }

  override def storeDivulgedContracts(
      requestCounter: RequestCounter,
      divulgences: Seq[SerializableContract],
  )(implicit traceContext: TraceContext): Future[Unit] =
    processingTime.event {
      storeElements(
        divulgences,
        StoredContract.fromDivulgedContract(_, requestCounter),
      )
    }

  // Not to be called directly: use contractsCache
  private def contractInsert(storedContract: StoredContract): DbAction.WriteOnly[Int] = {
    val contractId = storedContract.contractId
    val contract = storedContract.contract.rawContractInstance
    val metadata = storedContract.contract.metadata
    val ledgerCreateTime = storedContract.contract.ledgerCreateTime
    val requestCounter = storedContract.requestCounter
    val creatingTransactionId = storedContract.creatingTransactionIdO
    val template = storedContract.contract.contractInstance.unversioned.template
    val packageId = template.packageId
    val templateId = checked(String2066.tryCreate(template.qualifiedName.toString))

    // TODO(M40): Figure out if we should check that the contract instance remains the same and whether we should update the instance if not.
    // The instance payload is not being updated as uploading this payload on a previously set field is problematic for Oracle when it exceeds 32KB
    // NOTE : By not updating the instance payload the DbContractStore differs from the implementation of the
    // InMemoryContractStore (used for testing) which replaces created and divulged contract in full.
    profile match {
      case _: DbStorage.Profile.Postgres =>
        sqlu"""insert into contracts as c (
                 domain_id, contract_id, instance, metadata, 
                 ledger_create_time, request_counter, creating_transaction_id, package_id, template_id)
               values ($domainId, $contractId, $contract, $metadata, 
                 $ledgerCreateTime, $requestCounter, $creatingTransactionId, $packageId, $templateId)
               on conflict(domain_id, contract_id) do update 
                 set
                   metadata = $metadata,
                   ledger_create_time = $ledgerCreateTime,
                   request_counter = $requestCounter,
                   creating_transaction_id = $creatingTransactionId,
                   package_id = $packageId,
                   template_id = $templateId
                 where (c.creating_transaction_id is null and ($creatingTransactionId is not null or c.request_counter < $requestCounter)) or 
                       (c.creating_transaction_id is not null and $creatingTransactionId is not null and c.request_counter < $requestCounter)"""
      case _: DbStorage.Profile.H2 =>
        sqlu"""merge into contracts
               using dual
               on (domain_id = $domainId and contract_id = ${storedContract.contract.contractId})
               when matched and (
                 (creating_transaction_id is null and ($creatingTransactionId is not null or request_counter < $requestCounter)) or
                 (creating_transaction_id is not null and $creatingTransactionId is not null and request_counter < $requestCounter)
               ) then
                 update set
                   metadata = $metadata,
                   ledger_create_time = $ledgerCreateTime,
                   request_counter = $requestCounter,
                   creating_transaction_id = $creatingTransactionId,
                   package_id = $packageId,
                   template_id = $templateId
               when not matched then
                insert
                 (domain_id, contract_id, instance, metadata,
                  ledger_create_time, request_counter, creating_transaction_id, package_id, template_id)
                 values ($domainId, $contractId, $contract, $metadata,
                  $ledgerCreateTime, $requestCounter, $creatingTransactionId, $packageId, $templateId)"""
      case _: DbStorage.Profile.Oracle =>
        sqlu"""merge into contracts 
               using dual
               on (domain_id = $domainId and contract_id = ${storedContract.contract.contractId})
               when matched then 
                 update set
                   metadata = $metadata,
                   ledger_create_time = $ledgerCreateTime,
                   request_counter = $requestCounter,
                   creating_transaction_id = $creatingTransactionId,
                   package_id = $packageId,
                   template_id = $templateId
                 where (creating_transaction_id is null and ($creatingTransactionId is not null or request_counter < $requestCounter)) or 
                       (creating_transaction_id is not null and $creatingTransactionId is not null and request_counter < $requestCounter)
               when not matched then 
                insert
                 (domain_id, contract_id, instance, metadata, 
                  ledger_create_time, request_counter, creating_transaction_id, package_id, template_id)
                 values ($domainId, $contractId, $contract, $metadata, 
                  $ledgerCreateTime, $requestCounter, $creatingTransactionId, $packageId, $templateId)"""
    }
  }

  def deleteContract(
      id: LfContractId
  )(implicit traceContext: TraceContext): EitherT[Future, UnknownContract, Unit] =
    processingTime
      .eitherTEvent {
        lookupE(id)
          .flatMap { _ =>
            EitherT.right[UnknownContract](
              storage.update_(
                sqlu"delete from contracts where contract_id = $id and domain_id = $domainId",
                functionFullName,
              )
            )
          }
      }
      .thereafter(_ => cache.synchronous().invalidate(id))

  override def deleteIgnoringUnknown(
      contractIds: Iterable[LfContractId]
  )(implicit traceContext: TraceContext): Future[Unit] = {
    import DbStorage.Implicits.BuilderChain.*
    MonadUtil
      .batchedSequentialTraverse_(
        parallelism = 2 * maxDbConnections,
        chunkSize = maxContractIdSqlInListSize.value,
      )(contractIds.toSeq) { cids =>
        val inClause = sql"contract_id in (" ++
          cids
            .map(value => sql"$value")
            .toSeq
            .intercalate(sql", ") ++ sql")"
        processingTime
          .event {
            storage.update_(
              (sql"""delete from contracts where domain_id = $domainId and """ ++ inClause).asUpdate,
              functionFullName,
            )
          }
      }
      .thereafter(_ => cache.synchronous().invalidateAll(contractIds))
  }

  override def deleteDivulged(
      upTo: RequestCounter
  )(implicit traceContext: TraceContext): Future[Unit] =
    processingTime
      .event {
        val query = profile match {
          case _: DbStorage.Profile.Postgres | _: DbStorage.Profile.H2 =>
            sqlu"""delete from contracts
                 where domain_id = $domainId and request_counter <= $upTo and creating_transaction_id is null"""
          case _: DbStorage.Profile.Oracle =>
            // Here we use exactly the same expression as in idx_contracts_request_counter
            // to make sure the index is used.
            sqlu"""delete from contracts
                 where (case when creating_transaction_id is null then domain_id end) = $domainId and 
                       (case when creating_transaction_id is null then request_counter end) <= $upTo"""
        }

        storage.update_(query, functionFullName)
      }
      .thereafter(_ => cache.synchronous().invalidateAll())

  override def lookupStakeholders(ids: Set[LfContractId])(implicit
      traceContext: TraceContext
  ): EitherT[Future, UnknownContracts, Map[LfContractId, Set[LfPartyId]]] =
    NonEmpty.from(ids) match {
      case None => EitherT.rightT(Map.empty)

      case Some(idsNel) =>
        EitherT(
          idsNel.forgetNE.toSeq
            .parTraverse(id => lookupContract(id).toRight(id).value)
            .map(_.collectRight)
            .map { contracts =>
              Either.cond(
                contracts.sizeCompare(ids) == 0,
                contracts
                  .map(contract => contract.contractId -> contract.metadata.stakeholders)
                  .toMap,
                UnknownContracts(ids -- contracts.map(_.contractId).toSet),
              )
            }
        )
    }

  private def storeElements(
      elements: Seq[SerializableContract],
      fn: SerializableContract => StoredContract,
  )(implicit
      ec: ExecutionContext,
      traceContext: TraceContext,
  ): Future[Unit] = {
    elements.parTraverse_ { element =>
      val contract = fn(element)
      storage
        .queryAndUpdate(contractInsert(contract), functionFullName)
        .map { affectedRowsCount =>
          if (affectedRowsCount > 0)
            cache.put(contract.contractId, Future(Option(contract)))
          else
            cache.synchronous().invalidate(contract.contractId)
        }
        .thereafter {
          case Failure(_) => cache.synchronous().invalidate(contract.contractId)
          case Success(_) => ()
        }
    }
  }

  override def contractCount()(implicit traceContext: TraceContext): Future[Int] =
    processingTime.event {
      storage.query(sql"select count(*) from contracts".as[Int].head, functionFullName)
    }
}

object DbContractStore {
  case class AbortedDueToShutdownException(message: String) extends RuntimeException(message)
}
