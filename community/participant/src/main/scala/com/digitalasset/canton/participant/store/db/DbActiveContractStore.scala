// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.db

import cats.data.{Chain, EitherT}
import cats.syntax.foldable.*
import cats.syntax.parallel.*
import com.daml.lf.data.Ref.PackageId
import com.daml.nameof.NameOf.functionFullName
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.RequestCounter
import com.digitalasset.canton.config.CantonRequireTypes.String100
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.PositiveNumeric
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.metrics.TimedLoadGauge
import com.digitalasset.canton.participant.store.ActiveContractSnapshot.ActiveContractIdsChange
import com.digitalasset.canton.participant.store.ActiveContractStore.AcsError
import com.digitalasset.canton.participant.store.db.DbActiveContractStore.*
import com.digitalasset.canton.participant.store.{ActiveContractStore, ContractStore}
import com.digitalasset.canton.participant.util.TimeOfChange
import com.digitalasset.canton.protocol.ContractIdSyntax.*
import com.digitalasset.canton.protocol.{
  LfContractId,
  SourceDomainId,
  TargetDomainId,
  TransferDomainId,
}
import com.digitalasset.canton.resource.DbStorage.{DbAction, SQLActionBuilderChain}
import com.digitalasset.canton.resource.{DbStorage, DbStore}
import com.digitalasset.canton.store.db.{DbDeserializationException, DbPrunableByTimeDomain}
import com.digitalasset.canton.store.{IndexedDomain, IndexedStringStore}
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.FutureInstances.*
import com.digitalasset.canton.util.{Checked, CheckedT, ErrorUtil, IterableUtil}
import slick.jdbc.*
import slick.jdbc.canton.SQLActionBuilder

import scala.Ordered.orderingToOrdered
import scala.annotation.nowarn
import scala.collection.immutable.SortedMap
import scala.concurrent.{ExecutionContext, Future}

/** Active contracts journal
  *
  * This database table has the following indexes to support scaling query performance:
  * - CREATE index active_contracts_dirty_request_reset_idx ON active_contracts (domain_id, request_counter)
  *      used on startup of the SyncDomain to delete all dirty requests.
  * - CREATE index active_contracts_contract_id_idx ON active_contracts (contract_id)
  *      used in conflict detection for point wise lookup of the contract status.
  * - CREATE index active_contracts_ts_domain_id_idx ON active_contracts (ts, domain_id)
  *      used on startup by the SyncDomain to replay ACS changes to the ACS commitment processor.
  */
class DbActiveContractStore(
    override protected val storage: DbStorage,
    protected[this] override val domainId: IndexedDomain,
    enableAdditionalConsistencyChecks: Boolean,
    maxContractIdSqlInListSize: PositiveNumeric[Int],
    indexedStringStore: IndexedStringStore,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit val ec: ExecutionContext)
    extends ActiveContractStore
    with DbStore
    with DbPrunableByTimeDomain[AcsError] {

  import ActiveContractStore.*
  import DbStorage.Implicits.*
  import storage.api.*

  protected[this] override val pruning_status_table = "active_contract_pruning"

  /*
  Consider the scenario where a contract is created on domain D1, then transferred to D2, then to D3 and is finally archived.
  We will have the corresponding entries in the ActiveContractStore:
  - On D1, remoteDomain will initially be None and then Some(D2) (after the transfer-out)
  - On D2, remoteDomain will initially be Some(D1) and then Some(D3) (after the transfer-out)
  - On D3, remoteDomain will initially be Some(D2) and then None (after the archival).
   */
  private case class StoredActiveContract(
      change: ChangeType,
      timestamp: CantonTimestamp,
      rc: RequestCounter,
      remoteDomainIdIndex: Option[Int],
  ) {
    def toContractState(implicit ec: ExecutionContext): Future[ContractState] = {
      val statusF = change match {
        case ChangeType.Activation => Future.successful(Active)
        case ChangeType.Deactivation =>
          // In case of a deactivation, then `remoteDomainIdIndex` is empty iff it is a transfer-out,
          // in which case the corresponding domain is the target domain.
          // The same holds for `remoteDomainIdF`.
          remoteDomainIdF.map(
            _.fold[Status](Archived)(domainId => TransferredAway(TargetDomainId(domainId)))
          )
      }
      statusF.map(ContractState(_, rc, timestamp))
    }

    private def remoteDomainIdF: Future[Option[DomainId]] = {
      remoteDomainIdIndex.fold(Future.successful(None: Option[DomainId])) { index =>
        import TraceContext.Implicits.Empty.*
        IndexedDomain
          .fromDbIndexOT("active_contracts remote domain index", indexedStringStore)(index)
          .map(_.domainId)
          .value
      }
    }

  }

  private implicit val getResultTransferDetail: GetResult[TransferDetail] =
    GetResult(r => TransferDetail(DomainId.getResultDomainId(r)))

  private implicit val getResultStoredActiveContract: GetResult[StoredActiveContract] =
    GetResult(r =>
      StoredActiveContract(
        ChangeType.getResultChangeType(r),
        GetResult[CantonTimestamp].apply(r),
        GetResult[RequestCounter].apply(r),
        r.nextIntOption(),
      )
    )

  override protected val processingTime: TimedLoadGauge =
    storage.metrics.loadGaugeM("active-contract-store")

  def createContracts(contractIds: Seq[LfContractId], toc: TimeOfChange)(implicit
      traceContext: TraceContext
  ): CheckedT[Future, AcsError, AcsWarning, Unit] =
    processingTime.checkedTEvent {

      for {
        _ <- bulkInsert(contractIds, toc, ChangeType.Activation, remoteDomain = None)
        _ <-
          if (enableAdditionalConsistencyChecks) {
            performUnlessClosingCheckedT(
              "additional-consistency-check",
              Checked.result[AcsError, AcsWarning, Unit](
                logger.debug(
                  "Could not perform additional consistency check because node is shutting down"
                )
              ),
            ) {
              contractIds.parTraverse_ { contractId =>
                for {
                  _ <- checkCreateArchiveAtUnique(contractId, toc, ChangeType.Activation)
                  _ <- checkChangesBeforeCreation(contractId, toc)
                  _ <- checkTocAgainstEarliestArchival(contractId, toc)
                } yield ()
              }
            }
          } else {
            CheckedT.resultT[Future, AcsError, AcsWarning](())
          }
      } yield ()
    }

  def archiveContracts(contractIds: Seq[LfContractId], toc: TimeOfChange)(implicit
      traceContext: TraceContext
  ): CheckedT[Future, AcsError, AcsWarning, Unit] =
    processingTime.checkedTEvent {
      for {
        _ <- bulkInsert(contractIds, toc, ChangeType.Deactivation, remoteDomain = None)
        _ <-
          if (enableAdditionalConsistencyChecks) {
            performUnlessClosingCheckedT(
              "additional-consistency-check",
              Checked.result[AcsError, AcsWarning, Unit](
                logger.debug(
                  "Could not perform additional consistency check because node is shutting down"
                )
              ),
            ) {
              contractIds.parTraverse_ { contractId =>
                for {
                  _ <- checkCreateArchiveAtUnique(contractId, toc, ChangeType.Deactivation)
                  _ <- checkChangesAfterArchival(contractId, toc)
                  _ <- checkTocAgainstLatestCreation(contractId, toc)
                } yield ()
              }
            }
          } else {
            CheckedT.resultT[Future, AcsError, AcsWarning](())
          }
      } yield ()
    }

  private def indexedDomains(
      contractAndDomain: Seq[(LfContractId, TransferDomainId)]
  ): CheckedT[Future, AcsError, AcsWarning, Seq[(LfContractId, IndexedDomain)]] = {
    CheckedT.result(contractAndDomain.parTraverse { case (contractId, domainId) =>
      IndexedDomain
        .indexed(indexedStringStore)(domainId.unwrap)
        .map(s => (contractId, s))
    })
  }

  def transferInContracts(transferIns: Seq[(LfContractId, SourceDomainId)], toc: TimeOfChange)(
      implicit traceContext: TraceContext
  ): CheckedT[Future, AcsError, AcsWarning, Unit] =
    processingTime.checkedTEvent {
      for {
        bySourceDomainIndexed <- indexedDomains(transferIns).map(_.groupBy(_._2).toList)
        _ <- bySourceDomainIndexed.parTraverse_ { case (sourceDomain, contractIdsAndDomain) =>
          bulkInsert(
            contractIdsAndDomain.map(_._1),
            toc,
            ChangeType.Activation,
            remoteDomain = Some(sourceDomain),
          )
        }
        _ <- checkTransfersConsistency(transferIns, toc)
      } yield ()
    }

  def transferOutContracts(transferOuts: Seq[(LfContractId, TargetDomainId)], toc: TimeOfChange)(
      implicit traceContext: TraceContext
  ): CheckedT[Future, AcsError, AcsWarning, Unit] =
    processingTime.checkedTEvent {
      for {
        byTargetIndexed <- indexedDomains(transferOuts).map(_.groupBy(_._2).toList)
        _ <- byTargetIndexed.parTraverse_ { case (targetDomain, contractIdsAndDomain) =>
          bulkInsert(
            contractIdsAndDomain.map(_._1),
            toc,
            ChangeType.Deactivation,
            remoteDomain = Some(targetDomain),
          )
        }
        _ <- checkTransfersConsistency(transferOuts, toc)
      } yield ()
    }

  override def fetchStates(
      contractIds: Iterable[LfContractId]
  )(implicit traceContext: TraceContext): Future[Map[LfContractId, ContractState]] = {
    storage.profile match {
      case _: DbStorage.Profile.H2 | _: DbStorage.Profile.Oracle =>
        // With H2, it is faster to do lookup contracts individually than to use a range query
        contractIds
          .to(LazyList)
          .parTraverseFilter { contractId =>
            storage
              .querySingle(fetchContractStateQuery(contractId), functionFullName)
              .semiflatMap(storedContract =>
                storedContract.toContractState.map(res => (contractId -> res))
              )
              .value
          }
          .map(_.toMap)
      case _: DbStorage.Profile.Postgres =>
        NonEmpty.from(contractIds.toSeq) match {
          case None => Future.successful(Map.empty)
          case Some(contractIdsNel) =>
            import DbStorage.Implicits.BuilderChain.*

            val queries =
              DbStorage
                .toInClauses_("contract_id", contractIdsNel, maxContractIdSqlInListSize)
                .map { inClause =>
                  val query =
                    sql"""
                with ordered_changes(contract_id, change, ts, request_counter, remote_domain_id, row_num) as (
                  select contract_id, change, ts, request_counter, remote_domain_id,
                     ROW_NUMBER() OVER (partition by domain_id, contract_id order by ts desc, request_counter desc, change asc)
                   from active_contracts
                   where domain_id = $domainId and """ ++ inClause ++
                      sql"""
                )
                select contract_id, change, ts, request_counter, remote_domain_id
                from ordered_changes
                where row_num = 1;
                """

                  query.as[(LfContractId, StoredActiveContract)]
                }

            storage
              .sequentialQueryAndCombine(queries, functionFullName)
              .flatMap(_.toList.parTraverse { case (id, contract) =>
                contract.toContractState.map(cs => (id, cs))
              })
              .map(foundContracts => foundContracts.toMap)
        }

    }
  }

  override def packageUsage(
      pkg: PackageId,
      contractStore: ContractStore,
  )(implicit traceContext: TraceContext): Future[Option[(LfContractId)]] = {
    // The contractStore is unused
    // As we can directly query daml_contracts from the database

    import DbStorage.Implicits.*
    import DbStorage.Implicits.BuilderChain.*

    // TODO(i9480): Integrate with performance tests to check that we can remove packages when there are many contracts.

    val limitStatement: SQLActionBuilder = storage.profile match {
      case _: DbStorage.Profile.Oracle =>
        sql"""fetch first 1 rows only"""
      case _ => sql"limit 1"
    }

    val query =
      (sql"""
                with ordered_changes(contract_id, package_id, change, ts, request_counter, remote_domain_id, row_num) as (
                  select active_contracts.contract_id, contracts.package_id, change, ts, active_contracts.request_counter, remote_domain_id,
                     ROW_NUMBER() OVER (
                     partition by active_contracts.domain_id, active_contracts.contract_id
                     order by
                        ts desc,
                        active_contracts.request_counter desc,
                        change asc
                     )
                   from active_contracts join contracts
                       on active_contracts.contract_id = contracts.contract_id
                              and active_contracts.domain_id = contracts.domain_id
                   where active_contracts.domain_id = $domainId
                    and contracts.package_id = $pkg
                )
                select contract_id, package_id
                from ordered_changes
                where row_num = 1
                and change = 'activation'
                """ ++ limitStatement).as[(LfContractId)]

    val queryResult = storage.query(query, functionFullName)
    queryResult.map(_.headOption)

  }

  override def snapshot(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[SortedMap[LfContractId, CantonTimestamp]] =
    processingTime.event {
      logger.debug(s"Obtaining ACS snapshot at $timestamp")
      storage
        .query(
          snapshotQuery(SnapshotQueryParameter.Ts(timestamp), None),
          functionFullName,
        )
        .map(snapshot => SortedMap(snapshot *))
    }

  override def snapshot(rc: RequestCounter)(implicit
      traceContext: TraceContext
  ): Future[SortedMap[LfContractId, RequestCounter]] = processingTime.event {
    logger.debug(s"Obtaining ACS snapshot at $rc")
    storage
      .query(
        snapshotQuery(SnapshotQueryParameter.Rc(rc), None),
        functionFullName,
      )
      .map(snapshot => SortedMap(snapshot *))
  }

  override def contractSnapshot(contractIds: Set[LfContractId], timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[Map[LfContractId, CantonTimestamp]] = {
    processingTime.event {
      if (contractIds.isEmpty) Future.successful(Map.empty)
      else
        storage
          .query(
            snapshotQuery(SnapshotQueryParameter.Ts(timestamp), Some(contractIds)),
            functionFullName,
          )
          .map(_.toMap)
    }
  }

  private[this] def snapshotQuery[T](
      p: SnapshotQueryParameter[T],
      contractIds: Option[Set[LfContractId]],
  ): DbAction.ReadOnly[Seq[(LfContractId, T)]] = {
    import DbStorage.Implicits.BuilderChain.*

    val idsO = contractIds.map { ids =>
      sql"(" ++ ids.toList.map(id => sql"$id").intercalate(sql", ") ++ sql")"
    }

    implicit val getResultT: GetResult[T] = p.getResult

    // somehow, the compiler complains if it is not defined but indicates it as being unused
    @nowarn("cat=unused") implicit val setParameterT: SetParameter[T] = p.setParameter

    val ordering = sql" order by #${p.attribute} asc"

    storage.profile match {
      case _: DbStorage.Profile.H2 =>
        (sql"""
          select distinct(contract_id), #${p.attribute}
          from active_contracts AC
          where not exists(select * from active_contracts AC2 where domain_id = $domainId and AC.contract_id = AC2.contract_id
            and AC2.#${p.attribute} <= ${p.bound}
            and ((AC.ts, AC.request_counter) < (AC2.ts, AC2.request_counter)
              or (AC.ts = AC2.ts and AC.request_counter = AC2.request_counter and AC2.change = ${ChangeType.Deactivation})))
           and AC.#${p.attribute} <= ${p.bound} and domain_id = $domainId""" ++
          idsO.fold(sql"")(ids => sql" and AC.contract_id in " ++ ids) ++ ordering)
          .as[(LfContractId, T)]
      case _: DbStorage.Profile.Postgres =>
        (sql"""
          select distinct(contract_id), AC3.#${p.attribute} from active_contracts AC1
          join lateral
            (select #${p.attribute}, change from active_contracts AC2 where domain_id = $domainId
             and AC2.contract_id = AC1.contract_id and #${p.attribute} <= ${p.bound} order by ts desc, request_counter desc, change asc #${storage
            .limit(1)}) as AC3 on true
          where AC1.domain_id = $domainId and AC3.change = CAST(${ChangeType.Activation} as change_type)""" ++
          idsO.fold(sql"")(ids => sql" and AC1.contract_id in " ++ ids) ++ ordering)
          .as[(LfContractId, T)]
      case _: DbStorage.Profile.Oracle =>
        (sql"""select distinct(contract_id), AC3.#${p.attribute} from active_contracts AC1, lateral
          (select #${p.attribute}, change from active_contracts AC2 where domain_id = $domainId
             and AC2.contract_id = AC1.contract_id and #${p.attribute} <= ${p.bound}
             order by ts desc, request_counter desc, change desc
             fetch first 1 row only) AC3
          where AC1.domain_id = $domainId and AC3.change = 'activation'""" ++
          idsO.fold(sql"")(ids => sql" and AC1.contract_id in " ++ ids) ++ ordering)
          .as[(LfContractId, T)]
    }
  }

  override def doPrune(beforeAndIncluding: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): EitherT[Future, AcsError, Unit] = processingTime.eitherTEvent {

    // For each contract select the last deactivation before or at the timestamp.
    // If such a deactivation exists then delete all acs records up to and including the deactivation

    val query = storage.profile match {
      case _: DbStorage.Profile.Postgres =>
        sqlu"""
      	  with deactivation_counter(contract_id, request_counter) as (
            select contract_id, max(request_counter)
            from active_contracts
            where domain_id = ${domainId}
            and change = cast('deactivation' as change_type)
            and ts <= ${beforeAndIncluding}
            group by contract_id
          )
		  delete from active_contracts
          where (domain_id, contract_id, ts, request_counter, change) in (
			select ac.domain_id, ac.contract_id, ac.ts, ac.request_counter, ac.change
            from deactivation_counter dc
            join active_contracts ac on ac.domain_id = ${domainId} and ac.contract_id = dc.contract_id
            where ac.request_counter <= dc.request_counter
          );
        """
      case _: DbStorage.Profile.H2 =>
        sqlu"""
      	  with deactivation_counter(contract_id, request_counter) as (
            select contract_id, max(request_counter)
            from active_contracts
            where domain_id = ${domainId}
            and change = ${ChangeType.Deactivation}
            and ts <= ${beforeAndIncluding}
            group by contract_id
          )
		  delete from active_contracts
          where (domain_id, contract_id, ts, request_counter, change) in (
			select ac.domain_id, ac.contract_id, ac.ts, ac.request_counter, ac.change
            from deactivation_counter dc
            join active_contracts ac on ac.domain_id = ${domainId} and ac.contract_id = dc.contract_id
            where ac.request_counter <= dc.request_counter
          );
          """
      case _: DbStorage.Profile.Oracle =>
        sqlu"""delete from active_contracts where rowid in (
            with deactivation_counter(contract_id, request_counter) as (
                select contract_id, max(request_counter)
                from active_contracts
                where domain_id = ${domainId}
                and change = 'deactivation'
                and ts <= ${beforeAndIncluding}
                group by contract_id
            )
            select ac.rowid
            from deactivation_counter dc
            join active_contracts ac on ac.domain_id = ${domainId} and ac.contract_id = dc.contract_id
            where ac.request_counter <= dc.request_counter
        )"""
    }
    EitherT.right(
      for {
        nrPruned <- storage.queryAndUpdate(query, functionFullName)
      } yield {
        logger.info(
          s"Pruned at least $nrPruned entries from the ACS of domain $domainId older or equal to $beforeAndIncluding"
        )
      }
    )
  }

  def deleteSince(criterion: RequestCounter)(implicit traceContext: TraceContext): Future[Unit] =
    processingTime.event {
      val query =
        sqlu"delete from active_contracts where domain_id = $domainId and request_counter >= $criterion"
      storage
        .update(query, functionFullName)
        .map(count =>
          logger.debug(s"DeleteSince on $criterion removed at least $count ACS entries")
        )
    }

  override def changesBetween(fromExclusive: TimeOfChange, toInclusive: TimeOfChange)(implicit
      traceContext: TraceContext
  ): Future[LazyList[(TimeOfChange, ActiveContractIdsChange)]] =
    processingTime.event {
      ErrorUtil.requireArgument(
        fromExclusive <= toInclusive,
        s"Provided timestamps are in the wrong order: $fromExclusive and $toInclusive",
      )
      val changeQuery =
        sql"""select ts, request_counter, contract_id, change from active_contracts  where domain_id = $domainId and
                           ((ts = ${fromExclusive.timestamp} and request_counter > ${fromExclusive.rc}) or ts > ${fromExclusive.timestamp})
                           and
                           ((ts = ${toInclusive.timestamp} and request_counter <= ${toInclusive.rc}) or ts <= ${toInclusive.timestamp})
                           order by ts asc, request_counter asc"""
          .as[(CantonTimestamp, RequestCounter, LfContractId, ChangeType)]

      storage.query(changeQuery, operationName = "ACS: get changes between").map { res =>
        val groupedByTs = IterableUtil.spansBy(res)(entry => (entry._1, entry._2))
        groupedByTs.map { case ((ts, rc), changes) =>
          val (acts, deacts) = changes.partition { case (_, _, _, changeType) =>
            changeType == ChangeType.Activation
          }
          TimeOfChange(rc, ts) -> ActiveContractIdsChange(
            acts.map(_._3).toSet,
            deacts.map(_._3).toSet,
          )
        }
      }

    }

  override private[participant] def contractCount(
      timestamp: CantonTimestamp
  )(implicit traceContext: TraceContext): Future[Int] = {
    storage.query(
      sql"select count(distinct contract_id) from active_contracts where ts <= $timestamp"
        .as[Int]
        .head,
      functionFullName,
    )
  }

  private def checkTransfersConsistency(
      transfers: Seq[(LfContractId, TransferDomainId)],
      toc: TimeOfChange,
  )(implicit traceContext: TraceContext): CheckedT[Future, AcsError, AcsWarning, Unit] =
    if (enableAdditionalConsistencyChecks) {
      transfers.parTraverse_ { case (contractId, _) =>
        for {
          _ <- checkTocAgainstLatestCreation(contractId, toc)
          _ <- checkTocAgainstEarliestArchival(contractId, toc)
        } yield ()
      }
    } else CheckedT.pure(())

  private def checkChangesBeforeCreation(contractId: LfContractId, toc: TimeOfChange)(implicit
      traceContext: TraceContext
  ): CheckedT[Future, AcsError, AcsWarning, Unit] = {
    val query =
      storage.profile match {
        case _: DbStorage.Profile.Oracle =>
          sql"""select ts, request_counter from active_contracts
              where domain_id = $domainId and contract_id = $contractId
                and (ts < ${toc.timestamp} or (ts = ${toc.timestamp} and request_counter < ${toc.rc})) and operation != ${OperationType.Create}
              order by ts desc, request_counter desc, change asc"""
        case _ =>
          sql"""select ts, request_counter from active_contracts
              where domain_id = $domainId and contract_id = $contractId
                and (ts, request_counter) < (${toc.timestamp}, ${toc.rc}) and operation != CAST(${OperationType.Create} as operation_type)
              order by (ts, request_counter, change) desc"""
      }

    val result = storage.query(query.as[(CantonTimestamp, RequestCounter)], functionFullName)

    CheckedT(result.map { changes =>
      val warnings = changes.map { case (changeTs, changeRc) =>
        ChangeBeforeCreation(contractId, toc, TimeOfChange(changeRc, changeTs))
      }
      Checked.unit.appendNonaborts(Chain.fromSeq(warnings))
    })
  }

  private def checkChangesAfterArchival(contractId: LfContractId, toc: TimeOfChange)(implicit
      traceContext: TraceContext
  ): CheckedT[Future, AcsError, AcsWarning, Unit] = {

    val q =
      storage.profile match {
        case _: DbStorage.Profile.Oracle =>
          sql"""select ts, request_counter from active_contracts
              where domain_id = $domainId and contract_id = $contractId
                and (ts > ${toc.timestamp} or (ts = ${toc.timestamp} and request_counter > ${toc.rc})) and operation != ${OperationType.Archive}
              order by ts desc, request_counter desc, change asc"""

        case _ =>
          sql"""select ts, request_counter from active_contracts
              where domain_id = $domainId and contract_id = $contractId
                and (ts, request_counter) > (${toc.timestamp}, ${toc.rc}) and operation != CAST(${OperationType.Archive} as operation_type)
              order by (ts, request_counter, change) desc"""
      }

    val result = storage.query(q.as[(CantonTimestamp, RequestCounter)], functionFullName)

    CheckedT(result.map { changes =>
      val warnings = changes.map { case (changeTs, changeRc) =>
        ChangeAfterArchival(contractId, toc, TimeOfChange(changeRc, changeTs))
      }
      Checked.unit.appendNonaborts(Chain.fromSeq(warnings))
    })
  }

  private def checkCreateArchiveAtUnique(
      contractId: LfContractId,
      toc: TimeOfChange,
      change: ChangeType,
  )(implicit traceContext: TraceContext): CheckedT[Future, AcsError, AcsWarning, Unit] = {
    val operation = change match {
      case ChangeType.Activation => OperationType.Create
      case ChangeType.Deactivation => OperationType.Archive
    }
    val order = change match {
      case ChangeType.Activation => "desc" // find the latest creation
      case ChangeType.Deactivation => "asc" // find the earliest archival
    }
    val q =
      storage.profile match {
        case _: DbStorage.Profile.Oracle =>
          sql"""
        select ts, request_counter from active_contracts
        where domain_id = $domainId and contract_id = $contractId
          and (ts <> ${toc.timestamp} or request_counter <> ${toc.rc})
          and change = $change
          and operation = $operation
        order by ts #$order, request_counter #$order
        #${storage.limit(1)}
         """

        case _ =>
          sql"""
        select ts, request_counter from active_contracts
        where domain_id = $domainId and contract_id = $contractId
          and (ts, request_counter) <> (${toc.timestamp}, ${toc.rc})
          and change = CAST($change as change_type)
          and operation = CAST($operation as operation_type)
        order by (ts, request_counter) #$order
        #${storage.limit(1)}
         """

      }
    val query = q.as[(CantonTimestamp, RequestCounter)]
    CheckedT(storage.query(query, functionFullName).map { changes =>
      changes.headOption.fold(Checked.unit[AcsError, AcsWarning]) { case (changeTs, changeRc) =>
        val warn =
          if (change == ChangeType.Activation)
            DoubleContractCreation(contractId, TimeOfChange(changeRc, changeTs), toc)
          else DoubleContractArchival(contractId, TimeOfChange(changeRc, changeTs), toc)
        Checked.continue(warn)
      }
    })
  }

  /** Check that the given [[com.digitalasset.canton.participant.util.TimeOfChange]]
    * is not before the latest creation. Otherwise return a [[ChangeBeforeCreation]].
    */
  private def checkTocAgainstLatestCreation(contractId: LfContractId, toc: TimeOfChange)(implicit
      traceContext: TraceContext
  ): CheckedT[Future, AcsError, AcsWarning, Unit] =
    CheckedT(storage.query(fetchLatestCreation(contractId), functionFullName).map {
      case None => Checked.unit
      case Some(StoredActiveContract(_, ts, rc, _)) =>
        val storedToc = TimeOfChange(rc, ts)
        if (storedToc > toc) Checked.continue(ChangeBeforeCreation(contractId, storedToc, toc))
        else Checked.unit
    })

  /** Check that the given [[com.digitalasset.canton.participant.util.TimeOfChange]]
    * is not after the earliest archival. Otherwise return a [[ChangeAfterArchival]].
    */
  private def checkTocAgainstEarliestArchival(contractId: LfContractId, toc: TimeOfChange)(implicit
      traceContext: TraceContext
  ): CheckedT[Future, AcsError, AcsWarning, Unit] =
    CheckedT(storage.query(fetchEarliestArchival(contractId), functionFullName).map {
      case None => Checked.unit
      case Some(StoredActiveContract(_, ts, rc, _)) =>
        val storedToc = TimeOfChange(rc, ts)
        if (storedToc < toc) Checked.continue(ChangeAfterArchival(contractId, storedToc, toc))
        else Checked.unit
    })

  private def bulkInsert(
      contractIds: Seq[LfContractId],
      toc: TimeOfChange,
      change: ChangeType,
      remoteDomain: Option[IndexedDomain],
  )(implicit traceContext: TraceContext): CheckedT[Future, AcsError, AcsWarning, Unit] = {
    val operation = change match {
      case ChangeType.Activation =>
        if (remoteDomain.isEmpty) OperationType.Create else OperationType.TransferIn
      case ChangeType.Deactivation =>
        if (remoteDomain.isEmpty) OperationType.Archive else OperationType.TransferOut
    }

    val insertQuery = storage.profile match {
      case _: DbStorage.Profile.Oracle =>
        """merge /*+ INDEX ( active_contracts ( contract_id, ts, request_counter, change, domain_id ) ) */
          |into active_contracts
          |using (select ? contract_id, ? ts, ? request_counter, ? change, ? domain_id from dual) input
          |on (active_contracts.contract_id = input.contract_id and active_contracts.ts = input.ts and
          |    active_contracts.request_counter = input.request_counter and active_contracts.change = input.change and
          |    active_contracts.domain_id = input.domain_id)
          |when not matched then
          |  insert (contract_id, ts, request_counter, change, domain_id, operation, remote_domain_id)
          |  values (input.contract_id, input.ts, input.request_counter, input.change, input.domain_id, ?, ?)""".stripMargin
      case _: DbStorage.Profile.H2 | _: DbStorage.Profile.Postgres =>
        """insert into active_contracts(contract_id, ts, request_counter, change, domain_id, operation, remote_domain_id)
          values (?, ?, ?, CAST(? as change_type), ?, CAST(? as operation_type), ?)
          on conflict do nothing"""
    }
    val insertAll = DbStorage.bulkOperation_(insertQuery, contractIds, storage.profile) {
      pp => contractId =>
        pp >> contractId
        pp >> toc.timestamp
        pp >> toc.rc
        pp >> change
        pp >> domainId
        pp >> operation
        pp >> remoteDomain
    }

    def checkIdempotence(
        idsToCheck: NonEmpty[Seq[LfContractId]]
    ): CheckedT[Future, AcsError, AcsWarning, Unit] = {
      import DbStorage.Implicits.BuilderChain.*
      val contractIdsNotInsertedInClauses =
        DbStorage.toInClauses_("contract_id", idsToCheck, maxContractIdSqlInListSize)

      def query(inClause: SQLActionBuilderChain) = storage.profile match {
        case _: DbStorage.Profile.Oracle =>
          sql"select contract_id, remote_domain_id from active_contracts where domain_id = $domainId and " ++ inClause ++
            sql" and ts = ${toc.timestamp} and request_counter = ${toc.rc} and change = $change and (operation <> $operation or " ++
            (if (remoteDomain.isEmpty) sql"remote_domain_id is not null"
             else sql"remote_domain_id <> $remoteDomain") ++ sql")"
        case _ =>
          sql"select contract_id, remote_domain_id from active_contracts where domain_id = $domainId and " ++ inClause ++
            sql" and ts = ${toc.timestamp} and request_counter = ${toc.rc} and change = CAST($change as change_type) and (operation <> CAST($operation as operation_type) or " ++
            (if (remoteDomain.isEmpty) sql"remote_domain_id is not null"
             else sql"remote_domain_id <> $remoteDomain") ++ sql")"
      }

      val queries =
        contractIdsNotInsertedInClauses.map(inClause =>
          query(inClause).as[(LfContractId, Option[Int])]
        )
      val results: Future[List[(LfContractId, Option[DomainId])]] = storage
        .sequentialQueryAndCombine(queries, functionFullName)
        .flatMap(_.toList.parTraverseFilter {
          case (cid, None) => Future.successful(Some((cid, None)))
          case (cid, Some(remoteIdx)) =>
            IndexedDomain
              .fromDbIndexOT("active_contracts", indexedStringStore)(remoteIdx)
              .map { indexed =>
                (cid, Some(indexed.item))
              }
              .value
        })

      CheckedT(results.map { presentWithOtherValues =>
        val isActivation = change == ChangeType.Activation
        val detail = ActivenessChangeDetail(remoteDomain.map(_.item))
        presentWithOtherValues.traverse_ { case (contractId, previousRemoteDomain) =>
          val warn =
            if (isActivation)
              SimultaneousActivation(
                contractId,
                toc,
                ActivenessChangeDetail(previousRemoteDomain),
                detail,
              )
            else
              SimultaneousDeactivation(
                contractId,
                toc,
                ActivenessChangeDetail(previousRemoteDomain),
                detail,
              )
          Checked.continue(warn)
        }
      })
    }

    CheckedT.result(storage.queryAndUpdate(insertAll, functionFullName)).flatMap { (_: Unit) =>
      if (enableAdditionalConsistencyChecks) {
        // Check all contracts whether they have been inserted or are already there
        // We don't analyze the update counts
        // so that we can use the fast IGNORE_ROW_ON_DUPKEY_INDEX directive in Oracle
        NonEmpty.from(contractIds).map(checkIdempotence).getOrElse(CheckedT.pure(()))
      } else CheckedT.pure(())
    }
  }

  private def fetchLatestCreation(
      contractId: LfContractId
  ): DbAction.ReadOnly[Option[StoredActiveContract]] =
    fetchContractStateQuery(contractId, Some(OperationType.Create))

  private def fetchEarliestArchival(
      contractId: LfContractId
  ): DbAction.ReadOnly[Option[StoredActiveContract]] =
    fetchContractStateQuery(contractId, Some(OperationType.Archive), descending = false)

  private def fetchContractStateQuery(
      contractId: LfContractId,
      operationFilter: Option[OperationType] = None,
      descending: Boolean = true,
  ): DbAction.ReadOnly[Option[StoredActiveContract]] = {

    import DbStorage.Implicits.BuilderChain.*

    val baseQuery = sql"""select change, ts, request_counter, remote_domain_id from active_contracts
                          where domain_id = $domainId and contract_id = $contractId"""
    val opFilterQuery =
      storage.profile match {
        case _: DbStorage.Profile.Oracle =>
          operationFilter.fold(sql" ")(o => sql" and operation = $o")
        case _ =>
          operationFilter.fold(sql" ")(o => sql" and operation = CAST($o as operation_type)")
      }
    val (normal_order, reversed_order) = if (descending) ("desc", "asc") else ("asc", "desc")
    val orderQuery = storage.profile match {
      case _: DbStorage.Profile.Oracle =>
        sql" order by ts #$normal_order, request_counter #$normal_order, change #$normal_order #${storage
            .limit(1)}"
      case _ =>
        sql" order by ts #$normal_order, request_counter #$normal_order, change #$reversed_order #${storage
            .limit(1)}"
    }
    val query = baseQuery ++ opFilterQuery ++ orderQuery

    query.as[StoredActiveContract].headOption
  }
}

private object DbActiveContractStore {
  sealed trait SnapshotQueryParameter[T] {
    def attribute: String

    def bound: T

    def getResult: GetResult[T]
    def setParameter: SetParameter[T]
  }

  object SnapshotQueryParameter {
    final case class Ts(bound: CantonTimestamp) extends SnapshotQueryParameter[CantonTimestamp] {
      val attribute = "ts"
      val getResult: GetResult[CantonTimestamp] = implicitly[GetResult[CantonTimestamp]]
      val setParameter: SetParameter[CantonTimestamp] = implicitly[SetParameter[CantonTimestamp]]
    }

    final case class Rc(bound: RequestCounter) extends SnapshotQueryParameter[RequestCounter] {
      val attribute = "request_counter"
      val getResult: GetResult[RequestCounter] = implicitly[GetResult[RequestCounter]]
      val setParameter: SetParameter[RequestCounter] = implicitly[SetParameter[RequestCounter]]
    }
  }

  sealed trait ChangeType {
    def name: String

    // lazy val so that `kind` is initialized first in the subclasses
    final lazy val toDbPrimitive: String100 =
      // The Oracle DB schema allows up to 100 chars; Postgres, H2 map this to an enum
      String100.tryCreate(name)
  }

  object ChangeType {
    case object Activation extends ChangeType {
      override val name = "activation"
    }

    case object Deactivation extends ChangeType {
      override val name = "deactivation"
    }

    implicit val setParameterChangeType: SetParameter[ChangeType] = (v, pp) => pp >> v.toDbPrimitive
    implicit val getResultChangeType: GetResult[ChangeType] = GetResult(r =>
      r.nextString() match {
        case ChangeType.Activation.name => ChangeType.Activation
        case ChangeType.Deactivation.name => ChangeType.Deactivation
        case unknown => throw new DbDeserializationException(s"Unknown change type [$unknown]")
      }
    )
  }

  sealed trait OperationType extends Product with Serializable {
    val name: String

    // lazy val so that `kind` is initialized first in the subclasses
    final lazy val toDbPrimitive: String100 =
      // The Oracle DB schema allows up to 100 chars; Postgres, H2 map this to an enum
      String100.tryCreate(name)
  }

  object OperationType {
    case object Create extends OperationType {
      override val name = "create"
    }

    case object Archive extends OperationType {
      override val name = "archive"
    }

    case object TransferIn extends OperationType {
      override val name = "transfer-in"
    }

    case object TransferOut extends OperationType {
      override val name = "transfer-out"
    }

    implicit val setParameterOperationType: SetParameter[OperationType] = (v, pp) =>
      pp >> v.toDbPrimitive
    implicit val getResultChangeType: GetResult[OperationType] = GetResult(r =>
      r.nextString() match {
        case OperationType.Create.name => OperationType.Create
        case OperationType.Archive.name => OperationType.Archive
        case OperationType.TransferIn.name => OperationType.TransferIn
        case OperationType.TransferOut.name => OperationType.TransferOut
        case unknown => throw new DbDeserializationException(s"Unknown operation type [$unknown]")
      }
    )
  }

}
