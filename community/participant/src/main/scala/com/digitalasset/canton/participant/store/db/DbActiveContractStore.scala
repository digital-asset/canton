// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.db

import cats.data.{Chain, EitherT, NonEmptyList}
import cats.syntax.foldable._
import cats.syntax.list._
import cats.syntax.traverse._
import cats.syntax.traverseFilter._
import com.daml.lf.data.Ref.PackageId
import com.digitalasset.canton.DomainId
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.{PositiveNumeric, String100}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.metrics.MetricHandle.GaugeM
import com.digitalasset.canton.metrics.TimedLoadGauge
import com.digitalasset.canton.participant.RequestCounter
import com.digitalasset.canton.participant.store.ActiveContractSnapshot.ActiveContractIdsChange
import com.digitalasset.canton.participant.store.ActiveContractStore.AcsError
import com.digitalasset.canton.participant.store.{ActiveContractStore, ContractStore}
import com.digitalasset.canton.participant.util.TimeOfChange
import com.digitalasset.canton.protocol.ContractIdSyntax._
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.resource.{DbStorage, DbStore}
import com.digitalasset.canton.resource.DbStorage.{DbAction, SQLActionBuilderChain}
import com.digitalasset.canton.store.db.{DbDeserializationException, DbPrunableByTimeDomain}
import com.digitalasset.canton.store.{IndexedDomain, IndexedStringStore}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{Checked, CheckedT, ErrorUtil, IterableUtil}
import io.functionmeta.functionFullName
import slick.jdbc._
import slick.jdbc.canton.SQLActionBuilder

import scala.Ordered.orderingToOrdered
import scala.collection.immutable.SortedMap
import scala.concurrent.{ExecutionContext, Future}

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

  import ActiveContractStore._
  import DbStorage.Implicits._
  import storage.api._

  protected[this] override val pruning_status_table = "active_contract_pruning"

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
          remoteDomainIdF.map(_.fold[Status](Archived)(TransferredAway))
      }
      statusF.map { status =>
        ContractState(status, rc, timestamp)
      }
    }

    private def remoteDomainIdF: Future[Option[DomainId]] = {
      remoteDomainIdIndex.fold(Future.successful(None: Option[DomainId])) { index =>
        import TraceContext.Implicits.Empty._
        IndexedDomain
          .fromDbIndexOT("active_contracts remote domain index", indexedStringStore)(index)
          .map { x =>
            x.domainId
          }
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
        r.nextLong(),
        r.nextIntOption(),
      )
    )

  case class DbAcsError(reason: String) extends AcsError

  override protected val processingTime: GaugeM[TimedLoadGauge, Double] =
    storage.metrics.loadGaugeM("active-contract-store")

  def createContracts(contractIds: Seq[LfContractId], toc: TimeOfChange)(implicit
      traceContext: TraceContext
  ): CheckedT[Future, AcsError, AcsWarning, Unit] =
    processingTime.metric.checkedTEvent {

      for {
        _ <- bulkInsert(contractIds, toc, ChangeType.Activation, remoteDomain = None)
        _ <-
          if (enableAdditionalConsistencyChecks) {
            contractIds.traverse_ { contractId =>
              for {
                _ <- checkCreateArchiveAtUnique(contractId, toc, ChangeType.Activation)
                _ <- checkChangesBeforeCreation(contractId, toc)
                _ <- checkTocAgainstEarliestArchival(contractId, toc)
              } yield ()
            }
          } else {
            CheckedT.resultT[Future, AcsError, AcsWarning](())
          }
      } yield ()
    }

  def archiveContracts(contractIds: Seq[LfContractId], toc: TimeOfChange)(implicit
      traceContext: TraceContext
  ): CheckedT[Future, AcsError, AcsWarning, Unit] =
    processingTime.metric.checkedTEvent {
      for {
        _ <- bulkInsert(contractIds, toc, ChangeType.Deactivation, remoteDomain = None)
        _ <-
          if (enableAdditionalConsistencyChecks) {
            contractIds.traverse_ { contractId =>
              for {
                _ <- checkCreateArchiveAtUnique(contractId, toc, ChangeType.Deactivation)
                _ <- checkChangesAfterArchival(contractId, toc)
                _ <- checkTocAgainstLatestCreation(contractId, toc)
              } yield ()
            }
          } else {
            CheckedT.resultT[Future, AcsError, AcsWarning](())
          }
      } yield ()
    }

  private def indexedDomains(
      contractAndDomain: Seq[(LfContractId, DomainId)]
  ): CheckedT[Future, AcsError, AcsWarning, Seq[(LfContractId, IndexedDomain)]] = {
    CheckedT.result(contractAndDomain.traverse { case (contractId, domainId) =>
      IndexedDomain
        .indexed(indexedStringStore)(domainId)
        .map(s => (contractId, s))
    })
  }

  def transferInContracts(transferIns: Seq[(LfContractId, DomainId)], toc: TimeOfChange)(implicit
      traceContext: TraceContext
  ): CheckedT[Future, AcsError, AcsWarning, Unit] =
    processingTime.metric.checkedTEvent {
      for {
        byOriginIndexed <- indexedDomains(transferIns).map(_.groupBy(_._2).toList)
        _ <- byOriginIndexed.traverse_ { case (originDomain, contractIdsAndDomain) =>
          bulkInsert(
            contractIdsAndDomain.map(_._1),
            toc,
            ChangeType.Activation,
            remoteDomain = Some(originDomain),
          )
        }
        _ <- checkTransfersConsistency(transferIns, toc)
      } yield ()
    }

  def transferOutContracts(transferOuts: Seq[(LfContractId, DomainId)], toc: TimeOfChange)(implicit
      traceContext: TraceContext
  ): CheckedT[Future, AcsError, AcsWarning, Unit] =
    processingTime.metric.checkedTEvent {
      for {
        byTargetIndexed <- indexedDomains(transferOuts).map(_.groupBy(_._2).toList)
        _ <- byTargetIndexed.traverse_ { case (targetDomain, contractIdsAndDomain) =>
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
          .traverseFilter { contractId =>
            storage
              .querySingle(fetchContractStateQuery(contractId), functionFullName)
              .semiflatMap(storedContract =>
                storedContract.toContractState.map(res => (contractId -> res))
              )
              .value
          }
          .map(_.toMap)
      case _: DbStorage.Profile.Postgres =>
        contractIds.toList.toNel match {
          case None => Future.successful(Map.empty)
          case Some(contractIdsNel) =>
            import DbStorage.Implicits.BuilderChain._

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
              .flatMap(_.toList.traverse { case (id, contract) =>
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

    import DbStorage.Implicits.BuilderChain._
    import DbStorage.Implicits._

    // TODO(i7860): Integrate with performance tests to check that we can remove packages when there are many contracts.

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
  ): Future[Either[AcsError, SortedMap[LfContractId, CantonTimestamp]]] =
    processingTime.metric.event {
      logger.debug(s"Obtaining ACS snapshot at $timestamp")
      storage
        .query(snapshotQuery(timestamp, None), functionFullName)
        .map(snapshot => Right(SortedMap(snapshot: _*)))
    }

  override def contractSnapshot(contractIds: Set[LfContractId], timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): EitherT[Future, AcsError, Map[LfContractId, CantonTimestamp]] =
    processingTime.metric.eitherTEvent {
      if (contractIds.isEmpty) EitherT.pure(Map.empty)
      else
        EitherT
          .right(storage.query(snapshotQuery(timestamp, Some(contractIds)), functionFullName))
          .map(_.toMap)
    }

  private[this] def snapshotQuery(
      timestamp: CantonTimestamp,
      contractIds: Option[Set[LfContractId]],
  ): DbAction.ReadOnly[Seq[(LfContractId, CantonTimestamp)]] = {
    import DbStorage.Implicits.BuilderChain._

    val idsO = contractIds.map { ids =>
      sql"(" ++ ids.toList.map(id => sql"$id").intercalate(sql", ") ++ sql")"
    }

    storage.profile match {
      case _: DbStorage.Profile.H2 =>
        (sql"""
          select distinct(contract_id), ts
          from active_contracts AC
          where not exists(select * from active_contracts AC2 where domain_id = $domainId and AC.contract_id = AC2.contract_id
            and AC2.ts <= $timestamp 
            and ((AC.ts, AC.request_counter) < (AC2.ts, AC2.request_counter) 
              or (AC.ts = AC2.ts and AC.request_counter = AC2.request_counter and AC2.change = ${ChangeType.Deactivation})))
           and AC.ts <= $timestamp and domain_id = $domainId""" ++
          idsO.fold(sql"")(ids => sql" and AC.contract_id in " ++ ids))
          .as[(LfContractId, CantonTimestamp)]
      case _: DbStorage.Profile.Postgres =>
        (sql"""
          select distinct(contract_id), AC3.ts from active_contracts AC1
          join lateral
            (select ts, change from active_contracts AC2 where domain_id = $domainId
             and AC2.contract_id = AC1.contract_id and ts <= $timestamp order by ts desc, request_counter desc, change asc #${storage
          .limit(1)}) as AC3 on true
          where AC1.domain_id = $domainId and AC3.change = CAST(${ChangeType.Activation} as change_type)""" ++
          idsO.fold(sql"")(ids => sql" and AC1.contract_id in " ++ ids))
          .as[(LfContractId, CantonTimestamp)]
      case _: DbStorage.Profile.Oracle =>
        (sql"""select distinct(contract_id), AC3.ts from active_contracts AC1, lateral 
          (select ts, change from active_contracts AC2 where domain_id = $domainId
             and AC2.contract_id = AC1.contract_id and ts <= $timestamp 
             order by ts desc, request_counter desc, change desc
             fetch first 1 row only) AC3
          where AC1.domain_id = $domainId and AC3.change = 'activation'""" ++
          idsO.fold(sql"")(ids => sql" and AC1.contract_id in " ++ ids))
          .as[(LfContractId, CantonTimestamp)]
    }
  }

  override def doPrune(beforeAndIncluding: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): EitherT[Future, AcsError, Unit] = processingTime.metric.eitherTEvent {

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
    processingTime.metric.event {
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
    processingTime.metric.event {
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
      transfers: Seq[(LfContractId, DomainId)],
      toc: TimeOfChange,
  )(implicit traceContext: TraceContext): CheckedT[Future, AcsError, AcsWarning, Unit] =
    if (enableAdditionalConsistencyChecks) {
      transfers.traverse_ { case (contractId, _) =>
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
        """insert /*+  IGNORE_ROW_ON_DUPKEY_INDEX ( active_contracts ( contract_id, ts, request_counter, change, domain_id ) ) */
          into active_contracts(domain_id, contract_id, change, operation, ts, request_counter, remote_domain_id)
          values (?, ?, ?, ?, ?, ?, ?)"""
      case _: DbStorage.Profile.H2 | _: DbStorage.Profile.Postgres =>
        """insert into active_contracts(domain_id, contract_id, change, operation, ts, request_counter, remote_domain_id)
          values (?, ?, CAST(? as change_type), CAST(? as operation_type), ?, ?, ?)
          on conflict do nothing"""
    }
    val insertAll = DbStorage.bulkOperation_(insertQuery, contractIds, storage.profile) {
      pp => contractId =>
        pp >> domainId
        pp >> contractId
        pp >> change
        pp >> operation
        pp >> toc.timestamp
        pp >> toc.rc
        pp >> remoteDomain
    }

    def checkIdempotence(
        idsToCheck: NonEmptyList[LfContractId]
    ): CheckedT[Future, AcsError, AcsWarning, Unit] = {
      import DbStorage.Implicits.BuilderChain._
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
        .flatMap(_.toList.traverseFilter {
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
        contractIds.toList.toNel.map(checkIdempotence).getOrElse(CheckedT.pure(()))
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

    import DbStorage.Implicits.BuilderChain._

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
