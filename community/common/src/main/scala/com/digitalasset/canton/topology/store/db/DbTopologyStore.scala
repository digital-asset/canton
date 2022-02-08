// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.store.db

import cats.instances.future._
import cats.instances.list._
import cats.syntax.functorFilter._
import cats.syntax.traverse._
import com.digitalasset.canton.config.RequireTypes.LengthLimitedString.DisplayName
import com.digitalasset.canton.config.RequireTypes.{LengthLimitedString, String255}
import com.digitalasset.canton.crypto.PublicKey
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.MetricHandle.GaugeM
import com.digitalasset.canton.metrics.TimedLoadGauge
import com.digitalasset.canton.resource.DbStorage
import com.digitalasset.canton.resource.DbStorage.{DbAction, SQLActionBuilderChain}
import com.digitalasset.canton.topology._
import com.digitalasset.canton.topology.store.TopologyStore.InsertTransaction
import com.digitalasset.canton.topology.store._
import com.digitalasset.canton.topology.transaction._
import com.digitalasset.canton.tracing.TraceContext
import io.functionmeta.functionFullName
import slick.jdbc.GetResult
import slick.jdbc.canton.SQLActionBuilder

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}

class DbTopologyStoreFactory(
    storage: DbStorage,
    maxItemsInSqlQuery: Int,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit val executionContext: ExecutionContext)
    extends TopologyStoreFactory
    with NamedLogging {

  private val storeCache = TrieMap.empty[TopologyStoreId, TopologyStore]
  private val metadata = new DbPartyMetadataStore(storage)

  override def forId(storeId: TopologyStoreId): TopologyStore =
    storeCache.getOrElseUpdate(
      storeId,
      new DbTopologyStore(storage, storeId, maxItemsInSqlQuery, loggerFactory),
    )

  import storage.api._

  private val processingTime: GaugeM[TimedLoadGauge, Double] =
    storage.metrics.loadGaugeM("domain-identity-store-factory")

  override def allNonDiscriminated(implicit
      traceContext: TraceContext
  ): Future[Map[TopologyStoreId, TopologyStore]] =
    processingTime.metric.event {
      def filterStateStores(str: String): Boolean = {
        // state stores (and discriminated stores) are prefixed with an <something>::, but we don't want
        // to prevent people from using domains named <something>:: ...
        val (count, _) = str.foldLeft((0, '_')) {
          case ((count, ':'), ':') => (count + 1, ':')
          case ((count, _), next) => (count, next)
        }
        count < 2
      }
      for {
        data <- storage.query(
          sql"select distinct store_id from topology_transactions".as[String],
          functionFullName,
        )
      } yield data
        // We rely on the correct filtering of state and mediator stores to avoid possible runtime exceptions here
        // If this filtering doesn't work correctly, the '[S|<discriminator>T|<discriminator>S]::' suffix of filter
        // stores could make it four characters too long when we try to (unsafely) convert it to an UniqueIdentifier
        .filter(filterStateStores)
        .map { rawId =>
          val id = TopologyStoreId(rawId)
          id -> forId(id)
        }
        .toMap
    }

  override def partyMetadataStore(): PartyMetadataStore = metadata

}

class DbPartyMetadataStore(storage: DbStorage)(implicit ec: ExecutionContext)
    extends PartyMetadataStore {

  import DbStorage.Implicits.BuilderChain._
  import storage.api._

  private val processingTime: GaugeM[TimedLoadGauge, Double] =
    storage.metrics.loadGaugeM("party-metadata-store")

  override def metadataForParty(
      partyId: PartyId
  )(implicit traceContext: TraceContext): Future[Option[PartyMetadata]] =
    processingTime.metric.event {
      storage
        .query(
          metadataForPartyQuery(sql"party_id = $partyId #${storage.limit(1)}"),
          functionFullName,
        )
        .map(_.headOption)
    }

  private def metadataForPartyQuery(
      where: SQLActionBuilderChain
  ): DbAction.ReadOnly[Seq[PartyMetadata]] = {

    val query =
      sql"select party_id, display_name, participant_id, submission_id, effective_at, notified from party_metadata where " ++ where

    for {
      data <- query
        .as[(PartyId, Option[String], Option[String], String, CantonTimestamp, Boolean)]
    } yield {
      data.map {
        case (partyId, displayNameS, participantIdS, submissionId, effectiveAt, notified) =>
          val participantId =
            participantIdS
              .flatMap(x => UniqueIdentifier.fromProtoPrimitive_(x).toOption)
              .map(ParticipantId(_))
          val displayName = displayNameS.flatMap(String255.create(_).toOption)
          PartyMetadata(
            partyId,
            displayName,
            participantId = participantId,
          )(
            effectiveTimestamp = effectiveAt,
            submissionId = submissionId,
            notified = notified,
          )
      }
    }
  }

  override def insertOrUpdatePartyMetadata(
      partyId: PartyId,
      participantId: Option[ParticipantId],
      displayName: Option[DisplayName],
      effectiveTimestamp: CantonTimestamp,
      submissionId: String,
  )(implicit traceContext: TraceContext): Future[Unit] =
    processingTime.metric.event {
      val participantS = dbValue(participantId)
      val query = storage.profile match {
        case _: DbStorage.Profile.Postgres =>
          sqlu"""insert into party_metadata (party_id, display_name, participant_id, submission_id, effective_at) 
                    VALUES ($partyId, $displayName, $participantS, $submissionId, $effectiveTimestamp)
                 on conflict (party_id) do update
                  set
                    display_name = $displayName,
                    participant_id = $participantS,
                    submission_id = $submissionId,
                    effective_at = $effectiveTimestamp,
                    notified = false
                 """
        case _: DbStorage.Profile.H2 | _: DbStorage.Profile.Oracle =>
          sqlu"""merge into party_metadata 
                  using dual 
                  on (party_id = $partyId) 
                  when matched then 
                    update set
                      display_name = $displayName,
                      participant_id = $participantS,
                      submission_id = $submissionId,
                      effective_at = $effectiveTimestamp,
                      notified = ${false}                        
                  when not matched then 
                    insert (party_id, display_name, participant_id, submission_id, effective_at)
                    values ($partyId, $displayName, $participantS, $submissionId, $effectiveTimestamp)
                 """
      }
      storage.update_(query, functionFullName)
    }

  private def dbValue(participantId: Option[ParticipantId]): Option[String] =
    participantId.map(_.uid.toProtoPrimitive)

  /** mark the given metadata has having been successfully forwarded to the domain */
  override def markNotified(
      metadata: PartyMetadata
  )(implicit traceContext: TraceContext): Future[Unit] = processingTime.metric.event {
    val partyId = metadata.partyId
    val effectiveAt = metadata.effectiveTimestamp
    val query =
      sqlu"UPDATE party_metadata SET notified = ${true} WHERE party_id = $partyId and effective_at = $effectiveAt"
    storage.update_(query, functionFullName)
  }

  /** fetch the current set of party data which still needs to be notified */
  override def fetchNotNotified()(implicit traceContext: TraceContext): Future[Seq[PartyMetadata]] =
    processingTime.metric.event {
      storage
        .query(
          metadataForPartyQuery(sql"notified = ${false}"),
          functionFullName,
        )
    }

}

class DbTopologyStore(
    storage: DbStorage,
    storeId: TopologyStoreId,
    maxItemsInSqlQuery: Int,
    val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends TopologyStore
    with NamedLogging {

  import DbStorage.Implicits.BuilderChain._
  import storage.api._
  import storage.converters._

  private implicit val getResultSignedTopologyTransaction
      : GetResult[SignedTopologyTransaction[TopologyChangeOp]] =
    SignedTopologyTransaction.createGetResultDomainTopologyTransaction

  private val (transactionStoreIdName, stateStoreIdFilterName) = buildTransactionStoreNames(storeId)

  private val updatingTime: GaugeM[TimedLoadGauge, Double] =
    storage.metrics.loadGaugeM("topology-store-update")
  private val readTime: GaugeM[TimedLoadGauge, Double] =
    storage.metrics.loadGaugeM("topology-store-read")

  private def buildTransactionStoreNames(
      storeId: TopologyStoreId
  ): (LengthLimitedString, LengthLimitedString) = {
    def withPrefix(str: String): LengthLimitedString = {
      LengthLimitedString.tryCreate(str + "::", str.length + 2).tryConcatenate(storeId.dbString)
    }
    storeId match {
      case TopologyStoreId.DomainStore(_domainId, discriminator) if discriminator.isEmpty =>
        (storeId.dbString, withPrefix("S"))
      case TopologyStoreId.DomainStore(_domainId, discriminator) =>
        ((withPrefix(discriminator + "T"), withPrefix(discriminator + "S")))
      case TopologyStoreId.AuthorizedStore =>
        (storeId.dbString, withPrefix("S"))
      case TopologyStoreId.RequestedStore =>
        (storeId.dbString, withPrefix("S"))
    }
  }

  private def pathQuery(uniquePath: UniquePath): SQLActionBuilder = {

    val dbType = uniquePath.dbType
    val namespace = uniquePath.namespace

    sql"(transaction_type = $dbType AND namespace = $namespace" ++
      uniquePath.maybeUid
        .map { uid =>
          val identifier = uid.id
          sql" AND identifier = $identifier"
        }
        .getOrElse(sql"") ++
      uniquePath.maybeElementId
        .map { elementId =>
          val tmp = elementId.unwrap
          sql" AND element_id = $tmp"
        }
        .getOrElse(sql"") ++ sql")"

  }

  override private[topology] def doAppend(
      timestamp: CantonTimestamp,
      transactions: Seq[ValidatedTopologyTransaction],
  )(implicit traceContext: TraceContext): Future[Unit] = {

    val (updates, appends) = TopologyStore.appends(timestamp, transactions)
    updateAndInsert(transactionStoreIdName, timestamp, updates.toSeq, appends)

  }

  private def updateAndInsert(
      store: LengthLimitedString,
      timestamp: CantonTimestamp,
      deactivate: Seq[UniquePath],
      add: Seq[InsertTransaction],
  )(implicit traceContext: TraceContext): Future[Unit] = {

    val updateSeq = deactivate.toList.map(pathQuery)
    val appendSeq = add.toList
      .map { case InsertTransaction(transaction, validUntil, reasonT) =>
        val operation = transaction.operation
        val transactionType = transaction.uniquePath.dbType
        val namespace = transaction.uniquePath.namespace
        val identifier = transaction.uniquePath.maybeUid.map(_.id).map(_.unwrap).getOrElse("")
        val elementId = transaction.uniquePath.maybeElementId.map(_.unwrap).getOrElse("")
        val reason = reasonT.map(_.asString)
        val secondary = transaction match {
          case SignedTopologyTransaction(
                TopologyStateUpdate(_, TopologyStateUpdateElement(_, mapping: PartyToParticipant)),
                _,
                _,
              ) if mapping.side != RequestSide.From =>
            Some((mapping.participant.uid.id, mapping.participant.uid.namespace))
          case _ => None
        }
        val (secondaryId, secondaryNs) = secondary.unzip
        storage.profile match {
          case _: DbStorage.Profile.Oracle =>
            sql"SELECT $store, $timestamp, $validUntil, $transactionType, $namespace, $identifier, $elementId, $secondaryNs, $secondaryId, $operation, $transaction, $reason FROM dual"
          case _ =>
            sql"($store, $timestamp, $validUntil, $transactionType, $namespace, $identifier, $elementId, $secondaryNs, $secondaryId, $operation, $transaction, $reason)"
        }
      }

    lazy val updateAction =
      (sql"UPDATE topology_transactions SET valid_until = $timestamp WHERE store_id = $store AND (" ++
        updateSeq
          .intercalate(
            sql" OR "
          ) ++ sql") AND valid_until is null AND valid_from < $timestamp").asUpdate

    val insertAction = storage.profile match {
      case _: DbStorage.Profile.Postgres | _: DbStorage.Profile.H2 =>
        (sql"""INSERT INTO topology_transactions (store_id, valid_from, valid_until, transaction_type, namespace, 
                    identifier, element_id, secondary_namespace, secondary_identifier, operation, instance, ignore_reason) VALUES""" ++
          appendSeq.intercalate(sql", ") ++ sql" ON CONFLICT DO NOTHING").asUpdate
      case _: DbStorage.Profile.Oracle =>
        (sql"""INSERT 
               /*+  IGNORE_ROW_ON_DUPKEY_INDEX ( TOPOLOGY_TRANSACTIONS (store_id, transaction_type, namespace, identifier, element_id, valid_from, operation) ) */ 
               INTO topology_transactions (store_id, valid_from, valid_until, transaction_type, namespace, identifier, element_id, 
                                           secondary_namespace, secondary_identifier, operation, instance, ignore_reason)
               WITH UPDATES AS (""" ++
          appendSeq.intercalate(sql" UNION ALL ") ++
          sql") SELECT * FROM UPDATES").asUpdate
    }

    updatingTime.metric.event {
      storage.update_(
        dbioSeq(Seq((updateSeq.nonEmpty, updateAction), (add.nonEmpty, insertAction))),
        operationName = "append-topology-transactions",
      )
    }
  }

  private def dbioSeq[E <: Effect](
      actions: Seq[(Boolean, DBIOAction[_, NoStream, E])]
  ): DBIOAction[Unit, NoStream, E] =
    DBIO.seq(actions.filter(_._1).map(_._2): _*)

  private def queryForTransactions(
      store: LengthLimitedString,
      subQuery: SQLActionBuilder,
      limit: String = "",
      orderBy: String = " ORDER BY id ",
  )(implicit
      traceContext: TraceContext
  ): Future[StoredTopologyTransactions[TopologyChangeOp]] = {
    val query =
      sql"SELECT instance, valid_from, valid_until FROM topology_transactions WHERE store_id = $store" ++
        subQuery ++ sql" AND ignore_reason IS NULL #${orderBy} #${limit}"
    readTime.metric.event {
      storage
        .query(
          query.as[
            (SignedTopologyTransaction[TopologyChangeOp], CantonTimestamp, Option[CantonTimestamp])
          ],
          functionFullName,
        )
        .map(_.map { case (dt, validFrom, validUntil) =>
          StoredTopologyTransaction(validFrom, validUntil, dt)
        })
        .map(StoredTopologyTransactions(_))
    }
  }

  private def asOfQuery(asOf: CantonTimestamp, asOfInclusive: Boolean): SQLActionBuilder = {

    sql" AND valid_from #${if (asOfInclusive) "<="
    else "<"} $asOf AND (valid_until is NULL OR $asOf #${if (asOfInclusive) "<"
    else "<="} valid_until)"
  }

  override def timestamp(implicit traceContext: TraceContext): Future[Option[CantonTimestamp]] = {

    val query = sql"""SELECT valid_from FROM topology_transactions
           WHERE store_id = $transactionStoreIdName ORDER BY id DESC #${storage.limit(1)}"""

    readTime.metric
      .event {
        storage.query(query.as[Option[CantonTimestamp]].headOption, functionFullName)
      }
      .map(_.flatten)

  }

  override def headTransactions(implicit
      traceContext: TraceContext
  ): Future[StoredTopologyTransactions[TopologyChangeOp.Positive]] =
    queryForTransactions(
      transactionStoreIdName,
      sql"AND valid_until is NULL and (operation = ${TopologyChangeOp.Add} or operation = ${TopologyChangeOp.Replace})",
    ).map(_.collectOfType[TopologyChangeOp.Positive])

  override def findRemovalTransactionForMappings(
      mappings: Set[TopologyStateElement[TopologyMapping]]
  )(implicit
      traceContext: TraceContext
  ): Future[Seq[SignedTopologyTransaction[TopologyChangeOp.Remove]]] =
    if (mappings.isEmpty) Future.successful(Seq.empty)
    else {

      val mappingsQuery = mappings
        .collect { case x: TopologyStateUpdateElement => pathQuery(x.uniquePath) }
        .toList
        .intercalate(sql" OR ")

      queryForTransactions(
        transactionStoreIdName,
        sql"AND operation = ${TopologyChangeOp.Remove} AND (" ++ mappingsQuery ++ sql")",
      ).map(
        _.result
          .map(_.transaction)
          .mapFilter(TopologyChangeOp.select[TopologyChangeOp.Remove])
      )
    }

  override def findActiveTransactionsForMapping(
      mapping: TopologyMapping
  )(implicit
      traceContext: TraceContext
  ): Future[Seq[SignedTopologyTransaction[TopologyChangeOp.Add]]] = {
    val tmp = TopologyElementId.tryCreate("1")
    val ns = mapping.uniquePath(tmp).namespace
    val query = mapping.uniquePath(tmp).maybeUid.map(_.id) match {
      case None => sql"AND namespace = $ns"
      case Some(identifier) => sql"AND namespace = $ns AND identifier = $identifier"
    }

    queryForTransactions(
      transactionStoreIdName,
      sql"AND valid_until is NULL AND transaction_type = ${mapping.dbType}" ++ query,
    )
      .map(_.result.collect {
        case StoredTopologyTransaction(_validFrom, None, tr)
            if tr.transaction.element.mapping == mapping =>
          tr
      })
      .map(_.mapFilter(TopologyChangeOp.select[TopologyChangeOp.Add]))
  }

  override def allTransactions(implicit
      traceContext: TraceContext
  ): Future[StoredTopologyTransactions[TopologyChangeOp]] =
    queryForTransactions(transactionStoreIdName, sql"")

  /** query optimized for inspection */
  override def inspect(
      stateStore: Boolean,
      timeQuery: TimeQuery,
      recentTimestampO: Option[CantonTimestamp],
      ops: Option[TopologyChangeOp],
      typ: Option[DomainTopologyTransactionType],
      idFilter: String,
      namespaceOnly: Boolean,
  )(implicit
      traceContext: TraceContext
  ): Future[StoredTopologyTransactions[TopologyChangeOp]] = {
    val storeId = {
      if (stateStore)
        stateStoreIdFilterName
      else transactionStoreIdName
    }
    def getHeadStateQuery(): SQLActionBuilderChain = recentTimestampO match {
      case Some(value) => asOfQuery(value, asOfInclusive = false)
      case None => sql" AND valid_until is NULL"
    }
    val query1: SQLActionBuilderChain = timeQuery match {
      case TimeQuery.HeadState => getHeadStateQuery()
      case TimeQuery.Snapshot(asOf) =>
        asOfQuery(asOf = asOf, asOfInclusive = false)

      case TimeQuery.Range(None, None) =>
        sql"" // The case below insert an additional `AND` that we don't want
      case TimeQuery.Range(from, until) =>
        sql" AND " ++ ((from.toList.map(ts => sql"valid_from >= $ts") ++ until.toList.map(ts =>
          sql"valid_from <= $ts"
        ))
          .intercalate(sql" AND "))
    }
    val query2 = ops match {
      case Some(value) =>
        query1 ++ sql" AND operation = $value"
      case None => query1
    }
    val query3 =
      if (idFilter.isEmpty) query2
      else if (namespaceOnly) {
        query2 ++ sql" AND namespace LIKE ${idFilter + "%"}"
      } else {
        val splitted = idFilter.split(SafeSimpleString.delimiter)
        val prefix = splitted(0)
        val tmp = query2 ++ sql" AND identifier like ${prefix + "%"} "
        if (splitted.lengthCompare(1) > 0) {
          val suffix = splitted(1)
          tmp ++ sql" AND namespace like ${suffix + "%"} "
        } else
          tmp
      }
    val query4 = typ match {
      case Some(value) => query3 ++ sql" AND transaction_type = $value"
      case None => query3
    }
    queryForTransactions(storeId, query4)
  }

  override def exists(
      transaction: SignedTopologyTransaction[TopologyChangeOp]
  )(implicit traceContext: TraceContext): Future[Boolean] =
    queryForTransactions(
      transactionStoreIdName,
      sql"AND" ++ pathQuery(
        transaction.uniquePath
      ) ++ sql" AND operation = ${transaction.operation}",
    )
      .map(_.result.nonEmpty)

  /** query interface used by [[com.digitalasset.canton.topology.client.StoreBasedTopologySnapshot]] */
  override def findPositiveTransactions(
      asOf: CantonTimestamp,
      asOfInclusive: Boolean,
      includeSecondary: Boolean,
      types: Seq[DomainTopologyTransactionType],
      filterUid: Option[Seq[UniqueIdentifier]],
      filterNamespace: Option[Seq[Namespace]],
  )(implicit
      traceContext: TraceContext
  ): Future[PositiveStoredTopologyTransactions] =
    findPositiveTransactionsInStore(
      transactionStoreIdName,
      asOf,
      asOfInclusive,
      includeSecondary,
      types,
      filterUid,
      filterNamespace,
    )

  /** batching (by filterUid) version of finding transactions in store */
  private def findTransactionsInStore(
      store: LengthLimitedString,
      asOf: CantonTimestamp,
      asOfInclusive: Boolean,
      includeSecondary: Boolean,
      types: Seq[DomainTopologyTransactionType],
      filterUid: Option[Seq[UniqueIdentifier]],
      filterNamespace: Option[Seq[Namespace]],
      filterOps: Seq[TopologyChangeOp],
  )(implicit
      traceContext: TraceContext
  ): Future[StoredTopologyTransactions[TopologyChangeOp]] = {
    def forward(filterUidsNew: Option[Seq[UniqueIdentifier]]) =
      findTransactionsInStoreRaw(
        store,
        asOf,
        asOfInclusive,
        includeSecondary,
        types,
        filterUidsNew,
        filterNamespace,
        filterOps,
      )
    filterUid match {
      case None => forward(None)
      case Some(uids) if uids.length < maxItemsInSqlQuery => forward(filterUid)
      case Some(uids) =>
        uids
          .grouped(maxItemsInSqlQuery)
          .toList
          .traverse(lessUids => forward(Some(lessUids)))
          .map(all => StoredTopologyTransactions(all.flatMap(_.result)))
    }
  }

  /** unbatching version of finding transactions in store */
  private def findTransactionsInStoreRaw(
      store: LengthLimitedString,
      asOf: CantonTimestamp,
      asOfInclusive: Boolean,
      includeSecondary: Boolean,
      types: Seq[DomainTopologyTransactionType],
      filterUid: Option[Seq[UniqueIdentifier]],
      filterNamespace: Option[Seq[Namespace]],
      filterOps: Seq[TopologyChangeOp],
  )(implicit
      traceContext: TraceContext
  ): Future[StoredTopologyTransactions[TopologyChangeOp]] = {
    {
      {
        val hasUidFilter = filterUid.nonEmpty || filterNamespace.nonEmpty
        val count =
          filterUid.map(_.length).getOrElse(0) + filterNamespace.map(_.length).getOrElse(0)
        if (hasUidFilter && count == 0) {
          Future.successful(StoredTopologyTransactions.empty[TopologyChangeOp.Add])
        } else {
          val rangeQuery = asOfQuery(asOf, asOfInclusive)
          val opFilter = filterOps.map(op => sql"operation = $op").intercalate(sql" or ")
          val baseQuery =
            sql"AND (" ++ opFilter ++ sql") AND transaction_type IN (" ++ types.toList
              .map(s => sql"$s")
              .intercalate(sql", ") ++ sql")"

          val pathQuery: SQLActionBuilderChain =
            if (!hasUidFilter) sql""
            else {
              def genFilters(identifier: String, namespace: String): SQLActionBuilderChain = {
                val filterUidQ =
                  filterUid
                    .map(_.filterNot(uid => filterNamespace.exists(_.contains(uid.namespace))))
                    .toList
                    .flatMap(
                      _.map(uid =>
                        sql"(#$identifier = ${uid.id.unwrap} AND #$namespace = ${uid.namespace})"
                      )
                    )
                val filterNsQ =
                  filterNamespace.toList
                    .flatMap(_.map(ns => sql"(#$namespace = $ns)"))
                SQLActionBuilderChain(filterUidQ) ++ SQLActionBuilderChain(filterNsQ)
              }
              val plainFilter = genFilters("identifier", "namespace")
              val filters = if (includeSecondary) {
                plainFilter ++ genFilters("secondary_identifier", "secondary_namespace")
              } else plainFilter
              sql" AND (" ++ filters.intercalate(sql" OR ") ++ sql")"
            }
          queryForTransactions(store, rangeQuery ++ baseQuery ++ pathQuery)
        }
      }
    }
  }

  private def findPositiveTransactionsInStore(
      store: LengthLimitedString,
      asOf: CantonTimestamp,
      asOfInclusive: Boolean,
      includeSecondary: Boolean,
      types: Seq[DomainTopologyTransactionType],
      filterUid: Option[Seq[UniqueIdentifier]],
      filterNamespace: Option[Seq[Namespace]],
  )(implicit traceContext: TraceContext): Future[PositiveStoredTopologyTransactions] =
    findTransactionsInStore(
      store = store,
      asOf = asOf,
      asOfInclusive = asOfInclusive,
      includeSecondary = includeSecondary,
      types = types,
      filterUid = filterUid,
      filterNamespace = filterNamespace,
      filterOps = Seq(TopologyChangeOp.Add, TopologyChangeOp.Replace),
    ).map(_.positiveTransactions)

  /** query interface used by DomainIdentityManager to find the set of initial keys */
  override def findInitialState(
      uid: UniqueIdentifier
  )(implicit traceContext: TraceContext): Future[Map[KeyOwner, Seq[PublicKey]]] = {
    val batchNum = 100
    def go(
        offset: Long,
        acc: Map[KeyOwner, Seq[PublicKey]],
    ): Future[(Boolean, Map[KeyOwner, Seq[PublicKey]])] = {
      val query = sql"AND operation = ${TopologyChangeOp.Add}"
      val lm = storage.limit(batchNum, offset)
      val start = (false, 0, acc)
      queryForTransactions(transactionStoreIdName, query, lm)
        .map(_.toDomainTopologyTransactions.foldLeft(start) {
          case ((false, count, acc), transaction) =>
            val (bl, map) = TopologyStore.findInitialStateAccumulator(uid, acc, transaction)
            (bl, count + 1, map)
          case ((bl, count, map), _) => (bl, count + 1, map)
        })
        .flatMap {
          case (done, count, acc) if done || count < batchNum => Future.successful((done, acc))
          case (_, _, acc) => go(offset + batchNum, acc)
        }
    }
    go(0, Map()).map(_._2)
  }

  override def findStateTransactions(
      asOf: CantonTimestamp,
      asOfInclusive: Boolean,
      includeSecondary: Boolean,
      types: Seq[DomainTopologyTransactionType],
      filterUid: Option[Seq[UniqueIdentifier]],
      filterNamespace: Option[Seq[Namespace]],
  )(implicit traceContext: TraceContext): Future[PositiveStoredTopologyTransactions] =
    findPositiveTransactionsInStore(
      stateStoreIdFilterName,
      asOf,
      asOfInclusive,
      includeSecondary,
      types,
      filterUid,
      filterNamespace,
    )

  override def updateState(
      timestamp: CantonTimestamp,
      deactivate: Seq[UniquePath],
      positive: Seq[SignedTopologyTransaction[TopologyChangeOp.Positive]],
  )(implicit traceContext: TraceContext): Future[Unit] =
    updateAndInsert(
      stateStoreIdFilterName,
      timestamp,
      deactivate,
      positive.map { x =>
        InsertTransaction(x, None, None)
      },
    )

  override def findEffectiveTimestampsSince(
      timestamp: CantonTimestamp
  )(implicit traceContext: TraceContext): Future[Seq[CantonTimestamp]] = {
    val query =
      sql"SELECT DISTINCT valid_from FROM topology_transactions WHERE store_id = $transactionStoreIdName AND valid_from > $timestamp ORDER BY valid_from"
    readTime.metric.event {
      storage
        .query(
          query.as[
            CantonTimestamp
          ],
          functionFullName,
        )

    }
  }

  override def currentDispatchingWatermark(implicit
      traceContext: TraceContext
  ): Future[Option[CantonTimestamp]] = {
    val query =
      sql"SELECT watermark_ts FROM topology_dispatching WHERE store_id =$transactionStoreIdName"
        .as[CantonTimestamp]
        .headOption
    readTime.metric.event {
      storage
        .query(query, functionFullName)
    }
  }

  override def updateDispatchingWatermark(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[Unit] = {
    val query = storage.profile match {
      case _: DbStorage.Profile.Postgres =>
        sqlu"""insert into topology_dispatching (store_id, watermark_ts)
                    VALUES ($transactionStoreIdName, $timestamp)
                 on conflict (store_id) do update
                  set
                    watermark_ts = $timestamp
                 """
      case _: DbStorage.Profile.H2 | _: DbStorage.Profile.Oracle =>
        sqlu"""merge into topology_dispatching
                  using dual
                  on (store_id = $transactionStoreIdName)
                  when matched then
                    update set
                       watermark_ts = $timestamp
                  when not matched then
                    insert (store_id, watermark_ts)
                    values ($transactionStoreIdName, $timestamp)
                 """
    }
    updatingTime.metric.event {
      storage.update_(query, functionFullName)
    }
  }

  override def findDispatchingTransactionsAfter(
      timestamp: CantonTimestamp
  )(implicit traceContext: TraceContext): Future[StoredTopologyTransactions[TopologyChangeOp]] = {
    val subQuery =
      sql"AND valid_from > $timestamp AND (valid_until is NULL OR operation = ${TopologyChangeOp.Remove})"
    queryForTransactions(transactionStoreIdName, subQuery)
  }

  override def findTsOfParticipantStateChangesBefore(
      beforeExclusive: CantonTimestamp,
      participantId: ParticipantId,
      limit: Int,
  )(implicit traceContext: TraceContext): Future[Seq[CantonTimestamp]] = {
    val ns = participantId.uid.namespace
    val id = participantId.uid.id.unwrap
    val subQuery = sql" AND valid_from < $beforeExclusive " ++
      sql"AND transaction_type = ${DomainTopologyTransactionType.ParticipantState} " ++
      sql"AND namespace = $ns AND identifier = $id "
    val limitQ = storage.limit(limit)
    queryForTransactions(
      transactionStoreIdName,
      subQuery,
      limit = limitQ,
      orderBy = "ORDER BY valid_from DESC",
    ).map(_.result.map(_.validFrom))
  }

  override def findTransactionsInRange(
      asOfExclusive: CantonTimestamp,
      upToExclusive: CantonTimestamp,
  )(implicit traceContext: TraceContext): Future[StoredTopologyTransactions[TopologyChangeOp]] = {
    val subQuery =
      sql"""AND valid_from > $asOfExclusive AND valid_from < $upToExclusive"""
    queryForTransactions(
      transactionStoreIdName,
      subQuery,
    )
  }
}
