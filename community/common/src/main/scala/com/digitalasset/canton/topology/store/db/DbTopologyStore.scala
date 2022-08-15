// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.store.db

import cats.instances.future._
import cats.instances.list._
import cats.syntax.functorFilter._
import cats.syntax.traverse._
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.LengthLimitedString.DisplayName
import com.digitalasset.canton.config.RequireTypes.{
  LengthLimitedString,
  String185,
  String255,
  String300,
}
import com.digitalasset.canton.crypto.{Fingerprint, PublicKey}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.Lifecycle
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.metrics.MetricHandle.GaugeM
import com.digitalasset.canton.metrics.TimedLoadGauge
import com.digitalasset.canton.protocol.DynamicDomainParameters
import com.digitalasset.canton.resource.DbStorage.{DbAction, SQLActionBuilderChain}
import com.digitalasset.canton.resource.{DbStorage, DbStore}
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.topology._
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.store.TopologyStore.InsertTransaction
import com.digitalasset.canton.topology.store._
import com.digitalasset.canton.topology.transaction._
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion
import io.functionmeta.functionFullName
import slick.jdbc.GetResult
import slick.jdbc.canton.SQLActionBuilder

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}

class DbTopologyStoreFactory(
    override protected val storage: DbStorage,
    maxItemsInSqlQuery: Int,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit val executionContext: ExecutionContext)
    extends TopologyStoreFactory
    with DbStore {

  private val storeCache = TrieMap.empty[TopologyStoreId, TopologyStore[TopologyStoreId]]
  private val metadata = new DbPartyMetadataStore(storage, timeouts, loggerFactory)

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  override def forId[StoreId <: TopologyStoreId](storeId: StoreId): TopologyStore[StoreId] =
    storeCache
      .getOrElseUpdate(
        storeId,
        new DbTopologyStore(storage, storeId, maxItemsInSqlQuery, timeouts, loggerFactory),
      )
      .asInstanceOf[TopologyStore[StoreId]]

  import storage.api._

  private val processingTime: GaugeM[TimedLoadGauge, Double] =
    storage.metrics.loadGaugeM("domain-identity-store-factory")

  override def allNonDiscriminated(implicit
      traceContext: TraceContext
  ): Future[Seq[TopologyStore[TopologyStoreId]]] =
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
        .map(rawId => forId(TopologyStoreId(rawId)))
    }

  override def partyMetadataStore(): PartyMetadataStore = metadata

  override def onClosed(): Unit = Lifecycle.close(metadata)(logger)

}

class DbPartyMetadataStore(
    override protected val storage: DbStorage,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit
    ec: ExecutionContext
) extends PartyMetadataStore
    with DbStore {

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
        .as[(PartyId, Option[String], Option[String], String255, CantonTimestamp, Boolean)]
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
      submissionId: String255,
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

  private def dbValue(participantId: Option[ParticipantId]): Option[String300] =
    participantId.map(_.uid.toLengthLimitedString.asString300)

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

class DbTopologyStore[StoreId <: TopologyStoreId](
    override protected val storage: DbStorage,
    val storeId: StoreId,
    maxItemsInSqlQuery: Int,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends TopologyStore[StoreId]
    with DbStore {

  import DbStorage.Implicits.BuilderChain._
  import storage.api._
  import storage.converters._

  private implicit val getResultSignedTopologyTransaction
      : GetResult[SignedTopologyTransaction[TopologyChangeOp]] =
    SignedTopologyTransaction.createGetResultDomainTopologyTransaction

  private val (transactionStoreIdName, stateStoreIdFilterName) = buildTransactionStoreNames(storeId)
  private val isDomainStore = storeId match {
    case TopologyStoreId.DomainStore(_, _) => true
    case _ => false
  }

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
          sql" AND element_id = $elementId"
        }
        .getOrElse(sql"") ++ sql")"

  }

  override private[topology] def doAppend(
      sequenced: SequencedTime,
      effective: EffectiveTime,
      transactions: Seq[ValidatedTopologyTransaction],
  )(implicit traceContext: TraceContext): Future[Unit] = {
    val (updates, appends) = TopologyStore.appends(effective.value, transactions)
    updateAndInsert(transactionStoreIdName, sequenced, effective, updates.toSeq, appends)
  }

  @SuppressWarnings(Array("com.digitalasset.canton.SlickString"))
  private def updateAndInsert(
      store: LengthLimitedString,
      sequenced: SequencedTime,
      effective: EffectiveTime,
      deactivate: Seq[UniquePath],
      add: Seq[InsertTransaction],
  )(implicit traceContext: TraceContext): Future[Unit] = {

    val sequencedTs = sequenced.value
    val effectiveTs = effective.value
    val updateSeq = deactivate.toList.map(pathQuery)
    val appendSeq = add.toList
      .map { case InsertTransaction(transaction, validUntil, reasonT) =>
        val operation = transaction.operation
        val transactionType = transaction.uniquePath.dbType
        val namespace = transaction.uniquePath.namespace
        val identifier =
          transaction.uniquePath.maybeUid.map(_.id.toLengthLimitedString).getOrElse(String185.empty)
        val elementId =
          transaction.uniquePath.maybeElementId.fold(String255.empty)(_.toLengthLimitedString)
        val reason = reasonT.map(_.asString1GB)
        // TODO(i9591) clean me up and remove duplication
        val secondary = transaction match {
          case SignedTopologyTransaction(
                TopologyStateUpdate(_, TopologyStateUpdateElement(_, mapping: PartyToParticipant)),
                _,
                _,
              ) if mapping.side != RequestSide.From =>
            Some((mapping.participant.uid.id, mapping.participant.uid.namespace))
          case _ => None
        }
        val representativeProtocolVersion = transaction.transaction.representativeProtocolVersion
        val (secondaryId, secondaryNs) = secondary.unzip
        storage.profile match {
          case _: DbStorage.Profile.Oracle =>
            sql"SELECT $store, $sequencedTs, $effectiveTs, $validUntil, $transactionType, $namespace, $identifier, $elementId, $secondaryNs, $secondaryId, $operation, $transaction, $reason, $representativeProtocolVersion FROM dual"
          case _ =>
            sql"($store, $sequencedTs, $effectiveTs, $validUntil, $transactionType, $namespace, $identifier, $elementId, $secondaryNs, $secondaryId, $operation, $transaction, $reason, $representativeProtocolVersion)"
        }
      }

    lazy val updateAction =
      (sql"UPDATE topology_transactions SET valid_until = $effectiveTs WHERE store_id = $store AND (" ++
        updateSeq
          .intercalate(
            sql" OR "
          ) ++ sql") AND valid_until is null AND valid_from < $effectiveTs").asUpdate

    val insertAction = storage.profile match {
      case _: DbStorage.Profile.Postgres | _: DbStorage.Profile.H2 =>
        (sql"""INSERT INTO topology_transactions (store_id, sequenced, valid_from, valid_until, transaction_type, namespace, 
                    identifier, element_id, secondary_namespace, secondary_identifier, operation, instance, ignore_reason, representative_protocol_version) VALUES""" ++
          appendSeq.intercalate(sql", ") ++ sql" ON CONFLICT DO NOTHING").asUpdate
      case _: DbStorage.Profile.Oracle =>
        (sql"""INSERT 
               /*+  IGNORE_ROW_ON_DUPKEY_INDEX ( TOPOLOGY_TRANSACTIONS (store_id, transaction_type, namespace, identifier, element_id, valid_from, operation, representative_protocol_version) ) */ 
               INTO topology_transactions (store_id, sequenced, valid_from, valid_until, transaction_type, namespace, identifier, element_id, 
                                           secondary_namespace, secondary_identifier, operation, instance, ignore_reason, representative_protocol_version)
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
  ): DBIOAction[Unit, NoStream, E] = DBIO.seq(actions.collect {
    case (filter, action) if filter => action
  }: _*)

  private def queryForTransactions(
      store: LengthLimitedString,
      subQuery: SQLActionBuilder,
      limit: String = "",
      orderBy: String = " ORDER BY id ",
  )(implicit
      traceContext: TraceContext
  ): Future[StoredTopologyTransactions[TopologyChangeOp]] = {
    val query =
      sql"SELECT id, instance, sequenced, valid_from, valid_until FROM topology_transactions WHERE store_id = $store" ++
        subQuery ++ sql" AND ignore_reason IS NULL #${orderBy} #${limit}"
    readTime.metric.event {
      storage
        .query(
          query.as[
            (
                Long,
                SignedTopologyTransaction[TopologyChangeOp],
                Option[CantonTimestamp],
                CantonTimestamp,
                Option[CantonTimestamp],
            )
          ],
          functionFullName,
        )
        .flatMap(_.toList.traverse { case (id, dt, sequencedTsO, validFrom, validUntil) =>
          getOrComputeSequencedTime(store, id, sequencedTsO, validFrom).map { sequencedTs =>
            StoredTopologyTransaction(
              SequencedTime(sequencedTs),
              EffectiveTime(validFrom),
              validUntil.map(EffectiveTime(_)),
              dt,
            )
          }
        })
        .map(StoredTopologyTransactions(_))
    }
  }

  // TODO(#9014) remove once we move to 3.0
  /** Backwards compatible computation of sequencing time
    *
    * The algorithm works based on the assumption that the topology manager has not sent
    * an epsilon change that would lead to reordering of topology transactions.
    *
    * Let's assume we have parameter changes at (t3,e3), (t2, e2), (t1, e1), default: e0 = 0
    * with ti being the effective time
    *
    * Then, for a t4, we know that if (t4 - t3) > e3, then t4 was sequenced at t4 - e3. Otherwise, we repeat
    * with checking t4 against t2 and e2 etc.
    */
  private def getOrComputeSequencedTime(
      store: LengthLimitedString,
      id: Long,
      sequencedO: Option[CantonTimestamp],
      validFrom: CantonTimestamp,
  )(implicit
      traceContext: TraceContext
  ): Future[CantonTimestamp] =
    if (!isDomainStore)
      Future.successful(sequencedO.getOrElse(validFrom)) // only compute for domain stores
    else {
      def getParameterChangeBefore(
          ts: CantonTimestamp
      ): Future[Option[(CantonTimestamp, NonNegativeFiniteDuration)]] = {
        val typ = DomainTopologyTransactionType.DomainParameters
        // this is recursive, but terminates as we descend in time strictly.
        // It is also stack safe as trampolined by a `Future.flatmap` inside queryForTransactions.
        queryForTransactions(
          store,
          sql" AND transaction_type = ${typ} and valid_from < $ts",
          limit = storage.limit(1),
          orderBy = " ORDER BY valid_from DESC",
        ).map(
          _.result.map(x => (x.validFrom, x.transaction.transaction.element.mapping)).collectFirst {
            case (ts, change: DomainParametersChange) =>
              (ts.value, change.domainParameters.topologyChangeDelay)
          }
        )
      }
      def go(before: CantonTimestamp): Future[CantonTimestamp] = {
        getParameterChangeBefore(before).flatMap {
          case None =>
            // there is no parameter change before, so we use the default (which is 0)
            Future.successful(validFrom - DynamicDomainParameters.topologyChangeDelayIfAbsent)
          case Some((ts, epsilon)) =>
            val delta = validFrom - ts
            // check if (teffective - teffchange) > epsilon
            if (delta.compareTo(epsilon.duration) > 0) {
              Future.successful(validFrom - epsilon)
            } else {
              go(ts)
            }
        }
      }
      sequencedO.map(Future.successful).getOrElse {
        go(validFrom).flatMap { sequenced =>
          logger.info(
            s"Updating legacy topology transaction id=${id} with effective=${validFrom} to sequenced time=${sequenced}"
          )
          val query =
            sqlu"UPDATE topology_transactions SET sequenced = ${sequenced} WHERE id = $id AND store_id = $store"
          storage.update_(query, functionFullName).map(_ => sequenced)
        }
      }
    }

  private def asOfQuery(asOf: CantonTimestamp, asOfInclusive: Boolean): SQLActionBuilder = {

    sql" AND valid_from #${if (asOfInclusive) "<="
    else "<"} $asOf AND (valid_until is NULL OR $asOf #${if (asOfInclusive) "<"
    else "<="} valid_until)"
  }

  override def timestamp(
      useStateStore: Boolean
  )(implicit traceContext: TraceContext): Future[Option[(SequencedTime, EffectiveTime)]] = {
    val storeId = if (useStateStore) stateStoreIdFilterName else transactionStoreIdName
    queryForTransactions(storeId, sql"", storage.limit(1), orderBy = " ORDER BY id DESC")
      .map(_.result.headOption.map(tx => (tx.sequenced, tx.validFrom)))
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

  override def findPositiveTransactionsForMapping(
      mapping: TopologyMapping
  )(implicit
      traceContext: TraceContext
  ): Future[Seq[SignedTopologyTransaction[TopologyChangeOp.Positive]]] = {
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
      .map { x =>
        x.positiveTransactions.combine.result.collect {
          case storedTx if storedTx.transaction.transaction.element.mapping == mapping =>
            storedTx.transaction
        }
      }
  }

  override def allTransactions(implicit
      traceContext: TraceContext
  ): Future[StoredTopologyTransactions[TopologyChangeOp]] =
    queryForTransactions(transactionStoreIdName, sql"")

  @SuppressWarnings(Array("com.digitalasset.canton.SlickString"))
  override def inspectKnownParties(
      timestamp: CantonTimestamp,
      filterParty: String,
      filterParticipant: String,
      limit: Int,
  )(implicit
      traceContext: TraceContext
  ): Future[Set[PartyId]] = {
    val p2pm = DomainTopologyTransactionType.PartyToParticipant
    val pdsm = DomainTopologyTransactionType.ParticipantState
    val (filterPartyIdentifier, filterPartyNamespace) =
      UniqueIdentifier.splitFilter(filterParty, "%")
    val (filterParticipantIdentifier, filterParticipantNamespace) =
      UniqueIdentifier.splitFilter(filterParticipant, "%")
    val limitS = storage.limit(limit)
    val query =
      sql"""
        SELECT identifier, namespace FROM topology_transactions WHERE store_id = $stateStoreIdFilterName
            AND valid_from < $timestamp AND (valid_until IS NULL OR $timestamp <= valid_until)
            AND (
                (transaction_type = $p2pm AND identifier LIKE $filterPartyIdentifier AND namespace LIKE $filterPartyNamespace
                 AND secondary_identifier LIKE $filterParticipantIdentifier AND secondary_namespace LIKE $filterParticipantNamespace)
             OR (transaction_type = $pdsm AND identifier LIKE $filterPartyIdentifier AND namespace LIKE $filterPartyNamespace
                 AND identifier LIKE $filterParticipantIdentifier AND namespace LIKE $filterParticipantNamespace)
            ) AND ignore_reason IS NULL GROUP BY (identifier, namespace) #${limitS}"""
    readTime.metric.event {
      storage
        .query(
          query.as[
            (String, String)
          ],
          functionFullName,
        )
        .map(_.map { case (id, ns) =>
          PartyId(UniqueIdentifier(Identifier.tryCreate(id), Namespace(Fingerprint.tryCreate(ns))))
        }.toSet)
    }
  }

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
      case TimeQuery.HeadState =>
        getHeadStateQuery()
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
    @SuppressWarnings(Array("com.digitalasset.canton.SlickString"))
    val query3 =
      if (idFilter.isEmpty) query2
      else if (namespaceOnly) {
        query2 ++ sql" AND namespace LIKE ${idFilter + "%"}"
      } else {
        val (prefix, suffix) = UniqueIdentifier.splitFilter(idFilter, "%")
        val tmp = query2 ++ sql" AND identifier like $prefix "
        if (suffix.sizeCompare(1) > 0) {
          tmp ++ sql" AND namespace like $suffix "
        } else
          tmp
      }
    val query4 = typ match {
      case Some(value) => query3 ++ sql" AND transaction_type = $value"
      case None => query3
    }
    queryForTransactions(storeId, query4)
  }

  private def findStoredSql(
      transaction: TopologyTransaction[TopologyChangeOp],
      subQuery: SQLActionBuilder = sql"",
  )(implicit
      traceContext: TraceContext
  ): Future[StoredTopologyTransactions[TopologyChangeOp]] =
    queryForTransactions(
      transactionStoreIdName,
      sql"AND" ++ pathQuery(
        transaction.element.uniquePath
      ) ++ sql" AND operation = ${transaction.op}" ++ subQuery,
    )

  override def findStored(
      transaction: SignedTopologyTransaction[TopologyChangeOp]
  )(implicit
      traceContext: TraceContext
  ): Future[Option[StoredTopologyTransaction[TopologyChangeOp]]] =
    findStoredSql(transaction.transaction).map(_.result.headOption)

  override def findStoredNoSignature(
      transaction: TopologyTransaction[TopologyChangeOp]
  )(implicit
      traceContext: TraceContext
  ): Future[Seq[StoredTopologyTransaction[TopologyChangeOp]]] =
    findStoredSql(transaction).map(_.result)

  override def findStoredForVersion(
      transaction: TopologyTransaction[TopologyChangeOp],
      protocolVersion: ProtocolVersion,
  )(implicit
      traceContext: TraceContext
  ): Future[Option[StoredTopologyTransaction[TopologyChangeOp]]] = {
    val representativeProtocolVersion =
      TopologyTransaction.protocolVersionRepresentativeFor(protocolVersion)
    findStoredSql(
      transaction,
      sql" AND representative_protocol_version = $representativeProtocolVersion",
    ).map(_.result.headOption)
  }

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
                        sql"(#$identifier = ${uid.id} AND #$namespace = ${uid.namespace})"
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

  /** query interface used by DomainTopologyManager to find the set of initial keys */
  override def findInitialState(
      id: DomainTopologyManagerId
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
            val (bl, map) = TopologyStore.findInitialStateAccumulator(id.uid, acc, transaction)
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
      sequenced: SequencedTime,
      effective: EffectiveTime,
      deactivate: Seq[UniquePath],
      positive: Seq[SignedTopologyTransaction[TopologyChangeOp.Positive]],
  )(implicit traceContext: TraceContext): Future[Unit] =
    updateAndInsert(
      stateStoreIdFilterName,
      sequenced,
      effective,
      deactivate,
      positive.map { x =>
        InsertTransaction(x, None, None)
      },
    )

  override def findUpcomingEffectiveChanges(asOfInclusive: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[Seq[TopologyStore.Change]] = {
    queryForTransactions(
      transactionStoreIdName,
      sql"AND valid_from >= $asOfInclusive ",
      orderBy = " ORDER BY valid_from",
    ).map(res =>
      TopologyStore.Change.accumulateUpcomingEffectiveChanges(
        res.result
      )
    )
  }

  override def currentDispatchingWatermark(implicit
      traceContext: TraceContext
  ): Future[Option[CantonTimestamp]] = {
    val query =
      sql"SELECT watermark_ts FROM topology_dispatching WHERE store_id =$transactionStoreIdName"
        .as[CantonTimestamp]
        .headOption
    readTime.metric.event {
      storage.query(query, functionFullName)
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
      timestampExclusive: CantonTimestamp,
      limitO: Option[Int],
  )(implicit traceContext: TraceContext): Future[StoredTopologyTransactions[TopologyChangeOp]] = {
    val subQuery =
      sql"AND valid_from > $timestampExclusive AND (valid_until is NULL OR operation = ${TopologyChangeOp.Remove})"
    val limitQ = limitO.fold("")(storage.limit(_))
    queryForTransactions(transactionStoreIdName, subQuery, limitQ)
  }

  override def findParticipantOnboardingTransactions(
      participantId: ParticipantId,
      domainId: DomainId,
  )(implicit
      traceContext: TraceContext
  ): Future[Seq[SignedTopologyTransaction[TopologyChangeOp]]] = {
    val ns = participantId.uid.namespace
    val subQuery =
      sql"AND valid_until is NULL AND namespace = $ns AND transaction_type IN (" ++ TopologyStore.initialParticipantDispatchingSet.toList
        .map(s => sql"$s")
        .intercalate(sql", ") ++ sql")"
    queryForTransactions(transactionStoreIdName, subQuery).flatMap(
      TopologyStore.filterInitialParticipantDispatchingTransactions(
        participantId,
        domainId,
        this,
        loggerFactory,
        _,
      )
    )
  }

  override def findTsOfParticipantStateChangesBefore(
      beforeExclusive: CantonTimestamp,
      participantId: ParticipantId,
      limit: Int,
  )(implicit traceContext: TraceContext): Future[Seq[CantonTimestamp]] = {
    val ns = participantId.uid.namespace
    val id = participantId.uid.id
    val subQuery = sql" AND valid_from < $beforeExclusive " ++
      sql"AND transaction_type = ${DomainTopologyTransactionType.ParticipantState} " ++
      sql"AND namespace = $ns AND identifier = $id "
    val limitQ = storage.limit(limit)
    queryForTransactions(
      transactionStoreIdName,
      subQuery,
      limit = limitQ,
      orderBy = "ORDER BY valid_from DESC",
    ).map(_.result.map(_.validFrom.value))
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
