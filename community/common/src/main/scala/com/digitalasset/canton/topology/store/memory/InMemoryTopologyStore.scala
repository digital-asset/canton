// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.store.memory

import cats.syntax.functorFilter._
import com.digitalasset.canton.config.RequireTypes.LengthLimitedString.DisplayName
import com.digitalasset.canton.config.RequireTypes.String255
import com.digitalasset.canton.crypto.PublicKey
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.topology._
import com.digitalasset.canton.topology.store.TopologyStore.InsertTransaction
import com.digitalasset.canton.topology.store.TopologyStoreId.DomainStore
import com.digitalasset.canton.topology.store._
import com.digitalasset.canton.topology.transaction.TopologyChangeOp.{Add, Positive, Remove}
import com.digitalasset.canton.topology.transaction._
import com.digitalasset.canton.tracing.TraceContext

import java.util.concurrent.atomic.AtomicReference
import scala.collection.concurrent.TrieMap
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{ExecutionContext, Future, blocking}

class InMemoryTopologyStoreFactory(override protected val loggerFactory: NamedLoggerFactory)(
    implicit val executionContext: ExecutionContext
) extends TopologyStoreFactory
    with NamedLogging {

  private val stores = TrieMap.empty[TopologyStoreId, TopologyStore]
  private val metadata = new InMemoryPartyMetadataStore()

  override def forId(storeId: TopologyStoreId): TopologyStore =
    stores.getOrElseUpdate(storeId, new InMemoryTopologyStore(loggerFactory))

  def allNonDiscriminated(implicit
      traceContext: TraceContext
  ): Future[Map[TopologyStoreId, TopologyStore]] =
    Future.successful(removeDiscriminatedStores(stores.toMap))

  private def removeDiscriminatedStores(
      original: Map[TopologyStoreId, TopologyStore]
  ): Map[TopologyStoreId, TopologyStore] = {
    original.filter {
      case (DomainStore(id, disc), v) => disc.isEmpty
      case _ => true
    }
  }

  override def partyMetadataStore(): PartyMetadataStore = metadata

  override def close(): Unit = ()
}

class InMemoryPartyMetadataStore extends PartyMetadataStore {

  private val store = TrieMap[PartyId, PartyMetadata]()

  override def insertOrUpdatePartyMetadata(
      partyId: PartyId,
      participantId: Option[ParticipantId],
      displayName: Option[DisplayName],
      effectiveTimestamp: CantonTimestamp,
      submissionId: String255,
  )(implicit traceContext: TraceContext): Future[Unit] = {
    store.put(
      partyId,
      PartyMetadata(partyId, displayName, participantId)(
        effectiveTimestamp = effectiveTimestamp,
        submissionId = submissionId,
      ),
    )
    Future.unit

  }

  override def metadataForParty(partyId: PartyId)(implicit
      traceContext: TraceContext
  ): Future[Option[PartyMetadata]] =
    Future.successful(store.get(partyId))

  override def markNotified(
      metadata: PartyMetadata
  )(implicit traceContext: TraceContext): Future[Unit] = {
    store.get(metadata.partyId) match {
      case Some(cur) if cur.effectiveTimestamp == metadata.effectiveTimestamp =>
        store.put(
          metadata.partyId,
          metadata.copy()(
            effectiveTimestamp = metadata.effectiveTimestamp,
            submissionId = metadata.submissionId,
            notified = true,
          ),
        )
      case _ => ()
    }
    Future.unit
  }

  override def fetchNotNotified()(implicit traceContext: TraceContext): Future[Seq[PartyMetadata]] =
    Future.successful(store.values.filterNot(_.notified).toSeq)

  override def close(): Unit = ()
}

class InMemoryTopologyStore(val loggerFactory: NamedLoggerFactory)(implicit ec: ExecutionContext)
    extends TopologyStore
    with NamedLogging {

  private case class TopologyStoreEntry[+Op <: TopologyChangeOp](
      operation: Op,
      transaction: SignedTopologyTransaction[Op],
      from: CantonTimestamp,
      until: Option[CantonTimestamp],
      rejected: Option[String],
  ) {

    def toStoredTransaction: StoredTopologyTransaction[Op] =
      StoredTopologyTransaction(from, until, transaction)

    def secondaryUid: Option[UniqueIdentifier] = transaction match {
      case SignedTopologyTransaction(
            TopologyStateUpdate(
              _,
              TopologyStateUpdateElement(_, PartyToParticipant(side, _, participant, _)),
            ),
            _,
            _,
          ) if (side != RequestSide.From) =>
        Some(participant.uid)
      case _ => None
    }

  }

  // contains Add, Remove and Replace
  private val topologyTransactionStore = ArrayBuffer[TopologyStoreEntry[TopologyChangeOp]]()
  // contains only (Add, Replace) transactions that are authorized
  private val topologyStateStore = ArrayBuffer[TopologyStoreEntry[Positive]]()

  private[topology] override def doAppend(
      timestamp: CantonTimestamp,
      transactions: Seq[ValidatedTopologyTransaction],
  )(implicit traceContext: TraceContext): Future[Unit] = blocking(synchronized {

    val (updates, appends) = TopologyStore.appends(timestamp, transactions)

    // UPDATE topology_transactions SET valid_until = ts WHERE store_id = ... AND valid_until is NULL AND valid_from < ts AND path_id IN (updates)
    updates.foreach { upd =>
      val idx =
        topologyTransactionStore.indexWhere(x =>
          x.transaction.uniquePath == upd && x.until.isEmpty && x.from < timestamp
        )
      if (idx > -1) {
        val item = topologyTransactionStore(idx)
        topologyTransactionStore.update(idx, item.copy(until = Some(timestamp)))
      }
    }
    // INSERT INTO topology_transactions (path_id, store_id, valid_from, transaction_type, operation, instance) VALUES inserts ON CONFLICT DO NOTHING
    appends.foreach { case InsertTransaction(trans, validUntil, rejectionReason) =>
      val operation = trans.operation

      // be idempotent
      if (
        !topologyTransactionStore.exists(x =>
          x.transaction.uniquePath == trans.uniquePath && x.from == timestamp && x.operation == operation
        )
      ) {
        topologyTransactionStore.append(
          TopologyStoreEntry(
            operation,
            trans,
            timestamp,
            validUntil,
            rejectionReason.map(_.asString),
          )
        )
      }
    }
    Future.unit
  })

  private def asOfFilter(
      asOf: CantonTimestamp,
      asOfInclusive: Boolean,
  ): (CantonTimestamp, Option[CantonTimestamp]) => Boolean =
    if (asOfInclusive) { case (validFrom, validUntil) =>
      validFrom <= asOf && validUntil.forall(until => asOf < until)
    }
    else { case (validFrom, validUntil) =>
      validFrom < asOf && validUntil.forall(until => asOf <= until)
    }

  override def timestamp(implicit traceContext: TraceContext): Future[Option[CantonTimestamp]] =
    Future.successful(topologyTransactionStore.lastOption.map(_.from))

  private def filteredState(
      table: Seq[TopologyStoreEntry[TopologyChangeOp]],
      filter: TopologyStoreEntry[TopologyChangeOp] => Boolean,
  ): Future[StoredTopologyTransactions[TopologyChangeOp]] =
    Future.successful(
      StoredTopologyTransactions(
        table.collect {
          case entry if filter(entry) && entry.rejected.isEmpty =>
            entry.toStoredTransaction
        }
      )
    )

  override def headTransactions(implicit
      traceContext: TraceContext
  ): Future[StoredTopologyTransactions[Positive]] =
    filteredState(
      blocking(synchronized(topologyTransactionStore.toSeq)),
      x => x.until.isEmpty,
    ).map(_.collectOfType[Positive])

  /** finds transactions in the local store that would remove the topology state elements
    */
  override def findRemovalTransactionForMappings(
      mappings: Set[TopologyStateElement[TopologyMapping]]
  )(implicit
      traceContext: TraceContext
  ): Future[Seq[SignedTopologyTransaction[Remove]]] =
    Future.successful(
      blocking(synchronized(topologyTransactionStore.toSeq))
        .map(_.transaction)
        .mapFilter(TopologyChangeOp.select[Remove])
        .collect {
          case sit @ SignedTopologyTransaction(TopologyStateUpdate(_, element), _, _)
              if mappings.contains(element) =>
            sit
        }
    )

  override def findActiveTransactionsForMapping(
      mapping: TopologyMapping
  )(implicit
      traceContext: TraceContext
  ): Future[Seq[SignedTopologyTransaction[Add]]] =
    Future.successful(
      blocking(synchronized(topologyTransactionStore.toSeq))
        .collect { case entry if entry.until.isEmpty => entry.transaction }
        .mapFilter(TopologyChangeOp.select[Add])
        .collect {
          case sit if sit.transaction.element.mapping == mapping => sit
        }
    )

  override def allTransactions(implicit
      traceContext: TraceContext
  ): Future[StoredTopologyTransactions[TopologyChangeOp]] =
    filteredState(blocking(synchronized(topologyTransactionStore.toSeq)), _ => true)

  override def exists(transaction: SignedTopologyTransaction[TopologyChangeOp])(implicit
      traceContext: TraceContext
  ): Future[Boolean] =
    allTransactions.map(_.result.exists(_.transaction == transaction))

  override def findPositiveTransactions(
      asOf: CantonTimestamp,
      asOfInclusive: Boolean,
      includeSecondary: Boolean,
      types: Seq[DomainTopologyTransactionType],
      filterUid: Option[Seq[UniqueIdentifier]],
      filterNamespace: Option[Seq[Namespace]],
  )(implicit traceContext: TraceContext): Future[PositiveStoredTopologyTransactions] =
    findPositiveTransactionsInStore(
      topologyTransactionStore,
      asOf,
      asOfInclusive,
      includeSecondary,
      types,
      filterUid,
      filterNamespace,
    )

  /** query interface used by [[com.digitalasset.canton.topology.client.StoreBasedTopologySnapshot]] */
  override def findStateTransactions(
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
      topologyStateStore,
      asOf,
      asOfInclusive,
      includeSecondary,
      types,
      filterUid,
      filterNamespace,
    )

  private def findTransactionsInStore[Op <: TopologyChangeOp](
      store: ArrayBuffer[TopologyStoreEntry[Op]],
      asOf: CantonTimestamp,
      asOfInclusive: Boolean,
      includeSecondary: Boolean,
      types: Seq[DomainTopologyTransactionType],
      filterUid: Option[Seq[UniqueIdentifier]],
      filterNamespace: Option[Seq[Namespace]],
  ): Future[StoredTopologyTransactions[TopologyChangeOp]] = {
    val timeFilter = asOfFilter(asOf, asOfInclusive)
    def pathFilter(path: UniquePath): Boolean = {
      if (filterUid.isEmpty && filterNamespace.isEmpty)
        true
      else {
        path.maybeUid.exists(uid => filterUid.exists(_.contains(uid))) ||
        filterNamespace.exists(_.contains(path.namespace))
      }
    }
    // filter for secondary uids (required for cascading updates)
    def secondaryFilter(entry: TopologyStoreEntry[TopologyChangeOp]): Boolean =
      includeSecondary &&
        entry.secondaryUid.exists(uid =>
          filterNamespace.exists(_.contains(uid.namespace)) ||
            filterUid.exists(_.contains(uid))
        )

    filteredState(
      blocking(synchronized { store.toSeq }),
      entry => {
        timeFilter(entry.from, entry.until) &&
        types.contains(entry.transaction.uniquePath.dbType) &&
        (pathFilter(entry.transaction.uniquePath) || secondaryFilter(entry))
      },
    )
  }

  private def findPositiveTransactionsInStore[Op <: TopologyChangeOp](
      store: ArrayBuffer[TopologyStoreEntry[Op]],
      asOf: CantonTimestamp,
      asOfInclusive: Boolean,
      includeSecondary: Boolean,
      types: Seq[DomainTopologyTransactionType],
      filterUid: Option[Seq[UniqueIdentifier]],
      filterNamespace: Option[Seq[Namespace]],
  ): Future[PositiveStoredTopologyTransactions] =
    findTransactionsInStore(
      store = store,
      asOf = asOf,
      asOfInclusive = asOfInclusive,
      includeSecondary = includeSecondary,
      types = types,
      filterUid = filterUid,
      filterNamespace = filterNamespace,
    ).map(_.positiveTransactions)

  /** query interface used by DomainIdentityManager to find the set of initial keys */
  override def findInitialState(
      uid: UniqueIdentifier
  )(implicit traceContext: TraceContext): Future[Map[KeyOwner, Seq[PublicKey]]] = {
    val res = topologyTransactionStore.foldLeft((false, Map.empty[KeyOwner, Seq[PublicKey]])) {
      case ((false, acc), TopologyStoreEntry(Add, transaction, _, _, None)) =>
        TopologyStore.findInitialStateAccumulator(uid, acc, transaction)
      case (acc, _) => acc
    }
    Future.successful(res._2)
  }

  /** update active topology transaction to the active topology transaction table
    *
    * active means that for the key authorizing the transaction, there is a connected path to reach the root certificate
    */
  override def updateState(
      timestamp: CantonTimestamp,
      deactivate: Seq[UniquePath],
      positive: Seq[SignedTopologyTransaction[Positive]],
  )(implicit traceContext: TraceContext): Future[Unit] = {
    blocking(synchronized {
      val deactivateS = deactivate.toSet
      // UPDATE topology_state SET valid_until = ts WHERE store_id = ... AND valid_from < ts AND valid_until is NULL and path_id in Deactivate)
      deactivate.foreach { _up =>
        val idx = topologyStateStore.indexWhere(entry =>
          entry.from < timestamp && entry.until.isEmpty &&
            deactivateS.contains(entry.transaction.uniquePath)
        )
        if (idx != -1) {
          val item = topologyStateStore(idx)
          topologyStateStore.update(idx, item.copy(until = Some(timestamp)))
        }
      }
      // INSERT IGNORE (sit)
      positive.foreach { sit =>
        if (
          !topologyStateStore.exists(x =>
            x.transaction.uniquePath == sit.uniquePath && x.from == timestamp && x.operation == sit.operation
          )
        ) {
          topologyStateStore.append(TopologyStoreEntry(sit.operation, sit, timestamp, None, None))
        }
      }
    })
    Future.unit
  }

  override def inspectKnownParties(
      timestamp: CantonTimestamp,
      filterParty: String,
      filterParticipant: String,
      limit: Int,
  )(implicit traceContext: TraceContext): Future[Set[PartyId]] = {
    def filter(entry: TopologyStoreEntry[Positive]): Boolean = {
      // active
      entry.from < timestamp && entry.until.forall(until => timestamp <= until) &&
      // not rejected
      entry.rejected.isEmpty &&
      // matches either a party to participant mapping (with appropriate filters)
      ((entry.transaction.uniquePath.dbType == DomainTopologyTransactionType.PartyToParticipant &&
        entry.transaction.uniquePath.maybeUid.exists(_.toProtoPrimitive.startsWith(filterParty)) &&
        entry.secondaryUid.exists(_.toProtoPrimitive.startsWith(filterParticipant))) ||
        // or matches a participant with appropriate filters
        (entry.transaction.uniquePath.dbType == DomainTopologyTransactionType.ParticipantState &&
          entry.transaction.uniquePath.maybeUid
            .exists(_.toProtoPrimitive.startsWith(filterParty)) &&
          entry.transaction.uniquePath.maybeUid
            .exists(_.toProtoPrimitive.startsWith(filterParticipant))))
    }
    val topologyStateStoreSeq = blocking(synchronized(topologyStateStore.toSeq))
    Future.successful(
      topologyStateStoreSeq
        .foldLeft(Set.empty[PartyId]) {
          case (acc, elem) if acc.size >= limit || !filter(elem) => acc
          case (acc, elem) => elem.transaction.uniquePath.maybeUid.fold(acc)(x => acc + PartyId(x))
        }
    )
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
    val store = if (stateStore) topologyStateStore else topologyTransactionStore
    def mkAsOfFlt(asOf: CantonTimestamp): TopologyStoreEntry[TopologyChangeOp] => Boolean = entry =>
      asOfFilter(asOf, asOfInclusive = false)(entry.from, entry.until)
    val filter1: TopologyStoreEntry[TopologyChangeOp] => Boolean = timeQuery match {
      case TimeQuery.HeadState =>
        // use recent timestamp to avoid race conditions (as we are looking
        // directly into the store, while the recent time still needs to propagate)
        recentTimestampO.map(mkAsOfFlt).getOrElse(entry => entry.until.isEmpty)
      case TimeQuery.Snapshot(asOf) => mkAsOfFlt(asOf)
      case TimeQuery.Range(from, until) =>
        entry => from.forall(ts => entry.from >= ts) && until.forall(ts => entry.from <= ts)
    }

    val filter2: TopologyStoreEntry[TopologyChangeOp] => Boolean = entry =>
      ops.forall(_ == entry.operation)

    val filter3: TopologyStoreEntry[TopologyChangeOp] => Boolean = {
      if (idFilter.isEmpty) _ => true
      else if (namespaceOnly) { entry =>
        entry.transaction.uniquePath.namespace.fingerprint.unwrap.startsWith(idFilter)
      } else {
        val splitted = idFilter.split(SafeSimpleString.delimiter)
        val prefix = splitted(0)
        if (splitted.lengthCompare(1) > 0) {
          val suffix = splitted(1)
          entry: TopologyStoreEntry[TopologyChangeOp] =>
            entry.transaction.uniquePath.maybeUid.forall(_.id.unwrap.startsWith(prefix)) &&
              entry.transaction.uniquePath.namespace.fingerprint.unwrap.startsWith(suffix)
        } else { entry =>
          entry.transaction.uniquePath.maybeUid.forall(_.id.unwrap.startsWith(prefix))
        }
      }
    }
    filteredState(
      blocking(synchronized(store.toSeq)),
      entry =>
        typ.forall(_ == entry.transaction.uniquePath.dbType) && filter1(entry) && filter2(
          entry
        ) && filter3(entry),
    )
  }

  override def findEffectiveTimestampsSince(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[Seq[CantonTimestamp]] =
    Future.successful(
      blocking(synchronized(topologyTransactionStore.toSeq))
        .map(_.from)
        .filter(_ > timestamp)
        .sorted
        .distinct
    )

  private val watermark = new AtomicReference[Option[CantonTimestamp]](None)
  override def currentDispatchingWatermark(implicit
      traceContext: TraceContext
  ): Future[Option[CantonTimestamp]] =
    Future.successful(watermark.get())

  override def updateDispatchingWatermark(
      timestamp: CantonTimestamp
  )(implicit traceContext: TraceContext): Future[Unit] = {
    watermark.getAndSet(Some(timestamp)) match {
      case Some(old) if old > timestamp =>
        logger.error(
          s"Topology dispatching watermark is running backwards! new=$timestamp, old=${old}"
        )
      case _ => ()
    }
    Future.unit
  }

  override def findDispatchingTransactionsAfter(
      timestamp: CantonTimestamp
  )(implicit traceContext: TraceContext): Future[StoredTopologyTransactions[TopologyChangeOp]] =
    blocking(synchronized {
      val res = topologyTransactionStore
        .filter(x =>
          x.from > timestamp && (x.until.isEmpty || x.operation == TopologyChangeOp.Remove) && x.rejected.isEmpty
        )
        .map(_.toStoredTransaction)
        .toSeq
      Future.successful(StoredTopologyTransactions(res))
    })

  override def findTsOfParticipantStateChangesBefore(
      beforeExclusive: CantonTimestamp,
      participantId: ParticipantId,
      limit: Int,
  )(implicit traceContext: TraceContext): Future[Seq[CantonTimestamp]] = blocking(synchronized {
    val ret = topologyTransactionStore
      .filter(x =>
        x.from < beforeExclusive &&
          x.transaction.transaction.element.mapping.dbType == DomainTopologyTransactionType.ParticipantState &&
          x.transaction.uniquePath.maybeUid.contains(participantId.uid)
      )
      .map(_.from)
      .sorted(CantonTimestamp.orderCantonTimestamp.toOrdering.reverse)
      .take(limit)
    Future.successful(ret.toSeq)
  })

  override def findTransactionsInRange(
      asOfExclusive: CantonTimestamp,
      upToExclusive: CantonTimestamp,
  )(implicit traceContext: TraceContext): Future[StoredTopologyTransactions[TopologyChangeOp]] =
    blocking(synchronized {
      val ret = topologyTransactionStore
        .filter(x => x.from > asOfExclusive && x.from < upToExclusive && x.rejected.isEmpty)
        .map(_.toStoredTransaction)
      Future.successful(StoredTopologyTransactions(ret.toSeq))
    })

  override def close(): Unit = ()

}
