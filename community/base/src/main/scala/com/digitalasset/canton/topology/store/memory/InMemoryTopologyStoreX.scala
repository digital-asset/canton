// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.store.memory

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.store.StoredTopologyTransactionsX.{
  GenericStoredTopologyTransactionsX,
  PositiveStoredTopologyTransactionsX,
}
import com.digitalasset.canton.topology.store.ValidatedTopologyTransactionX.GenericValidatedTopologyTransactionX
import com.digitalasset.canton.topology.store.{
  StoredTopologyTransactionX,
  StoredTopologyTransactionsX,
  TimeQueryX,
  TopologyStoreId,
  TopologyStoreX,
}
import com.digitalasset.canton.topology.transaction.SignedTopologyTransactionX.GenericSignedTopologyTransactionX
import com.digitalasset.canton.topology.transaction.TopologyMappingX.MappingHash
import com.digitalasset.canton.topology.transaction.TopologyTransactionX.TxHash
import com.digitalasset.canton.topology.transaction.{
  TopologyChangeOp,
  TopologyChangeOpX,
  TopologyMappingX,
  TopologyTransactionX,
}
import com.digitalasset.canton.topology.{Namespace, SafeSimpleString, UniqueIdentifier}
import com.digitalasset.canton.tracing.TraceContext

import java.util.concurrent.atomic.AtomicReference
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{ExecutionContext, Future, blocking}

class InMemoryTopologyStoreX[+StoreId <: TopologyStoreId](
    val storeId: StoreId,
    val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends TopologyStoreX[StoreId]
    with NamedLogging {

  override def close(): Unit = {}

  private case class TopologyStoreEntry(
      transaction: GenericSignedTopologyTransactionX,
      sequenced: SequencedTime,
      from: EffectiveTime,
      rejected: Option[String],
  ) {
    def toStoredTransaction: StoredTopologyTransactionX[TopologyChangeOpX, TopologyMappingX] =
      StoredTopologyTransactionX(sequenced, from, until.get(), transaction)

    val until = new AtomicReference[Option[EffectiveTime]](rejected.map(_ => from))
  }

  private val topologyTransactionStore = ArrayBuffer[TopologyStoreEntry]()

  override def findProposalsByTxHash(
      asOfExclusive: EffectiveTime,
      hashes: Seq[TxHash],
  )(implicit traceContext: TraceContext): Future[Seq[GenericSignedTopologyTransactionX]] = {
    val hashSet = hashes.toSet
    findFilter(
      asOfExclusive,
      entry => hashSet.contains(entry.transaction.transaction.hash) && entry.transaction.isProposal,
    )
  }

  private def findFilter(
      asOfExclusive: EffectiveTime,
      filter: TopologyStoreEntry => Boolean,
  ): Future[Seq[GenericSignedTopologyTransactionX]] = {
    blocking {
      synchronized {
        val res = topologyTransactionStore
          .filter(x =>
            x.from.value < asOfExclusive.value
              && x.rejected.isEmpty
              && (x.until.get().forall(_.value <= asOfExclusive.value))
              && filter(x)
          )
          .map(_.transaction)
          .toSeq
        Future.successful(res)
      }
    }
  }

  override def findTransactionsForMapping(asOfExclusive: EffectiveTime, hashes: Seq[MappingHash])(
      implicit traceContext: TraceContext
  ): Future[Seq[GenericSignedTopologyTransactionX]] = {
    val hashSet = hashes.toSet
    findFilter(
      asOfExclusive,
      entry =>
        !entry.transaction.isProposal && hashSet.contains(
          entry.transaction.transaction.mapping.uniqueKey
        ),
    )
  }

  override def findValidTransactions(
      asOfExclusive: EffectiveTime,
      filter: GenericSignedTopologyTransactionX => Boolean,
  ): Future[Seq[GenericSignedTopologyTransactionX]] =
    findFilter(asOfExclusive, entry => !entry.transaction.isProposal && filter(entry.transaction))

  override def update(
      sequenced: SequencedTime,
      effective: EffectiveTime,
      removeMapping: Set[TopologyMappingX.MappingHash],
      removeTxs: Set[TopologyTransactionX.TxHash],
      additions: Seq[GenericValidatedTopologyTransactionX],
  )(implicit traceContext: TraceContext): Future[Unit] =
    blocking {
      synchronized {
        // transactionally
        // UPDATE txs SET valid_until = effective WHERE effective < $effective AND valid_from is NULL
        //    AND ((mapping_hash IN $removeMapping) OR (tx_hash IN $removeTxs))
        // INSERT IGNORE DUPLICATES (...)
        topologyTransactionStore.foreach { tx =>
          if (
            tx.from.value < effective.value && tx.until.get().isEmpty && (removeMapping.contains(
              tx.transaction.transaction.mapping.uniqueKey
            ) || removeTxs.contains(tx.transaction.transaction.hash))
          )
            tx.until.set(Some(effective))
        }
        topologyTransactionStore.appendAll(
          additions.map(tx =>
            TopologyStoreEntry(
              tx.transaction,
              sequenced,
              from = effective,
              rejected = tx.rejectionReason.map(_.toString),
            )
          )
        )
        Future.unit
      }
    }

  override def dumpStoreContent()(implicit traceContext: TraceContext): Unit = {
    blocking {
      synchronized {
        logger.debug(
          topologyTransactionStore
            .map(_.toString)
            .mkString("Topology Store Content[", ", ", "]")
        )

      }
    }
  }

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

  private def filteredState(
      table: Seq[TopologyStoreEntry],
      filter: TopologyStoreEntry => Boolean,
  ): Future[StoredTopologyTransactionsX[TopologyChangeOpX, TopologyMappingX]] =
    Future.successful(
      StoredTopologyTransactionsX(
        table.collect {
          case entry if filter(entry) && entry.rejected.isEmpty =>
            entry.toStoredTransaction
        }
      )
    )

  override def inspect(
      proposals: Boolean,
      timeQuery: TimeQueryX,
      recentTimestampO: Option[CantonTimestamp],
      ops: Option[TopologyChangeOpX],
      typ: Option[TopologyMappingX.Code],
      idFilter: String,
      namespaceOnly: Boolean,
  )(implicit
      traceContext: TraceContext
  ): Future[StoredTopologyTransactionsX[TopologyChangeOpX, TopologyMappingX]] = {
    def mkAsOfFlt(asOf: CantonTimestamp): TopologyStoreEntry => Boolean = entry =>
      asOfFilter(asOf, asOfInclusive = false)(entry.from.value, entry.until.get().map(_.value))

    val filter1: TopologyStoreEntry => Boolean = timeQuery match {
      case TimeQueryX.HeadState =>
        // use recent timestamp to avoid race conditions (as we are looking
        // directly into the store, while the recent time still needs to propagate)
        recentTimestampO.map(mkAsOfFlt).getOrElse(entry => entry.until.get().isEmpty)
      case TimeQueryX.Snapshot(asOf) => mkAsOfFlt(asOf)
      case TimeQueryX.Range(from, until) =>
        entry =>
          from.forall(ts => entry.from.value >= ts) && until.forall(ts => entry.from.value <= ts)
    }

    val filter2: TopologyStoreEntry => Boolean = entry =>
      ops.forall(_ == entry.transaction.operation)

    val filter3: TopologyStoreEntry => Boolean = {
      if (idFilter.isEmpty) _ => true
      else if (namespaceOnly) { entry =>
        entry.transaction.signatures.exists(_.signedBy.unwrap.startsWith(idFilter))
      } else {
        val split = idFilter.split(SafeSimpleString.delimiter)
        val prefix = split(0)
        if (split.lengthCompare(1) > 0) {
          val suffix = split(1)
          entry: TopologyStoreEntry =>
            entry.transaction.transaction.mapping.maybeUid.forall(_.id.unwrap.startsWith(prefix)) &&
              entry.transaction.transaction.mapping.namespace.fingerprint.unwrap.startsWith(suffix)
        } else { entry =>
          entry.transaction.transaction.mapping.maybeUid.forall(_.id.unwrap.startsWith(prefix))
        }
      }
    }
    filteredState(
      blocking(synchronized(topologyTransactionStore.toSeq)),
      entry =>
        typ.forall(
          _ == entry.transaction.transaction.mapping.code
        ) && (entry.transaction.isProposal == proposals) && filter1(entry) && filter2(
          entry
        ) && filter3(entry),
    )
  }

  override def findPositiveTransactions(
      asOf: CantonTimestamp,
      asOfInclusive: Boolean,
      isProposal: Boolean,
      types: Seq[TopologyMappingX.Code],
      filterUid: Option[Seq[UniqueIdentifier]],
      filterNamespace: Option[Seq[Namespace]],
  )(implicit traceContext: TraceContext): Future[PositiveStoredTopologyTransactionsX] =
    findTransactionsInStore(asOf, asOfInclusive, isProposal, types, filterUid, filterNamespace).map(
      _.collectOfType[TopologyChangeOpX.Replace]
    )

  private def findTransactionsInStore[Op <: TopologyChangeOp](
      asOf: CantonTimestamp,
      asOfInclusive: Boolean,
      isProposal: Boolean,
      types: Seq[TopologyMappingX.Code],
      filterUid: Option[Seq[UniqueIdentifier]],
      filterNamespace: Option[Seq[Namespace]],
  ): Future[GenericStoredTopologyTransactionsX] = {
    val timeFilter = asOfFilter(asOf, asOfInclusive)
    def pathFilter(mapping: TopologyMappingX): Boolean = {
      if (filterUid.isEmpty && filterNamespace.isEmpty)
        true
      else {
        mapping.maybeUid.exists(uid => filterUid.exists(_.contains(uid))) ||
        filterNamespace.exists(_.contains(mapping.namespace))
      }
    }
    filteredState(
      blocking(synchronized { topologyTransactionStore.toSeq }),
      entry => {
        timeFilter(entry.from.value, entry.until.get().map(_.value)) &&
        types.contains(entry.transaction.transaction.mapping.code) &&
        (pathFilter(entry.transaction.transaction.mapping)) &&
        entry.transaction.isProposal == isProposal
      },
    )
  }

  /** store an initial set of topology transactions as given into the store */
  override def bootstrap(
      snapshot: GenericStoredTopologyTransactionsX
  )(implicit traceContext: TraceContext): Future[Unit] = Future {
    blocking {
      synchronized {
        topologyTransactionStore
          .appendAll(
            snapshot.result.map { tx =>
              TopologyStoreEntry(tx.transaction, tx.sequenced, tx.validFrom, rejected = None)
            }
          )
          .discard
      }
    }
  }
}
