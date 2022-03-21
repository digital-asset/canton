// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.store

import cats.syntax.traverse._
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.LengthLimitedString.DisplayName
import com.digitalasset.canton.config.RequireTypes.{LengthLimitedString, String256M, String255}
import com.digitalasset.canton.crypto.{PublicKey, SignatureCheckError}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{ErrorLoggingContext, NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.resource.{DbStorage, MemoryStorage, Storage}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology._
import com.digitalasset.canton.topology.admin.{v0 => topoV0}
import com.digitalasset.canton.topology.store.db.DbTopologyStoreFactory
import com.digitalasset.canton.topology.store.memory.InMemoryTopologyStoreFactory
import com.digitalasset.canton.topology.transaction._
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.ErrorUtil
import com.digitalasset.canton.ProtoDeserializationError
import com.google.common.annotations.VisibleForTesting

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{ExecutionContext, Future}

final case class StoredTopologyTransaction[+Op <: TopologyChangeOp](
    validFrom: CantonTimestamp,
    validUntil: Option[CantonTimestamp],
    transaction: SignedTopologyTransaction[Op],
) extends PrettyPrinting {
  override def pretty: Pretty[StoredTopologyTransaction.this.type] =
    prettyOfClass(
      param("validFrom", _.validFrom),
      param("validUntil", _.validUntil),
      param("op", _.transaction.transaction.op),
      param("mapping", _.transaction.transaction.element.mapping),
    )
}

/** the party metadata used to inform the ledger api server
  *
  * the first class parameters correspond to the relevant information, whereas the
  * second class parameters are synchronisation information used during crash recovery.
  * we don't want these in an equality comparison.
  */
final case class PartyMetadata(
    partyId: PartyId,
    displayName: Option[DisplayName],
    participantId: Option[ParticipantId],
)(
    val effectiveTimestamp: CantonTimestamp,
    val submissionId: String255,
    val notified: Boolean = false,
)

trait PartyMetadataStore extends AutoCloseable {

  def metadataForParty(partyId: PartyId)(implicit
      traceContext: TraceContext
  ): Future[Option[PartyMetadata]]

  def insertOrUpdatePartyMetadata(
      partyId: PartyId,
      participantId: Option[ParticipantId],
      displayName: Option[DisplayName],
      effectiveTimestamp: CantonTimestamp,
      submissionId: String255,
  )(implicit traceContext: TraceContext): Future[Unit]

  /** mark the given metadata as having been successfully forwarded to the domain */
  def markNotified(metadata: PartyMetadata)(implicit traceContext: TraceContext): Future[Unit]

  /** fetch the current set of party data which still needs to be notified */
  def fetchNotNotified()(implicit traceContext: TraceContext): Future[Seq[PartyMetadata]]

}

sealed trait TopologyStoreId {

  def filterName: String = dbString.unwrap
  def dbString: LengthLimitedString

}
object TopologyStoreId {

  /** A topology store storing sequenced topology transactions
    *
    * @param domainId the domain id of the store
    * @param discriminator the discriminator of the store. only used for embedded mediator topology stores
    */
  final case class DomainStore(domainId: DomainId, discriminator: String = "")
      extends TopologyStoreId {
    lazy val dbString: LengthLimitedString = domainId.toLengthLimitedString
  }
  // authorized transactions (the topology managers store)
  object AuthorizedStore extends TopologyStoreId {
    val dbString = String255.tryCreate("Authorized")
  }
  // requested transactions (requests sent via domain service)
  object RequestedStore extends TopologyStoreId {
    val dbString = String255.tryCreate("Requested")
  }
  def apply(fName: String): TopologyStoreId = fName match {
    case "Authorized" => AuthorizedStore
    case "Requested" => RequestedStore
    case domain => DomainStore(DomainId(UniqueIdentifier.tryFromProtoPrimitive(domain)))
  }
}

trait TopologyStoreFactory extends AutoCloseable {

  def forId(storeId: TopologyStoreId): TopologyStore

  /** returns all stores except the discriminated store of the embedded mediator */
  def allNonDiscriminated(implicit
      traceContext: TraceContext
  ): Future[Map[TopologyStoreId, TopologyStore]]

  def partyMetadataStore(): PartyMetadataStore

}

object TopologyStoreFactory {
  def apply(storage: Storage, timeouts: ProcessingTimeout, loggerFactory: NamedLoggerFactory)(
      implicit ec: ExecutionContext
  ): TopologyStoreFactory =
    storage match {
      case _: MemoryStorage => new InMemoryTopologyStoreFactory(loggerFactory)
      case jdbc: DbStorage =>
        // TODO(rv) propagate this value into config values
        new DbTopologyStoreFactory(jdbc, maxItemsInSqlQuery = 100, timeouts, loggerFactory)
    }
}

sealed trait TopologyTransactionRejection extends PrettyPrinting {
  def asString: String
  def asString1GB: String256M =
    String256M.tryCreate(asString, Some("topology transaction rejection"))
}
object TopologyTransactionRejection {
  object NotAuthorized extends TopologyTransactionRejection {
    def asString: String = "Not authorized"
    override def pretty: Pretty[NotAuthorized.type] = prettyOfString(_ => asString)
  }
  case class SignatureCheckFailed(err: SignatureCheckError) extends TopologyTransactionRejection {
    def asString: String = err.toString
    override def pretty: Pretty[SignatureCheckFailed] = prettyOfClass(param("err", _.err))
  }
  case class WrongDomain(wrong: DomainId) extends TopologyTransactionRejection {
    def asString: String = show"Wrong domain $wrong"
    override def pretty: Pretty[WrongDomain] = prettyOfClass(param("wrong", _.wrong))
  }
}

final case class ValidatedTopologyTransaction(
    transaction: SignedTopologyTransaction[TopologyChangeOp],
    rejectionReason: Option[TopologyTransactionRejection],
)

object ValidatedTopologyTransaction {
  def valid(
      transaction: SignedTopologyTransaction[TopologyChangeOp]
  ): ValidatedTopologyTransaction =
    ValidatedTopologyTransaction(transaction, None)
}

sealed trait TimeQuery {
  def toProtoV0: topoV0.BaseQuery.TimeQuery
}
object TimeQuery {

  /** Determine the headstate.
    */
  object HeadState extends TimeQuery {
    override def toProtoV0: topoV0.BaseQuery.TimeQuery =
      topoV0.BaseQuery.TimeQuery.HeadState(com.google.protobuf.empty.Empty())
  }
  case class Snapshot(asOf: CantonTimestamp) extends TimeQuery {
    override def toProtoV0: topoV0.BaseQuery.TimeQuery =
      topoV0.BaseQuery.TimeQuery.Snapshot(asOf.toProtoPrimitive)
  }
  case class Range(from: Option[CantonTimestamp], until: Option[CantonTimestamp])
      extends TimeQuery {
    override def toProtoV0: topoV0.BaseQuery.TimeQuery = topoV0.BaseQuery.TimeQuery.Range(
      topoV0.BaseQuery.TimeRange(from.map(_.toProtoPrimitive), until.map(_.toProtoPrimitive))
    )
  }

  def fromProto(
      proto: topoV0.BaseQuery.TimeQuery,
      fieldName: String,
  ): ParsingResult[TimeQuery] =
    proto match {
      case topoV0.BaseQuery.TimeQuery.Empty =>
        Left(ProtoDeserializationError.FieldNotSet(fieldName))
      case topoV0.BaseQuery.TimeQuery.Snapshot(value) =>
        CantonTimestamp.fromProtoPrimitive(value).map(Snapshot)
      case topoV0.BaseQuery.TimeQuery.HeadState(_) => Right(HeadState)
      case topoV0.BaseQuery.TimeQuery.Range(value) =>
        for {
          fromO <- value.from.traverse(CantonTimestamp.fromProtoPrimitive)
          toO <- value.until.traverse(CantonTimestamp.fromProtoPrimitive)
          _ <- Either.cond(
            fromO.nonEmpty || toO.nonEmpty,
            (),
            ProtoDeserializationError
              .ValueDeserializationError(
                "range",
                "At least one of from / to needs to be set within a time range query",
              ),
          )
        } yield Range(fromO, toO)
    }

}

abstract class TopologyStore(implicit ec: ExecutionContext) extends AutoCloseable {
  this: NamedLogging =>

  private val monotonicityCheck = new AtomicReference[Option[CantonTimestamp]](None)

  protected def monotonicityTimeCheckUpdate(ts: CantonTimestamp): Option[CantonTimestamp] =
    monotonicityCheck.getAndSet(Some(ts))

  /** returns the current dispatching watermark
    *
    * for topology transaction dispatching, we keep track up to which point in time
    * we have mirrored the authorized store to the remote store
    *
    * the timestamp always refers to the timestamp of the authorized store!
    */
  def currentDispatchingWatermark(implicit
      traceContext: TraceContext
  ): Future[Option[CantonTimestamp]]

  /** update the dispatching watermark for this target store */
  def updateDispatchingWatermark(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[Unit]

  /** returns transactions that should be dispatched to the domain */
  def findDispatchingTransactionsAfter(
      timestamp: CantonTimestamp
  )(implicit traceContext: TraceContext): Future[StoredTopologyTransactions[TopologyChangeOp]]

  /** returns an descending ordered list of timestamps of when participant state changes occurred before a certain point in time */
  def findTsOfParticipantStateChangesBefore(
      beforeExclusive: CantonTimestamp,
      participantId: ParticipantId,
      limit: Int,
  )(implicit traceContext: TraceContext): Future[Seq[CantonTimestamp]]

  /** Returns an ordered list of transactions from the transaction store within a certain range */
  def findTransactionsInRange(asOfExclusive: CantonTimestamp, upToExclusive: CantonTimestamp)(
      implicit traceContext: TraceContext
  ): Future[StoredTopologyTransactions[TopologyChangeOp]]

  def timestamp(implicit traceContext: TraceContext): Future[Option[CantonTimestamp]]

  /** set of topology transactions which are active */
  def headTransactions(implicit
      traceContext: TraceContext
  ): Future[StoredTopologyTransactions[TopologyChangeOp.Positive]]

  /** finds transactions in the local store that would remove the topology state elements */
  def findRemovalTransactionForMappings(
      mappings: Set[TopologyStateElement[TopologyMapping]]
  )(implicit
      traceContext: TraceContext
  ): Future[Seq[SignedTopologyTransaction[TopologyChangeOp.Remove]]]

  def findActiveTransactionsForMapping(mapping: TopologyMapping)(implicit
      traceContext: TraceContext
  ): Future[Seq[SignedTopologyTransaction[TopologyChangeOp.Add]]]

  @VisibleForTesting
  def allTransactions(implicit
      traceContext: TraceContext
  ): Future[StoredTopologyTransactions[TopologyChangeOp]]

  def exists(transaction: SignedTopologyTransaction[TopologyChangeOp])(implicit
      traceContext: TraceContext
  ): Future[Boolean]

  /** bootstrap a sequencer node state from a topology transaction collection */
  def bootstrap(
      collection: StoredTopologyTransactions[TopologyChangeOp.Positive]
  )(implicit traceContext: TraceContext): Future[Unit] =
    collection.result
      .groupBy(_.validFrom)
      .toList
      .sortBy { case (validFrom, _) => validFrom }
      .traverse { case (validFrom, transactions) =>
        val txs = transactions.map(tx => ValidatedTopologyTransaction(tx.transaction, None))
        for {
          _ <- append(validFrom, txs)
          _ <- updateState(
            validFrom,
            deactivate = Seq.empty,
            positive = transactions.map(_.transaction),
          )
        } yield ()
      }
      .map(_ => ())

  /** add validated topology transaction as is to the topology transaction table */
  def append(timestamp: CantonTimestamp, transactions: Seq[ValidatedTopologyTransaction])(implicit
      traceContext: TraceContext
  ): Future[Unit] = {
    monotonicityTimeCheckUpdate(timestamp).foreach { prev =>
      ErrorUtil.requireState(
        prev < timestamp,
        s"Append is not monotonically increasing. However, the topology store is based on this assumption. Previous: $prev, Next: $timestamp",
      )
    }
    doAppend(timestamp, transactions)
  }

  private[topology] def doAppend(
      timestamp: CantonTimestamp,
      transactions: Seq[ValidatedTopologyTransaction],
  )(implicit traceContext: TraceContext): Future[Unit]

  /** returns the set of positive transactions
    *
    * this function is used by the topology processor to determine the set of transaction, such that
    * we can perform cascading updates if there was a certificate revocation
    *
    * @param asOfInclusive whether the search interval should include the current timepoint or not. the state at t is
    *                      defined as "exclusive" of t, whereas for updating the state, we need to be able to query inclusive.
    * @param includeSecondary some topology transactions have an "secondary" uid. currently, this only applies to the
    *                         party to participant mapping where the secondary uid is the participant uid.
    *                         we need this information during cascading updates of participant certificates.
    */
  def findPositiveTransactions(
      asOf: CantonTimestamp,
      asOfInclusive: Boolean,
      includeSecondary: Boolean,
      types: Seq[DomainTopologyTransactionType],
      filterUid: Option[Seq[UniqueIdentifier]],
      filterNamespace: Option[Seq[Namespace]],
  )(implicit
      traceContext: TraceContext
  ): Future[PositiveStoredTopologyTransactions]

  /** query interface used by DomainIdentityManager to find the set of initial keys */
  def findInitialState(uid: UniqueIdentifier)(implicit
      traceContext: TraceContext
  ): Future[Map[KeyOwner, Seq[PublicKey]]]

  /** update active topology transaction to the active topology transaction table
    *
    * active means that for the key authorizing the transaction, there is a connected path to reach the root certificate
    */
  def updateState(
      timestamp: CantonTimestamp,
      deactivate: Seq[UniquePath],
      positive: Seq[SignedTopologyTransaction[TopologyChangeOp.Positive]],
  )(implicit traceContext: TraceContext): Future[Unit]

  /** query optimized for inspection
    *
    * @param recentTimestampO if exists, use this timestamp for the head state to prevent race conditions on the console
    */
  def inspect(
      stateStore: Boolean,
      timeQuery: TimeQuery,
      recentTimestampO: Option[CantonTimestamp],
      ops: Option[TopologyChangeOp],
      typ: Option[DomainTopologyTransactionType],
      idFilter: String,
      namespaceOnly: Boolean,
  )(implicit
      traceContext: TraceContext
  ): Future[StoredTopologyTransactions[TopologyChangeOp]]

  /** find active topology transactions
    *
    * active / state means that for the key authorizing the transaction, there is a connected path to reach the root certificate
    * this function is used for updating and by the lookup client [[com.digitalasset.canton.topology.client.StoreBasedTopologySnapshot]]
    *
    * @param asOfInclusive whether the search interval should include the current timepoint or not. the state at t is
    *                      defined as "exclusive" of t, whereas for updating the state, we need to be able to query inclusive.
    * @param includeSecondary some topology transactions have an "secondary" uid. currently, this only applies to the
    *                         party to participant mapping where the secondary uid is the participant uid.
    *                         we need this information during cascading updates of participant certificates.
    */
  def findStateTransactions(
      asOf: CantonTimestamp,
      asOfInclusive: Boolean,
      includeSecondary: Boolean,
      types: Seq[DomainTopologyTransactionType],
      filterUid: Option[Seq[UniqueIdentifier]],
      filterNamespace: Option[Seq[Namespace]],
  )(implicit traceContext: TraceContext): Future[PositiveStoredTopologyTransactions]

  /** fetch the effective time updates greater than a certain timestamp
    *
    * this function is used to recover the future effective timestamp such that we can reschedule "pokes" of the
    * topology client and updates of the acs commitment processor on startup
    */
  def findEffectiveTimestampsSince(timestamp: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[Seq[CantonTimestamp]]

}

object TopologyStore {

  private[topology] case class InsertTransaction(
      transaction: SignedTopologyTransaction[TopologyChangeOp],
      validUntil: Option[CantonTimestamp],
      rejectionReason: Option[TopologyTransactionRejection],
  ) {
    def op: TopologyChangeOp = transaction.transaction.op
  }

  /** collect all actions on the topology store during an append
    *
    * this function computes the paths of all transactions that will "expire" because of some
    * removal in this update block and calculates what needs to be inserted into the store.
    * we also insert transient data (for debugability and completeness).
    */
  private[topology] def appends(
      timestamp: CantonTimestamp,
      transactions: Seq[ValidatedTopologyTransaction],
  )(implicit loggingContext: ErrorLoggingContext): (
      Set[UniquePath], // updates
      Seq[InsertTransaction],
  ) = {
    val logger = loggingContext.logger
    implicit val traceContext: TraceContext = loggingContext.traceContext

    case class PendingInsert(include: Boolean, entry: InsertTransaction)
    type PendingInsertIdx = Int

    // random access array in order to adjust pending insertions
    val inserts = new Array[PendingInsert](transactions.length)

    def adjustPending(
        index: PendingInsertIdx,
        pending: Map[UniquePath, Seq[PendingInsertIdx]],
        warnOnDuplicate: Boolean = true,
    ): Map[UniquePath, Seq[PendingInsertIdx]] = {
      val pendingInsert = inserts(index)
      val op = pendingInsert.entry.op
      val path = pendingInsert.entry.transaction.uniquePath

      val previous = pending.getOrElse(path, Seq())
      val previousI = previous.map(ii => (ii, inserts(ii))).filter { case (_, pendingInsert) =>
        pendingInsert.include
      }

      op match {
        // if this one is an add (resp., replace): only dedupe conflicting adds (resp., replaces)
        case TopologyChangeOp.Add | TopologyChangeOp.Replace =>
          previousI.foreach { case (ii, item) =>
            // ignore conflicting add
            if (item.entry.op == op) {
              inserts(ii) = item.copy(include = false)
              if (warnOnDuplicate)
                logger.warn(
                  s"Discarding duplicate ${op.toString} (#$ii): ${item.entry.transaction.uniquePath}"
                )
            }
          // malicious domain: theoretically we could check here if a certificate has already been revoked
          // previously. however, we assume that the domain topology manager would not do that generally (and we would
          // have to check this also against all revocations in the database as well).
          // TODO(i4933) check for permanent revocations
          }
        // if this one is a remove: deprecate pending adds and dedupe conflicting removes
        case TopologyChangeOp.Remove =>
          previousI.foreach { case (ii, item) =>
            if (item.entry.op == TopologyChangeOp.Remove) {
              // ignore conflicting remove
              inserts(ii) = item.copy(include = false)
              logger.info(
                s"Discarding conflicting removal (#$ii): ${item.entry.transaction.uniquePath}"
              )
            } else {
              // deprecate pending add
              inserts(ii) = item.copy(entry = item.entry.copy(validUntil = Some(timestamp)))
            }
          }
      }
      pending + (path -> (previous :+ index))
    }

    def validUntil(x: SignedTopologyTransaction[TopologyChangeOp]): Option[CantonTimestamp] =
      x.operation match {
        case TopologyChangeOp.Remove => Some(timestamp)
        case _ => None
      }

    // iterate over all transactions and adjust the validity period of any transient or special transaction
    val (updates, _) =
      transactions.zipWithIndex.foldLeft(
        (Set.empty[UniquePath], Map.empty[UniquePath, Seq[PendingInsertIdx]])
      ) {
        case (
              (updates, pending),
              (ValidatedTopologyTransaction(x: SignedTopologyTransaction[_], reason), index),
            ) =>
          inserts(index) = PendingInsert(
            include = true,
            InsertTransaction(x, validUntil(x), reason),
          )

          (x.transaction.op: TopologyChangeOp) match {
            case TopologyChangeOp.Remove | TopologyChangeOp.Replace =>
              // if this removal (or replace) is not authorized, then don't update the current exiting records
              val newUpdates =
                if (reason.isEmpty)
                  updates + x.uniquePath
                else updates
              (newUpdates, adjustPending(index, pending))

            case TopologyChangeOp.Add => (updates, adjustPending(index, pending))
          }
      }

    (
      updates,
      inserts.collect {
        case insert if insert.include =>
          val insertTx = insert.entry
          // mark all rejected transactions to be validFrom = validUntil
          insertTx.rejectionReason.fold(insertTx)(_ => insertTx.copy(validUntil = Some(timestamp)))
      }.toSeq,
    )
  }

  /** Initial state accumulator
    *
    * Initially, when bootstrapping a domain, we need to know the domain topology manager and the sequencer
    * key(s) before they have been sequenced. Therefore, we'll look at the couple of first transactions of the
    * authorized domain topology store.
    *
    * This accumulator should be iterated over until the boolean flag says its done.
    */
  private[topology] def findInitialStateAccumulator(
      uid: UniqueIdentifier,
      accumulated: Map[KeyOwner, Seq[PublicKey]],
      transaction: SignedTopologyTransaction[TopologyChangeOp],
  ): (Boolean, Map[KeyOwner, Seq[PublicKey]]) = {
    // we are done once we observe a transaction that does not act on our uid
    val done =
      transaction.uniquePath.maybeUid.nonEmpty && !transaction.uniquePath.maybeUid.contains(uid) &&
        accumulated.isDefinedAt(SequencerId(uid)) && accumulated.isDefinedAt(
          DomainTopologyManagerId(uid)
        )
    if (done || transaction.uniquePath.dbType != DomainTopologyTransactionType.OwnerToKeyMapping) {
      (done, accumulated)
    } else {
      transaction match {
        case SignedTopologyTransaction(
              TopologyStateUpdate(
                TopologyChangeOp.Add,
                TopologyStateUpdateElement(_, OwnerToKeyMapping(owner, key)),
              ),
              _,
              _,
            ) if owner.code == SequencerId.Code || owner.code == DomainTopologyManagerId.Code =>
          (false, accumulated.updated(owner, accumulated.getOrElse(owner, Seq()) :+ key))
        case _ => (false, accumulated)
      }
    }
  }

}
