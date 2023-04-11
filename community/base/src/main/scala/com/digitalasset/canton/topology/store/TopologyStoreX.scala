// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.store

import cats.syntax.traverse.*
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.resource.{DbStorage, MemoryStorage, Storage}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.admin.v1 as topoV1
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.store.StoredTopologyTransactionsX.PositiveStoredTopologyTransactionsX
import com.digitalasset.canton.topology.store.ValidatedTopologyTransactionX.GenericValidatedTopologyTransactionX
import com.digitalasset.canton.topology.store.memory.InMemoryTopologyStoreX
import com.digitalasset.canton.topology.transaction.SignedTopologyTransactionX.GenericSignedTopologyTransactionX
import com.digitalasset.canton.topology.transaction.TopologyMappingX.MappingHash
import com.digitalasset.canton.topology.transaction.TopologyTransactionX.TxHash
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.topology.{Namespace, UniqueIdentifier}
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.ClassTag

final case class StoredTopologyTransactionX[+Op <: TopologyChangeOpX, +M <: TopologyMappingX](
    sequenced: SequencedTime,
    validFrom: EffectiveTime,
    validUntil: Option[EffectiveTime],
    transaction: SignedTopologyTransactionX[Op, M],
) extends PrettyPrinting {
  override def pretty: Pretty[StoredTopologyTransactionX.this.type] =
    prettyOfClass(
      param("sequenced", _.sequenced.value),
      param("validFrom", _.validFrom.value),
      paramIfDefined("validUntil", _.validUntil.map(_.value)),
      param("op", _.transaction.transaction.op),
      param("serial", _.transaction.transaction.serial),
      param("mapping", _.transaction.transaction.mapping),
    )

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  def selectMapping[TargetMapping <: TopologyMappingX: ClassTag] = transaction
    .selectMapping[TargetMapping]
    .map(_ => this.asInstanceOf[StoredTopologyTransactionX[Op, TargetMapping]])

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  def selectOp[TargetOp <: TopologyChangeOpX: ClassTag] = transaction
    .selectOp[TargetOp]
    .map(_ => this.asInstanceOf[StoredTopologyTransactionX[TargetOp, M]])
}

object StoredTopologyTransactionX {
  type GenericStoredTopologyTransactionX =
    StoredTopologyTransactionX[TopologyChangeOpX, TopologyMappingX]
}

final case class ValidatedTopologyTransactionX[+Op <: TopologyChangeOpX, +M <: TopologyMappingX](
    transaction: SignedTopologyTransactionX[Op, M],
    rejectionReason: Option[TopologyTransactionRejection] = None,
) {}

object ValidatedTopologyTransactionX {
  type GenericValidatedTopologyTransactionX =
    ValidatedTopologyTransactionX[TopologyChangeOpX, TopologyMappingX]
}
abstract class TopologyStoreX[+StoreID <: TopologyStoreId](implicit
    val ec: ExecutionContext
) extends AutoCloseable
    with BaseTopologyStore[StoreID, GenericValidatedTopologyTransactionX] {
  this: NamedLogging =>

  def findProposalsByTxHash(effective: EffectiveTime, hashes: Seq[TxHash])(implicit
      traceContext: TraceContext
  ): Future[Seq[GenericSignedTopologyTransactionX]]

  def findTransactionsForMapping(
      effective: EffectiveTime,
      hashes: Seq[MappingHash],
  )(implicit traceContext: TraceContext): Future[Seq[GenericSignedTopologyTransactionX]]

  def findValidTransactions(
      asOfExclusive: EffectiveTime,
      filter: GenericSignedTopologyTransactionX => Boolean,
  ): Future[Seq[GenericSignedTopologyTransactionX]]

  /** returns the set of positive transactions
    *
    * this function is used by the topology processor to determine the set of transaction, such that
    * we can perform cascading updates if there was a certificate revocation
    *
    * @param asOfInclusive whether the search interval should include the current timepoint or not. the state at t is
    *                      defined as "exclusive" of t, whereas for updating the state, we need to be able to query inclusive.
    */
  def findPositiveTransactions(
      asOf: CantonTimestamp,
      asOfInclusive: Boolean,
      isProposal: Boolean,
      types: Seq[TopologyMappingX.Code],
      filterUid: Option[Seq[UniqueIdentifier]],
      filterNamespace: Option[Seq[Namespace]],
  )(implicit
      traceContext: TraceContext
  ): Future[PositiveStoredTopologyTransactionsX]

  /** add validated topology transaction as is to the topology transaction table */
  def update(
      sequenced: SequencedTime,
      effective: EffectiveTime,
      removeMapping: Set[MappingHash],
      removeTxs: Set[TxHash],
      additions: Seq[GenericValidatedTopologyTransactionX],
  )(implicit
      traceContext: TraceContext
  ): Future[Unit]

  // TODO(#11255) only a temporary crutch to inspect the topology state
  def dumpStoreContent()(implicit traceContext: TraceContext): Unit

  /** query optimized for inspection
    *
    * @param proposals if true, query only for proposals instead of approved transaction mappings
    * @param recentTimestampO if exists, use this timestamp for the head state to prevent race conditions on the console
    */
  def inspect(
      proposals: Boolean,
      timeQuery: TimeQueryX,
      recentTimestampO: Option[CantonTimestamp],
      ops: Option[TopologyChangeOpX],
      typ: Option[TopologyMappingX.Code],
      idFilter: String,
      namespaceOnly: Boolean,
  )(implicit
      traceContext: TraceContext
  ): Future[StoredTopologyTransactionsX[TopologyChangeOpX, TopologyMappingX]]

}

sealed trait TimeQueryX {
  def toProtoV1: topoV1.BaseQuery.TimeQuery
}
object TimeQueryX {
  object HeadState extends TimeQueryX {
    override def toProtoV1: topoV1.BaseQuery.TimeQuery =
      topoV1.BaseQuery.TimeQuery.HeadState(com.google.protobuf.empty.Empty())
  }
  final case class Snapshot(asOf: CantonTimestamp) extends TimeQueryX {
    override def toProtoV1: topoV1.BaseQuery.TimeQuery =
      topoV1.BaseQuery.TimeQuery.Snapshot(asOf.toProtoPrimitive)
  }
  final case class Range(from: Option[CantonTimestamp], until: Option[CantonTimestamp])
      extends TimeQueryX {
    override def toProtoV1: topoV1.BaseQuery.TimeQuery = topoV1.BaseQuery.TimeQuery.Range(
      topoV1.BaseQuery.TimeRange(from.map(_.toProtoPrimitive), until.map(_.toProtoPrimitive))
    )
  }

  def fromProto(
      proto: topoV1.BaseQuery.TimeQuery,
      fieldName: String,
  ): ParsingResult[TimeQueryX] =
    proto match {
      case topoV1.BaseQuery.TimeQuery.Empty =>
        Left(ProtoDeserializationError.FieldNotSet(fieldName))
      case topoV1.BaseQuery.TimeQuery.Snapshot(value) =>
        CantonTimestamp.fromProtoPrimitive(value).map(Snapshot)
      case topoV1.BaseQuery.TimeQuery.HeadState(_) => Right(HeadState)
      case topoV1.BaseQuery.TimeQuery.Range(value) =>
        for {
          fromO <- value.from.traverse(CantonTimestamp.fromProtoPrimitive)
          toO <- value.until.traverse(CantonTimestamp.fromProtoPrimitive)
        } yield Range(fromO, toO)
    }
}

object TopologyStoreX {
  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  def apply[StoreID <: TopologyStoreId](
      storeId: TopologyStoreId,
      storage: Storage,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      ec: ExecutionContext
  ): TopologyStoreX[StoreID] =
    storage match {
      case _: MemoryStorage =>
        (new InMemoryTopologyStoreX(storeId, loggerFactory)).asInstanceOf[TopologyStoreX[StoreID]]
      case jdbc: DbStorage =>
        // TODO(#11255) implement me!
        (new InMemoryTopologyStoreX(storeId, loggerFactory)).asInstanceOf[TopologyStoreX[StoreID]]
    }
}
