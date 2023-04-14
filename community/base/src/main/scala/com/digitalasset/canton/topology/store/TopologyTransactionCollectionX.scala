// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology.store

import cats.syntax.functorFilter.*
import cats.syntax.traverse.*
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.{DynamicDomainParameters, v0}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.processing.{EffectiveTime, SequencedTime}
import com.digitalasset.canton.topology.store.StoredTopologyTransactionX.GenericStoredTopologyTransactionX
import com.digitalasset.canton.topology.transaction.*
import com.digitalasset.canton.version.*

import scala.reflect.ClassTag

final case class StoredTopologyTransactionsX[+Op <: TopologyChangeOpX, +M <: TopologyMappingX](
    result: Seq[StoredTopologyTransactionX[Op, M]]
) extends HasVersionedWrapper[StoredTopologyTransactionsX[TopologyChangeOpX, TopologyMappingX]]
    with PrettyPrinting {

  override protected def companionObj = StoredTopologyTransactionsX

  override def pretty: Pretty[StoredTopologyTransactionsX.this.type] = prettyOfParam(
    _.result
  )

  def toTopologyState: List[TopologyMappingX] =
    result.map(_.transaction.transaction.mapping).toList

  // note, we are reusing v0, as v0 just expects bytestrings ...
  def toProtoV0: v0.TopologyTransactions = v0.TopologyTransactions(
    items = result.map { item =>
      v0.TopologyTransactions.Item(
        sequenced = Some(item.sequenced.toProtoPrimitive),
        validFrom = Some(item.validFrom.toProtoPrimitive),
        validUntil = item.validUntil.map(_.toProtoPrimitive),
        // these transactions are serialized as versioned topology transactions
        transaction = item.transaction.toByteString,
      )
    }
  )

  def collectOfType[T <: TopologyChangeOpX: ClassTag]: StoredTopologyTransactionsX[T, M] =
    StoredTopologyTransactionsX(
      result.mapFilter(_.selectOp[T])
    )

  /** Split transactions into certificates and everything else (used when uploading to a participant) */
  def splitCertsAndRest: StoredTopologyTransactionsX.CertsAndRest = {
    val certTypes = Set(
      TopologyMappingX.Code.NamespaceDelegationX,
      TopologyMappingX.Code.UnionspaceDefinitionX,
      TopologyMappingX.Code.IdentifierDelegationX,
    )
    val empty = Seq.empty[GenericStoredTopologyTransactionX]
    val (certs, rest) = result.foldLeft((empty, empty)) { case ((certs, rest), tx) =>
      if (certTypes.contains(tx.transaction.transaction.mapping.code))
        (certs :+ tx, rest)
      else
        (certs, rest :+ tx)
    }
    StoredTopologyTransactionsX.CertsAndRest(certs, rest)
  }

  /** The timestamp of the last topology transaction (if there is at least one)
    * adjusted by topology change delay
    */
  def lastChangeTimestamp: Option[CantonTimestamp] = {
    val epsilon = result
      .map(_.transaction.transaction.mapping)
      .collect { case x: DomainParametersStateX =>
        x.parameters.topologyChangeDelay
      }
      .lastOption
      .getOrElse(DynamicDomainParameters.topologyChangeDelayIfAbsent)
    val timestamp = result
      .map(_.validFrom.value)
      .maxOption
    timestamp.map(_.minus(epsilon.duration))
  }
}

object StoredTopologyTransactionsX
    extends HasVersionedMessageCompanion[
      StoredTopologyTransactionsX[TopologyChangeOpX, TopologyMappingX],
    ] {

  type GenericStoredTopologyTransactionsX =
    StoredTopologyTransactionsX[TopologyChangeOpX, TopologyMappingX]
  type PositiveStoredTopologyTransactionsX =
    StoredTopologyTransactionsX[TopologyChangeOpX.Replace, TopologyMappingX]

  val supportedProtoVersions: SupportedProtoVersions = SupportedProtoVersions(
    ProtoVersion(0) -> ProtoCodec(
      // TODO(#12373) Adapt when releasing BFT
      ProtocolVersion.v3,
      supportedProtoVersion(v0.TopologyTransactions)(fromProtoV0),
      _.toProtoV0.toByteString,
    )
  )

  def fromProtoV0(
      value: v0.TopologyTransactions
  ): ParsingResult[GenericStoredTopologyTransactionsX] = {
    def parseItem(
        item: v0.TopologyTransactions.Item
    ): ParsingResult[GenericStoredTopologyTransactionX] = {
      for {
        sequenced <- ProtoConverter.parseRequired(
          SequencedTime.fromProtoPrimitive,
          "sequenced",
          item.sequenced,
        )
        validFrom <- ProtoConverter.parseRequired(
          EffectiveTime.fromProtoPrimitive,
          "valid_from",
          item.validFrom,
        )
        validUntil <- item.validFrom.traverse(EffectiveTime.fromProtoPrimitive)
        transaction <- SignedTopologyTransactionX.fromByteString(item.transaction)
      } yield StoredTopologyTransactionX(
        sequenced,
        validFrom,
        validUntil,
        transaction,
      )
    }
    value.items
      .traverse(parseItem)
      .map(StoredTopologyTransactionsX(_))
  }

  final case class CertsAndRest(
      certs: Seq[GenericStoredTopologyTransactionX],
      rest: Seq[GenericStoredTopologyTransactionX],
  )

  def empty: GenericStoredTopologyTransactionsX =
    StoredTopologyTransactionsX[TopologyChangeOpX, TopologyMappingX](Seq())

  override protected def name: String = "topology transactions"
}
