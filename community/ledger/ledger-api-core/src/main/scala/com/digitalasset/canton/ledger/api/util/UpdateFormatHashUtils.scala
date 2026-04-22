// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.util

import com.daml.ledger.api.v2.transaction_filter.{
  EventFormat,
  ParticipantAuthorizationTopologyFormat,
  TopologyFormat,
  TransactionFormat,
  UpdateFormat,
}
import com.digitalasset.canton.crypto.{Hash, HashBuilder}
import com.google.protobuf.ByteString

object UpdateFormatHashUtils {
  @SuppressWarnings(Array("com.digitalasset.canton.ProtobufToByteString"))
  def hashEventFormat[T <: HashBuilder](
      builder: T
  )(eventFormat: EventFormat): T =
    // since EventFormat contains a Map, we need to be careful with calculating the hash
    builder
      .addMap(eventFormat.filtersByParty)((hashBuilder, key) => hashBuilder.addString(key))(
        (hashBuilder, value) => hashBuilder.addByteString(value.toByteString)
      )
      .addOptional(eventFormat.filtersForAnyParty.map(_.toByteString), _.addByteString)
      .addBool(eventFormat.verbose)

  def hashTopologyFormat[T <: HashBuilder](
      hashBuilder: T
  )(topologyFormat: TopologyFormat): T =
    hashBuilder.addOptional(
      topologyFormat.includeParticipantAuthorizationEvents,
      hashParticipangAuthorizationFormat,
    )

  private def hashParticipangAuthorizationFormat[T <: HashBuilder](hashBuilder: T)(
      participantAuthorizationFormat: ParticipantAuthorizationTopologyFormat
  ): T = participantAuthorizationFormat.parties.foldLeft(hashBuilder)(_.addString(_))

  def hashTransactionFormat[T <: HashBuilder](hashBuilder: T)(
      transactionFormat: TransactionFormat
  ): T =
    hashBuilder
      .addOptional(transactionFormat.eventFormat, hashEventFormat)
      .addInt(transactionFormat.transactionShape.value)

  def hashUpdateFormat[T <: HashBuilder](hashBuilder: T)(uf: UpdateFormat): T =
    hashBuilder
      .addOptional(uf.includeReassignments, hashEventFormat)
      .addOptional(
        uf.includeTopologyEvents,
        hashTopologyFormat,
      )
      .addOptional(
        uf.includeTransactions,
        hashTransactionFormat,
      )

  def toChecksum(hash: Hash): ByteString = hash.unwrap.substring(0, 4)
}
