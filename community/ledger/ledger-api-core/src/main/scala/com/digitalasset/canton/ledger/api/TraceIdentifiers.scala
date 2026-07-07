// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api

import com.daml.ledger.api.v2.reassignment.Reassignment
import com.daml.ledger.api.v2.topology_transaction.TopologyTransaction
import com.daml.ledger.api.v2.transaction.Transaction
import com.daml.tracing.SpanAttribute
import com.digitalasset.canton.ledger.participant.state.index.IndexUpdateService.ReceivedAcsCommitment

import scala.collection.mutable

/** Extracts identifiers from Protobuf messages to correlate traces.
  */
object TraceIdentifiers {

  private def setIfNotEmpty(
      attributes: mutable.Builder[(SpanAttribute, String), Map[SpanAttribute, String]],
      attribute: SpanAttribute,
      value: String,
  ): Unit =
    if (value.nonEmpty) attributes += attribute -> value

  private def setIfNotZero(
      attributes: mutable.Builder[(SpanAttribute, String), Map[SpanAttribute, String]],
      attribute: SpanAttribute,
      value: Long,
  ): Unit =
    if (value != 0) attributes += attribute -> value.toString

  /** Extract identifiers from a transaction message.
    */
  def fromTransaction(transaction: Transaction): Map[SpanAttribute, String] = {
    val attributes = Map.newBuilder[SpanAttribute, String]

    setIfNotZero(attributes, SpanAttribute.Offset, transaction.offset)
    setIfNotEmpty(attributes, SpanAttribute.CommandId, transaction.commandId)
    setIfNotEmpty(attributes, SpanAttribute.TransactionId, transaction.updateId)
    setIfNotEmpty(attributes, SpanAttribute.WorkflowId, transaction.workflowId)

    attributes.result()
  }

  /** Extract identifiers from a reassignment message.
    */
  def fromReassignment(reassignment: Reassignment): Map[SpanAttribute, String] = {
    val attributes = Map.newBuilder[SpanAttribute, String]

    setIfNotZero(attributes, SpanAttribute.Offset, reassignment.offset)
    setIfNotEmpty(attributes, SpanAttribute.CommandId, reassignment.commandId)
    setIfNotEmpty(attributes, SpanAttribute.TransactionId, reassignment.updateId)
    setIfNotEmpty(attributes, SpanAttribute.WorkflowId, reassignment.workflowId)

    attributes.result()
  }

  def fromTopologyTransaction(
      topologyTransaction: TopologyTransaction
  ): Map[SpanAttribute, String] = {
    val attributes = Map.newBuilder[SpanAttribute, String]

    setIfNotZero(attributes, SpanAttribute.Offset, topologyTransaction.offset)
    setIfNotEmpty(attributes, SpanAttribute.TransactionId, topologyTransaction.updateId)

    attributes.result()
  }

  /** Extract identifiers from a received ACS commitment.
    */
  def fromAcsCommitment(acsCommitment: ReceivedAcsCommitment): Map[SpanAttribute, String] = {
    val attributes = Map.newBuilder[SpanAttribute, String]

    setIfNotZero(attributes, SpanAttribute.Offset, acsCommitment.offset.unwrap)
    setIfNotEmpty(attributes, SpanAttribute.TransactionId, acsCommitment.updateId)

    attributes.result()
  }
}
