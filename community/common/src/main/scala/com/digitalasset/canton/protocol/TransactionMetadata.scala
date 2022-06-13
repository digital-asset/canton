// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import com.daml.ledger.participant.state.v2.TransactionMeta
import com.daml.lf.transaction.Transaction.Metadata
import com.digitalasset.canton.data.CantonTimestamp

/** Collects the metadata of a LF transaction to the extent that is needed in Canton
  *
  * @param ledgerTime The ledger time of the transaction
  * @param submissionTime The submission time of the transaction
  * @param seeds The node seeds by node ID
  */
case class TransactionMetadata(
    ledgerTime: CantonTimestamp,
    submissionTime: CantonTimestamp,
    seeds: Map[LfNodeId, LfHash],
)

object TransactionMetadata {
  def fromLf(ledgerTime: CantonTimestamp, metadata: Metadata): TransactionMetadata =
    TransactionMetadata(
      ledgerTime = ledgerTime,
      submissionTime = CantonTimestamp(metadata.submissionTime),
      seeds = metadata.nodeSeeds.toSeq.toMap,
    )

  def fromTransactionMeta(meta: TransactionMeta): Either[String, TransactionMetadata] = {
    for {
      seeds <- meta.optNodeSeeds.toRight("Node seeds must be specified")
    } yield TransactionMetadata(
      CantonTimestamp(meta.ledgerEffectiveTime),
      CantonTimestamp(meta.submissionTime),
      seeds.toSeq.toMap,
    )
  }

}
