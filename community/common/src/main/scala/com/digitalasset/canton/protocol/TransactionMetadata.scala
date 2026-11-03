// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.protocol.TransactionMetadata.ContractKeyHashMismatches
import com.digitalasset.daml.lf.crypto
import com.digitalasset.daml.lf.data.{ImmArray, Time}
import com.digitalasset.daml.lf.transaction.NodeId
import com.digitalasset.daml.lf.transaction.Transaction.Metadata

/** Collects the metadata of a LF transaction to the extent that is needed in Canton
  *
  * @param ledgerTime
  *   The ledger time of the transaction
  * @param preparationTime
  *   The preparation time of the transaction
  * @param seeds
  *   The node seeds by node ID
  */
final case class TransactionMetadata(
    ledgerTime: CantonTimestamp,
    preparationTime: CantonTimestamp,
    seeds: Map[LfNodeId, LfHash],
    contractKeyHashMismatches: ContractKeyHashMismatches,
)

object TransactionMetadata {
  def fromLf(ledgerTime: CantonTimestamp, metadata: Metadata): TransactionMetadata =
    TransactionMetadata(
      ledgerTime = ledgerTime,
      preparationTime = CantonTimestamp(metadata.preparationTime),
      seeds = metadata.nodeSeeds.toSeq.toMap,
      contractKeyHashMismatches = metadata.contractKeyMismatches,
    )

  def fromTransactionMeta(
      metaLedgerEffectiveTime: Time.Timestamp,
      metaPreparationTime: Time.Timestamp,
      metaOptNodeSeeds: Option[ImmArray[(NodeId, crypto.Hash)]],
  ): Either[String, TransactionMetadata] =
    for {
      seeds <- metaOptNodeSeeds.toRight("Node seeds must be specified")
    } yield TransactionMetadata(
      CantonTimestamp(metaLedgerEffectiveTime),
      CantonTimestamp(metaPreparationTime),
      seeds.toSeq.toMap,
      // Contract key hash mismatches are caught during interpretation in Phase 1 already
      // and don't need to be propagated here.
      Map.empty,
    )

  type ContractKeyHashMismatches =
    Map[LfContractId, (Option[LfGlobalKeyWithMaintainers], Option[LfGlobalKeyWithMaintainers])]
}
