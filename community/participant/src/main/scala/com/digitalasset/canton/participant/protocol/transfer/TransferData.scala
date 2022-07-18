// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.transfer

import com.digitalasset.canton.data.{CantonTimestamp, FullTransferOutTree}
import com.digitalasset.canton.participant.RequestCounter
import com.digitalasset.canton.protocol.messages.DeliveredTransferOutResult
import com.digitalasset.canton.protocol.{SerializableContract, TransactionId, TransferId}
import com.digitalasset.canton.topology.{DomainId, MediatorId}
import com.digitalasset.canton.util.OptionUtil
import com.digitalasset.canton.version.SourceProtocolVersion

/** Stores the data for a transfer that needs to be passed from the source domain to the target domain. */
case class TransferData(
    sourceProtocolVersion: SourceProtocolVersion,
    transferOutTimestamp: CantonTimestamp,
    transferOutRequestCounter: RequestCounter,
    transferOutRequest: FullTransferOutTree,
    transferOutDecisionTime: CantonTimestamp,
    contract: SerializableContract,
    creatingTransactionId: TransactionId,
    transferOutResult: Option[DeliveredTransferOutResult],
) {

  require(
    contract.contractId == transferOutRequest.contractId,
    s"Supplied contract with ID ${contract.contractId} differs from the ID ${transferOutRequest.contractId} of the transfer-out request.",
  )

  def targetDomain: DomainId = transferOutRequest.targetDomain

  def sourceDomain: DomainId = transferOutRequest.sourceDomain

  def transferId: TransferId = TransferId(transferOutRequest.sourceDomain, transferOutTimestamp)

  def sourceMediator: MediatorId = transferOutRequest.mediatorId

  def addTransferOutResult(result: DeliveredTransferOutResult): Option[TransferData] =
    mergeTransferOutResult(Some(result))

  def mergeWith(other: TransferData): Option[TransferData] = {
    if (this eq other) Some(this)
    else
      other match {
        case TransferData(
              `sourceProtocolVersion`,
              `transferOutTimestamp`,
              `transferOutRequestCounter`,
              `transferOutRequest`,
              `transferOutDecisionTime`,
              `contract`,
              `creatingTransactionId`,
              otherResult,
            ) =>
          mergeTransferOutResult(otherResult)
        case _ => None
      }
  }

  private[this] def mergeTransferOutResult(
      result: Option[DeliveredTransferOutResult]
  ): Option[TransferData] = {
    val oldResult = this.transferOutResult
    OptionUtil
      .mergeEqual(oldResult, result)
      .map(merged => if (merged eq oldResult) this else this.copy(transferOutResult = merged))
  }
}
