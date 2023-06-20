// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.transfer

import com.digitalasset.canton.data.{CantonTimestamp, FullTransferOutTree}
import com.digitalasset.canton.participant.GlobalOffset
import com.digitalasset.canton.protocol.messages.DeliveredTransferOutResult
import com.digitalasset.canton.protocol.{
  SerializableContract,
  SourceDomainId,
  TargetDomainId,
  TransactionId,
  TransferId,
}
import com.digitalasset.canton.topology.MediatorRef
import com.digitalasset.canton.util.OptionUtil
import com.digitalasset.canton.version.Transfer.SourceProtocolVersion
import com.digitalasset.canton.{RequestCounter, TransferCounterO}

/** Stores the data for a transfer that needs to be passed from the source domain to the target domain. */
final case class TransferData(
    sourceProtocolVersion: SourceProtocolVersion,
    transferOutTimestamp: CantonTimestamp,
    transferOutRequestCounter: RequestCounter,
    transferOutRequest: FullTransferOutTree,
    transferOutDecisionTime: CantonTimestamp,
    contract: SerializableContract,
    creatingTransactionId: TransactionId,
    transferOutResult: Option[DeliveredTransferOutResult],
    transferOutGlobalOffset: Option[GlobalOffset],
    transferInGlobalOffset: Option[GlobalOffset],
) {

  require(
    contract.contractId == transferOutRequest.contractId,
    s"Supplied contract with ID ${contract.contractId} differs from the ID ${transferOutRequest.contractId} of the transfer-out request.",
  )

  def targetDomain: TargetDomainId = transferOutRequest.targetDomain

  def sourceDomain: SourceDomainId = transferOutRequest.sourceDomain

  def transferId: TransferId = TransferId(transferOutRequest.sourceDomain, transferOutTimestamp)

  def sourceMediator: MediatorRef = transferOutRequest.mediator
  def transferCounter: TransferCounterO = transferOutRequest.transferCounter

  def addTransferOutResult(result: DeliveredTransferOutResult): Option[TransferData] =
    mergeTransferOutResult(Some(result))

  def addTransferOutGlobalOffset(offset: GlobalOffset): Option[TransferData] =
    mergeTransferOutGlobalOffset(Some(offset))

  def addTransferInGlobalOffset(offset: GlobalOffset): Option[TransferData] =
    mergeTransferInGlobalOffset(Some(offset))

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
              otherTransferOutGlobalOffset,
              otherTransferInGlobalOffset,
            ) =>
          mergeTransferOutResult(otherResult)
            .flatMap(_.mergeTransferOutGlobalOffset(otherTransferOutGlobalOffset))
            .flatMap(_.mergeTransferInGlobalOffset(otherTransferInGlobalOffset))
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

  private def mergeTransferOutGlobalOffset(
      offset: Option[GlobalOffset]
  ): Option[TransferData] = {
    val oldResult = this.transferOutGlobalOffset
    OptionUtil
      .mergeEqual(oldResult, offset)
      .map(merged => if (merged eq oldResult) this else this.copy(transferOutGlobalOffset = merged))
  }

  private def mergeTransferInGlobalOffset(
      offset: Option[GlobalOffset]
  ): Option[TransferData] = {
    val oldResult = this.transferInGlobalOffset
    OptionUtil
      .mergeEqual(oldResult, offset)
      .map(merged => if (merged eq oldResult) this else this.copy(transferInGlobalOffset = merged))
  }
}
