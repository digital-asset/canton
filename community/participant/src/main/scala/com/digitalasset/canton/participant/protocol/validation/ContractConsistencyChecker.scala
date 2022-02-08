// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.validation

import cats.data.Validated
import cats.syntax.either._
import cats.syntax.foldable._
import com.digitalasset.canton.data.{CantonTimestamp, TransactionView}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.LfContractId

object ContractConsistencyChecker {

  /** Indicates that the given transaction uses a contract that has been created with a ledger time
    * after the ledger time of the transaction.
    */
  case class ReferenceToFutureContractError(
      contractId: LfContractId,
      contractCreationTime: CantonTimestamp,
      transactionLedgerTime: CantonTimestamp,
  ) extends PrettyPrinting {
    override def pretty: Pretty[ReferenceToFutureContractError] = prettyOfClass(
      param("contract id", _.contractId),
      param("contract creation time", _.contractCreationTime),
      param("transaction ledger time", _.transactionLedgerTime),
    )
  }

  private def usedContracts(
      rootViews: List[TransactionView]
  ): List[(LfContractId, CantonTimestamp)] = {
    val coreInputs = List.newBuilder[(LfContractId, CantonTimestamp)]

    def go(view: TransactionView): Unit = {
      coreInputs ++= view.viewParticipantData.unwrap
        .map(_.coreInputs.map { case (cid, inputContract) =>
          (cid, inputContract.contract.ledgerCreateTime)
        })
        .getOrElse(List.empty)

      view.subviews.foreach(_.unwrap.foreach(go))
    }

    rootViews.foreach(go)
    coreInputs.result()
  }

  /** Checks for all unblinded [[com.digitalasset.canton.data.ViewParticipantData]] nodes in the `rootViews`
    * that the [[com.digitalasset.canton.data.ViewParticipantData.coreInputs]] have a ledger time
    * no later than `ledgerTime`.
    */
  def assertInputContractsInPast(
      rootViews: List[TransactionView],
      ledgerTime: CantonTimestamp,
  ): Either[List[ReferenceToFutureContractError], Unit] = {
    val contracts = usedContracts(rootViews)
    contracts
      .traverse_ { case (coid, let) =>
        Validated.condNec(
          let <= ledgerTime,
          (),
          ReferenceToFutureContractError(coid, let, ledgerTime),
        )
      }
      .toEither
      .leftMap(_.toList)
  }
}
