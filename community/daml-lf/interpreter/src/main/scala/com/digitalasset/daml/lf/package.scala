// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf

import com.digitalasset.daml.lf.transaction.{TransactionError => TxErr}
import com.digitalasset.daml.lf.interpretation.{Error => IE}
import com.digitalasset.daml.lf.speedy.SError.SErrorCrash

package object speedy {

  val Compiler = compiler.Compiler
  type Compiler = compiler.Compiler

  private[speedy] def convTxError(context: => String, err: TxErr): IE = {
    err match {
      case TxErr.DuplicateContractId(contractId) =>
        throw SErrorCrash(context, s"Unexpected duplicate contract ID ${contractId}")
      case TxErr.DuplicateContractKey(key) =>
        IE.DuplicateContractKey(key)
      case TxErr.InconsistentContractKey(key) =>
        IE.InconsistentContractKey(key)
    }
  }

}
