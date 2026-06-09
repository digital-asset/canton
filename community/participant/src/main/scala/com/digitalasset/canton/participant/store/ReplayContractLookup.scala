// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import com.digitalasset.canton.protocol.{GenContractInstance, LfContractId, LfGlobalKey}
import com.digitalasset.daml.lf.transaction.FatContractInstance

class ReplayContractLookup(
    private val contracts: Map[LfContractId, GenContractInstance],
    private val keys: Map[LfGlobalKey, Seq[LfContractId]],
) {

  contracts.foreach { case (id, contract) =>
    require(
      contract.contractId == id,
      s"Tried to store contract $contract under the wrong id $id",
    )
  }

  def lookup(id: LfContractId): Option[GenContractInstance] = contracts.get(id)

  def lookupInst(id: LfContractId): Option[FatContractInstance] = lookup(id).map(_.inst)

  def lookupKey(key: LfGlobalKey): Seq[FatContractInstance] = {

    val orderedCids = keys.getOrElse(key, Seq.empty)

    val orderedContracts = for {
      cid <- orderedCids
      contract <- contracts.get(cid)
    } yield contract.inst

    val observed = orderedCids.toSet

    // TODO(#32184): Once completed otherContracts (and the performance hit building) can be removed as
    // it will not long be mandated that all contracts using the key must be included (only ones
    // required for resolution, included in keys above, are required).
    // If this is not done then the performance of this code should be made O(N log N).
    val otherContracts = contracts.values
      .filter(_.inst.contractKeyWithMaintainers.exists(_.globalKey == key))
      .collect {
        case c if !observed.contains(c.contractId) => c.inst
      }

    orderedContracts ++ otherContracts

  }

}
