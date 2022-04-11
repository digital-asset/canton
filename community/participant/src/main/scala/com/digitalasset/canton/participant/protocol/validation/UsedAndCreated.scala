// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.validation

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.data.TransactionViewTree
import com.digitalasset.canton.participant.protocol.conflictdetection.{
  ActivenessCheck,
  ActivenessSet,
}
import com.digitalasset.canton.participant.store.ContractKeyJournal
import com.digitalasset.canton.protocol.{
  LfContractId,
  LfGlobalKey,
  SerializableContract,
  WithContractHash,
}

case class UsedAndCreated(
    witnessedAndDivulged: Map[LfContractId, SerializableContract],
    checkActivenessTxInputs: Set[LfContractId],
    consumedInputsOfHostedStakeholders: Map[LfContractId, WithContractHash[Set[LfPartyId]]],
    maybeCreated: Map[LfContractId, Option[SerializableContract]],
    transient: Map[LfContractId, WithContractHash[Set[LfPartyId]]],
    rootViewsWithContractKeys: NonEmpty[Seq[
      (TransactionViewTree, Map[LfGlobalKey, Option[LfContractId]])
    ]],
    uckFreeKeysOfHostedMaintainers: Set[LfGlobalKey],
    uckUpdatedKeysOfHostedMaintainers: Map[LfGlobalKey, ContractKeyJournal.Status],
    hostedInformeeStakeholders: Set[LfPartyId],
) {
  def activenessSet: ActivenessSet = {
    val contractCheck = ActivenessCheck(
      checkFresh = maybeCreated.keySet,
      checkFree = Set.empty,
      checkActive = checkActivenessTxInputs,
      lock = consumedInputsOfHostedStakeholders.keySet ++ created.keySet,
    )
    val keyCheck = ActivenessCheck(
      checkFresh = Set.empty,
      checkFree = uckFreeKeysOfHostedMaintainers,
      checkActive = Set.empty,
      lock = uckUpdatedKeysOfHostedMaintainers.keySet,
    )
    ActivenessSet(
      contracts = contractCheck,
      transferIds = Set.empty,
      keys = keyCheck,
    )
  }

  def created: Map[LfContractId, SerializableContract] = maybeCreated.collect {
    case (cid, Some(sc)) => cid -> sc
  }

}
