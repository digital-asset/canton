// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.validation

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.data.TransactionViewTree
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
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
import com.digitalasset.canton.{LfKeyResolver, LfPartyId}

case class UsedAndCreated(
    contracts: UsedAndCreatedContracts,
    keys: InputAndUpdatedKeys,
    hostedInformeeStakeholders: Set[LfPartyId],
) {
  def activenessSet: ActivenessSet =
    ActivenessSet(
      contracts = contracts.activenessCheck,
      transferIds = Set.empty,
      keys = keys.activenessCheck,
    )
}

case class UsedAndCreatedContracts(
    witnessedAndDivulged: Map[LfContractId, SerializableContract],
    checkActivenessTxInputs: Set[LfContractId],
    consumedInputsOfHostedStakeholders: Map[LfContractId, WithContractHash[Set[LfPartyId]]],
    maybeCreated: Map[LfContractId, Option[SerializableContract]],
    transient: Map[LfContractId, WithContractHash[Set[LfPartyId]]],
) {
  def activenessCheck: ActivenessCheck[LfContractId] =
    ActivenessCheck(
      checkFresh = maybeCreated.keySet,
      checkFree = Set.empty,
      checkActive = checkActivenessTxInputs,
      lock = consumedInputsOfHostedStakeholders.keySet ++ created.keySet,
    )

  def created: Map[LfContractId, SerializableContract] = maybeCreated.collect {
    case (cid, Some(sc)) => cid -> sc
  }
}

/** @param rootViewsWithContractKeys is a key resolver that is suitable for reinterpreting each root view
  * @param uckFreeKeysOfHostedMaintainers: keys that must be free before executing the transaction.
  * @param uckUpdatedKeysOfHostedMaintainers: keys that will be updated by the transaction.
  *   The value indicates the new status after the transaction.
  */
case class InputAndUpdatedKeys(
    rootViewsWithContractKeys: NonEmpty[Seq[(TransactionViewTree, LfKeyResolver)]],
    uckFreeKeysOfHostedMaintainers: Set[LfGlobalKey],
    uckUpdatedKeysOfHostedMaintainers: Map[LfGlobalKey, ContractKeyJournal.Status],
) extends PrettyPrinting {
  def activenessCheck: ActivenessCheck[LfGlobalKey] =
    ActivenessCheck(
      checkFresh = Set.empty,
      checkFree = uckFreeKeysOfHostedMaintainers,
      checkActive = Set.empty,
      lock = uckUpdatedKeysOfHostedMaintainers.keySet,
    )

  override def pretty: Pretty[InputAndUpdatedKeys] = prettyOfClass(
    param("key resolver", _.rootViewsWithContractKeys.map(_._2)),
    paramIfNonEmpty("uck free keys of hosted maintainers", _.uckFreeKeysOfHostedMaintainers),
    paramIfNonEmpty("uck updated keys of hosted maintainers", _.uckUpdatedKeysOfHostedMaintainers),
  )
}
