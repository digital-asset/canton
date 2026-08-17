// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import com.digitalasset.canton.LfPartyId

/** Holds information about contracts and their various roles within a transaction throughout
  * transaction processing.
  *
  * @param witnessed
  *   itemizes contracts that are witnessed by the participant
  * @param checkActivenessTxInputs
  *   contract ids of input contracts used by the transaction and to be verified for activeness
  * @param consumedInputsOfHostedStakeholders
  *   contract ids of consumed input contracts along with each contract's stakeholders
  * @param used
  *   all used (=input) contracts
  * @param maybeCreated
  *   contracts created as part of the transaction where the contract instance may be None due to
  *   Daml exception rollbacks
  * @param transient
  *   contract ids of transient contracts, created and also archived by the transaction
  * @param maybeUnknown
  *   contract ids that may be unknown due to party onboarding and pending ACS import
  */
final case class UsedAndCreatedContracts(
    witnessed: Map[LfContractId, GenContractInstance],
    checkActivenessTxInputs: Set[LfContractId],
    consumedInputsOfHostedStakeholders: Map[LfContractId, Set[LfPartyId]],
    used: Map[LfContractId, GenContractInstance],
    maybeCreated: Map[LfContractId, Option[NewContractInstance]],
    transient: Map[LfContractId, Set[LfPartyId]],
    maybeUnknown: Set[LfContractId],
) {
  def created: Map[LfContractId, NewContractInstance] =
    maybeCreated.collect { case (cid, Some(sc)) => cid -> sc }
}

object UsedAndCreatedContracts {
  val empty: UsedAndCreatedContracts = UsedAndCreatedContracts(
    witnessed = Map.empty,
    checkActivenessTxInputs = Set.empty,
    consumedInputsOfHostedStakeholders = Map.empty,
    used = Map.empty,
    maybeCreated = Map.empty,
    transient = Map.empty,
    maybeUnknown = Set.empty,
  )
}
