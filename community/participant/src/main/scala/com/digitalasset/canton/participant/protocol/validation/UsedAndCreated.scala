// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.validation

import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.participant.protocol.conflictdetection.{
  ActivenessCheck,
  ActivenessSet,
}
import com.digitalasset.canton.protocol.{HostedOnboardingParties, UsedAndCreatedContracts}

/** Holds information about contracts involved in a transaction and information about hosted
  * transaction informees.
  * @param contracts
  *   used and created contracts involved in a transaction
  * @param hostedWitnesses
  *   locally hosted informees witnessing parts of the transaction
  * @param hostedOnboardingInformees
  *   locally hosted informees onboarding via party replication at the time of the transaction
  */
private[protocol] final case class UsedAndCreated(
    contracts: UsedAndCreatedContracts,
    hostedWitnesses: Set[LfPartyId],
    hostedOnboardingInformees: Set[LfPartyId],
) {
  def activenessSet: ActivenessSet = {
    val check = ActivenessCheck.tryCreate(
      checkFresh = contracts.maybeCreated.keySet,
      checkFree = Set.empty,
      // Don't check legitimately unknown contracts for activeness.
      checkActive = contracts.checkActivenessTxInputs -- contracts.maybeUnknown,
      // Let key locking know not to flag legitimately unknown contracts (such as those associated
      // with party onboarding) as activeness errors.
      // Note that created contracts are always locked, even if they are transient.
      lock =
        contracts.consumedInputsOfHostedStakeholders.keySet -- contracts.maybeUnknown ++ contracts.created.keySet,
      lockMaybeUnknown =
        (contracts.consumedInputsOfHostedStakeholders.keySet intersect contracts.maybeUnknown) -- contracts.created.keySet,
      needPriorState = Set.empty,
    )

    ActivenessSet(
      contracts = check,
      reassignmentIds = Set.empty,
    )
  }

  lazy val hostedOnboardingPartiesO: Option[HostedOnboardingParties] =
    HostedOnboardingParties(hostedOnboardingInformees, hostedWitnesses)
}
