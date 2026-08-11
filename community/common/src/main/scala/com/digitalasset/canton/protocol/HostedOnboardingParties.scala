// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.nonempty.NonEmpty

/** Tracks hosted parties broken up by onboarding and non-onboarding parties on behalf of a
  * transaction during party replication.
  *
  * @param hostedOnboardingParties
  *   parties hosted by the local participant that are onboarding at the request-id time of the
  *   associated transaction
  * @param hostedParties
  *   parties hosted by the local participant, a superset of hosted onboarding parties
  */
final case class HostedOnboardingParties(
    hostedOnboardingParties: NonEmpty[Set[LfPartyId]],
    hostedParties: NonEmpty[Set[LfPartyId]],
) extends PrettyPrinting {
  private val violations = hostedOnboardingParties.diff(hostedParties)
  require(
    violations.isEmpty,
    s"hosted onboarding parties need to be a subset of hosted parties, but $violations are not hosted parties",
  )

  private lazy val hostedNonOnboardingParties: Set[LfPartyId] =
    hostedParties.forgetNE -- hostedOnboardingParties.forgetNE

  override protected def pretty: Pretty[HostedOnboardingParties] = prettyOfClass(
    param("onboarding", _.hostedOnboardingParties),
    param("hosted", _.hostedParties),
  )

  /** Return subset of onboarding parties if the provided set of parties contains at least one
    * hosted onboarding party and no non-onboarding party. When this function returns a non-empty
    * set on behalf of a provided reference party set, this is an indication to the caller that
    * onboarding-specific transaction processing is needed.
    */
  def hostedPartiesIfAllOnboarding(parties: Set[LfPartyId]): Option[NonEmpty[Set[LfPartyId]]] =
    NonEmpty
      .from(parties intersect hostedOnboardingParties)
      .filterNot(_ => isAnyPartyFullyHosted(parties))

  /** Helper to indicate whether any parties in a specified reference set are hosted fully hosted,
    * i.e. not onboarding.
    */
  def isAnyPartyFullyHosted(parties: Set[LfPartyId]): Boolean =
    parties.exists(hostedNonOnboardingParties.contains)
}

object HostedOnboardingParties {
  def apply(
      hostedOnboardingParties: Set[LfPartyId],
      hostedParties: Set[LfPartyId],
  ): Option[HostedOnboardingParties] = NonEmpty
    .from(hostedOnboardingParties)
    .flatMap(hostedOnboarding =>
      NonEmpty.from(hostedParties).map(HostedOnboardingParties(hostedOnboarding, _))
    )
}
