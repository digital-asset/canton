// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.command

/** Shared vocabulary for the enforcement decision span and the `decisions` metric, so outcome and
  * reason values can't drift apart between the two.
  */
private[apiserver] object TrafficEnforcementOutcome {
  val OutcomeAttribute: String = "traffic_enforcement_outcome"
  val ReasonAttribute: String = "traffic_enforcement_reason"

  val Accepted: String = "accepted"
  val Rejected: String = "rejected"
  val Skipped: String = "skipped"
  val Degraded: String = "degraded"
  val Failed: String = "failed"

  val InsufficientBalance: String = "insufficient_balance"
  val MultiPartySubmission: String = "multi_party_submission"
  val AdminParty: String = "admin_party"
  val NonSingletonActAs: String = "non_singleton_act_as"
  val EnforcementDisabled: String = "enforcement_disabled"
  val LookupUnavailable: String = "lookup_unavailable"
  val LookupFailed: String = "lookup_failed"
}
