// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.command

/** Shared vocabulary for the enforcement decision and completion spans and their metrics, so
  * outcome and reason values can't drift apart between them.
  */
private[canton] object TrafficEnforcementOutcome {
  val OutcomeAttribute: String = "traffic_enforcement_outcome"
  val ReasonAttribute: String = "traffic_enforcement_reason"
  val ActAsCategoryAttribute: String = "traffic_enforcement_act_as_category"

  val Accepted: String = "accepted"
  val Rejected: String = "rejected"
  val Skipped: String = "skipped"
  val Degraded: String = "degraded"
  val Failed: String = "failed"
  val Charged: String = "charged"

  val InsufficientBalance: String = "insufficient_balance"
  val MultiPartySubmission: String = "multi_party_submission"
  val AdminParty: String = "admin_party"
  val NonSingletonActAs: String = "non_singleton_act_as"
  val EnforcementDisabled: String = "enforcement_disabled"
  val LookupUnavailable: String = "lookup_unavailable"
  val LookupFailed: String = "lookup_failed"
  val CorrelationConflict: String = "correlation_conflict"
  val MarkerWriteFailed: String = "marker_write_failed"
  val ZeroCost: String = "zero_cost"
}
