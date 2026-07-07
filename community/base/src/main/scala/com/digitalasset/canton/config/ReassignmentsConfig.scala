// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

/** Configuration relating to reassignments.
  *
  * Currently empty: unassignment requests are validated against the target topology at this
  * participant's local target timestamp. This config is kept as the home for the upcoming
  * bounded-wait timeout, which will let a participant wait (up to the timeout) for the target
  * topology requested by the submitter before validating. TODO(i33545): reuse this config.
  */
final case class ReassignmentsConfig()
