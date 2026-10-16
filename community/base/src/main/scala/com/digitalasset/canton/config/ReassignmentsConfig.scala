// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

/** Configuration relating to reassignments.
  *
  * @param enableAutomaticAssignment
  *   From protocol version 37, if true, once an unassignment that this participant did not initiate
  *   is committed, the participant automatically submits the assignment. It also completes the
  *   pending assignments when it reconnects to a synchronizer. Up to protocol version 36, the
  *   assignment exclusivity timeout of the target synchronizer is used.
  * @param automaticAssignmentDelay
  *   From protocol version 37: the automatic assignment is submitted between `ts +
  *   automaticAssignmentDelay` and `ts + 2*automaticAssignmentDelay`, where ts is the record time
  *   of the unassignment. This leaves time for the submitter of the unassignment to submit the
  *   assignment itself.
  */
final case class ReassignmentsConfig(
    enableAutomaticAssignment: Boolean = true,
    automaticAssignmentDelay: NonNegativeFiniteDuration = NonNegativeFiniteDuration.ofSeconds(60),
)
