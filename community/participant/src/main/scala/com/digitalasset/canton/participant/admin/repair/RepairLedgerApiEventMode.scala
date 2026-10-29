// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin.repair

import com.digitalasset.canton.ReassignmentCounter
import com.digitalasset.canton.participant.ParticipantNodeParameters

/** Determines how contracts added (ACS import, repair add) or removed (repair purge) by repair
  * commands are represented on the Ledger API.
  */
private[repair] sealed trait RepairLedgerApiEventMode extends Product with Serializable {

  /** Whether a contract with the given reassignment counter is represented as an Assigned
    * (respectively Unassigned) event rather than a Created (respectively Archive) event.
    */
  def useReassignmentEvent(reassignmentCounter: ReassignmentCounter): Boolean
}

private[repair] object RepairLedgerApiEventMode {

  /** Always use Assigned/Unassigned events. */
  case object Reassignments extends RepairLedgerApiEventMode {
    override def useReassignmentEvent(reassignmentCounter: ReassignmentCounter): Boolean = true
  }

  /** Use Created/Archive events for contracts with a zero reassignment counter, and
    * Assigned/Unassigned events for contracts with a non-zero reassignment counter.
    */
  case object ByReassignmentCounter extends RepairLedgerApiEventMode {
    override def useReassignmentEvent(reassignmentCounter: ReassignmentCounter): Boolean =
      reassignmentCounter != ReassignmentCounter.Genesis
  }

  def fromParameters(parameters: ParticipantNodeParameters): RepairLedgerApiEventMode =
    if (parameters.enableAllLedgerApiReassignments) Reassignments
    else ByReassignmentCounter
}
