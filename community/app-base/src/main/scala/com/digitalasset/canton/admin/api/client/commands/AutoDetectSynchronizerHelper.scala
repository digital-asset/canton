// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.console.commands

import com.digitalasset.canton.console.{
  ConsoleEnvironment,
  InstanceReference,
  MediatorReference,
  ParticipantReference,
  SequencerReference,
}
import com.digitalasset.canton.topology.SynchronizerId

trait AutoDetectSynchronizerHelper {
  protected def consoleEnvironment: ConsoleEnvironment
  protected def instance: InstanceReference

  // Auto-detect a single synchronizer for key operations.  We require exactly
  // one registered and connected synchronizer to avoid accidentally selecting
  // a healthy one when all the others temporarliy disconnect.
  protected def autodetectSynchronizer(
      operation: String
  ): SynchronizerId = {
    def err(message: String) = consoleEnvironment.raiseError(
      s"Cannot auto-detect synchronizer for $operation: $message"
    )
    instance match {
      case sequencer: SequencerReference => sequencer.synchronizer_id
      case mediator: MediatorReference =>
        mediator.health.status.successOption
          .map(_.synchronizerId.logical)
          .getOrElse(
            err("not connected")
          )
      case participant: ParticipantReference =>
        participant.synchronizers.list_all_registered() match {
          case Seq(registered) if registered.isConnected =>
            registered.psid.toOption
              .map(_.logical)
              .getOrElse(
                err("no synchronizer ID was set")
              )
          case registered => {
            val connected = registered.filter(_.isConnected)
            err(
              s"multiple synchronizers: ${registered.length} registered, ${connected.length} connected"
            )
          }
        }
      case other => err(s"unsupported node type: ${other.getClass.getSimpleName}")
    }
  }
}
