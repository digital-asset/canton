// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.DiscardOps
import com.digitalasset.canton.concurrent.{ExecutionContextIdlenessExecutorService, Threading}
import com.digitalasset.canton.console.ParticipantReference
import com.digitalasset.canton.environment.Environment
import com.digitalasset.canton.examples.Iou.{Amount, Iou}
import com.digitalasset.canton.logging.NamedLogging
import com.digitalasset.canton.participant.ledger.api.client.DecodeUtil

import java.util.concurrent.atomic.AtomicBoolean

/** Enables tests to run workload in the background across specified participants that share
  * a domain while executing test code.
  */
trait BackgroundWorkloadRunner[E <: Environment] {
  this: NamedLogging =>

  protected def withWorkload[T](participants: NonEmpty[Seq[ParticipantReference]])(code: => T)(
      implicit env: TestEnvironment[E]
  ): T = {
    val stop = new AtomicBoolean(false)
    val scheduler = Threading.singleThreadedExecutor(
      "test-workload-runner",
      logger,
    )
    try {
      scheduler.submit(new WorkloadRunner(scheduler, stop, participants))
      code
    } finally {
      stop.set(true)

      // Sleep one second to avoid shutdown-related test flakiness. This allows the last workload
      // request to finish cleanly.
      Threading.sleep(1000)
      scheduler.shutdown()
    }
  }

  private class WorkloadRunner(
      scheduler: ExecutionContextIdlenessExecutorService,
      stop: AtomicBoolean,
      participants: NonEmpty[Seq[ParticipantReference]],
  )(implicit
      env: TestEnvironment[E]
  ) extends Runnable {
    var participantIndex: Int = 0

    def node(i: Int): ParticipantReference = participants(
      (participantIndex + i) % participants.size
    )

    def payer: ParticipantReference = node(0)

    def owner: ParticipantReference = node(1)

    override def run(): Unit = {
      if (!stop.get) {
        pay(payer, owner)

        // switch submitter before scheduling again
        participantIndex += 1
        scheduler.submit(this)
      }
    }

    private def pay(payer: ParticipantReference, owner: ParticipantReference): Unit = {
      import env.*
      val cid = DecodeUtil
        .decodeAllCreated(Iou)(
          payer.ledger_api.commands.submit_flat(
            Seq(payer.adminParty),
            Seq(
              Iou(
                payer.adminParty.toPrim,
                owner.adminParty.toPrim,
                Amount(3.50, "CHF"),
                List(),
              ).create.command
            ),
            da.name,
          )
        )
        .headOption
        .getOrElse(throw new IllegalStateException("unexpected empty result"))
        .contractId

      owner.ledger_api.commands
        .submit_flat(
          Seq(owner.adminParty),
          Seq(cid.exerciseCall().command),
          optTimeout = None,
        )
        .discard
    }

  }

}
