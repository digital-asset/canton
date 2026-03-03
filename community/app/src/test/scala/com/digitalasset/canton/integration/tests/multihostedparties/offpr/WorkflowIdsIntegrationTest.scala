// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.multihostedparties.offpr

import com.daml.ledger.api.v2.reassignment.ReassignmentEvent
import com.daml.ledger.javaapi.data.Command
import com.digitalasset.canton.BaseTest.UnsupportedExternalPartyTest.MultiRootNodeSubmission
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.examples.java.iou.{Amount, Iou}
import com.digitalasset.canton.integration.{ConfigTransforms, EnvironmentDefinition}
import com.digitalasset.canton.topology.Party
import monocle.syntax.all.*

import java.time.{Duration, Instant}
import java.util.Collections

/** Tests for Offline Party Replication focusing on Workflow IDs.
  *
  * Verifies that when a party is replicated from a source to a target participant, the resulting
  * events (standard transactions or reassignments) are correctly batched according to their
  * original ledger time. It also ensures that these batches can be correlated using a shared
  * Workflow ID prefix with sequential suffixes, and that this Workflow ID prefix can be
  * custom-configured.
  */
abstract class WorkflowIdsIntegrationTestBase(
    alphaMultiSynchronizerSupport: Boolean = false
) extends OfflinePartyReplicationIntegrationTestBase {

  override lazy val environmentDefinition: EnvironmentDefinition =
    super.environmentDefinition
      .addConfigTransforms(
        ConfigTransforms.updateAllParticipantConfigs_(
          _.focus(_.parameters.alphaMultiSynchronizerSupport)
            .replace(alphaMultiSynchronizerSupport)
        )
      )

  // Normalized data structure to handle both Transactions (Standard) and Reassignments (Alpha Multi-Synchronizer Support)
  private case class NormalizedEvent(timestamp: Instant, workflowId: String, eventCount: Int)

  "Migrations are grouped by ledger time and can be correlated through the workflow ID" onlyRunWithLocalParty MultiRootNodeSubmission in {
    implicit env =>
      import env.*
      val clock = env.environment.simClock.value

      source = participant1
      target = participant2

      // create some IOUs, we'll expect the migration to group together those sharing the
      // ledger time (i.e. they have been created in the same transaction)
      for (commands <- Seq(ious(alice, 3), ious(alice, 4), ious(alice, 1), ious(alice, 2))) {
        clock.advance(Duration.ofMillis(1L))
        source.ledger_api.javaapi.commands.submit(actAs = Seq(alice), commands = commands)
      }

      replicateParty(clock, alice)

      // Check that the transactions generated for the migration are actually grouped as
      // expected and that their workflow IDs can be used to correlate those transactions
      val events = fetchEvents(alice, expectedCount = 4)

      withClue("Events should be grouped by ledger time") {
        events.map(_.timestamp).distinct should have size 4
      }
      withClue("Events should share the same workflow ID prefix") {
        events.map(_.workflowId.dropRight(4)).distinct should have size 1
      }

      inside(events) { case Seq(e1, e2, e3, e4) =>
        e1.eventCount shouldBe 3
        e1.workflowId should endWith("-1")

        e2.eventCount shouldBe 4
        e2.workflowId should endWith("-2")

        e3.eventCount shouldBe 1
        e3.workflowId should endWith("-3")

        e4.eventCount shouldBe 2
        e4.workflowId should endWith("-4")
      }

  }

  "The workflow ID prefix must be configurable" in { implicit env =>
    import env.*
    val clock = env.environment.simClock.value

    source = participant2
    target = participant3

    val workflowIdPrefix = "SOME_WORKFLOW_ID_123"

    for (commands <- Seq(ious(bob, 1), ious(bob, 1))) {
      clock.advance(Duration.ofMillis(1L))
      source.ledger_api.javaapi.commands.submit(actAs = Seq(bob), commands = commands)
    }

    replicateParty(clock, bob, workflowIdPrefix)

    // Check that the workflow ID prefix is set as specified
    val events = fetchEvents(bob, expectedCount = 2)

    inside(events) { case Seq(e1, e2) =>
      e1.workflowId shouldBe s"$workflowIdPrefix-1"
      e2.workflowId shouldBe s"$workflowIdPrefix-2"
    }
  }

  // Fetches either Transactions or Reassignments based on configuration and normalizes them
  private def fetchEvents(
      party: Party,
      expectedCount: Int,
  ): Seq[NormalizedEvent] =
    if (alphaMultiSynchronizerSupport) {
      val reassignments = target.ledger_api.updates
        .reassignments(Set(party), completeAfter = PositiveInt.tryCreate(expectedCount))
      reassignments.map { r =>
        // Extract timestamp from the first 'Assigned' event in the reassignment
        val timestamp = r.reassignment.events
          .collectFirst { case ReassignmentEvent(assigned: ReassignmentEvent.Event.Assigned) =>
            assigned.assigned.value.createdEvent.value.createdAt.value
          }
          .getOrElse(
            fail(s"Reassignment ${r.reassignment.updateId} had no Assigned event with timestamp")
          )

        NormalizedEvent(
          timestamp.asJavaInstant,
          r.reassignment.workflowId,
          r.reassignment.events.size,
        )
      }
    } else {
      val txs = target.ledger_api.javaapi.updates
        .transactions(Set(party), completeAfter = PositiveInt.tryCreate(expectedCount))
      import scala.jdk.CollectionConverters.ListHasAsScala
      txs.map { tx =>
        val t = tx.getTransaction.get
        NormalizedEvent(t.getEffectiveAt, t.getWorkflowId, t.getEvents.asScala.size)
      }
    }

  private def ious(party: Party, n: Int): Seq[Command] = {
    import scala.jdk.CollectionConverters.IteratorHasAsScala
    def iou =
      new Iou(
        party.toProtoPrimitive,
        party.toProtoPrimitive,
        new Amount(java.math.BigDecimal.ONE, "USD"),
        Collections.emptyList,
      )
    Seq.fill(n)(iou).flatMap(_.create.commands.iterator.asScala)
  }
}

final class WorkflowIdsIntegrationTest extends WorkflowIdsIntegrationTestBase

final class WorkflowIdsReassignmentIntegrationTest
    extends WorkflowIdsIntegrationTestBase(alphaMultiSynchronizerSupport = true)
