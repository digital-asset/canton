// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.toxiproxy.slow

import com.digitalasset.canton.concurrent.Threading
import com.digitalasset.canton.config.PositiveFiniteDuration
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.plugins.UsePostgres
import com.digitalasset.canton.integration.plugins.toxiproxy.*
import com.digitalasset.canton.integration.tests.examples.IouSyntax
import com.digitalasset.canton.integration.tests.toxiproxy.ToxiproxyHelpers
import com.digitalasset.canton.integration.tests.traffic.TeaDbTestHelpers
import com.digitalasset.canton.integration.util.{TestUtils, TrafficControlUtils}
import com.digitalasset.canton.platform.config.TrafficEnforcementServerConfig.ProjectionConfig
import com.digitalasset.canton.platform.config.{
  TrafficAccountingConfig,
  TrafficEnforcementServerConfig,
}
import com.digitalasset.canton.tea.projection.{ActAsCategory, UnchargedReason}
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.canton.util.ResourceUtil
import monocle.macros.syntax.lens.*

import java.util.UUID
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.CollectionHasAsScala

private[slow] trait TeaDatabaseFaultTestBase
    extends CommunityIntegrationTest
    with SharedEnvironment {

  protected val proxyConf: ProxyConfig =
    ParticipantToPostgres("participant1-to-postgres", "participant1")
  protected val toxiproxyPlugin =
    new UseToxiproxy(UseToxiproxy.ToxiproxyConfig(List(proxyConf)))

  protected var alice: PartyId = _

  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(toxiproxyPlugin)

  protected def getProxy: RunningProxy =
    toxiproxyPlugin.runningToxiproxy.getProxy(proxyConf.name).value

  protected def removeToxiproxies(): Unit =
    ToxiproxyHelpers.removeAllProxies(toxiproxyPlugin.runningToxiproxy.controllingToxiproxyClient)

  /** Submits `count` transactions and returns their summed traffic cost, retrying each one since
    * the participant rejects submissions while it flips back to active.
    */
  protected def submitAndSumCost(party: PartyId, count: Int)(implicit
      env: TestConsoleEnvironment
  ): Long =
    Seq.fill(count)(submitIou(party).cost).sum

  protected def submitIou(party: PartyId)(implicit env: TestConsoleEnvironment): Submitted = {
    import env.*
    eventually(timeUntilSuccess = 60.seconds, retryOnTestFailuresOnly = false) {
      val commandId = UUID.randomUUID().toString
      val submissionId = UUID.randomUUID().toString
      val iouCmd = IouSyntax.testIou(party, party, 10.0).create().commands().asScala.toSeq
      val transaction = participant1.ledger_api.javaapi.commands
        .submit(Seq(party), iouCmd, commandId = commandId, submissionId = submissionId)
      Submitted(commandId, submissionId, transaction.getPaidTrafficCost)
    }
  }
}

private[slow] final case class Submitted(commandId: String, submissionId: String, cost: Long)

/** Verifies that accounting-only TEA recovers from an outage of the shared participant-database
  * connection, without losing or double-counting any debits.
  */
final class TrafficEnforcementDatabaseFaultIntegrationTest extends TeaDatabaseFaultTestBase {

  private val teaServerName = "tea-fault-server"

  override def environmentDefinition: EnvironmentDefinition =
    ToxiproxyHelpers.environmentDefinitionDefault
      .addConfigTransform(
        ConfigTransforms.updateParticipantConfig("participant1")(
          _.focus(_.trafficAccounting).replace(
            TrafficAccountingConfig(
              enabled = true,
              enforceCostOnSubmissions = false,
              trafficEnforcementServer = TrafficEnforcementServerConfig.Internal(
                teaServerName,
                // Faster restart backoff so recovery is quicker.
                ProjectionConfig(maxProjectionRestartBackoff = PositiveFiniteDuration.ofSeconds(2)),
              ),
            )
          )
        )
      )
      .withSetup { implicit env =>
        import env.*
        participant1.synchronizers.connect_local(sequencer1, alias = daName)
        participant1.dars.upload(CantonExamplesPath)
        alice = participant1.parties.enable("Alice")
      }
      .withTrafficControl(
        TestUtils.waitForTargetTimeOnSynchronizerNode(wallClock.now, logger),
        trafficControlParameters = TrafficControlUtils.predictableTraffic,
        topUpAllMembers = true,
        disableCommitments = true,
      )
      .withTeardown(_ => removeToxiproxies())

  "TEA in accounting-only mode" should {
    "recover after a participant-DB outage without losing debits" in { implicit env =>
      import env.*

      val initialBalance =
        participant1.ledger_api.traffic.get_account(alice.toProtoPrimitive).balance

      val batch1Cost = submitAndSumCost(alice, count = 3)

      // DB connection outage, plus recovery window right after.
      val batch2Cost = loggerFactory.suppressWarningsAndErrors {
        getProxy.underlying.disable()
        try {
          Threading.sleep(1000)
        } finally {
          getProxy.underlying.enable()
        }

        eventually(timeUntilSuccess = 60.seconds, retryOnTestFailuresOnly = false) {
          participant1.ledger_api.traffic.get_account(alice.toProtoPrimitive).discard
        }

        submitAndSumCost(alice, count = 3)
      }

      val expectedBalance = initialBalance - (batch1Cost + batch2Cost)
      eventually() {
        participant1.ledger_api.traffic
          .get_account(alice.toProtoPrimitive)
          .balance shouldBe expectedBalance
      }
    }
  }
}

/** More involved version of [[TrafficEnforcementDatabaseFaultIntegrationTest]]:
  *   - pushes a fixed backlog of completions while TEA is disabled, and stores accounts for half of
  *     them, so TEA charges those and records the others as uncharged
  *   - then restarts with TEA enabled to ingest that whole backlog from the start
  *   - cuts the DB connection right after the second of those events so it's interrupted and has to
  *     resume after reconnect
  */
final class TrafficEnforcementDatabaseFaultCatchUpIntegrationTest
    extends TeaDatabaseFaultTestBase
    with TeaDbTestHelpers {

  private val teaServerName = "tea-fault-catchup-server"

  // Counts events TEA has committed, so the hook below
  // can disable the DB connection at the right time.
  private val committedCount = new java.util.concurrent.atomic.AtomicInteger(0)

  // No `.withSetup`/`.withTrafficControl` here, since `manualCreateEnvironmentWithPreviousState`
  // would rerun them against the restarted environment too.
  override def environmentDefinition: EnvironmentDefinition =
    ToxiproxyHelpers.environmentDefinitionDefault
      .updateTestingConfig(
        _.focus(_.trafficEnforcementProjectionEventCommitted).replace { () =>
          if (committedCount.incrementAndGet() == 2) {
            getProxy.underlying.disable()
          }
        }
      )
      .withTeardown(_ => removeToxiproxies())

  "TEA in accounting-only mode" should {
    "not lose or double count completions when a DB outage interrupts catch-up" in { implicit env =>
      import env.*

      participant1.synchronizers.connect_local(sequencer1, alias = daName)
      participant1.dars.upload(CantonExamplesPath)
      alice = participant1.parties.enable("Alice")
      TrafficControlUtils.applyTrafficControl(
        TestUtils.waitForTargetTimeOnSynchronizerNode(wallClock.now, logger),
        trafficControlParameters = TrafficControlUtils.predictableTraffic,
        topUpAllMembers = true,
        disableCommitments = true,
      )

      // Build a fixed backlog of completions before TEA started ingesting.
      val baselineOffset = participant1.ledger_api.state.end()
      val backlog = Seq.fill(4)(submitIou(alice))
      // Completions 1 and 3 have an account, so the outage after the second commit falls between
      // two charged completions.
      val charged = Seq(backlog(0), backlog(2))
      val uncharged = Seq(backlog(1), backlog(3))
      charged.foreach { submitted =>
        insertCorrelationIntoDb(
          participant1,
          changeIdHash(submitted.commandId, Set(alice)),
          submitted.submissionId,
          Some(alice.toProtoPrimitive),
        )
      }

      val enableTeaFromBaseline = ConfigTransforms.updateParticipantConfig("participant1")(
        _.focus(_.trafficAccounting).replace(
          TrafficAccountingConfig(
            enabled = true,
            enforceCostOnSubmissions = false,
            trafficEnforcementServer = TrafficEnforcementServerConfig.Internal(
              teaServerName,
              ProjectionConfig(
                maxProjectionRestartBackoff = PositiveFiniteDuration.ofSeconds(2),
                initialCompletionOffsetBeginExclusive = Some(baselineOffset),
              ),
            ),
          )
        )
      )

      participant1.synchronizers.disconnect(daName)
      nodes.local.stop()

      // TEA starts catching up, and hits the outage, while the environment is still starting.
      loggerFactory.suppressWarningsAndErrors {
        val restartedEnv = manualCreateEnvironmentWithPreviousState(
          env.actualConfig,
          enableTeaFromBaseline,
          // Keep the Postgres and toxiproxy plugins' existing state (DB content and running proxy);
          // only start TEA on top of the preserved database.
          runPlugins = _ ne toxiproxyPlugin,
        )

        // No reconnect needed to the synchronizer: TEA ingestion and the balance check don't need it.
        ResourceUtil.withResource(restartedEnv) { restarted =>
          eventually(timeUntilSuccess = 60.seconds, retryOnTestFailuresOnly = false) {
            committedCount.get() should be >= 2
          }
          Threading.sleep(1000)
          getProxy.underlying.enable()

          eventually(timeUntilSuccess = 60.seconds, retryOnTestFailuresOnly = false) {
            forEvery(uncharged) { submitted =>
              unchargedFromDb(
                restarted.participant1,
                changeIdHash(submitted.commandId, Set(alice)),
              ) shouldBe Seq(
                (
                  submitted.submissionId,
                  submitted.cost,
                  UnchargedReason.MissingCorrelation,
                  ActAsCategory.SingleNonAdminActingParty,
                )
              )
            }
            restarted.participant1.ledger_api.traffic
              .get_account(alice.toProtoPrimitive)
              .balance shouldBe -charged.map(_.cost).sum
          }
          forEvery(charged) { submitted =>
            val hash = changeIdHash(submitted.commandId, Set(alice))
            correlationsFromDb(restarted.participant1, hash) shouldBe empty
            unchargedFromDb(restarted.participant1, hash) shouldBe empty
          }
        }
      }
    }
  }
}
