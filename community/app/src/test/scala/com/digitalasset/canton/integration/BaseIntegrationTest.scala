// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration

import com.daml.ledger.api.refinements.ApiTypes.TemplateId
import com.daml.ledger.api.v1.value.Identifier
import com.digitalasset.canton.config.TimeoutDuration
import com.digitalasset.canton.console.{
  CommandFailure,
  LocalParticipantReference,
  ParticipantReference,
}
import com.digitalasset.canton.environment.Environment
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.{BaseTest, DomainAlias, RepeatableTestSuiteTest}
import org.scalactic.source
import org.scalactic.source.Position
import org.scalatest.wordspec.FixtureAnyWordSpec
import org.scalatest.{Assertion, ConfigMap, Outcome}
import scala.concurrent.duration._

import scala.collection.immutable

/** A highly opinionated base trait for writing integration tests interacting with a canton environment using console commands.
  * Tests must mixin a further [[EnvironmentSetup]] implementation to define when the canton environment is setup around the individual tests:
  *   - [[IsolatedEnvironments]] will construct a fresh environment for each test.
  *   - [[SharedEnvironment]] will construct only a single environment and reuse this for each test executed in the test class.
  *
  * Test classes must override [[HasEnvironmentDefinition.environmentDefinition]] to describe how they would like their environment configured.
  *
  * This here is the abstract definition. A concrete integration test must derive the configuration type. Have a look at
  * the EnvironmentDefinition objects in the community or enterprise app sub-project.
  *
  * Code blocks interacting with the environment are provided a [[TestEnvironment]] instance. [[TestEnvironment]] provides all implicits and commands to
  * interact with the environment as if you were operating in the canton console. For convenience you will want to mark this value as an `implicit`
  * and import the instances members into your scope (see `withSetup` and tests in the below example).
  * [[TestEnvironment]] also includes [[CommonTestAliases]] which will give you references to domains and participants commonly used in our tests.
  * If your test attempts to use a participant or domain which is not configured in your environment it will immediately fail.
  *
  * By default sbt will attempt to run many tests concurrently. This can be problematic for starting many integration tests starting
  * many canton environments concurrently is very resource intensive. We use [[ConcurrentEnvironmentLimiter]] to limit
  * how many environments are running concurrently. By default this limit is 2 but can be modified by setting the system property [[ConcurrentEnvironmentLimiter.IntegrationTestConcurrencyLimit]].
  *
  * All integration tests must be located in package [[com.digitalasset.canton.integration.tests]] or a subpackage thereof.
  * This is required to correctly compute unit test coverage.
  */
private[integration] trait BaseIntegrationTest[E <: Environment, TCE <: TestConsoleEnvironment[E]]
    extends FixtureAnyWordSpec
    with BaseTest
    with RepeatableTestSuiteTest
    with HasEnvironmentDefinition[E, TCE] {
  this: EnvironmentSetup[E, TCE] =>

  type FixtureParam = TCE

  override protected def withFixture(test: OneArgTest): Outcome = {
    val integrationTestPackage = "com.digitalasset.canton.integration.tests"
    getClass.getName should startWith(
      integrationTestPackage
    ) withClue s"\nAll integration tests must be located in $integrationTestPackage or a subpackage thereof."

    super[RepeatableTestSuiteTest].withFixture(new TestWithSetup(test))
  }

  /** Version of [[com.digitalasset.canton.logging.SuppressingLogger.assertThrowsAndLogs]] that is specifically
    * tailored to [[com.digitalasset.canton.console.CommandFailure]].
    */
  // We cannot define this in SuppressingLogger, because CommandFailure is not visible there.
  def assertThrowsAndLogsCommandFailures(within: => Any, assertions: (LogEntry => Assertion)*)(
      implicit pos: source.Position
  ): Assertion =
    loggerFactory.assertThrowsAndLogs[CommandFailure](
      within,
      assertions.map(assertion => { entry: LogEntry =>
        assertion(entry)
        entry.commandFailureMessage
        succeed
      }): _*
    )

  /** Similar to [[com.digitalasset.canton.console.commands.ParticipantAdministration#ping]]
    * But unlike `ping`, this version mixes nicely with `eventually`.
    */
  def assertPingSucceeds(
      sender: ParticipantReference,
      receiver: ParticipantReference,
      timeoutMillis: Long = 20000,
      workflowId: String = "",
      id: String = "",
  ): Assertion =
    withClue(s"${sender.name} was unable to ping ${receiver.name} within ${timeoutMillis}ms:") {
      sender.health.maybe_ping(
        receiver.id,
        TimeoutDuration(timeoutMillis.millis),
        workflowId,
        id,
      ) shouldBe defined
    }

  /** Similar to [[com.digitalasset.canton.console.commands.ParticipantAdministration#ping]]
    * But unlike `ping`, this version mixes nicely with `eventually` and it waits until all pong contracts have been archived.
    */
  def assertPingSucceedsAndIsClean(
      sender: ParticipantReference,
      receiver: ParticipantReference,
      timeoutMillis: Long = 20000,
      workflowId: String = "",
      id: String = "",
  ): Assertion = {
    assertPingSucceeds(sender, receiver, timeoutMillis, workflowId, id)
    // wait for pong to be archived before proceeding to avoid race conditions
    eventually() {
      import com.digitalasset.canton.participant.admin.{workflows => W}
      forEvery(Seq(sender, receiver)) { p =>
        p.ledger_api.acs
          .of_party(p.id.adminParty, filterTemplates = Seq(W.PingPong.Pong.id)) shouldBe empty
      }
    }
  }

  def assertNumberOfStoredContracts(
      participantRef: LocalParticipantReference,
      domain: DomainAlias,
      templateId: TemplateId,
      expectedNumber: Long,
  ): Assertion = {

    val Identifier(_, module, entity) = scalaz.Tag.unwrap(templateId)
    val contracts =
      participantRef.testing.pcs_search(domain, filterTemplate = s"$module:$entity").map(_._2)

    contracts should have size expectedNumber
  }

  def assertNumberOfStoredContracts(
      domain: DomainAlias,
      templateId: TemplateId,
      expectedNumber: Long,
  )(implicit env: TCE): Assertion = {
    import env._
    // Check all participants that are connected to domain
    for (p <- participants.local if p.domains.list_connected().exists(_.domainAlias == domain)) {
      assertNumberOfStoredContracts(p, domain, templateId, expectedNumber)
    }
    succeed
  }

  class TestWithSetup(test: OneArgTest) extends NoArgTest {
    override val configMap: ConfigMap = test.configMap
    override val name: String = test.name
    override val scopes: immutable.IndexedSeq[String] = test.scopes
    override val text: String = test.text
    override val tags: Set[String] = test.tags
    override val pos: Option[Position] = test.pos

    override def apply(): Outcome = {
      val environment = provideEnvironment
      val testOutcome = {
        try test.toNoArgTest(environment)()
        finally testFinished(environment)
      }
      testOutcome
    }
  }
}
