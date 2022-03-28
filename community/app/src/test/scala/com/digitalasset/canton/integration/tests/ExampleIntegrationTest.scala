// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import better.files._
import com.digitalasset.canton.ConsoleScriptRunner
import com.digitalasset.canton.environment.Environment
import com.digitalasset.canton.integration.CommunityTests.{
  CommunityIntegrationTest,
  IsolatedCommunityEnvironments,
}
import com.digitalasset.canton.integration.tests.ExampleIntegrationTest.{
  advancedConfiguration,
  repairConfiguration,
  simpleTopology,
}
import com.digitalasset.canton.integration.{
  CommunityConfigTransforms,
  CommunityEnvironmentDefinition,
}
import com.digitalasset.canton.logging.NamedLogging
import com.digitalasset.canton.tracing.TracingConfig
import com.digitalasset.canton.util.ShowUtil._
import monocle.macros.syntax.lens._

import scala.concurrent.blocking

abstract class ExampleIntegrationTest(configPaths: File*)
    extends CommunityIntegrationTest
    with IsolatedCommunityEnvironments
    with HasConsoleScriptRunner {

  override lazy val environmentDefinition: CommunityEnvironmentDefinition =
    CommunityEnvironmentDefinition
      .fromFiles(configPaths: _*)
      .clearConfigTransforms() // intentionally don't want to randomize ports as we've allocated each config their own range manually (see app/src/test/resources/README.md)
      .addConfigTransform(
        CommunityConfigTransforms.uniqueH2DatabaseNames
      ) // but lets not share databases
      .addConfigTransform(config =>
        config.focus(_.monitoring.tracing.propagation).replace(TracingConfig.Propagation.Enabled)
      )
}

trait HasConsoleScriptRunner { this: NamedLogging =>
  import org.scalatest.EitherValues._
  def runScript(scriptPath: File)(implicit env: Environment): Unit = {
    val () = ConsoleScriptRunner.run(env, scriptPath.toJava, logger = logger).value
  }
}

object ExampleIntegrationTest {
  lazy val examplesPath: File = "community" / "app" / "src" / "pack" / "examples"
  lazy val simpleTopology: File = examplesPath / "01-simple-topology"
  lazy val createDamlApp: File = examplesPath / "04-create-daml-app"
  lazy val advancedConfiguration: File = examplesPath / "03-advanced-configuration"
  lazy val composabilityConfiguration: File = examplesPath / "05-composability"
  lazy val messagingConfiguration: File = examplesPath / "06-messaging"
  lazy val repairConfiguration: File = examplesPath / "07-repair"
  lazy val advancedConfTestEnv: File =
    "community" / "app" / "src" / "test" / "resources" / "advancedConfDef.env"

  def ensureSystemProperties(kvs: (String, String)*): Unit = blocking(synchronized {
    kvs.foreach { case (key, value) =>
      Option(System.getProperty(key)) match {
        case Some(oldValue) =>
          require(
            oldValue == value,
            show"Trying to set incompatible system properties for ${key.singleQuoted}. Old: ${oldValue.doubleQuoted}, new: ${value.doubleQuoted}.",
          )
        case None =>
          System.setProperty(key, value)
      }
    }
  })
}

class SimplePingExampleIntegrationTest
    extends ExampleIntegrationTest(simpleTopology / "simple-topology.conf") {

  "run simple-ping.canton successfully" in { implicit env =>
    runScript(simpleTopology / "simple-ping.canton")(env.environment)
  }
}

class RepairExampleIntegrationTest
    extends ExampleIntegrationTest(
      advancedConfiguration / "storage" / "h2.conf",
      repairConfiguration / "domain-repair-lost.conf",
      repairConfiguration / "domain-repair-new.conf",
      repairConfiguration / "domain-export-ledger.conf",
      repairConfiguration / "domain-import-ledger.conf",
      repairConfiguration / "participant1.conf",
      repairConfiguration / "participant2.conf",
      repairConfiguration / "participant3.conf",
      repairConfiguration / "participant4.conf",
      repairConfiguration / "enable-preview-commands.conf",
    ) {
  "deploy repair user-manual topology and initialize" in { implicit env =>
    ExampleIntegrationTest.ensureSystemProperties("canton-examples.dar-path" -> CantonExamplesPath)
    runScript(repairConfiguration / "domain-repair-init.canton")(env.environment)
  }

  "deploy ledger import user-manual topology and initialize" in { implicit env =>
    ExampleIntegrationTest.ensureSystemProperties(
      "canton-examples.dar-path" -> CantonExamplesPath
    )
    runScript(repairConfiguration / "import-ledger-init.canton")(env.environment)
  }
}
