// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.examples

import com.digitalasset.canton.integration.plugins.UsePostgres
import com.digitalasset.canton.integration.tests.examples.ExampleIntegrationTest.composabilityConfiguration
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransform,
  ConfigTransforms,
}

sealed abstract class ComposabilityExampleIntegrationTest(
    scripts: Seq[String],
    enableAutomaticAssignment: Boolean,
) extends ExampleIntegrationTest(composabilityConfiguration / "composability.conf")
    with CommunityIntegrationTest {

  registerPlugin(new UsePostgres(loggerFactory))

  // Scripts that reassign manually must not race with an automatic assignment of the same
  // reassignment: it locks the contract and the manual assignment fails with
  // LOCAL_VERDICT_LOCKED_CONTRACTS.
  override protected def additionalConfigTransform: Seq[ConfigTransform] =
    if (enableAutomaticAssignment) Seq.empty else Seq(ConfigTransforms.disableAutomaticAssignment)

  "Canton" should {
    forEvery(scripts.zipWithIndex) { case (script, idx) =>
      s"run $script successfully" in { implicit env =>
        ExampleIntegrationTest.ensureSystemProperties(
          "canton-examples.dar-path" -> CantonExamplesPath
        )
        if (idx > 0) {
          // Since the nodes use in-memory storage, stopping and restarting completely resets them.
          env.nodes.local.foreach { n =>
            n.stop()
            n.start()
          }
        }
        runScript(composabilityConfiguration / script)(env.environment)
      }
    }
  }
}

final class ComposabilityExampleIntegrationTestPostgres
    extends ComposabilityExampleIntegrationTest(
      Seq("composability1.canton"),
      enableAutomaticAssignment = false,
    ) {
  registerPlugin(new UsePostgres(loggerFactory))
}

final class ComposabilityAutoReassignmentExampleIntegrationTestPostgres
    extends ComposabilityExampleIntegrationTest(
      Seq("composability-auto-reassignment.canton", "composability2.canton"),
      enableAutomaticAssignment = true,
    ) {
  registerPlugin(new UsePostgres(loggerFactory))
}
