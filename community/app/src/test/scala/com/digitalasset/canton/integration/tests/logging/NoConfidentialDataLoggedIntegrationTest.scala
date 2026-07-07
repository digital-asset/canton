// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.logging

import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.logging.SuppressionRule
import monocle.macros.syntax.lens.*
import org.slf4j.event.Level

/** Test to check that we don't log confidential contract data by accident */
class NoConfidentialDataLoggedIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment {

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1_S1M1
      .addConfigTransform(ConfigTransforms.allInMemory)
      // obviously if this is on then we log confidential data ...
      // but it allowed me to test that the test works ...
      .addConfigTransform(_.focus(_.monitoring.logging.api.messagePayloads).replace(false))

  "we don't log confidential data by default" in { implicit env =>
    import env.*

    participant1.synchronizers.connect_local(sequencer1, daName)

    val confidential = "CONFIDENTIAL123"
    val alice = participant1.parties.enable("Alice")
    val pkg = participant1.packages.find_by_module("Canton.Internal.Ping").loneElement.packageId
    loggerFactory.assertLogsSeq(rule = SuppressionRule.LevelAndAbove(Level.DEBUG))(
      participant1.ledger_api.commands.submit(
        Seq(alice),
        Seq(
          ledger_api_utils.create(
            pkg,
            "Canton.Internal.Ping",
            "Ping",
            Map[String, Any](
              "id" -> confidential,
              "initiator" -> alice,
              "responder" -> alice,
            ),
          )
        ),
      ),
      forAll(_)(_.message should not include (confidential)),
    )
  }
}
