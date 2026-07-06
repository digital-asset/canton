// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.examples

import com.digitalasset.canton.integration.CommunityIntegrationTest
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.tests.examples.ExampleIntegrationTest.{
  referenceConfiguration,
  repairConfiguration,
}

sealed abstract class RepairExampleIntegrationTest
    extends ExampleIntegrationTest(
      referenceConfiguration / "storage" / "postgres.conf",
      repairConfiguration / "synchronizer-repair-lost.conf",
      repairConfiguration / "synchronizer-repair-new.conf",
      repairConfiguration / "participant1.conf",
      repairConfiguration / "participant2.conf",
      repairConfiguration / "enable-preview-commands.conf",
    )
    with CommunityIntegrationTest {
  "deploy repair user-manual topology and initialize" in { implicit env =>
    ExampleIntegrationTest.ensureSystemProperties("canton-examples.dar-path" -> CantonExamplesPath)
    runScript(repairConfiguration / "synchronizer-repair-init.canton")(env.environment)
  }
}

final class RepairExampleBftOrderingIntegrationTestPostgres extends RepairExampleIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
}
