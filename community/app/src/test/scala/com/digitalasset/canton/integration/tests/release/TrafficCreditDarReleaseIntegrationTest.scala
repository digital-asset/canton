// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.release

import scala.sys.process.*

/** Checks that the canton-traffic-credit DAR ships with the release artifact, both as a classpath
  * resource loaded on startup and as a file in `dars/` for manual upload.
  */
class TrafficCreditDarReleaseIntegrationTest extends ReleaseArtifactIntegrationTestUtils {
  // turn off cache-dir to avoid compilation errors due to concurrent cache access
  private lazy val cacheTurnOff = s"$resourceDir/config-snippets/disable-ammonite-cache.conf"
  private lazy val simpleConf =
    "community/app/src/pack/examples/01-simple-topology/simple-topology.conf"
  private lazy val trafficAccountingDamlTopUpConf =
    s"$resourceDir/config-snippets/traffic-accounting-daml-top-up.conf"

  private def runCanton(processLogger: FixtureParam, bootstrapScript: String, extraArgs: String*) =
    (Seq(cantonBin, "--config", simpleConf) ++ extraArgs ++ Seq(
      "--verbose",
      "--no-tty",
      "--config",
      cacheTurnOff,
      "--bootstrap",
      s"$resourceDir/scripts/$bootstrapScript",
    )) ! processLogger

  "The release artifact" should {
    "load the traffic credit DAR on startup when Daml top-ups are enabled" in { processLogger =>
      runCanton(
        processLogger,
        "traffic-credit-dar-autoload.canton",
        "--config",
        trafficAccountingDamlTopUpConf,
      )
      checkOutput(
        processLogger,
        shouldContain = Seq("Traffic credit DAR was loaded automatically."),
      )
    }

    "allow uploading the bundled traffic credit DAR" in { processLogger =>
      runCanton(processLogger, "traffic-credit-dar-upload.canton")
      checkOutput(
        processLogger,
        shouldContain = Seq("Bundled traffic credit DAR was uploaded."),
      )
    }
  }
}
