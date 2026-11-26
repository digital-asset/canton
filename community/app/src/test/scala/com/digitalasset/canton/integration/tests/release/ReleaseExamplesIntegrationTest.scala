// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.release

import better.files.File
import com.digitalasset.canton.config.NonNegativeDuration
import com.digitalasset.canton.console.ConsoleMacros.utils

import scala.sys.process.Process

/** Test _some_ exemples from the release artifact. In particular the ones requiring external
  * scripts to be run on the participant.
  */
class ReleaseExamplesIntegrationTest extends ReleaseArtifactIntegrationTestUtils {
  "The release examples" should {
    "successfully run the interactive topology example" in { processLogger =>
      val interactiveTopologyDir = File(s"$cantonDir/examples/08-interactive-submission")
      val cantonBinRel = interactiveTopologyDir.relativize(File(cantonBin))

      var cantonProcess: Option[Process] = None
      val portsFile = interactiveTopologyDir / "canton_ports.json"
      portsFile.deleteOnExit(swallowIOExceptions = true)

      try {
        cantonProcess = Some(
          Process(
            s"$cantonBinRel --no-tty --config interactive-submission.conf --bootstrap bootstrap.canton",
            cwd = interactiveTopologyDir.toJava,
          ).run(processLogger)
        )
        try {
          utils.retry_until_true(NonNegativeDuration.ofSeconds(80))(portsFile.exists)
        } catch {
          case ex: IllegalStateException =>
            // The ports file is written only after the bootstrap script completed. Surface the
            // output of the Canton process, otherwise a timeout here gives no hint why.
            logger.error(
              s"Canton did not write $portsFile in time. Process output:\n${processLogger.output()}"
            )
            throw ex
        }

        Process(
          "./interactive_topology_example.sh",
          cwd = interactiveTopologyDir.toJava,
        ).!(processLogger) shouldBe 0
      } finally {
        cantonProcess.foreach(_.destroy())
      }
    }
  }
}
