// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package testing.snapshot

import com.digitalasset.canton.buildinfo.BuildInfo
import com.digitalasset.canton.config.RequireTypes.Port
import com.digitalasset.canton.integration.util.EntitySyntax
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
  TestConsoleEnvironment,
}
import com.digitalasset.daml.lf.data.Ref
import monocle.macros.syntax.lens.*
import org.scalatest.{Assertion, BeforeAndAfterAll}

import java.nio.file.{FileSystems, Files, Path}

/** Generate and save snapshot data by running all Daml script code within given Dar file(s).
  *
  * The following environment variables provide test arguments:
  *   - DAR_DIR: all Dar files in this directory have all their script code ran to generate data for
  *     the snapshot file.
  *   - SNAPSHOT_DIR: defines the (base) directory used for storing snapshot data. Snapshot files
  *     are saved in the file with path $SNAPSHOT_DIR/snapshot-participant0*.bin
  */
// Integration tests need to live in the package com.digitalasset.canton.integration.tests, so we
// make the test base an abstract class
abstract class GenerateSnapshotsBase
    extends CommunityIntegrationTest
    with SharedEnvironment
    with EntitySyntax
    with BeforeAndAfterAll {

  private var snapshotDir: Path = _
  private var scriptDarDir: Path = _

  override protected def beforeAll(): Unit = {
    assume(
      Seq("DAR_DIR", "SNAPSHOT_DIR")
        .forall(envVar => sys.env.contains(envVar)),
      "The environment variables DAR_DIR and SNAPSHOT_DIR all need to be set",
    )

    snapshotDir = Path.of(sys.env("SNAPSHOT_DIR"))
    scriptDarDir = Path.of(sys.env("DAR_DIR"))

    super.beforeAll()
  }

  lazy val participantId = Ref.ParticipantId.assertFromString("participant1")
  lazy val snapshotFileMatcher =
    FileSystems
      .getDefault()
      .getPathMatcher(s"glob:$snapshotDir/snapshot-$participantId*.bin")
  lazy val darFileMatcher =
    FileSystems
      .getDefault()
      .getPathMatcher(s"glob:$scriptDarDir/*.dar")

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1_S1M1
      .addConfigTransforms(
        ConfigTransforms.enableNonStandardConfig,
        ConfigTransforms.updateAllParticipantConfigs_(
          _.focus(_.parameters.engine.snapshotDir).replace(Some(snapshotDir))
        ),
      )
      .withSetup { implicit env =>
        import env.*

        participants.local.foreach { participant =>
          participant.synchronizers.connect_local(sequencer1, alias = daName)
        }
      }

  private def runWhenEnvVarSet(name: String)(testFun: TestConsoleEnvironment => Assertion): Unit =
    if (sys.env.contains("STANDALONE")) {
      name.in(testFun)
    } else {
      name.ignore(testFun)
    }

  runWhenEnvVarSet("Generate snapshot data") { implicit env =>
    import env.*

    Files.list(scriptDarDir).filter(darFileMatcher.matches).forEach { scriptDarPath =>
      runScript(
        scriptDarPath,
        participant1.config.ledgerApi.address,
        participant1.config.ledgerApi.port,
      )
    }
    val snapshotFiles = Files.list(snapshotDir).filter(snapshotFileMatcher.matches).toList
    snapshotFiles.size() should be(1)
  }

  private def getEnv(name: String, default: String): String =
    sys.props.get(name) match {
      case Some(value) =>
        // on CI we should get the value from the configMap
        value
      case None =>
        logger.warn(s"Using default value for $name: $default.")
        default
    }

  private def runScript(scriptDarPath: Path, host: String, port: Port): Unit = {
    println(s"Generating snapshot data using script code in $scriptDarPath")

    val cmd = List(
      List("dpm", "script"),
      List("--dar", scriptDarPath.toFile.toString),
      List("--all"),
      List("--ledger-host", host),
      List("--ledger-port", port.unwrap.toString),
      List("--static-time"),
      List("--max-inbound-message-size", Int.MaxValue.toString),
      List("--upload-dar", "true"),
    ).flatten

    val env = Seq(
      "DAML_VERSION" -> getEnv("damlVersion", BuildInfo.damlLibrariesVersion),
      "DPM_REGISTRY" -> getEnv("dpmRegistry", "europe-docker.pkg.dev/da-images/public-unstable"),
    )
    val stdout = new StringBuilder
    val stderr = new StringBuilder
    val tmpDir = Files.createTempDirectory("dpm-script")
    val dummyProjectFile =
      """override-components:
        |  daml-script:
        |    version: $DAML_VERSION
        |""".stripMargin

    try {
      Files.write(tmpDir.resolve("daml.yaml"), dummyProjectFile.getBytes)

      val logger =
        sys.process.ProcessLogger(stdout.append(_).append("\n"), stderr.append(_).append("\n"))
      val exitCode = sys.process.Process(cmd, cwd = Some(tmpDir.toFile), env*) ! logger

      exitCode match {
        case 1 =>
          // Check that all daml script test failures are due to GetTime calls and duplicate party allocation failures
          val failureLines = stdout
            .toString()
            .split("\n")
            .filter(_.contains("FAILURE"))
          assert(
            failureLines
              .forall { line =>
                line.contains(
                  "UNIMPLEMENTED: Method not found: com.daml.ledger.api.v2.testing.TimeService/GetTime"
                )
                || line.contains("Party already exists")
              },
            s"dpm script failed with exit code 1: \n" + stdout.toString(),
          )
          println(
            s"Daml-script in dar file: $scriptDarPath had the following failures:\n ${failureLines.map("- " + _).mkString("\n")}"
          )
        case 0 =>
        // do nothing
        case _ =>
          throw new java.lang.AssertionError(
            s"dpm script failed with exit code $exitCode: \n" + stdout.toString()
          )
      }
    } catch {
      case scala.util.control.NonFatal(cause) =>
        throw new Error(s"daml script failed: ${cause.getMessage}\n" + stderr.toString(), cause)
    } finally {
      // The call should not create any files
      tmpDir.resolve("daml.yaml").toFile.delete()
      tmpDir.toFile.delete()
    }
  }
}
