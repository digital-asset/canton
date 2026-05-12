// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package testing.snapshot

import com.digitalasset.canton.LfPackageId
import com.digitalasset.canton.buildinfo.BuildInfo
import com.digitalasset.canton.config.RequireTypes.Port
import com.digitalasset.canton.integration.util.EntitySyntax
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.daml.lf.archive.DarDecoder
import com.digitalasset.daml.lf.data.Ref
import monocle.macros.syntax.lens.*
import org.scalatest.BeforeAndAfterEach

import java.nio.file.{FileSystems, Files, Path}

// Integration tests need to live in the package com.digitalasset.canton.integration.tests, so we
// make the test base an abstract class
abstract class ExtractSnapshotChoicesITBase
    extends CommunityIntegrationTest
    with SharedEnvironment
    with EntitySyntax
    with BeforeAndAfterEach {

  private val participantId = Ref.ParticipantId.assertFromString("participant1")
  private val snapshotDir = Files.createTempDirectory("ReplayBenchmarkTest")
  private val snapshotFileMatcher =
    FileSystems.getDefault
      .getPathMatcher(s"glob:$snapshotDir/snapshot-$participantId*.bin")
  private val darFile = "ReplayBenchmark.dar"
  private val darPath: Path =
    Option(getClass.getClassLoader.getResource(darFile))
      .map(path => Path.of(path.getPath))
      .getOrElse(throw new IllegalArgumentException(s"Cannot find resource $darFile"))
  private val ReplayBenchmarkPkgId: LfPackageId = getPkgId(darPath)
  private val AddChoiceName = s"$ReplayBenchmarkPkgId:ReplayBenchmark:T:Add"
  private val SubChoiceName = s"$ReplayBenchmarkPkgId:ReplayBenchmark:T:Sub"

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

  override def afterEach(): Unit =
    Files.newDirectoryStream(snapshotDir).forEach(Files.delete)

  "Correctly extract choice names from a snapshot file with and without filtering" in {
    implicit env =>
      import env.*

      // Generate a snapshot file by running all daml-script's in ReplayBenchmark.dar
      runScript(
        participant1.config.ledgerApi.address,
        participant1.config.ledgerApi.port,
      )
      val snapshotFiles = Files.list(snapshotDir).filter(snapshotFileMatcher.matches).toList
      snapshotFiles.size() should be(1)
      val snapshotFile = snapshotFiles.get(0)

      TransactionSnapshot.getAllTopLevelChoiceNames(snapshotFile) should equal(
        Set(AddChoiceName, SubChoiceName)
      )

      TransactionSnapshot.getAllTopLevelChoiceNames(
        snapshotFile,
        stepCountFilter = Some(190),
      ) should equal(Set(AddChoiceName, SubChoiceName))

      TransactionSnapshot.getAllTopLevelChoiceNames(
        snapshotFile,
        txNodeCountFilter = Some(3),
      ) should equal(Set(AddChoiceName))

      TransactionSnapshot.getAllTopLevelChoiceNames(
        snapshotFile,
        stepCountFilter = Some(193),
        txNodeCountFilter = Some(3),
      ) should equal(Set(AddChoiceName))
  }

  private def getPkgId(darPath: Path): LfPackageId =
    DarDecoder.assertReadArchiveFromFile(darPath.toFile).main._1

  private def getEnv(name: String, default: String): String =
    sys.props.get(name) match {
      case Some(value) =>
        // on CI we should get the value from the configMap
        value
      case None =>
        logger.warn(s"Using default value for $name: $default.")
        default
    }

  private def runScript(host: String, port: Port): Unit = {
    println(s"Generating snapshot data using script code in $darPath")

    val cmd = List(
      List("dpm", "script"),
      List("--dar", darPath.toFile.toString),
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
    Files.write(tmpDir.resolve("daml.yaml"), dummyProjectFile.getBytes)

    try {
      val logger =
        sys.process.ProcessLogger(stdout.append(_).append("\n"), stderr.append(_).append("\n"))
      val exitCode = sys.process.Process(cmd, cwd = Some(tmpDir.toFile), env*) ! logger

      assert(exitCode == 0, s"dpm script failed with exit code $exitCode: \n" + stdout.toString())
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
