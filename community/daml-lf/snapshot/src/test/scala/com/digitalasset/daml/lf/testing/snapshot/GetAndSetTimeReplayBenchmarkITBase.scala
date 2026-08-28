// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package testing.snapshot

import com.digitalasset.canton.LfPackageId
import com.digitalasset.canton.buildinfo.BuildInfo
import com.digitalasset.canton.integration.util.EntitySyntax
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
}
import com.digitalasset.canton.topology.transaction.VettedPackage
import com.digitalasset.canton.util.SetupPackageVetting
import com.digitalasset.daml.lf.archive.DarDecoder
import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.value.ContractIdVersion
import io.circe.*
import io.circe.parser.*
import monocle.macros.syntax.lens.*
import org.apache.commons.io.FileUtils
import org.scalatest.BeforeAndAfterAll

import java.nio.file.*

// Integration tests need to live in the package com.digitalasset.canton.integration.tests, so we
// make the test base an abstract class
abstract class GetAndSetTimeReplayBenchmarkITBase(
    contractIdVersion: ContractIdVersion
) extends CommunityIntegrationTest
    with SharedEnvironmentWithStaticTime
    with EntitySyntax
    with BeforeAndAfterAll {

  // Used for test debugging
  val debug = true

  val participantId = Ref.ParticipantId.assertFromString("participant1")
  val snapshotDir = Files.createTempDirectory("GetAndSetTimeReplayBenchmarkIT")
  val snapshotFileMatcher =
    FileSystems
      .getDefault()
      .getPathMatcher(s"glob:$snapshotDir/snapshot-$participantId*.bin")
  val darFile = "ReplayBenchmark.dar"
  val darPath: Path =
    Option(getClass.getClassLoader.getResource(darFile))
      .map(path => Path.of(path.getPath))
      .getOrElse(throw new IllegalArgumentException(s"Cannot find resource $darFile"))
  val ReplayBenchmarkPkgId: LfPackageId = getPkgId(darPath)
  val dummyProjectFile =
    """override-components:
      |  daml-script:
      |    version: $DAML_VERSION
      |""".stripMargin
  Files.write(snapshotDir.resolve("daml.yaml"), dummyProjectFile.getBytes)

  if (debug) {
    println(s"Transaction snapshot directory is $snapshotDir")
  }

  private implicit val decodeResult: Decoder[Either[String, Json]] = Decoder.instance { c =>
    c.downField("error").as[String].map(Left(_)) orElse c.downField("result").as[Json].map(Right(_))
  }

  override lazy val environmentDefinition: EnvironmentDefinition =
    super.environmentDefinition
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

        // Upload the test DAR and vet its packages
        SetupPackageVetting(
          Set(s"${darPath.getParent.toFile.getAbsolutePath}/$darFile"),
          targetTopology = Map(
            daId -> participants.all
              .map(
                _ -> VettedPackage
                  .unbounded(
                    Seq(ReplayBenchmarkPkgId)
                  )
                  .toSet
              )
              .toMap
          ),
        )
      }

  final override def afterAll(): Unit = {
    if (!debug) {
      FileUtils.deleteDirectory(snapshotDir.toFile)
    }
    super.afterAll()
  }

  "Transaction snapshots that use GetTime and SetTime" should {
    "be replayable and valid" in { implicit env =>
      // Use dpm to run a script against an instrumented canton instance and snag transaction snapshot data in a file
      runDamlScriptTest("GetAndSetTime:createAndSetTime")

      val snapshotFiles = Files.list(snapshotDir).filter(snapshotFileMatcher.matches).toList
      snapshotFiles.size() should be(1)

      val snapshotFile = snapshotFiles.get(0)
      Files.exists(snapshotFile) should be(true)
      Files.size(snapshotFile) should be > 0L

      // Replay and validate the snapshot file
      val benchmark = new ReplayBenchmark
      benchmark.darDir = darPath.getParent.toFile.getAbsolutePath
      benchmark.choiceName = "GetAndSetTime:T:Add"
      benchmark.entriesFile = snapshotFile.toFile.getAbsolutePath
      benchmark.contractIdVersion = contractIdVersion.toString

      noException should be thrownBy benchmark.init()
    }
  }

  private def runDamlScriptTest(testScriptId: String)(implicit env: FixtureParam): Unit = {
    import env.participant1

    val host = participant1.config.ledgerApi.address
    val port = participant1.config.ledgerApi.port
    val outputFile = Files.createTempFile(snapshotDir, testScriptId, ".json")
    val cmd = List(
      List("dpm", "script"),
      List("--dar", s"${darPath.getParent.toFile.getAbsolutePath}/$darFile"),
      List("--script-name", testScriptId),
      List("--ledger-host", host),
      List("--ledger-port", port.unwrap.toString),
      List("--static-time"),
      List("--max-inbound-message-size", Int.MaxValue.toString),
      List("--upload-dar", "false"),
      List("--json-test-summary", outputFile.toString),
    ).flatten
    val (stdout, stderr) = run(cmd)
    val resultOrErr = for {
      output <- scala.util.Try(Files.readString(outputFile)).toEither
      json <- parse(output)
      result <- json.as[Map[String, Either[String, Json]]]
    } yield result

    resultOrErr match {
      case Right(value) =>
        value.getOrElse(
          testScriptId,
          scriptError(
            cmd,
            stdout,
            stderr,
            s"failed to get a test summary for script: $testScriptId",
          ),
        ) match {
          case Right(_) =>
            ()
          case Left(err) =>
            scriptError(cmd, stdout, stderr, s"$testScriptId failed with: $err")
        }
      case Left(err) =>
        scriptError(
          cmd,
          stdout,
          stderr,
          s"failed to parse script output: ${err.getMessage}",
        )
    }
  }

  private def run(cmd: List[String]): (String, String) = {
    val env = Seq(
      "DAML_VERSION" -> getEnv("damlVersion", BuildInfo.damlLibrariesVersion),
      "DPM_REGISTRY" -> getEnv("dpmRegistry", "europe-docker.pkg.dev/da-images/public-unstable"),
    )
    val stderr = new StringBuilder
    val stdout = new StringBuilder
    val logger =
      sys.process.ProcessLogger(stdout.append(_).append("\n"), stderr.append(_).append("\n"))

    sys.process.Process(
      cmd,
      cwd = Some(snapshotDir.toFile),
      env*
    ) ! logger

    (stdout.result(), stderr.result())
  }

  private def scriptError(
      cmd: List[String],
      stdout: String,
      stderr: String,
      cause: String,
  ): Nothing = {
    Console.err.println(
      s"""running command failed:
         |  command: ${cmd.mkString(" ")}
         |  cause: $cause
         |  cwd: $snapshotDir (switch GetAndSetTimeReplayBenchmarkITBase.debug to true to keep this temporary directory)
         |  DAML_VERSION=${getEnv("damlVersion", "<not set>")}
         |  DPM_REGISTRY=${getEnv("dpmRegistry", "<not set>")}
         |  stdout: $stdout
         |  stderr: $stderr
         |""".stripMargin
    )
    throw new java.lang.Error(s"command failed: $cause")
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
}
