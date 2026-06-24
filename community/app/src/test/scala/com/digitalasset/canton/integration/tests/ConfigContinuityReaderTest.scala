// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.CantonConfig
import com.digitalasset.canton.config.ConfigErrors.{
  CannotParseFilesError,
  CannotReadFilesError,
  GenericConfigError,
  SubstitutionError,
  ValidationError,
}
import com.digitalasset.canton.integration.tests.ConfigContinuityReaderTest.Transforms
import com.digitalasset.canton.integration.tests.manual.S3Synchronization
import com.digitalasset.canton.version.ReleaseVersion
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.wordspec.AnyWordSpec

/** Simple test that loads config files in the /config folder for each release and verifies they
  * parse in this Canton version
  */
final class ConfigContinuityReaderTest extends AnyWordSpec with BaseTest with S3Synchronization {

  private lazy val allTransforms: Map[(Int, Int, Int), Transforms] = Map(
    (3, 5, 0) -> Transforms(
      Seq(
        // removed old code paths for async writer refactoring introduced via feature flag into 3.3
        "canton.sequencers.sequencer1.parameters.async-writer.enabled"
      )
    ),
    (3, 5, 2) -> Transforms(
      Seq(
        // remove contract state mode flag introduced in preview release and removed for 3.5
        "canton.participants.participant1.parameters.engine.contract-state-mode"
      )
    ),
  )

  /** Make the config parsable by applying some transformations. It basically makes some breaking
    * changes legitimate.
    */
  private def makeParsable(parsedConfig: Config, sourceVersion: ReleaseVersion): Config = {
    val transforms = allTransforms.foldLeft(Transforms.empty) {
      case (acc, ((major, minor, patch), paths))
          if sourceVersion.major < major ||
            major == sourceVersion.major && minor > sourceVersion.minor ||
            major == sourceVersion.major && minor == sourceVersion.minor && patch > sourceVersion.patch =>
        Transforms(acc.removePaths ++ paths.removePaths)
      case (acc, _) => acc
    }
    transforms.removePaths.foldLeft(parsedConfig) { case (config, removedPath) =>
      config.withoutPath(removedPath)
    }
  }

  "Data continuity config" should {
    S3Dump
      .getDumpBaseDirectoriesForVersion(None)
      // Filter-out dumps that don't contain the config
      .filter { case (_, releaseVersion) => releaseVersion >= ReleaseVersion.tryCreate("3.4.10-a") }
      .foreach { case (directory, releaseVersion) =>
        s"parse default config for version $releaseVersion in ${directory.localDownloadPath}" in {
          val initialConfigFile = directory.localDownloadPath / "default.conf"
          val transformedConfigFile = directory.localDownloadPath / "transformed_default.conf"

          val parsedConfig = ConfigFactory.parseFile(initialConfigFile.toJava)
          val updatedConfig = makeParsable(parsedConfig, releaseVersion)

          transformedConfigFile.write(
            updatedConfig.root().render(CantonConfig.defaultConfigRenderer)
          )

          val parsedResult = CantonConfig
            .parseAndLoad(Seq(transformedConfigFile).map(_.toJava), None)
          parsedResult match {
            case Right(config) =>
              config shouldBe a[CantonConfig]

            case Left(error) =>
              val specificClue = error match {
                case e: GenericConfigError.Error if e.cause.contains("Unknown key") =>
                  """The PureConfig `ConfigReader` failed while mapping the resolved HOCON tree to the `CantonConfig` Scala case classes.
                    |Because we enforce `allowUnknownKeys = false`, any key present in the historical S3 dump (latest release code) that does not exist in the current Scala AST causes a `ConvertFailure(UnknownKey)` when calling `pureconfig.ConfigSource.load[CantonConfig]` during the configuration loading phase.
                    |
                    |RESOLUTION PATHS:
                    |1. IF YOU DID NOT TOUCH CONFIGS (The test randomly failed on your branch):
                    |   FIX: Check `main` and rebase!
                    |   A recent patch release (e.g., 3.5.x) introduced a new config key and published its dump to S3, but your branch is missing the corresponding Scala case classes.
                    |   - If the forward-port has been merged: Pull the latest `main` and rebase your branch.
                    |   - If `main` is also failing this test: The forward-port PR from the release branch hasn't been merged yet.
                    |   
                    |2. IF YOU ARE ACTIVELY REMOVING OR RENAMING A CONFIG KEY:
                    |   FIX: Add a deprecation or transform step!
                    |   - Add a `DeprecatedConfigPath` mapping to `DeprecatedFieldsFor` in `CantonConfig.scala`.
                    |   - Or if backwards compatibility is NOT required: Add the path to `allTransforms` in `ConfigContinuityReaderTest.scala`. This simply deletes the key from the historical dump before the test parses it.
                    |""".stripMargin

                case _: GenericConfigError.Error =>
                  """The PureConfig `ConfigReader` failed while deriving the `CantonConfig` AST. The historical HOCON file is valid, but it cannot be successfully converted into Canton's configuration case classes.
                    |This is the miscellaneous category of configuration errors.
                    |
                    |RESOLUTION PATH:
                    |Check if a recent code change altered a field type or added a mandatory field without a default value, making older configurations unconvertible.
                    |""".stripMargin

                case _: CannotParseFilesError.Error =>
                  """`CannotParseFilesError` is thrown because a config file doesn't contain valid HOCON format (e.g., a forgotten bracket).
                    |
                    |RESOLUTION PATH:
                    |Inspect the `transformed_default.conf` generated by this test, or the raw dump downloaded from S3. It contains structurally invalid HOCON.
                    |""".stripMargin

                case _: ValidationError.Error =>
                  """PureConfig successfully mapped the HOCON to the `CantonConfig` case classes, but it failed Canton's internal configuration validation logic, yielding a `ValidationError`.
                    |
                    |RESOLUTION PATH:
                    |Review the specific validation error causes printed in the log trace. The values in the historical dump violate current validation rules.
                    |""".stripMargin

                case _: SubstitutionError.Error =>
                  """`SubstitutionError`, caused possibly by attempting to use an environment variable that is not defined within a config-file.
                    |
                    |RESOLUTION PATHS:
                    |1. If the error is an undefined environment variable: Ensure that variable is exported in your test environment.
                    |2. If the error is an unresolved HOCON path reference (${path}): The historical file is referencing an internal configuration section that no longer exists on your branch. Use `allTransforms` in the test to strip it out.
                    |""".stripMargin

                case _: CannotReadFilesError.Error =>
                  """Canton cannot read the configuration file, yielding a `CannotReadFilesError`
                    |
                    |RESOLUTION PATH:
                    |Verify that the file system paths in the test setup are correct and that the test has permission to read the file it just created.
                    |""".stripMargin

                case _ =>
                  """An unexpected configuration error occurred during the test."""
              }
              fail(
                s"""CONFIG CONTINUITY FAILED ($releaseVersion)
                   | DIAGNOSIS:
                   |$specificClue
                   | RAW TRACE:
                   |${error.cause}""".stripMargin
              )
          }
        }
      }
  }
}

private object ConfigContinuityReaderTest {
  final case class Transforms(removePaths: Seq[String])

  object Transforms {
    lazy val empty: Transforms = Transforms(Nil)
  }
}
