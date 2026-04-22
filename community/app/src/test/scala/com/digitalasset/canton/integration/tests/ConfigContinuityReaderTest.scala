// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.CantonConfig
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
    )
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
        s"parse default config for version $releaseVersion" in {
          val initialConfigFile = directory.localDownloadPath / "default.conf"
          val transformedConfigFile = directory.localDownloadPath / "transformed_default.conf"

          val parsedConfig = ConfigFactory.parseFile(initialConfigFile.toJava)
          val updatedConfig = makeParsable(parsedConfig, releaseVersion)

          transformedConfigFile.write(
            updatedConfig.root().render(CantonConfig.defaultConfigRenderer)
          )

          CantonConfig
            .parseAndLoad(Seq(transformedConfigFile).map(_.toJava), None)
            .value shouldBe a[CantonConfig]
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
