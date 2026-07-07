// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.resource

import better.files.*
import cats.syntax.functorFilter.*
import com.digitalasset.canton.config.DbConfig
import com.digitalasset.canton.util.ReleaseUtils
import com.digitalasset.canton.version.ReleaseVersion
import org.scalatest.wordspec.AnyWordSpec

/** Flyway SQL and Scala-based migrations should not be changed after we have Canton GA as that will
  * break existing deployments. These tests enforce this desired read-only property/immutability of
  * the Flyway migrations by computing a SHA256 checksum of each migration and checking that it
  * hasn't changed compared to the saved SHA256 checksum. Checksum for new Flyway SQL migration
  * files are added during the release process. Scala-based migrations carry no Flyway checksum of
  * their own, so we guard them the same way: each `.scala` migration file is accompanied by a
  * `.sha256` digest, and a missing or mismatching digest fails this test.
  */
final class FlywayChecksumsTest extends AnyWordSpec {

  "Postgres flyway migration files" should {
    "always have a valid SHA-256 digest file accompanied" in {
      compareChecksums(DbConfig.postgresMigrationsPathStable)
    }
  }

  "H2 flyway migration files" should {
    "always have a valid SHA-256 digest file accompanied" in {
      compareChecksums(DbConfig.h2MigrationsPathStable)
    }
  }

  "Scala-based flyway migration files" should {
    "always have a valid SHA-256 digest file accompanied" in {
      assertScalaChecksums()
    }
  }

  private val resourceLoader: Resource = Resource.from(Thread.currentThread().getContextClassLoader)
  private def compareChecksums(migrationPath: String): Unit = {

    val resourcePath = migrationPath.stripPrefix("classpath:")
    val sha256Files = resourceLoader
      .getAsString(resourcePath)
      .split(System.lineSeparator())
      .toSeq
      .filter(_.endsWith(".sha256"))

    val sqlFiles = resourceLoader
      .getAsString(resourcePath)
      .split(System.lineSeparator())
      .toSeq
      .filter(_.endsWith(".sql"))

    requireChecksumFilesForReleasedVersionsExist(resourcePath, sha256Files, sqlFiles)
    assertChecksums(resourcePath, sha256Files)
  }

  private def requireChecksumFilesForReleasedVersionsExist(
      resourcePath: String,
      sha256Files: Seq[String],
      sqlFiles: Seq[String],
  ): Unit = {
    // Throws if the string does not contain `major.minor` nor `initial`
    def releaseVersionFrom(filename: String): Option[String] =
      "\\d+\\.\\d+".r
        .findFirstIn(filename)
        .orElse(
          if (filename.contains("initial"))
            None
          else
            sys.error(
              s"$filename or its .sql counterpart does not have a release version (major.minor)!"
            )
        )

    val stableReleasedVersions = ReleaseUtils.previousSupportedStableReleases
      .map { case ReleaseVersion(major, minor, _, _) => s"$major.$minor" }

    val missingSqlChecksumFiles =
      sqlFiles.map(_.replace(".sql", ".sha256")).diff(sha256Files).mapFilter { missingSha =>
        val shaRequired = releaseVersionFrom(missingSha) match {
          case Some(releaseVersion) => stableReleasedVersions.contains(releaseVersion)
          case None =>
            false // TODO(#16458) Should be true so that missing sha for initial migration makes the test fail
        }

        Option.when(shaRequired)(missingSha)
      }

    if (missingSqlChecksumFiles.nonEmpty) {
      sys.error(
        s"Missing the following .sha256 file(s) in $resourcePath: ${missingSqlChecksumFiles.mkString(", ")}"
      )
    }
  }

  private def assertChecksums(resourcePath: String, sha256Files: Seq[String]): Unit =
    sha256Files
      .foreach { sha256Filename =>
        val sqlFileName = sha256Filename.replace(".sha256", ".sql")
        val prevSha256Value =
          resourceLoader
            .asString(s"$resourcePath/$sha256Filename")
            .getOrElse(
              sys.error(
                s"Unable to load $sha256Filename. Was the directory structure of migration files changed?"
              )
            )
            .replace("\n", "")
        val currSha256Value = File(
          resourceLoader
            .url(s"$resourcePath/$sqlFileName")
            .getOrElse(sys.error(s"""Unable to find SQL file $sqlFileName!
                 | Did you rename the SQL-file whose corresponding checksum was in $sha256Filename?
                 | If you did, then you need to rename the corresponding .sha256 file too!
                 |""".stripMargin))
            .getFile
        ).sha256.toLowerCase
        assert(
          prevSha256Value == currSha256Value,
          s"Sha256 checksum of migration file $sqlFileName has changed!",
        )
      }

  /** Scala-based migrations live in the source tree (not on the classpath as resources).
    */
  private def assertScalaChecksums(): Unit = {
    val repoRoot = Iterator
      .iterate(File(sys.props("user.dir")))(_.parent)
      .take(64)
      .find(dir => (dir / "build.sbt").exists)
      .getOrElse(sys.error("Unable to locate the repository root (no build.sbt found)"))
    val scalaMigrationDir =
      repoRoot / "community" / "common" / "src" / "main" / "scala" / "db" / "migration" / "canton"
    assert(
      scalaMigrationDir.isDirectory,
      s"Expected Scala migration directory at $scalaMigrationDir",
    )

    val scalaFiles = scalaMigrationDir.listRecursively.filter(_.extension.contains(".scala")).toSeq
    assert(scalaFiles.nonEmpty, s"No Scala migration files found under $scalaMigrationDir")

    scalaFiles.foreach { scalaFile =>
      val sha256File = File(s"${scalaFile.pathAsString.stripSuffix(".scala")}.sha256")
      assert(
        sha256File.exists,
        s"Missing .sha256 digest for Scala migration ${scalaFile.name}. Run recompute-sha256sums.sh.",
      )
      val prevSha256Value = sha256File.contentAsString.replace("\n", "").toLowerCase
      val currSha256Value = scalaFile.sha256.toLowerCase
      assert(
        prevSha256Value == currSha256Value,
        s"Sha256 checksum of Scala migration ${scalaFile.name} has changed! Frozen migrations must " +
          "not be modified; if this change is intentional and not yet released, run recompute-sha256sums.sh.",
      )
    }
  }

}
