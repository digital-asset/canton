// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.manual

import cats.data.NonEmptyList
import com.digitalasset.canton.annotations.UnstableTest
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.integration.tests.manual.S3Synchronization.ContinuityDumpRef
import com.digitalasset.canton.util.ReleaseUtils
import com.digitalasset.canton.version.{ProtocolVersion, ReleaseVersion}

import scala.annotation.nowarn

final class BasicDataContinuityTestPostgresShard1
    extends BasicDataContinuityTest
    with DataContinuityTestFixturePostgres {
  registerPlugin(plugin)
  override def dumpDirectories(): List[(ContinuityDumpRef, ProtocolVersion)] =
    SplitReleaseVersion.currentRun(S3Dump.getDumpDirectories(_)).first
}

final class BasicDataContinuityTestPostgresShard2
    extends BasicDataContinuityTest
    with DataContinuityTestFixturePostgres {
  registerPlugin(plugin)

  override def dumpDirectories(): List[(ContinuityDumpRef, ProtocolVersion)] =
    SplitReleaseVersion.currentRun(S3Dump.getDumpDirectories(_)).second
}

final class BasicDataContinuityTestPostgresShard3
    extends BasicDataContinuityTest
    with DataContinuityTestFixturePostgres {
  registerPlugin(plugin)

  override def dumpDirectories(): List[(ContinuityDumpRef, ProtocolVersion)] =
    SplitReleaseVersion.currentRun(S3Dump.getDumpDirectories(_)).third
}

final class BasicDataContinuityTestPostgresShard4
    extends BasicDataContinuityTest
    with DataContinuityTestFixturePostgres {
  registerPlugin(plugin)

  override def dumpDirectories(): List[(ContinuityDumpRef, ProtocolVersion)] =
    SplitReleaseVersion.currentRun(S3Dump.getDumpDirectories(_)).fourth
}

@UnstableTest // TODO(i33868): remove this once the test is no longer flaky
final class SynchronizerChangeDataContinuityTestPostgresShard1
    extends SynchronizerChangeDataContinuityTest
    with DataContinuityTestFixturePostgres {
  registerPlugin(plugin)
  override def dumpDirectories(): List[(ContinuityDumpRef, ProtocolVersion)] =
    SplitReleaseVersion.currentRun(S3Dump.getDumpDirectories(_)).first
}

@UnstableTest // TODO(i33868): remove this once the test is no longer flaky
final class SynchronizerChangeDataContinuityTestPostgresShard2
    extends SynchronizerChangeDataContinuityTest
    with DataContinuityTestFixturePostgres {
  registerPlugin(plugin)
  override def dumpDirectories(): List[(ContinuityDumpRef, ProtocolVersion)] =
    SplitReleaseVersion.currentRun(S3Dump.getDumpDirectories(_)).second
}

@UnstableTest // TODO(i33868): remove this once the test is no longer flaky
final class SynchronizerChangeDataContinuityTestPostgresShard3
    extends SynchronizerChangeDataContinuityTest
    with DataContinuityTestFixturePostgres {
  registerPlugin(plugin)

  override def dumpDirectories(): List[(ContinuityDumpRef, ProtocolVersion)] =
    SplitReleaseVersion.currentRun(S3Dump.getDumpDirectories(_)).third
}

@UnstableTest // TODO(i33868): remove this once the test is no longer flaky
final class SynchronizerChangeDataContinuityTestPostgresShard4
    extends SynchronizerChangeDataContinuityTest
    with DataContinuityTestFixturePostgres {
  registerPlugin(plugin)
  override def dumpDirectories(): List[(ContinuityDumpRef, ProtocolVersion)] =
    SplitReleaseVersion.currentRun(S3Dump.getDumpDirectories(_)).fourth
}

/*
If you want to run the test locally on all the folders, uncomment this class
Because of hardcoded ports, this should be ran locally only.

class BasicDataContinuityTestPostgres_all
    extends BasicDataContinuityTest
    with DataContinuityTestFixturePostgres {
  registerPlugin(plugin)
  override def dumpDirectories(): List[(ContinuityDumpRef, ProtocolVersion)] =
    // In order to load dumps produced locally by a previous local run of
    // CreateBasicDataContinuityDumpsPostgres_all, replace S3Dump with LocalDump.
    S3Dump.getDumpDirectories()
}

class SynchronizerChangeDataContinuityTestPostgres_all
    extends SynchronizerChangeDataContinuityTest
    with DataContinuityTestFixturePostgres {
  registerPlugin(plugin)
  override def dumpDirectories(): List[(ContinuityDumpRef, ProtocolVersion)] =
    // In order to load dumps produced locally by a previous local run of
    // CreateSynchronizerChangeDataContinuityDumpsPostgres_all, replace S3Dump with LocalDump.
    S3Dump.getDumpDirectories()
}
 */

object SplitReleaseVersion {

  private val numberOfClasses = PositiveInt.tryCreate(4)

  /** Set to "true" by the CI workflow on regular PR branches to restrict the run to the latest
    * release of the current line. Full history is kept on `main` and `release-line` branches, where
    * it is "false" or unset. See `.github/workflows/_test_data_continuity_dumps.yml`.
    */
  private val latestReleaseOnly: Boolean =
    sys.env.get("DATA_CONTINUITY_LATEST_ONLY").exists(_.equalsIgnoreCase("true"))

  final case class Split(
      first: List[(ContinuityDumpRef, ProtocolVersion)],
      second: List[(ContinuityDumpRef, ProtocolVersion)],
      third: List[(ContinuityDumpRef, ProtocolVersion)],
      fourth: List[(ContinuityDumpRef, ProtocolVersion)],
  )

  /** Splits the dumps for this CI run into the four shard classes: the full history by default, or
    * just the latest release of the current line when the run is scoped down (regular PRs).
    *
    * `fetchDumps` is supplied by the caller because the dump source (`S3Dump`) is a member of the
    * `S3Synchronization` test trait and is only in scope inside the test classes, not in this
    * object. It is called with the `(major, minor)` line to scope to, or `None` for full history.
    *
    * A scoped run finds no dumps when the current line has not been released yet, in which case
    * there is no prior release to check continuity against and the run legitimately no-ops. Full
    * runs are left to fail loudly via `split` if the S3 listing ever comes back empty, since that
    * would signal a misconfiguration rather than a not-yet-released line.
    */
  def currentRun(
      fetchDumps: Option[(Int, Int)] => List[(ContinuityDumpRef, ProtocolVersion)]
  ): Split = {
    val dumps = fetchDumps(Option.when(latestReleaseOnly)(ReleaseVersion.current.majorMinor))
    if (latestReleaseOnly && dumps.isEmpty) Split(Nil, Nil, Nil, Nil)
    else split(dumps)
  }

  @nowarn("msg=match may not be exhaustive")
  def split(allDumpDirectories: List[(ContinuityDumpRef, ProtocolVersion)]): Split = {
    val List(first, second, third, fourth) =
      ReleaseUtils.shard(NonEmptyList.fromListUnsafe(allDumpDirectories), numberOfClasses)
    Split(first, second, third, fourth)
  }
}
