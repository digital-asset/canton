// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.manual

import cats.data.NonEmptyList
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.integration.tests.manual.S3Synchronization.ContinuityDumpRef
import com.digitalasset.canton.util.ReleaseUtils
import com.digitalasset.canton.version.ProtocolVersion

import scala.annotation.nowarn

final class BasicDataContinuityTestPostgresShard1
    extends BasicDataContinuityTest
    with DataContinuityTestFixturePostgres {
  registerPlugin(plugin)
  override def dumpDirectories(): List[(ContinuityDumpRef, ProtocolVersion)] =
    SplitReleaseVersion.split(S3Dump.getDumpDirectories()).first
}

final class BasicDataContinuityTestPostgresShard2
    extends BasicDataContinuityTest
    with DataContinuityTestFixturePostgres {
  registerPlugin(plugin)

  override def dumpDirectories(): List[(ContinuityDumpRef, ProtocolVersion)] =
    SplitReleaseVersion.split(S3Dump.getDumpDirectories()).second
}

final class BasicDataContinuityTestPostgresShard3
    extends BasicDataContinuityTest
    with DataContinuityTestFixturePostgres {
  registerPlugin(plugin)

  override def dumpDirectories(): List[(ContinuityDumpRef, ProtocolVersion)] =
    SplitReleaseVersion.split(S3Dump.getDumpDirectories()).third
}

final class BasicDataContinuityTestPostgresShard4
    extends BasicDataContinuityTest
    with DataContinuityTestFixturePostgres {
  registerPlugin(plugin)

  override def dumpDirectories(): List[(ContinuityDumpRef, ProtocolVersion)] =
    SplitReleaseVersion.split(S3Dump.getDumpDirectories()).fourth
}

final class SynchronizerChangeDataContinuityTestPostgresShard1
    extends SynchronizerChangeDataContinuityTest
    with DataContinuityTestFixturePostgres {
  registerPlugin(plugin)
  override def dumpDirectories(): List[(ContinuityDumpRef, ProtocolVersion)] =
    SplitReleaseVersion.split(S3Dump.getDumpDirectories()).first
}

final class SynchronizerChangeDataContinuityTestPostgresShard2
    extends SynchronizerChangeDataContinuityTest
    with DataContinuityTestFixturePostgres {
  registerPlugin(plugin)
  override def dumpDirectories(): List[(ContinuityDumpRef, ProtocolVersion)] =
    SplitReleaseVersion.split(S3Dump.getDumpDirectories()).second
}

final class SynchronizerChangeDataContinuityTestPostgresShard3
    extends SynchronizerChangeDataContinuityTest
    with DataContinuityTestFixturePostgres {
  registerPlugin(plugin)

  override def dumpDirectories(): List[(ContinuityDumpRef, ProtocolVersion)] =
    SplitReleaseVersion.split(S3Dump.getDumpDirectories()).third
}

final class SynchronizerChangeDataContinuityTestPostgresShard4
    extends SynchronizerChangeDataContinuityTest
    with DataContinuityTestFixturePostgres {
  registerPlugin(plugin)
  override def dumpDirectories(): List[(ContinuityDumpRef, ProtocolVersion)] =
    SplitReleaseVersion.split(S3Dump.getDumpDirectories()).fourth
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

  final case class Split(
      first: List[(ContinuityDumpRef, ProtocolVersion)],
      second: List[(ContinuityDumpRef, ProtocolVersion)],
      third: List[(ContinuityDumpRef, ProtocolVersion)],
      fourth: List[(ContinuityDumpRef, ProtocolVersion)],
  )

  @nowarn("msg=match may not be exhaustive")
  def split(allDumpDirectories: List[(ContinuityDumpRef, ProtocolVersion)]): Split = {
    val List(first, second, third, fourth) =
      ReleaseUtils.shard(NonEmptyList.fromListUnsafe(allDumpDirectories), numberOfClasses)
    Split(first, second, third, fourth)
  }
}
