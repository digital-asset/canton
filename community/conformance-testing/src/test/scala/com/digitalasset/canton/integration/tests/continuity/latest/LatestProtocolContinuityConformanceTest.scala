// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.continuity.latest

import com.digitalasset.canton.integration.tests.continuity.{
  ProtocolContinuityConformanceTest,
  ProtocolContinuityConformanceTestParticipant,
  ProtocolContinuityConformanceTestPing,
  ProtocolContinuityConformanceTestSynchronizer,
}
import com.digitalasset.canton.util.ReleaseUtils

/** The Protocol continuity tests test that we don't accidentally break protocol compatibility with
  * respect to the Ledger API. The tests are executed against the latest patch (stable or
  * snapshot/RC, whichever is most recent) of the most recent supported `(major, minor)` release
  * line.
  *
  * If you add new config parameters, these tests may start to fail to start up (check the log files
  * starting with "external-<node>". You need to remove these new config parameters via
  * [[ProtocolContinuityConformanceTest.removeConfigPaths]].
  */
trait LatestProtocolContinuityConformanceTest extends ProtocolContinuityConformanceTest {
  override lazy val testedReleases: List[ReleaseUtils.TestedRelease] = {
    val all = ProtocolContinuityConformanceTest.previousSupportedReleases(logger)
    val latestMinor = all.map(_.releaseVersion.majorMinor).max1
    List(all.filter(_.releaseVersion.majorMinor == latestMinor).maxBy(_.releaseVersion))
  }

  protected val numShards: Int = 1
  override val shard: Int = 0
}

class LatestProtocolContinuityConformanceTestSynchronizer
    extends ProtocolContinuityConformanceTestSynchronizer
    with LatestProtocolContinuityConformanceTest

class LatestProtocolContinuityConformanceTestParticipant
    extends ProtocolContinuityConformanceTestParticipant
    with LatestProtocolContinuityConformanceTest {

  override def disableBinaryVersionEnforcement: Boolean = true

}

class LatestProtocolContinuityConformanceTestPing
    extends ProtocolContinuityConformanceTestPing
    with LatestProtocolContinuityConformanceTest
