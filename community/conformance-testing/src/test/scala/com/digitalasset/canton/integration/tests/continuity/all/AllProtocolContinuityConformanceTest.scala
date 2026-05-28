// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.continuity.all

import com.digitalasset.canton.integration.tests.continuity.{
  ProtocolContinuityConformanceTest,
  ProtocolContinuityConformanceTestParticipant,
  ProtocolContinuityConformanceTestPing,
  ProtocolContinuityConformanceTestSynchronizer,
}
import com.digitalasset.canton.util.ReleaseUtils

/** The Protocol continuity tests test that we don't accidentally break protocol compatibility with
  * respect to the Ledger API. For each previously supported `(major, minor)` release line, the
  * tests are executed against the latest stable release (when one exists) and the latest non-stable
  * release (snapshot/RC, when one exists).
  */
sealed trait AllProtocolContinuityConformanceTest extends ProtocolContinuityConformanceTest {
  lazy val testedReleases: List[ReleaseUtils.TestedRelease] =
    ProtocolContinuityConformanceTest.previousSupportedReleases(logger).forgetNE

  protected val numShards: Int = 6
  protected def shard: Int
}

final class ProtocolContinuityShard0ConformanceTestSynchronizer
    extends ProtocolContinuityConformanceTestSynchronizer
    with AllProtocolContinuityConformanceTest {
  override def shard: Int = 0
}

final class ProtocolContinuityShard1ConformanceTestSynchronizer
    extends ProtocolContinuityConformanceTestSynchronizer
    with AllProtocolContinuityConformanceTest {
  override def shard: Int = 1
}

final class ProtocolContinuityShard2ConformanceTestSynchronizer
    extends ProtocolContinuityConformanceTestSynchronizer
    with AllProtocolContinuityConformanceTest {
  override def shard: Int = 2
}

final class ProtocolContinuityShard3ConformanceTestSynchronizer
    extends ProtocolContinuityConformanceTestSynchronizer
    with AllProtocolContinuityConformanceTest {
  override def shard: Int = 3
}

final class ProtocolContinuityShard4ConformanceTestSynchronizer
    extends ProtocolContinuityConformanceTestSynchronizer
    with AllProtocolContinuityConformanceTest {
  override def shard: Int = 4
}

final class ProtocolContinuityShard5ConformanceTestSynchronizer
    extends ProtocolContinuityConformanceTestSynchronizer
    with AllProtocolContinuityConformanceTest {
  override def shard: Int = 5
}

final class ProtocolContinuityShard0ConformanceTestParticipant
    extends ProtocolContinuityConformanceTestParticipant
    with AllProtocolContinuityConformanceTest {
  override def shard: Int = 0
}

final class ProtocolContinuityShard1ConformanceTestParticipant
    extends ProtocolContinuityConformanceTestParticipant
    with AllProtocolContinuityConformanceTest {
  override def shard: Int = 1
}

final class ProtocolContinuityShard2ConformanceTestParticipant
    extends ProtocolContinuityConformanceTestParticipant
    with AllProtocolContinuityConformanceTest {
  override def shard: Int = 2
}

final class ProtocolContinuityShard3ConformanceTestParticipant
    extends ProtocolContinuityConformanceTestParticipant
    with AllProtocolContinuityConformanceTest {
  override def shard: Int = 3
}

final class ProtocolContinuityShard4ConformanceTestParticipant
    extends ProtocolContinuityConformanceTestParticipant
    with AllProtocolContinuityConformanceTest {
  override def shard: Int = 4
}

final class ProtocolContinuityShard5ConformanceTestParticipant
    extends ProtocolContinuityConformanceTestParticipant
    with AllProtocolContinuityConformanceTest {
  override def shard: Int = 5
}

final class AllProtocolContinuityConformanceTestPing
    extends ProtocolContinuityConformanceTestPing
    with AllProtocolContinuityConformanceTest {
  override def shard: Int = 0
}
