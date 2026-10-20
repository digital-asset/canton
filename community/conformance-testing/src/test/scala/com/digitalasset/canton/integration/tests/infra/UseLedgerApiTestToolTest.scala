// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.infra

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.integration.plugins.UseLedgerApiTestTool.{
  findAllCoreVersions,
  findMatchingVersions,
  latestRelease,
  latestReleases,
}
import com.digitalasset.canton.util.ReleaseUtils
import com.digitalasset.canton.version.ReleaseVersion
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

final class UseLedgerApiTestToolTest extends AnyFlatSpec with Matchers with BaseTest {
  private val versions = Seq(
    "3.3.0-snapshot.20250416.15779.0.v6cccc0c4",
    "3.3.0-ad-hoc.20250905.16091.0.v704bf59d",
    "3.3.0-snapshot.20251007.16123.0.v670c8fae",
    "3.3.1-snapshot.20251007.16123.0.v670c8fae", // not existing just for testing patches
    "3.3.10-snapshot.20251007.16123.0.v670c8fae", // not existing just for testing patches
    "3.4.0-snapshot.20250429.15866.0.vc8f10812",
    "3.4.0-snapshot.20251003.17075.0.v69d92264",
  ).map(ReleaseVersion.tryCreate)

  "findAllCoreVersions" should "find the major.minor.patch releases correctly" in {
    findAllCoreVersions(versions) shouldBe Seq((3, 3, 0), (3, 3, 1), (3, 3, 10), (3, 4, 0))
  }

  "findMatchingVersions" should "find and sort all the versions matching the given release" in {
    findMatchingVersions(versions, (3, 3, 0)).map(_.fullVersion) shouldBe Seq(
      "3.3.0-ad-hoc.20250905.16091.0.v704bf59d",
      "3.3.0-snapshot.20250416.15779.0.v6cccc0c4",
      "3.3.0-snapshot.20251007.16123.0.v670c8fae",
    )

    findMatchingVersions(versions, (3, 4, 0)).map(_.fullVersion) shouldBe Seq(
      "3.4.0-snapshot.20250429.15866.0.vc8f10812",
      "3.4.0-snapshot.20251003.17075.0.v69d92264",
    )
  }

  private val allReleases = ReleaseUtils.allPreviousReleases

  "latestRelease" should "be able to fetch the latest version from S3" in {
    val latestTool = latestRelease(logger)

    latestTool.major shouldBe 3
    latestTool shouldBe >=(ReleaseVersion.current)
  }

  "latestReleases" should "be able to fetch the lapitt for each release from S3" in {
    val releasedTools = latestReleases(logger)

    releasedTools should not be empty

    val currentMajorMinor =
      ReleaseVersion(ReleaseVersion.current.major, ReleaseVersion.current.minor, 0)

    // We only test releases starting from 3.3.0, as earlier releases are no longer supported
    val releases33andLater = allReleases.filter(_ > ReleaseVersion(3, 3, 0))

    val foundReleaseTools = releasedTools
      .filter(_ <= currentMajorMinor)
      .filter(_.isStable)

    val expectedReleases = releases33andLater.dropRight(1)

    foundReleaseTools should contain allElementsOf expectedReleases
  }
}
