// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton

import com.digitalasset.canton.annotations.{MaxProtocolVersion, MinProtocolVersion}
import com.digitalasset.canton.version.ProtocolVersion
import org.scalatest.wordspec.{AnyWordSpec, AnyWordSpecLike}
import org.scalatest.{Assertion, Filter, Suite, Tag}

final class ProtocolVersionSuiteChecksTest extends AnyWordSpec with BaseTest {

  "ProtocolVersionSuiteChecks" should {
    "not ignore tests of an unannotated suite" in {
      checkIgnored(new UnannotatedSuite, expectIgnored = false)
    }

    "ignore all tests of a suite requiring a later protocol version" in {
      checkIgnored(
        new MinDevSuite,
        expectIgnored = testedProtocolVersion < ProtocolVersion.dev,
      )
    }

    "ignore all tests of a suite requiring an earlier protocol version" in {
      checkIgnored(
        new MaxV34Suite,
        expectIgnored = testedProtocolVersion > ProtocolVersion.v34,
      )
    }

    "not ignore any test of a suite whose declared range contains the tested protocol version" in {
      checkIgnored(
        new MinV34MaxDevSuite,
        expectIgnored =
          testedProtocolVersion < ProtocolVersion.v34 || testedProtocolVersion > ProtocolVersion.dev,
      )
    }
  }

  private def checkIgnored(suite: Suite, expectIgnored: Boolean): Assertion = {
    val ignoredTests = suite.tags.collect {
      case (testName, tags) if tags.contains(classOf[org.scalatest.Ignore].getName) => testName
    }.toSet

    // The suite's own tags must be preserved either way.
    suite.tags(taggedTestName) should contain(exampleTag.name)

    if (expectIgnored) {
      ignoredTests shouldBe suite.testNames
      // Without any test to run, scalatest also skips `beforeAll`.
      suite.expectedTestCount(Filter()) shouldBe 0
    } else {
      ignoredTests shouldBe empty
      suite.expectedTestCount(Filter()) shouldBe 2
    }
  }

  private lazy val exampleTag: Tag = Tag("ProtocolVersionSuiteChecksTestTag")
  private lazy val taggedTestName: String = "an example should run a tagged test"

  // The example suites are nested, so that scalatest does not discover them as suites of their own.
  private trait ExampleSuite extends AnyWordSpecLike with BaseTest with ProtocolVersionSuiteChecks {
    "an example" should {
      "run a plain test" in { succeed }
      "run a tagged test" taggedAs exampleTag in { succeed }
    }
  }

  private class UnannotatedSuite extends ExampleSuite

  @MinProtocolVersion("dev")
  private class MinDevSuite extends ExampleSuite

  @MaxProtocolVersion("34")
  private class MaxV34Suite extends ExampleSuite

  @MinProtocolVersion("34")
  @MaxProtocolVersion("dev")
  private class MinV34MaxDevSuite extends ExampleSuite
}
