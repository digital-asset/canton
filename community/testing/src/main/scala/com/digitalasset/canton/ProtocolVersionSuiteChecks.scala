// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton

import com.digitalasset.canton.annotations.{MaxProtocolVersion, MinProtocolVersion}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion
import org.scalatest.{Ignore, Suite, SuiteMixin}

/** Ignores all tests of a suite that declares a protocol version range through
  * [[com.digitalasset.canton.annotations.MinProtocolVersion]] /
  * [[com.digitalasset.canton.annotations.MaxProtocolVersion]], if `testedProtocolVersion` is
  * outside of that range. Use this to disable an entire suite; use
  * [[TestPredicateFiltersFixtureAnyWordSpec]] and the `ProtocolVersionChecks*` traits to disable
  * individual test cases or blocks.
  *
  * Mix this in after the scalatest style trait. It is already part of [[BaseTestWordSpec]] and of
  * `BaseIntegrationTest`; it deliberately is not part of [[BaseTest]], because `BaseTest` is also
  * mixed into classes that are not scalatest suites and into traits that stack onto
  * [[org.scalatest.SuiteMixin]]. Neither `Suite` nor `SuiteMixin` can therefore be a parent of
  * `BaseTest`.
  */
trait ProtocolVersionSuiteChecks extends SuiteMixin { this: Suite & TestEssentials =>

  /** Why `testedProtocolVersion` is outside of the range declared by
    * [[com.digitalasset.canton.annotations.MinProtocolVersion]] /
    * [[com.digitalasset.canton.annotations.MaxProtocolVersion]] on this suite, if it is.
    */
  private lazy val unsupportedTestedProtocolVersion: Option[String] = {
    val minProtocolVersion = Option(getClass.getAnnotation(classOf[MinProtocolVersion]))
      .map(annotation => ProtocolVersion.tryCreate(annotation.value()))
    val maxProtocolVersion = Option(getClass.getAnnotation(classOf[MaxProtocolVersion]))
      .map(annotation => ProtocolVersion.tryCreate(annotation.value()))

    val reason = minProtocolVersion
      .filter(testedProtocolVersion < _)
      .map(min => s"runs only with protocol version $min or a later one")
      .orElse(
        maxProtocolVersion
          .filter(testedProtocolVersion > _)
          .map(max => s"runs only with protocol version $max or an earlier one")
      )

    reason.foreach { r =>
      logger.info(
        s"Ignoring all tests of ${getClass.getName}, as the suite $r, " +
          s"but the tested protocol version is $testedProtocolVersion."
      )(TraceContext.empty)
    }

    reason
  }

  /** Reports all tests of this suite as ignored, if `testedProtocolVersion` is outside of the range
    * declared by [[com.digitalasset.canton.annotations.MinProtocolVersion]] /
    * [[com.digitalasset.canton.annotations.MaxProtocolVersion]].
    *
    * No test is then expected to run, which makes `BeforeAndAfterAll.beforeAll` (and therefore the
    * creation of an integration test environment) get skipped as well.
    *
    * Being an `abstract override`, this stacks onto the `tags` of a scalatest style trait, so
    * mixing this trait in too early fails to compile rather than silently having no effect.
    */
  abstract override def tags: Map[String, Set[String]] = {
    val inheritedTags = super.tags

    if (unsupportedTestedProtocolVersion.isEmpty) inheritedTags
    else
      inheritedTags ++ testNames.map { testName =>
        // Same tag as used by @org.scalatest.Ignore and by `ignore` in the scalatest style traits
        testName -> (inheritedTags.getOrElse(testName, Set.empty) + classOf[Ignore].getName)
      }
  }
}
