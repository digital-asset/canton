// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform

import com.digitalasset.daml.lf.data.Ref
import com.digitalasset.daml.lf.data.Ref.NameTypeConRef
import org.scalatest.exceptions.TestFailedException
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TemplatePartiesFilterSpec extends AnyFlatSpec with Matchers {

  private def templateId(name: String): Ref.NameTypeConRef =
    NameTypeConRef.fromString(s"#Test:Test:$name") match {
      case Left(str) => throw new TestFailedException(s"can't create template id, $str", 0)
      case Right(templateId) => templateId
    }

  private val testData =
    Seq(
      (templateId("test"), Set.empty[String]),
      (templateId("test"), Set("alice")),
      (templateId("test"), Set("bob")),
      (templateId("other"), Set("alice", "bob")),
    )

  "TemplateParties Filter" should "allow all parties" in {
    val testFilter = TemplatePartiesFilter(
      Map.empty,
      None,
    )
    val results = testData.map { case (templateId, parties) =>
      testFilter(templateId = templateId, parties = parties)
    }
    results should contain theSameElementsInOrderAs Seq(false, true, true, true)
  }

  "TemplateParties Filter" should "filter empty parties" in {
    val testFilter = TemplatePartiesFilter(
      Map.empty,
      Some(Set()),
    )
    val results = testData.map { case (templateId, parties) =>
      testFilter(templateId = templateId, parties = parties)
    }
    results should contain theSameElementsInOrderAs Seq(false, false, false, false)
  }

  "TemplateParties Filter" should "filter by parties only" in {
    val testFilter = TemplatePartiesFilter(
      Map.empty,
      Some(Set(Party.assertFromString("alice"))),
    )
    val results = testData.map { case (templateId, parties) =>
      testFilter(templateId = templateId, parties = parties)
    }
    results should contain theSameElementsInOrderAs Seq(false, true, false, true)
  }

  "TemplateParties Filter" should "filter by template parties" in {
    val testFilter = TemplatePartiesFilter(
      Map(
        templateId("test") -> Some(
          Set(Party.assertFromString("alice"), Party.assertFromString("bob"))
        )
      ),
      Some(Set(Party.assertFromString("cecil"))),
    )
    val results = testData.map { case (templateId, parties) =>
      testFilter(templateId = templateId, parties = parties)
    }
    results should contain theSameElementsInOrderAs Seq(false, true, true, false)
  }

  "TemplateParties Filter" should "filter by template only" in {
    val testFilter = TemplatePartiesFilter(
      Map(
        templateId("other") -> Some(
          Set(Party.assertFromString("alice"), Party.assertFromString("bob"))
        )
      ),
      Some(Set()),
    )
    val results = testData.map { case (templateId, parties) =>
      testFilter(templateId = templateId, parties = parties)
    }
    results should contain theSameElementsInOrderAs Seq(false, false, false, true)
  }
}
