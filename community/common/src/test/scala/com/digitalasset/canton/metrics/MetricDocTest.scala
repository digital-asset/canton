// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.metrics

import com.codahale.metrics
import com.daml.metrics.MetricHandle.Timer
import com.digitalasset.canton.BaseTest
import org.scalatest.wordspec.AnyWordSpec

class MetricDocTest extends AnyWordSpec with BaseTest {

  lazy val tm = new metrics.Timer()
  class DocVar {
    @MetricDoc.Tag("varred summary", "varred desc")
    val varred = Timer.NoOpTimer("varred")
  }

  class DocItem {
    @MetricDoc.Tag("top summary", "top desc")
    val top = Timer.NoOpTimer("top")
    val utop = Timer.NoOpTimer("utop")
    object nested {
      @MetricDoc.Tag("nested.n1 summary", "n1 desc")
      val n1 = Timer.NoOpTimer("nested.n1")
      val u1 = Timer.NoOpTimer("nested.u1")
      object nested2 {
        @MetricDoc.Tag("nested.n2 summary", "n2 desc")
        val n2 = Timer.NoOpTimer("nested.n2")
        val u2 = Timer.NoOpTimer("nested.u2")
      }
    }
    val other = new DocVar()

  }

  "embedded docs" should {
    "find nested items" in {

      val itm = new DocItem()
      val items = MetricDoc.getItems(itm)

      val expected =
        Seq("varred", "nested.n1", "nested.n2", "top").map(nm => (nm, s"${nm} summary")).toSet
      items.map(x => (x.name, x.tag.summary)).toSet shouldBe expected

    }
  }

}
