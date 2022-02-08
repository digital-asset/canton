// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.util.TraversableUtilTest.CompareOnlyFirst
import org.scalatest.wordspec.AnyWordSpec

/** Tests for [[com.digitalasset.canton.util.TraversableUtil]] */
class TraversableUtilTest extends AnyWordSpec with BaseTest {
  "TraversableUtil" should {

    "find the max elements of a list" in {
      TraversableUtil.maxList(List(2, 1, 2, 2, 0, 1, 0, 2)) shouldEqual List(2, 2, 2, 2)
      TraversableUtil.maxList(List(1, 2, 3)) shouldEqual List(3)
      TraversableUtil.maxList(List(3, 2, 1)) shouldEqual List(3)
      TraversableUtil.maxList[Int](List.empty) shouldEqual List.empty

      val onlyFirsts =
        List((2, 2), (4, 2), (4, 1), (4, 3), (2, 1), (3, 5), (0, 4), (4, 4)).map(x =>
          CompareOnlyFirst(x._1, x._2)
        )
      TraversableUtil.maxList(onlyFirsts) shouldEqual onlyFirsts
        .filter(x => x.first == 4)
        .reverse
    }

    "find the min elements of a list" in {
      TraversableUtil.minList(List(0, 1, 0, 0, 2, 1, 2, 0)) shouldEqual List(0, 0, 0, 0)
      TraversableUtil.minList(List(3, 2, 1)) shouldEqual List(1)
      TraversableUtil.minList(List(1, 2, 3)) shouldEqual List(1)
      TraversableUtil.minList[Int](List.empty) shouldEqual List.empty

      val onlyFirsts =
        List((0, 2), (1, 3), (0, 1), (3, 3), (0, 5)).map(x => CompareOnlyFirst(x._1, x._2))
      TraversableUtil.minList(onlyFirsts) shouldEqual onlyFirsts
        .filter(x => x.first == 0)
        .reverse
    }
  }

}
object TraversableUtilTest {
  case class CompareOnlyFirst(first: Int, second: Int) extends Ordered[CompareOnlyFirst] {
    override def compare(that: CompareOnlyFirst): Int = first.compareTo(that.first)
  }
}
