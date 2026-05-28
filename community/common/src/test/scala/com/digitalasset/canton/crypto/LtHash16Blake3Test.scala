// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import com.digitalasset.canton.BaseTest
import org.scalatest.wordspec.AnyWordSpec

class LtHash16Blake3Test extends AnyWordSpec with BaseTest {

  "LtHash16Blake3" should {
    "be commutative" in {
      val h1 = LtHash16Blake3.empty
      val h2 = LtHash16Blake3.empty
      h1.add("a".getBytes)
      h1.add("b".getBytes)
      h2.add("b".getBytes)
      h2.add("a".getBytes)
      h1.getByteString shouldBe h2.getByteString
    }

    "be non-trivial" in {
      val h = LtHash16Blake3.empty
      val empty = h.getByteString
      h.add("a".getBytes)
      val a = h.getByteString
      empty should not equal a
      h.add("b".getBytes)
      val b = h.getByteString
      a should not equal b
      empty should not equal b
    }

    "removal should be the inverse of addition" in {
      val h = LtHash16Blake3.empty
      val emptyBytes = h.getByteString
      val abc = "abc".getBytes()
      h.add(abc)
      val abcBytes = h.getByteString
      val defg = "defg".getBytes()
      h.add(defg)
      h.remove(defg)
      h.getByteString shouldBe abcBytes
      h.remove(abc)
      h.getByteString shouldBe emptyBytes
    }

    "empty is neutral for union and removal" in {
      val h1 = LtHash16Blake3.empty
      h1.add("abc".getBytes())
      val abcDigest = h1.getByteString
      val h2 = LtHash16Blake3.empty
      h1.union(h2)
      h1.getByteString shouldBe abcDigest

      h1.removeAll(h2)
      h1.getByteString shouldBe abcDigest
    }

    "union should be the same as repeated addition" in {
      val h1 = LtHash16Blake3.empty
      val h2 = LtHash16Blake3.empty
      val h3 = LtHash16Blake3.empty
      val abc = "abc".getBytes()
      val `def` = "def".getBytes()
      val ghi = "ghi".getBytes()
      h1.add(abc)
      h1.add(`def`)
      h1.add(ghi)

      h2.add(abc)
      h3.add(`def`)
      h3.add(ghi)
      h2.union(h3)

      h1.getByteString shouldBe h2.getByteString
    }

    "removalAll should be the inverse of union" in {
      val h1 = LtHash16Blake3.empty
      val h3 = LtHash16Blake3.empty
      val abc = "abc".getBytes()
      val `def` = "def".getBytes()
      val ghi = "ghi".getBytes()
      h1.add(abc)
      val abcDigest = h1.getByteString
      h1.add(`def`)
      h1.add(ghi)

      h3.add(`def`)
      h3.add(ghi)
      h1.removeAll(h3)

      h1.getByteString shouldBe abcDigest
    }
  }
}
