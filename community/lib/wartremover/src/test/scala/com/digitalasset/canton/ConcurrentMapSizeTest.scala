// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.digitalasset.canton

import org.scalatest.Assertion
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.wartremover.test.WartTestTraverser

import scala.collection.concurrent.TrieMap

class ConcurrentMapSizeTest extends AnyWordSpec with Matchers {

  private def assertErrors(result: WartTestTraverser.Result, expectedErrors: Int): Assertion = {
    result.errors.length shouldBe expectedErrors
    result.errors.foreach {
      _ should include(ConcurrentMapSize.message)
    }
    succeed
  }

  "ConcurrentMapSize" should {

    "detect calls to size on a TrieMap" in {
      val result = WartTestTraverser(ConcurrentMapSize) {
        val x = TrieMap.empty[Int, Int]
        x.size
        ()
      }
      assertErrors(result, 1)
    }

    "detect calls to size on a concurrent Map" in {
      val result = WartTestTraverser(ConcurrentMapSize) {
        val x: scala.collection.concurrent.Map[Int, Int] = TrieMap.empty[Int, Int]
        x.size
        ()
      }
      assertErrors(result, 1)
    }

    "allow calls to size on other classes" in {
      val result = WartTestTraverser(ConcurrentMapSize) {
        val x = Map.empty[Int, Int]
        x.size
        ()
      }
      assertErrors(result, 0)
    }

    "fail to detect calls on upcast concurrent maps" in {
      val result = WartTestTraverser(ConcurrentMapSize) {
        val x: scala.collection.mutable.Map[Int, Int] = TrieMap.empty[Int, Int]
        x.size
        ()
      }
      assertErrors(result, 0) // Limitations on upcasts
    }

    "can detect renamed calls to toByteString on generated protobuf messages (fails on Scala 2)" in {
      val result = WartTestTraverser(ConcurrentMapSize) {
        val x = ??? : scala.collection.concurrent.Map[Int, Int]
        import x.size as foo
        foo
        ()
      }
      if (ScalaVersion.isScala3) assertErrors(result, 1)
      else assertErrors(result, 0) // Limitations on Scala 2
    }
  }
}
