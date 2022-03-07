// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates
//
// Proprietary code. All rights reserved.

package com.digitalasset.canton

import com.digitalasset.canton.DiscardedFutureTest.TraitWithFuture
import org.scalatest.Assertion
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.wartremover.test.WartTestTraverser

import scala.concurrent.Future

class DiscardedFutureTest extends AnyWordSpec with Matchers with org.mockito.MockitoSugar {

  def assertIsError(result: WartTestTraverser.Result): Assertion = {
    result.errors.length should be >= 1
    result.errors.foreach { _ should include(DiscardedFuture.message) }
    succeed
  }

  "DiscardedFuture" should {
    "detect statements in blocks that discard a future" in {
      val result = WartTestTraverser(DiscardedFuture) {
        Future.successful(())
        ()
      }
      assertIsError(result)
    }

    "detect statements in class defs that discard a future" in {
      val result = WartTestTraverser(DiscardedFuture) {
        class Foo(x: Int) {
          Future.successful(x)
        }
        new Foo(5)
      }
      assertIsError(result)
    }

    "allow explicit discard calls" in {
      val result = WartTestTraverser(DiscardedFuture) {
        val _ = Future.successful(())
      }
      result.errors shouldBe List.empty
    }

    "allow Mockito verify calls" in {
      val result = WartTestTraverser(DiscardedFuture) {
        val mocked = mock[TraitWithFuture]
        verify(mocked).returnsFuture
        verify(mocked).returnsFutureNoArgs()
        verify(mocked).returnsFutureOneArg(0)
        verify(mocked).returnsFutureTwoArgs(1)("")
        verify(mocked).returnsFutureThreeArgs(2)("string")(new Object)
      }
      result.errors shouldBe List.empty
    }
  }
}

object DiscardedFutureTest {
  trait TraitWithFuture {
    def returnsFuture: Future[Unit]
    def returnsFutureNoArgs(): Future[Unit]
    def returnsFutureOneArg(x: Int): Future[Int]
    def returnsFutureTwoArgs(x: Int)(y: String): Future[Int]
    def returnsFutureThreeArgs(x: Int)(y: String)(z: Any): Future[Int]
  }
}
