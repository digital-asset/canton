// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates.
// Proprietary code. All rights reserved.

package com.digitalasset.canton

import cats.Applicative
import cats.data.{EitherT, OptionT}
import cats.syntax.either.*
import com.digitalasset.canton.DiscardedFutureTest.{
  Transformer0,
  Transformer1,
  WannabeFuture,
  assertErrors,
}
import org.scalatest.Assertion
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.wartremover.test.WartTestTraverser

import scala.concurrent.{ExecutionContext, Future}

class DiscardedFutureTest extends AnyWordSpec with Matchers with DiscardedFutureTestWithMockito {

  implicit val ec: ExecutionContext = ExecutionContext.global

  "DiscardedFuture" should {
    "detect statements in blocks that discard a future" in {
      val result = WartTestTraverser(DiscardedFuture) {
        Future.unit
        ()
      }
      assertErrors(result, 1)
    }

    "detect statements in class defs that discard a future" in {
      val result = WartTestTraverser(DiscardedFuture) {
        class Foo(x: Int) {
          Future.successful(x)
        }
        new Foo(5)
      }
      assertErrors(result, 1)
    }

    "allow explicit discard calls" in {
      val result = WartTestTraverser(DiscardedFuture) {
        val _ = Future.unit
      }
      assertErrors(result, 0)
    }

    "detects discarded futures wrapped in an EitherT" in {
      val result = WartTestTraverser(DiscardedFuture) {
        EitherT(Future.successful(Either.unit))
        ()
      }
      assertErrors(result, 1)
    }

    "detects discarded futures wrapped in an OptionT" in {
      val result = WartTestTraverser(DiscardedFuture) {
        OptionT(Future.successful(Option(())))
        ()
      }
      assertErrors(result, 1)
    }

    "detects discarded futures that are deeply wrapped" in {
      val result = WartTestTraverser(DiscardedFuture) {
        OptionT(EitherT(Future.successful(Either.right(Option(())))))
        ()
      }
      assertErrors(result, 1)
    }

    "detects annotated future-like classes" in {
      val result = WartTestTraverser(DiscardedFuture) {
        new WannabeFuture[Int]()
        ()
      }
      assertErrors(result, 1)
    }

    "detects annotated transformers of future-like computations" in {
      val result = WartTestTraverser(DiscardedFuture) {
        new Transformer0[Future, Int]
        new Transformer1[Int, Future]
        new Transformer0[Transformer1[*, OptionT[WannabeFuture, *]], Int]
        ()
      }
      assertErrors(result, 3)
    }

    "allow annotated transformers of future-unlike computations" in {
      val result = WartTestTraverser(DiscardedFuture) {
        new Transformer0[List, Int]
        new Transformer0[Transformer1[*, OptionT[List, *]], Int]
        ()
      }
      result.errors shouldBe empty
    }

    "detects nested annotated future-like classes" in {
      val result = WartTestTraverser(DiscardedFuture) {
        EitherT(new WannabeFuture[Either[String, Int]])
        ()
      }
      assertErrors(result, 1)
    }

    "detect discarded futures inside foreach" in {
      val result = WartTestTraverser(DiscardedFuture) {
        Seq.empty[Int].foreach(i => Future.successful(i))
        List.empty[Int].foreach(i => EitherT(Future.successful(Either.right(i))))
        Iterable(1).foreach(_ => OptionT(Future.successful(Option.empty[Int])))
        Iterator(1).foreach(_ => new WannabeFuture())
        ()
      }
      assertErrors(result, 4)
    }

    "detect discarded futures inside tapEach" in {
      val result = WartTestTraverser(DiscardedFuture) {
        Seq.empty[Int].tapEach(i => Future.successful(i))
        List.empty[Int].tapEach(i => EitherT(Future.successful(Either.right(i))))
        Iterable(1).tapEach(_ => OptionT(Future.successful(Option.empty[Int])))
        Iterator(1).tapEach(_ => new WannabeFuture())
        ()
      }
      assertErrors(result, 4)
    }

    "detect wildcard aliases of futures in for-comprehensions" in {
      val result = WartTestTraverser(DiscardedFuture) {
        for {
          a <- Future.successful(1)
          _ = Future.unit
          b <- Future.successful(a)
        } yield b
      }
      assertErrors(result, 1)
    }

    "detect nested futures in a for comprehension" in {
      val result = WartTestTraverser(DiscardedFuture) {
        for {
          _ <- Future.successful(Future.unit)
        } yield ()
      }
      assertErrors(result, 1)
    }

    "detect nested futures followed by a guard in a for comprehension" in {
      val result = WartTestTraverser(DiscardedFuture) {
        for {
          _ <- Future.successful(Future.unit)
          if true
        } yield ()
      }
      assertErrors(result, 1)
    }

    "detect nested futures discarded by a wildcard lambda" in {
      val result = WartTestTraverser(DiscardedFuture) {
        Future.successful(Future.unit).map(_ => ())
      }
      assertErrors(result, 1)
    }

    "allow nested futures used via a placeholder" in {
      val result = WartTestTraverser(DiscardedFuture) {
        Future.successful(Future.unit).map(_.isCompleted)
      }
      assertErrors(result, 0)
    }

    "allow nested futures used in a match scrutinee" in {
      val result = WartTestTraverser(DiscardedFuture) {
        Future
          .successful(Future.unit)
          .map(inner => inner.isCompleted match { case _ => () })
      }
      assertErrors(result, 0)
    }

    "detect a wildcard alias of a future as the last step of a for-comprehension" in {
      val result = WartTestTraverser(DiscardedFuture) {
        for {
          a <- Future.successful(1)
          _ = EitherT(Future.successful(Either.unit))
        } yield a
      }
      assertErrors(result, 1)
    }

    "detect wildcard aliases of futures in for-comprehensions over other monads" in {
      val result = WartTestTraverser(DiscardedFuture) {
        for {
          a <- Option(1)
          _ = Future.unit
        } yield a
      }
      assertErrors(result, 1)
    }

    "detect several wildcard aliases of futures in one for-comprehension step" in {
      val result = WartTestTraverser(DiscardedFuture) {
        for {
          a <- Option(1)
          _ = Future.unit
          _ = Future.successful(a)
        } yield a
      }
      assertErrors(result, 2)
    }

    "detect a wildcard alias of a future between named aliases" in {
      val result = WartTestTraverser(DiscardedFuture) {
        for {
          a <- Option(1)
          x = 1
          _ = Future.successful(a)
          z = 3
          b <- Option(x + z)
        } yield b
      }
      assertErrors(result, 1)
    }

    // The compiler splits more than 21 consecutive aliases into several blocks with nested tuples.
    // Each block ends in its own tuple, so the wart still sees every alias.
    "detect wildcard aliases of futures in more than 22 consecutive aliases" in {
      val result = WartTestTraverser(DiscardedFuture) {
        for {
          a <- Option(1)
          _ = Future.unit
          b2 = 2
          b3 = 3
          b4 = 4
          b5 = 5
          b6 = 6
          b7 = 7
          b8 = 8
          b9 = 9
          b10 = 10
          b11 = 11
          b12 = 12
          b13 = 13
          b14 = 14
          b15 = 15
          b16 = 16
          b17 = 17
          b18 = 18
          b19 = 19
          b20 = 20
          b21 = 21
          b22 = 22
          _ = Future.unit
          b24 = 24
        } yield a + b2 + b3 + b4 + b5 + b6 + b7 + b8 + b9 + b10 + b11 + b12 + b13 + b14 + b15 +
          b16 + b17 + b18 + b19 + b20 + b21 + b22 + b24
      }
      assertErrors(result, 2)
    }

    "detect wildcard aliases of annotated future-like classes in for-comprehensions" in {
      val result = WartTestTraverser(DiscardedFuture) {
        for {
          a <- Option(1)
          _ = new WannabeFuture[Int]()
        } yield a
      }
      assertErrors(result, 1)
    }

    "allow named aliases of futures in for-comprehensions" in {
      val result = WartTestTraverser(DiscardedFuture) {
        for {
          a <- Future.successful(1)
          f = Future.unit
          _ <- f
        } yield a
      }
      assertErrors(result, 0)
    }

    "allow wildcard aliases of non-futures in for-comprehensions" in {
      val result = WartTestTraverser(DiscardedFuture) {
        for {
          a <- Future.successful(1)
          _ = ()
          _ = a
        } yield a
      }
      assertErrors(result, 0)
    }

    "allow hand-written tuples of futures" in {
      val result = WartTestTraverser(DiscardedFuture) {
        val x = Future.unit
        (1, x)
      }
      assertErrors(result, 0)
    }

    "allow futures passed as named arguments to functions returning tuples" in {
      def pair(a: Int, f: Future[Unit]): (Int, Future[Unit]) = (a, f)
      val result = WartTestTraverser(DiscardedFuture) {
        pair(f = Future.unit, a = 1)
      }
      assertErrors(result, 0)
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
    def returnsFutureTypeArgs[A](x: A): Future[Unit]
  }

  @DoNotDiscardLikeFuture class WannabeFuture[A]

  @FutureTransformer(0) class Transformer0[F[_], A]
  object Transformer0 {
    implicit def applicativeTransformer0[F[_]](implicit
        F: Applicative[F]
    ): Applicative[Transformer0[F, *]] = ???
  }

  @FutureTransformer(1) class Transformer1[A, F[_]]
  object Transformer1 {
    implicit def applicativeTransformer1[F[_]](implicit
        F: Applicative[F]
    ): Applicative[Transformer1[*, F]] = ???
  }

  def assertErrors(result: WartTestTraverser.Result, expectedErrors: Int): Assertion = {
    import Matchers.*
    result.errors.length shouldBe expectedErrors
    result.errors.foreach(_ should include(DiscardedFuture.message))
    succeed
  }
}
