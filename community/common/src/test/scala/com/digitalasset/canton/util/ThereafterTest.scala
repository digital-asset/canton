// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import cats.data.{EitherT, OptionT}
import cats.syntax.either._
import cats.{Applicative, Functor}
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, UnlessShutdown}
import com.digitalasset.canton.{BaseTest, HasExecutionContext}
import org.scalatest.wordspec.AnyWordSpec

import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future, Promise, blocking}
import scala.util.{Failure, Success, Try}

trait ThereafterTest extends AnyWordSpec with BaseTest {

  def thereafter[F[_], Content[_]](
      sut: Thereafter[F, Content],
      fixture: ThereafterTest.Fixture[F, Content],
  )(implicit ec: ExecutionContext): Unit = {

    "run the body once upon completion" in {
      forEvery(fixture.contents) { content =>
        val hasRun = new AtomicBoolean(false)
        val x = fixture.fromContent(content)
        val res = sut.thereafter(x) { _ =>
          fixture.isCompleted(x) shouldBe true
          val swapped = hasRun.compareAndSet(false, true)
          swapped shouldBe true
          ()
        }
        fixture.await(res) shouldBe content
        hasRun.get shouldBe true
      }
    }

    "run the body only afterwards" in {
      val hasRun = new AtomicBoolean(false)
      val promise = Promise[Int]()
      val x = fixture.fromFuture(promise.future)
      val res = sut.thereafter(x) { content =>
        promise.future.isCompleted shouldBe true
        fixture.theContent(content) shouldBe 42
        val swapped = hasRun.compareAndSet(false, true)
        swapped shouldBe true
        ()
      }
      promise.success(42)
      val y = fixture.await(res)
      fixture.theContent(y) shouldBe 42
      hasRun.get shouldBe true
    }

    "run the body even after failure" in {
      val ex = new RuntimeException("EXCEPTION")
      val hasRun = new AtomicBoolean(false)
      val promise = Promise[Unit]()
      val x = fixture.fromFuture(promise.future)
      val res = sut.thereafter(x) { content =>
        fixture.isCompleted(x) shouldBe true
        promise.future.isCompleted shouldBe true
        val swapped = hasRun.compareAndSet(false, true)
        swapped shouldBe true
        Try(fixture.theContent(content)) shouldBe Failure(ex)
        ()
      }
      promise.failure(ex)
      val y = fixture.await(res)
      Try(fixture.theContent(y)) shouldBe Failure(ex)
    }

    "propagate an exception in the body" in {
      val ex = new RuntimeException("BODY FAILURE")
      val x = fixture.fromFuture(Future.successful(()))
      val res = sut.thereafter(x)(_content => throw ex)
      val y = fixture.await(res)
      Try(fixture.theContent(y)) shouldBe Failure(ex)
    }

    "chain failure and body failure via suppression" in {
      val ex1 = new RuntimeException("EXCEPTION")
      val ex2 = new RuntimeException("BODY FAILURE")
      val x = fixture.fromFuture(Future.failed[Unit](ex1))
      val res = sut.thereafter(x)(_content => throw ex2)
      val y = fixture.await(res)
      Try(fixture.theContent(y)) shouldBe Failure(ex1)
      ex1.getSuppressed should contain(ex2)
    }

    "chain body exceptions via suppression" in {
      val ex1 = new RuntimeException("BODY FAILURE 1")
      val ex2 = new RuntimeException("BODY FAILURE 2")
      val x = fixture.fromFuture(Future.successful(()))
      val y = sut.thereafter(x)(_ => throw ex1)
      val res = sut.thereafter(y)(_ => throw ex2)
      val z = fixture.await(res)
      Try(fixture.theContent(z)) shouldBe Failure(ex1)
      ex1.getSuppressed should contain(ex2)
    }

    "rethrow the exception in the body" in {
      val ex = new RuntimeException("FAILURE")
      val x = fixture.fromFuture(Future.failed[Unit](ex))
      val res = sut.thereafter(x)(_ => throw ex)
      val z = fixture.await(res)
      Try(fixture.theContent(z)) shouldBe Failure(ex)
    }
  }
}

object ThereafterTest {

  trait Fixture[F[_], Content[_]] {
    type X
    def fromFuture[A](x: Future[A])(implicit ec: ExecutionContext): F[A]
    def fromContent[A](content: Content[A])(implicit ec: ExecutionContext): F[A]
    def isCompleted[A](x: F[A]): Boolean
    def await[A](x: F[A])(implicit ec: ExecutionContext): Content[A]
    def contents: Seq[Content[X]]
    def theContent[A](content: Content[A]): A
  }

  /** Test that the scala compiler finds the [[Thereafter]] implicits */
  private def implicitResolutionTest(): Unit = {
    import Thereafter.syntax._

    @SuppressWarnings(Array("org.wartremover.warts.Null"))
    implicit val ec: ExecutionContext = null

    EitherT.rightT[Future, Unit]("EitherT Future").thereafter(_ => ())
    EitherT.rightT[FutureUnlessShutdown, Unit]("EitherT FutureUnlessShutdown").thereafter(_ => ())
    OptionT.pure[Future]("OptionT Future").thereafter(_ => ())
    OptionT.pure[FutureUnlessShutdown]("OptionT FutureUnlessShutdown").thereafter(_ => ())

    // Type inference still cannot cope with several Thereafter transformers :-(
    // We explicitly have to summon the instance!
    {
      implicit val thereafter = Thereafter[EitherT[EitherT[Future, Unit, *], Unit, *]].summon
      EitherT.rightT[EitherT[Future, Unit, *], Unit]("EitherT EitherT Future").thereafter(_ => ())
    }
    {
      implicit val thereafter = Thereafter[OptionT[OptionT[Future, *], *]].summon
      OptionT.pure[OptionT[Future, *]]("OptionT OptionT Future").thereafter(_ => ())
    }

  }
}

class FutureThereafterTest extends ThereafterTest with HasExecutionContext {
  "Future" should {
    behave like thereafter(Thereafter[Future].summon, FutureThereafterTest.fixture)
  }
}

object FutureThereafterTest {
  lazy val fixture: ThereafterTest.Fixture[Future, Try] =
    new ThereafterTest.Fixture[Future, Try] {
      override type X = Any
      override def fromFuture[A](x: Future[A])(implicit ec: ExecutionContext): Future[A] = x
      override def fromContent[A](content: Try[A])(implicit ec: ExecutionContext): Future[A] =
        Future.fromTry(content)
      override def isCompleted[A](x: Future[A]): Boolean = x.isCompleted
      override def await[A](x: Future[A])(implicit ec: ExecutionContext): Try[A] = blocking {
        Await.result(x.transform(Success(_)), Duration.Inf)
      }
      override def contents: Seq[Try[X]] = FutureThereafterTest.contents
      override def theContent[A](content: Try[A]): A =
        content.fold(err => throw err, Predef.identity)
    }
  lazy val contents: Seq[Try[Any]] =
    Seq(Success(()), Success(5), Failure(new RuntimeException("failure")))
}

class FutureUnlessShutdownThereafterTest extends ThereafterTest with HasExecutionContext {
  "FutureUnlessShutdown" should {
    behave like thereafter(
      Thereafter[FutureUnlessShutdown].summon,
      FutureUnlessShutdownThereafterTest.fixture,
    )
  }
}

object FutureUnlessShutdownThereafterTest {
  lazy val fixture
      : ThereafterTest.Fixture[FutureUnlessShutdown, Lambda[a => Try[UnlessShutdown[a]]]] =
    new ThereafterTest.Fixture[FutureUnlessShutdown, Lambda[a => Try[UnlessShutdown[a]]]] {
      override type X = Any
      override def fromFuture[A](x: Future[A])(implicit
          ec: ExecutionContext
      ): FutureUnlessShutdown[A] =
        FutureUnlessShutdown.outcomeF(x)
      override def fromContent[A](content: Try[UnlessShutdown[A]])(implicit
          ec: ExecutionContext
      ): FutureUnlessShutdown[A] = FutureUnlessShutdown(Future.fromTry(content))
      override def isCompleted[A](x: FutureUnlessShutdown[A]): Boolean = x.unwrap.isCompleted
      override def await[A](
          x: FutureUnlessShutdown[A]
      )(implicit ec: ExecutionContext): Try[UnlessShutdown[A]] =
        blocking {
          Await.result(x.unwrap.transform(Success(_)), Duration.Inf)
        }
      override def contents: Seq[Try[UnlessShutdown[X]]] =
        Success(UnlessShutdown.AbortedDueToShutdown) +:
          FutureThereafterTest.contents.map(_.map(UnlessShutdown.Outcome(_)))
      override def theContent[A](content: Try[UnlessShutdown[A]]): A =
        content.fold(err => throw err, _.onShutdown(throw new NoSuchElementException("No outcome")))
    }
}

class EitherTThereafterTest extends ThereafterTest with HasExecutionContext {
  "EitherT" when {
    "applied to Future" should {
      behave like thereafter(
        Thereafter[EitherT[Future, Unit, *]].summon,
        EitherTThereafterTest.fixture(FutureThereafterTest.fixture, Seq(())),
      )
    }

    "applied to FutureUnlessShutdown" should {
      implicit val appTryUnlessShutdown = Applicative[Try].compose[UnlessShutdown]

      behave like thereafter(
        Thereafter[EitherT[FutureUnlessShutdown, String, *]].summon,
        EitherTThereafterTest.fixture(
          FutureUnlessShutdownThereafterTest.fixture,
          Seq("left", "another left"),
        ),
      )
    }
  }
}

object EitherTThereafterTest {
  def fixture[F[_], Content[_], E](base: ThereafterTest.Fixture[F, Content], lefts: Seq[E])(implicit
      M: Functor[F],
      C: Applicative[Content],
  ): ThereafterTest.Fixture[EitherT[F, E, *], Lambda[a => Content[Either[E, a]]]] =
    new ThereafterTest.Fixture[EitherT[F, E, *], Lambda[a => Content[Either[E, a]]]] {
      override type X = Any
      override def fromFuture[A](x: Future[A])(implicit ec: ExecutionContext): EitherT[F, E, A] =
        EitherT(M.map(base.fromFuture(x))(Right(_)))
      override def fromContent[A](content: Content[Either[E, A]])(implicit
          ec: ExecutionContext
      ): EitherT[F, E, A] =
        EitherT(base.fromContent(content))
      override def isCompleted[A](x: EitherT[F, E, A]): Boolean = base.isCompleted(x.value)
      override def await[A](x: EitherT[F, E, A])(implicit
          ec: ExecutionContext
      ): Content[Either[E, A]] =
        base.await(x.value)
      override def contents: Seq[Content[Either[E, X]]] =
        lefts.map(l => C.pure(Either.left[E, X](l))) ++ base.contents.map(
          C.map(_)(Either.right[E, X](_))
        )
      override def theContent[A](content: Content[Either[E, A]]): A =
        base
          .theContent(content)
          .valueOr(l => throw new NoSuchElementException(s"Left($l) is not a Right"))
    }
}

class OptionTThereafterTest extends ThereafterTest with HasExecutionContext {
  "OptionT" when {
    "applied to Future" should {
      behave like thereafter(
        Thereafter[OptionT[Future, *]].summon,
        OptionTThereafterTest.fixture(FutureThereafterTest.fixture),
      )
    }

    "applied to FutureUnlessShutdown" should {
      implicit val appTryUnlessShutdown = Applicative[Try].compose[UnlessShutdown]

      behave like thereafter(
        Thereafter[OptionT[FutureUnlessShutdown, *]].summon,
        OptionTThereafterTest.fixture(FutureUnlessShutdownThereafterTest.fixture),
      )
    }
  }
}

object OptionTThereafterTest {
  def fixture[F[_], Content[_]](
      base: ThereafterTest.Fixture[F, Content]
  )(implicit M: Functor[F], C: Applicative[Content]) =
    new ThereafterTest.Fixture[OptionT[F, *], Lambda[a => Content[Option[a]]]] {
      override type X = Any
      override def fromFuture[A](x: Future[A])(implicit ec: ExecutionContext): OptionT[F, A] =
        OptionT(M.map(base.fromFuture(x))(Option(_)))
      override def fromContent[A](content: Content[Option[A]])(implicit
          ec: ExecutionContext
      ): OptionT[F, A] =
        OptionT(base.fromContent(content))
      override def isCompleted[A](x: OptionT[F, A]): Boolean = base.isCompleted(x.value)
      override def await[A](x: OptionT[F, A])(implicit ec: ExecutionContext): Content[Option[A]] =
        base.await(x.value)
      override def contents: Seq[Content[Option[X]]] =
        base.contents.map(C.map(_)(Option[X](_))) :+ C.pure(None)
      override def theContent[A](content: Content[Option[A]]): A =
        base
          .theContent(content)
          .getOrElse(throw new NoSuchElementException("The option should not be empty"))
    }
}
