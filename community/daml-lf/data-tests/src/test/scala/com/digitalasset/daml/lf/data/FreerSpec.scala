// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.data

import cats.~>
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.collection.mutable.ListBuffer

class FreerSpec extends AnyWordSpec with Matchers {

  import FreerSpec.*

  "Freer.start" should {

    "return a Pure step for a pure program" in {
      val (res, log) = interp(pure(42))
      res shouldBe Right(42)
      log shouldBe empty
    }

    "return an Error step for a failed program" in {
      val (res, log) = interp(raise[Int]("boom"))
      res shouldBe Left("boom")
      log shouldBe empty
    }

    "expose the pending effect for an impure program" in {
      ask("x").start match {
        case im: Freer.Step.Impure[Op, String, ?, Int] => im.fx shouldBe Ask("x")
        case other => fail(s"expected an Impure step, got $other")
      }
    }
  }

  "Freer.map" should {

    "transform the result of a pure program" in {
      interp(pure(2).map(_ + 1))._1 shouldBe Right(3)
    }

    "defer over a pending effect" in {
      val (res, log) = interp(ask("x").map(_ + 1), Map("x" -> 41))
      res shouldBe Right(42)
      log shouldBe List("ask:x")
    }
  }

  "Freer.flatMap" should {

    "thread effects and continuations in order" in {
      val program: F[Int] =
        for {
          _ <- emit("a")
          n <- ask("x")
          _ <- emit(s"got-$n")
        } yield n * 2

      val (res, log) = interp(program, Map("x" -> 5))
      res shouldBe Right(10)
      log shouldBe List("a", "ask:x", "got-5")
    }

    "short-circuit on an error and discard the pending continuation" in {
      val program =
        emit("a").flatMap(_ => raise[Int]("boom")).flatMap(_ => emit("b").map(_ => 1))

      val (res, log) = interp(program)
      res shouldBe Left("boom")
      log shouldBe List("a")
    }

    "short-circuit on an error produced after an effect" in {
      val program =
        ask("x")
          .flatMap(n => raise[Int](s"bad-$n"))
          .flatMap(_ => emit("after").map(_ => 0))

      val (res, log) = interp(program, Map("x" -> 9))
      res shouldBe Left("bad-9")
      log shouldBe List("ask:x")
    }
  }

  "the monad laws" should {

    val f = (n: Int) => emit(s"f-$n").map(_ => n + 1)
    val g = (n: Int) => emit(s"g-$n").map(_ => n * 2)

    "hold left identity: pure(a).flatMap(f) == f(a)" in {
      interp(pure(3).flatMap(f)) shouldBe interp(f(3))
    }

    "hold right identity: m.flatMap(pure) == m" in {
      val m = ask("x")
      interp(m.flatMap(pure), Map("x" -> 7)) shouldBe interp(m, Map("x" -> 7))
    }

    "hold associativity: m.flatMap(f).flatMap(g) == m.flatMap(x => f(x).flatMap(g))" in {
      val m = ask("x")
      val lhs = m.flatMap(f).flatMap(g)
      val rhs = m.flatMap((x: Int) => f(x).flatMap(g))
      interp(lhs, Map("x" -> 4)) shouldBe interp(rhs, Map("x" -> 4))
    }
  }

  "Freer.start (stack safety)" should {

    "reduce a long left-nested chain of binds without overflowing the stack" in {
      val n = 100000
      val program: F[Long] =
        (1 to n).foldLeft(pure(0L)) { (acc, i) =>
          acc.flatMap(s => pure(s + i))
        }

      val (res, log) = interp(program)
      res shouldBe Right(n.toLong * (n + 1) / 2)
      log shouldBe empty
    }

    "reduce deep bind chains on both sides of an effect suspension" in {
      val n = 50000
      def chain(seed: F[Long], from: Int, to: Int): F[Long] =
        (from to to).foldLeft(seed)((acc, i) => acc.flatMap(s => pure(s + i)))

      // deep chain, then an effect, then another deep chain resumed from the frozen stack
      val program: F[Long] =
        chain(pure(0L), 1, n)
          .flatMap(s => ask("x").map(x => s + x))
          .flatMap(s => chain(pure(s), 1, n))

      val (res, log) = interp(program, Map("x" -> 7))
      val triangular = n.toLong * (n + 1) / 2
      res shouldBe Right(2 * triangular + 7)
      log shouldBe List("ask:x")
    }
  }

  "Freer.void" should {

    "run the effects but discard the result" in {
      val (res, log) = interp(ask("x").void, Map("x" -> 5))
      res shouldBe Right(())
      log shouldBe List("ask:x")
    }

    "discard the result of a pure program" in {
      interp(pure(42).void)._1 shouldBe Right(())
    }
  }

  "the cats Monad instance" should {
    import cats.syntax.all.*

    "thread effects left-to-right via traverse" in {
      val program: F[List[Int]] =
        List(1, 2, 3).traverse(i => emit(s"e-$i").map(_ => i * 10))
      val (res, log) = interp(program)
      res shouldBe Right(List(10, 20, 30))
      log shouldBe List("e-1", "e-2", "e-3")
    }

    "short-circuit a traverse on the first error" in {
      val program: F[List[Int]] =
        List(1, 2, 3).traverse(i =>
          if (i == 2) raise[Int](s"boom-$i") else emit(s"e-$i").map(_ => i)
        )
      val (res, log) = interp(program)
      res shouldBe Left("boom-2")
      log shouldBe List("e-1")
    }

    "stay stack-safe for a large traverse" in {
      val n = 50000
      val program: F[Int] =
        (1 to n).toList.traverse(_ => pure(1)).map(_.sum)
      interp(program)._1 shouldBe Right(n)
    }
  }

  "Freer.consume" should {

    "answer effects and thread results" in {
      val (res, log) = interp(
        for {
          _ <- emit("a")
          n <- ask("x")
          _ <- emit(s"got-$n")
        } yield n * 2,
        Map("x" -> 5),
      )
      res shouldBe Right(10)
      log shouldBe List("a", "ask:x", "got-5")
    }

    "short-circuit when the handler returns Left" in {
      val log = ListBuffer.empty[String]
      val handler = new (Op ~> Either[String, *]) {
        def apply[X](op: Op[X]): Either[String, X] =
          op match {
            case Emit(s) =>
              log += s
              Right(())
            case Ask(key) => Left(s"cannot-answer-$key")
          }
      }

      val program = emit("a").flatMap(_ => ask("x")).flatMap(n => emit(s"got-$n").map(_ => n))
      program.consume(handler) shouldBe Left("cannot-answer-x")
      log.toList shouldBe List("a")
    }

    "short-circuit on a program error without running later effects" in {
      val program = emit("a").flatMap(_ => raise[Int]("boom")).flatMap(_ => emit("b").map(_ => 1))
      val (res, log) = interp(program)
      res shouldBe Left("boom")
      log shouldBe List("a")
    }

    "stay stack-safe for a long effect chain" in {
      val n = 100000
      val program: F[Int] =
        (1 to n).foldLeft(pure(0))((acc, i) => acc.flatMap(s => emit(i.toString).map(_ => s + 1)))
      interp(program)._1 shouldBe Right(n)
    }
  }
}

object FreerSpec {

  sealed trait Op[+A]
  final case class Emit(s: String) extends Op[Unit]
  final case class Ask(key: String) extends Op[Int]

  private type F[A] = Freer[Op, String, A]

  private def pure[A](a: A): F[A] = Freer.pure[Op, String, A](a)

  private def raise[A](e: String): F[A] = Freer.raise[Op, String](e)

  private def emit(s: String): F[Unit] = Freer.lift[Op, String, Unit](Emit(s))

  private def ask(key: String): F[Int] = Freer.lift[Op, String, Int](Ask(key))

  /** Drives a program to completion against a fixed set of answers, returning the outcome together
    * with the ordered log of effects that were run.
    */
  private def interp[A](
      program: F[A],
      answers: Map[String, Int] = Map.empty,
  ): (Either[String, A], List[String]) = {
    val log = ListBuffer.empty[String]
    val handler = new (Op ~> Either[String, *]) {
      def apply[X](op: Op[X]): Either[String, X] =
        op match {
          case Emit(s) =>
            log += s
            Right(())
          case Ask(key) =>
            log += s"ask:$key"
            Right(answers(key))
        }
    }
    (program.consume(handler), log.toList)
  }
}
