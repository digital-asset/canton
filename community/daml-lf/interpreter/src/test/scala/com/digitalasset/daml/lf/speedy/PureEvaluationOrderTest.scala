// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package speedy

import com.digitalasset.canton.logging.SuppressingLogging
import com.digitalasset.daml.lf.language.{Ast, LanguageVersion}
import com.digitalasset.daml.lf.testing.parser.Implicits.SyntaxHelper
import com.digitalasset.daml.lf.testing.parser.ParserParameters
import org.scalatest.Inside
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

class PureEvaluationOrderWithKeyTest_V23 extends PureEvaluationOrderTest(LanguageVersion.v2_3)
class PureEvaluationOrderWithKeyTest_V2Dev extends PureEvaluationOrderTest(LanguageVersion.v2_dev)

abstract class PureEvaluationOrderTest(languageVersion: LanguageVersion)
    extends AnyFreeSpec
    with Matchers
    with Inside
    with SuppressingLogging {

  implicit val parserParameters: ParserParameters[this.type] =
    ParserParameters.default.copy(languageVersion = languageVersion)

  val pkg =
    p"""
      metadata ( 'pure-evaluation-order-test' : '1.0.0' )

      module M {

        val foldl: forall (a: *) (b: *). (a -> b -> a) -> a -> List b -> a = /\ (a: *) (b: *).
          \(f: a -> b -> a) (acc: a) (xs: List b) ->
            case xs of
              Nil -> acc
            | Cons x xs -> M:foldl @a @b f (f acc x) xs;

        val foldr: forall (a: *) (b: *). (b -> a -> a) -> a -> List b -> a = /\ (a: *) (b: *).
          \(f: b -> a -> a) (acc: a) (xs: List b) ->
            case xs of
              Nil -> acc
            |Cons x xs -> f x (M:foldr @a @b f acc xs);

         val f: Text -> Text -> Text =
          \(x: Text) -> TRACE @(Text -> Text) x \(y: Text) -> TRACE @Text y (APPEND_TEXT x y);
      }
     """

  val compiledPackages = SpeedyTestLib.typeAndCompile(pkg)

  private def runCmdFlow(expr: Ast.Expr): (Either[Throwable, SValue], Seq[String]) = {
    val logger = new RecordingMachineLogger(MachineLogger())
    val sexpr = compiledPackages.compiler.unsafeCompile(expr)
    val machine = Speedy.Machine.fromPureSExpr(compiledPackages, sexpr, logger)
    machine.runPure() -> logger.recordedMessages
  }

  "pure evaluation order" - {

    "native foldl match LF implementation" in {

      val (x, refMsgs) =
        runCmdFlow(e"""(M:foldl @Text @Text) M:f "0" (Cons @Text ["1", "2", "3"] (Nil @Text))""")

      val (y, msgs) =
        runCmdFlow(e"""FOLDL @Text @Text M:f "0" (Cons @Text ["1", "2", "3"] (Nil @Text))""")

      x shouldBe Right(SValue.SText("0123"))
      y shouldBe x

      refMsgs shouldBe List("0", "1", "01", "2", "012", "3")
      msgs shouldBe refMsgs
    }

    "native foldr match LF implementation" in {

      val (x, refMsgs) =
        runCmdFlow(e"""M:foldr @Text @Text M:f "0" (Cons @Text ["1", "2", "3"] (Nil @Text))""")

      val (y, msgs) =
        runCmdFlow(e"""FOLDR @Text @Text M:f "0" (Cons @Text ["1", "2", "3"] (Nil @Text))""")

      x shouldBe Right(SValue.SText("1230"))
      y shouldBe x

      refMsgs shouldBe List("3", "0", "2", "30", "1", "230")
      msgs shouldBe refMsgs
    }
  }
}
