// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package speedy

import com.digitalasset.canton.logging.SuppressingLogging
import com.digitalasset.daml.lf.data.{Bytes, ImmArray, Ref}
import com.digitalasset.daml.lf.interpretation.{Error => IE}
import com.digitalasset.daml.lf.language.LanguageVersion
import com.digitalasset.daml.lf.speedy.SExpr.SEApp
import com.digitalasset.daml.lf.speedy.SValue.{SParty, SText}
import com.digitalasset.daml.lf.testing.parser.Implicits.SyntaxHelper
import com.digitalasset.daml.lf.testing.parser.ParserParameters
import com.digitalasset.daml.lf.transaction.{ExternalCallResult, Node}
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.collection.immutable.ArraySeq

class ExternalCallTest extends AnyWordSpec with Matchers with Inside with SuppressingLogging {

  private val alice = Ref.Party.assertFromString("Alice")
  private val transactionSeed = crypto.Hash.hashPrivateKey("ExternalCallTest.scala")

  implicit private val parserParameters: ParserParameters[this.type] =
    ParserParameters(
      defaultPackageId = Ref.PackageId.assertFromString("-pkg-"),
      languageVersion = LanguageVersion.v2_dev,
    )

  private val pkgs = SpeedyTestLib.typeAndCompile(p"""
    metadata ( '-pkg-' : '1.0.0' )

    module M {
      record @serializable T = { party: Party };
      record @serializable Boom = {};
      exception Boom = { message \(e: M:Boom) -> "Boom" };

      template (this: T) = {
        precondition True;
        signatories Cons @Party [M:T {party} this] (Nil @Party);
        observers Nil @Party;

        choice Call (self) (arg: Unit) : Text,
          controllers Cons @Party [M:T {party} this] (Nil @Party)
          to EXTERNAL_CALL "ext" "fun" "0a0b" "c0ff";

        choice CallInTry (self) (arg: Unit) : Text,
          controllers Cons @Party [M:T {party} this] (Nil @Party)
          to try @Text
            (EXTERNAL_CALL "ext" "fun" "0a0b" "c0ff")
          catch e -> Some @(Update Text) (upure @Text "fallback");

        choice CallInTryRollback (self) (arg: Unit) : Text,
          controllers Cons @Party [M:T {party} this] (Nil @Party)
          to try @Text
            ubind result: Text <- EXTERNAL_CALL "ext" "fun" "0a0b" "c0ff"
            in throw @(Update Text) @M:Boom (M:Boom {})
          catch e -> Some @(Update Text) (upure @Text "fallback");
      };

      val run : Party -> Update Text = \(party: Party) ->
        ubind cid: ContractId M:T <- create @M:T M:T { party = party }
        in exercise @M:T Call cid ();

      val runInTry : Party -> Update Text = \(party: Party) ->
        ubind cid: ContractId M:T <- create @M:T M:T { party = party }
        in exercise @M:T CallInTry cid ();

      val runInTryRollback : Party -> Update Text = \(party: Party) ->
        ubind cid: ContractId M:T <- create @M:T M:T { party = party }
        in exercise @M:T CallInTryRollback cid ();

      val runAtRoot : Update Text =
        EXTERNAL_CALL "ext" "fun" "0a0b" "c0ff";
    }
  """)

  "SBExternalCall" should {
    "resume through NeedExternalCall and record the result on the exercise node" in {
      val machine = Speedy.Machine.fromUpdateSExpr(
        pkgs,
        transactionSeed,
        SEApp(pkgs.compiler.unsafeCompile(e"M:run"), ArraySeq(SParty(alice))),
        Set(alice),
        MachineLogger(),
      )

      var questions = 0
      val result = SpeedyTestLib.runTxQ[Question.Update](
        {
          case Question.Update.NeedExternalCall(extensionId, functionId, configHash, input, callback) =>
            questions += 1
            extensionId shouldBe "ext"
            functionId shouldBe "fun"
            configHash shouldBe "0a0b"
            input shouldBe "c0ff"
            callback(Right("beef"))
          case other =>
            fail(s"Unexpected question: $other")
        },
        machine,
      )

      result shouldBe Right(SText("beef"))
      questions shouldBe 1

      inside(machine.finish) { case Right(commit) =>
        val exerciseNodes = commit.tx.nodes.collect { case (_, exercise: Node.Exercise) => exercise }
        exerciseNodes should have size 1
        exerciseNodes.head.externalCallResults shouldBe ImmArray(
          ExternalCallResult(
            extensionId = "ext",
            functionId = "fun",
            config = Bytes.assertFromString("0a0b"),
            input = Bytes.assertFromString("c0ff"),
            output = Bytes.assertFromString("beef"),
          )
        )
      }
    }

    "record the result on the enclosing exercise node when called inside try/catch" in {
      val machine = Speedy.Machine.fromUpdateSExpr(
        pkgs,
        transactionSeed,
        SEApp(pkgs.compiler.unsafeCompile(e"M:runInTry"), ArraySeq(SParty(alice))),
        Set(alice),
        MachineLogger(),
      )

      var questions = 0
      val result = SpeedyTestLib.runTxQ[Question.Update](
        {
          case Question.Update.NeedExternalCall(extensionId, functionId, configHash, input, callback) =>
            questions += 1
            extensionId shouldBe "ext"
            functionId shouldBe "fun"
            configHash shouldBe "0a0b"
            input shouldBe "c0ff"
            callback(Right("beef"))
          case other =>
            fail(s"Unexpected question: $other")
        },
        machine,
      )

      result shouldBe Right(SText("beef"))
      questions shouldBe 1

      inside(machine.finish) { case Right(commit) =>
        val exerciseNodes = commit.tx.nodes.collect { case (_, exercise: Node.Exercise) => exercise }
        exerciseNodes should have size 1
        exerciseNodes.head.externalCallResults shouldBe ImmArray(
          ExternalCallResult(
            extensionId = "ext",
            functionId = "fun",
            config = Bytes.assertFromString("0a0b"),
            input = Bytes.assertFromString("c0ff"),
            output = Bytes.assertFromString("beef"),
          )
        )
      }
    }

    "record the result on the enclosing exercise node when try/catch rolls back" in {
      val machine = Speedy.Machine.fromUpdateSExpr(
        pkgs,
        transactionSeed,
        SEApp(pkgs.compiler.unsafeCompile(e"M:runInTryRollback"), ArraySeq(SParty(alice))),
        Set(alice),
        MachineLogger(),
      )

      var questions = 0
      val result = SpeedyTestLib.runTxQ[Question.Update](
        {
          case Question.Update.NeedExternalCall(extensionId, functionId, configHash, input, callback) =>
            questions += 1
            extensionId shouldBe "ext"
            functionId shouldBe "fun"
            configHash shouldBe "0a0b"
            input shouldBe "c0ff"
            callback(Right("beef"))
          case other =>
            fail(s"Unexpected question: $other")
        },
        machine,
      )

      result shouldBe Right(SText("fallback"))
      questions shouldBe 1

      inside(machine.finish) { case Right(commit) =>
        val exerciseNodes = commit.tx.nodes.collect { case (_, exercise: Node.Exercise) => exercise }
        exerciseNodes should have size 1
        exerciseNodes.head.externalCallResults shouldBe ImmArray(
          ExternalCallResult(
            extensionId = "ext",
            functionId = "fun",
            config = Bytes.assertFromString("0a0b"),
            input = Bytes.assertFromString("c0ff"),
            output = Bytes.assertFromString("beef"),
          )
        )
      }
    }

    "reject non-hex external call outputs" in {
      val machine = Speedy.Machine.fromUpdateSExpr(
        pkgs,
        transactionSeed,
        SEApp(pkgs.compiler.unsafeCompile(e"M:run"), ArraySeq(SParty(alice))),
        Set(alice),
        MachineLogger(),
      )

      val result = SpeedyTestLib.runTxQ[Question.Update](
        {
          case Question.Update.NeedExternalCall(_, _, _, _, callback) =>
            callback(Right("hello"))
          case other =>
            fail(s"Unexpected question: $other")
        },
        machine,
      )

      inside(result) { case Left(SError.SErrorDamlException(IE.UserError(message))) =>
        message should include("Invalid hex encoding in external call output")
      }
    }

    "surface external call failures and not record a result" in {
      val machine = Speedy.Machine.fromUpdateSExpr(
        pkgs,
        transactionSeed,
        SEApp(pkgs.compiler.unsafeCompile(e"M:run"), ArraySeq(SParty(alice))),
        Set(alice),
        MachineLogger(),
      )

      val result = SpeedyTestLib.runTxQ[Question.Update](
        {
          case Question.Update.NeedExternalCall(_, _, _, _, callback) =>
            callback(Left(Question.Update.ExternalCallError(503, "upstream unavailable", Some("req-123"))))
          case other =>
            fail(s"Unexpected question: $other")
        },
        machine,
      )

      inside(result) { case Left(SError.SErrorDamlException(IE.UserError(message))) =>
        message should include("External call failed: upstream unavailable")
        message should include("status=503")
        message should include("requestId=req-123")
        message should include("extensionId=ext")
        message should include("functionId=fun")
      }
      machine.ptx.externalCallResults shouldBe empty
    }

    "reject root-level external calls before issuing NeedExternalCall" in {
      val machine = Speedy.Machine.fromUpdateSExpr(
        pkgs,
        transactionSeed,
        pkgs.compiler.unsafeCompile(e"M:runAtRoot"),
        Set(alice),
        MachineLogger(),
      )

      var questions = 0
      val result = SpeedyTestLib.runTxQ[Question.Update](
        {
          case Question.Update.NeedExternalCall(_, _, _, _, callback) =>
            questions += 1
            callback(Right("beef"))
          case other =>
            fail(s"Unexpected question: $other")
        },
        machine,
      )

      questions shouldBe 0
      inside(result) { case Left(SError.SErrorDamlException(IE.UserError(message))) =>
        message should include("External calls are only supported within exercise context")
      }
    }
  }
}
