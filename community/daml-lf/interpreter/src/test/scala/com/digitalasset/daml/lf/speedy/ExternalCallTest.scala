// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package speedy

import com.daml.scalautil.Statement.discard
import com.digitalasset.canton.logging.SuppressingLogging
import com.digitalasset.daml.lf.data.{Bytes, ImmArray, Ref}
import com.digitalasset.daml.lf.interpretation.Error as IE
import com.digitalasset.daml.lf.language.LanguageVersion
import com.digitalasset.daml.lf.speedy.SExpr.{SEApp, SExpr}
import com.digitalasset.daml.lf.speedy.SValue.{SParty, SText}
import com.digitalasset.daml.lf.testing.parser.Implicits.SyntaxHelper
import com.digitalasset.daml.lf.testing.parser.ParserParameters
import com.digitalasset.daml.lf.transaction.{ExternalCallResult, Node, SerializationVersion}
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

        choice CallEmptyPayloads (self) (arg: Unit) : Text,
          controllers Cons @Party [M:T {party} this] (Nil @Party)
          to EXTERNAL_CALL "ext" "fun" "" "";

        choice CallBadConfig (self) (arg: Unit) : Text,
          controllers Cons @Party [M:T {party} this] (Nil @Party)
          to EXTERNAL_CALL "ext" "fun" "zzzz" "c0ff";

        choice CallBadInput (self) (arg: Unit) : Text,
          controllers Cons @Party [M:T {party} this] (Nil @Party)
          to EXTERNAL_CALL "ext" "fun" "0a0b" "nope";

        choice CallOddLengthInput (self) (arg: Unit) : Text,
          controllers Cons @Party [M:T {party} this] (Nil @Party)
          to EXTERNAL_CALL "ext" "fun" "0a0b" "0";

        choice CallUppercaseConfig (self) (arg: Unit) : Text,
          controllers Cons @Party [M:T {party} this] (Nil @Party)
          to EXTERNAL_CALL "ext" "fun" "0A0B" "c0ff";

        choice CallUppercaseInput (self) (arg: Unit) : Text,
          controllers Cons @Party [M:T {party} this] (Nil @Party)
          to EXTERNAL_CALL "ext" "fun" "0a0b" "C0FF";

        choice CallPaddedConfig (self) (arg: Unit) : Text,
          controllers Cons @Party [M:T {party} this] (Nil @Party)
          to EXTERNAL_CALL "ext" "fun" " 0a0b" "c0ff";

        choice CallPaddedInput (self) (arg: Unit) : Text,
          controllers Cons @Party [M:T {party} this] (Nil @Party)
          to EXTERNAL_CALL "ext" "fun" "0a0b" "c0ff ";

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

      val runEmptyPayloads : Party -> Update Text = \(party: Party) ->
        ubind cid: ContractId M:T <- create @M:T M:T { party = party }
        in exercise @M:T CallEmptyPayloads cid ();

      val runBadConfig : Party -> Update Text = \(party: Party) ->
        ubind cid: ContractId M:T <- create @M:T M:T { party = party }
        in exercise @M:T CallBadConfig cid ();

      val runBadInput : Party -> Update Text = \(party: Party) ->
        ubind cid: ContractId M:T <- create @M:T M:T { party = party }
        in exercise @M:T CallBadInput cid ();

      val runOddLengthInput : Party -> Update Text = \(party: Party) ->
        ubind cid: ContractId M:T <- create @M:T M:T { party = party }
        in exercise @M:T CallOddLengthInput cid ();

      val runUppercaseConfig : Party -> Update Text = \(party: Party) ->
        ubind cid: ContractId M:T <- create @M:T M:T { party = party }
        in exercise @M:T CallUppercaseConfig cid ();

      val runUppercaseInput : Party -> Update Text = \(party: Party) ->
        ubind cid: ContractId M:T <- create @M:T M:T { party = party }
        in exercise @M:T CallUppercaseInput cid ();

      val runPaddedConfig : Party -> Update Text = \(party: Party) ->
        ubind cid: ContractId M:T <- create @M:T M:T { party = party }
        in exercise @M:T CallPaddedConfig cid ();

      val runPaddedInput : Party -> Update Text = \(party: Party) ->
        ubind cid: ContractId M:T <- create @M:T M:T { party = party }
        in exercise @M:T CallPaddedInput cid ();

      val runInTry : Party -> Update Text = \(party: Party) ->
        ubind cid: ContractId M:T <- create @M:T M:T { party = party }
        in exercise @M:T CallInTry cid ();

      val runInTryRollback : Party -> Update Text = \(party: Party) ->
        ubind cid: ContractId M:T <- create @M:T M:T { party = party }
        in exercise @M:T CallInTryRollback cid ();
    }
  """)

  private def machineForRun(run: SExpr): Speedy.UpdateMachine =
    Speedy.Machine.fromUpdateSExpr(
      pkgs,
      transactionSeed,
      SEApp(run, ArraySeq(SParty(alice))),
      Set(alice),
      MachineLogger(),
    )

  private def rejectConfigOrInputBeforeExternalCall(run: SExpr): Unit = {
    val machine = machineForRun(run)
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
    assertPreparationFailed(result)
  }

  private def assertPreparationFailed(result: Either[SError.SError, SValue]): Unit =
    discard(
      inside(result) {
        case Left(
              SError.SErrorDamlException(
                IE.ExternalCall(IE.ExternalCall.PreparationFailed("ext", "fun", message))
              )
            ) =>
          message should include("Invalid external call config or input")
          message should include("expected canonical lowercase hex")
      }
    )

  private def assertInvalidOutput(result: Either[SError.SError, SValue]): Unit =
    discard(
      inside(result) {
        case Left(
              SError.SErrorDamlException(
                IE.ExternalCall(
                  IE.ExternalCall.ExecutionFailed(
                    "ext",
                    "fun",
                    IE.ExternalCall.ExecutionFailed.InvalidOutput(message),
                  )
                )
              )
            ) =>
          message should include("Invalid external call output")
          message should include("expected canonical lowercase hex")
      }
    )

  private def assertCallFailed(result: Either[SError.SError, SValue]): Unit =
    discard(
      inside(result) {
        case Left(
              SError.SErrorDamlException(
                IE.ExternalCall(
                  IE.ExternalCall.ExecutionFailed(
                    "ext",
                    "fun",
                    IE.ExternalCall.ExecutionFailed.CallFailed(message),
                  )
                )
              )
            ) =>
          message shouldBe "upstream unavailable"
      }
    )

  "SBExternalCall" should {
    Seq(
      ("populated", e"M:run", "0a0b", "c0ff", "beef"),
      ("empty", e"M:runEmptyPayloads", "", "", ""),
    ).foreach { case (label, expr, expectedConfig, expectedInput, expectedOutput) =>
      s"resume NeedExternalCall and record the result on the exercise node ($label payloads)" in {
        val machine = machineForRun(pkgs.compiler.unsafeCompile(expr))

        var questions = 0
        val result = SpeedyTestLib.runTxQ[Question.Update](
          {
            case Question.Update
                  .NeedExternalCall(extensionId, functionId, configHash, input, callback) =>
              questions += 1
              extensionId shouldBe "ext"
              functionId shouldBe "fun"
              configHash shouldBe expectedConfig
              input shouldBe expectedInput
              callback(Right(expectedOutput))
            case other =>
              fail(s"Unexpected question: $other")
          },
          machine,
        )

        result shouldBe Right(SText(expectedOutput))
        questions shouldBe 1

        inside(machine.finish) { case Right(commit) =>
          val exerciseNodes = commit.tx.nodes.collect { case (_, exercise: Node.Exercise) =>
            exercise
          }
          exerciseNodes should have size 1
          exerciseNodes.head.version shouldBe SerializationVersion.minExternalCallResults
          exerciseNodes.head.externalCallResults shouldBe ImmArray(
            ExternalCallResult(
              extensionId = "ext",
              functionId = "fun",
              config = Bytes.assertFromString(expectedConfig),
              input = Bytes.assertFromString(expectedInput),
              output = Bytes.assertFromString(expectedOutput),
            )
          )
        }
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
          case Question.Update
                .NeedExternalCall(extensionId, functionId, configHash, input, callback) =>
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
        val exerciseNodes = commit.tx.nodes.collect { case (_, exercise: Node.Exercise) =>
          exercise
        }
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
          case Question.Update
                .NeedExternalCall(extensionId, functionId, configHash, input, callback) =>
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
        val exerciseNodes = commit.tx.nodes.collect { case (_, exercise: Node.Exercise) =>
          exercise
        }
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

      assertInvalidOutput(result)
    }

    "reject non-canonical external call outputs" in {
      Seq("BEEF", "beef ").foreach { output =>
        withClue(s"output=$output") {
          val machine = machineForRun(pkgs.compiler.unsafeCompile(e"M:run"))
          val result = SpeedyTestLib.runTxQ[Question.Update](
            {
              case Question.Update.NeedExternalCall(_, _, _, _, callback) =>
                callback(Right(output))
              case other =>
                fail(s"Unexpected question: $other")
            },
            machine,
          )

          assertInvalidOutput(result)
        }
      }
    }

    "reject malformed config hex before issuing NeedExternalCall" in {
      val machine = Speedy.Machine.fromUpdateSExpr(
        pkgs,
        transactionSeed,
        SEApp(pkgs.compiler.unsafeCompile(e"M:runBadConfig"), ArraySeq(SParty(alice))),
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
      assertPreparationFailed(result)
    }

    "reject malformed input hex before issuing NeedExternalCall" in {
      val machine = Speedy.Machine.fromUpdateSExpr(
        pkgs,
        transactionSeed,
        SEApp(pkgs.compiler.unsafeCompile(e"M:runBadInput"), ArraySeq(SParty(alice))),
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
      assertPreparationFailed(result)
    }

    "reject non-canonical config and input hex before issuing NeedExternalCall" in {
      Seq(
        "uppercase config" -> pkgs.compiler.unsafeCompile(e"M:runUppercaseConfig"),
        "padded config" -> pkgs.compiler.unsafeCompile(e"M:runPaddedConfig"),
        "uppercase input" -> pkgs.compiler.unsafeCompile(e"M:runUppercaseInput"),
        "padded input" -> pkgs.compiler.unsafeCompile(e"M:runPaddedInput"),
        "odd-length input" -> pkgs.compiler.unsafeCompile(e"M:runOddLengthInput"),
      ).foreach { case (label, run) =>
        withClue(label) {
          rejectConfigOrInputBeforeExternalCall(run)
        }
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
            callback(Left(Question.Update.NeedExternalCall.Error("upstream unavailable")))
          case other =>
            fail(s"Unexpected question: $other")
        },
        machine,
      )

      assertCallFailed(result)
      machine.ptx.externalCallResults shouldBe empty
    }

    "treat external call failures as uncatchable inside try/catch" in {
      val machine = Speedy.Machine.fromUpdateSExpr(
        pkgs,
        transactionSeed,
        SEApp(pkgs.compiler.unsafeCompile(e"M:runInTry"), ArraySeq(SParty(alice))),
        Set(alice),
        MachineLogger(),
      )

      val result = SpeedyTestLib.runTxQ[Question.Update](
        {
          case Question.Update.NeedExternalCall(_, _, _, _, callback) =>
            callback(Left(Question.Update.NeedExternalCall.Error("upstream unavailable")))
          case other =>
            fail(s"Unexpected question: $other")
        },
        machine,
      )

      assertCallFailed(result)
      machine.ptx.externalCallResults shouldBe empty
    }
  }
}
