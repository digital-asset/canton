// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package engine

import com.digitalasset.canton.logging.SuppressingLogging
import com.digitalasset.daml.lf.command.{ApiCommand, ApiCommands}
import com.digitalasset.daml.lf.crypto.Hash
import com.digitalasset.daml.lf.data.{ImmArray, Ref, Time}
import com.digitalasset.daml.lf.engine.Result.lookupHandler
import com.digitalasset.daml.lf.interpretation.{ExecutionMode, InterpretationConfig}
import com.digitalasset.daml.lf.language.LanguageVersion
import com.digitalasset.daml.lf.testing.parser.Implicits.SyntaxHelper
import com.digitalasset.daml.lf.testing.parser.ParserParameters
import com.digitalasset.daml.lf.transaction.{Node, SerializationVersion, Versioned}
import com.digitalasset.daml.lf.value.{ContractIdVersion, Value}
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class Uint256EngineTestUpdateMachine extends Uint256EngineTest(ExecutionMode.UpdateMachine)
class Uint256EngineTestConductor extends Uint256EngineTest(ExecutionMode.Conductor)

abstract class Uint256EngineTest(executionMode: ExecutionMode)
    extends AnyWordSpec
    with Matchers
    with Inside
    with SuppressingLogging {

  implicit private val parserParameters: ParserParameters[this.type] =
    ParserParameters(
      defaultPackageId = Ref.PackageId.assertFromString("-uint256-engine-test-"),
      languageVersion = LanguageVersion.v2_dev,
    )

  private val pkgId = parserParameters.defaultPackageId
  private val pkg = p"""
    metadata ( '-uint256-engine-test-' : '1.0.0' )

    module M {
      record @serializable View = { v: Uint256 };
      interface (this: I) = {
        viewtype M:View;
      };

      record @serializable WithUint256 = { party: Party, v: Uint256 };
      template (this: WithUint256) = {
        precondition True;
        signatories Cons @Party [M:WithUint256 {party} this] (Nil @Party);
        observers Nil @Party;
        implements M:I {
          view = M:View { v = M:WithUint256 {v} this };
        };
      };

      record @serializable Plain = { party: Party };
      template (this: Plain) = {
        precondition True;
        signatories Cons @Party [M:Plain {party} this] (Nil @Party);
        observers Nil @Party;

        choice Inc (self) (x: Uint256) : Uint256,
          controllers Cons @Party [M:Plain {party} this] (Nil @Party)
          to upure @Uint256 (ADD_UINT256 x 0x1);
      };
    }
  """

  private val alice = Ref.Party.assertFromString("Alice")
  private val participantId = Ref.ParticipantId.assertFromString("participant")
  private val submissionSeed = Hash.hashPrivateKey("Uint256EngineTest")
  private val let = Time.Timestamp.now()

  private def id(name: String) = Ref.Identifier(pkgId, Ref.QualifiedName.assertFromString(name))
  private val withUint256Id = id("M:WithUint256")
  private val plainId = id("M:Plain")
  private val interfaceId = id("M:I")

  private val u42 = data.Uint256.assertFromLong(42L)
  private val u43 = data.Uint256.assertFromLong(43L)

  private def withUint256Arg(v: Value) =
    Value.ValueRecord(None, ImmArray(None -> Value.ValueParty(alice), None -> v))
  private val plainArg = Value.ValueRecord(None, ImmArray(None -> Value.ValueParty(alice)))

  private def newEngine(): Engine = {
    val engine = new Engine(Engine.DevConfig.copy(executionMode = executionMode), loggerFactory)
    engine.preloadPackage(pkgId, pkg).consume(lookupHandler()) shouldBe Right(())
    engine
  }

  private def submit(cmd: ApiCommand) =
    newEngine()
      .submit(
        submitters = Set(alice),
        cmds = ApiCommands(ImmArray(cmd), let, "uint256-engine-test"),
        participantId = participantId,
        submissionSeed = submissionSeed,
        contractIdVersion = ContractIdVersion.V1,
        interpretationConfig = InterpretationConfig.Dev,
        prefetchKeys = Seq.empty,
      )
      .consume(lookupHandler())

  "Engine.submit" should {

    "version a create node carrying a Uint256 with minUint256 and encode it" in {
      val arg = withUint256Arg(Value.ValueUint256(u42))
      inside(submit(ApiCommand.Create(withUint256Id.toRef, arg))) { case Right((tx, _)) =>
        val Seq(create) = tx.nodes.values.collect { case create: Node.Create => create }.toSeq
        create.arg shouldBe arg
        create.version shouldBe SerializationVersion.minUint256
        tx.version shouldBe SerializationVersion.minUint256

        inside(TransactionCoder.encodeTransaction(tx)) { case Right(encoded) =>
          inside(TransactionCoder.decodeTransaction(encoded)) { case Right(decoded) =>
            decoded.nodes.values.collect { case create: Node.Create =>
              create.arg
            }.toSeq shouldBe Seq(arg)
          }
        }
      }
    }

    "version only the nodes carrying a Uint256 with minUint256" in {
      val cmd = ApiCommand.CreateAndExercise(
        plainId.toRef,
        plainArg,
        Ref.ChoiceName.assertFromString("Inc"),
        Value.ValueUint256(u42),
      )
      inside(submit(cmd)) { case Right((tx, _)) =>
        val Seq(create) = tx.nodes.values.collect { case create: Node.Create => create }.toSeq
        create.version should be < SerializationVersion.minUint256

        val Seq(exercise) = tx.nodes.values.collect { case exe: Node.Exercise => exe }.toSeq
        exercise.chosenValue shouldBe Value.ValueUint256(u42)
        exercise.exerciseResult shouldBe Some(Value.ValueUint256(u43))
        exercise.version shouldBe SerializationVersion.minUint256
      }
    }

    "reject a non-Uint256 value where a Uint256 is expected" in {
      val arg = withUint256Arg(Value.ValueText("42"))
      inside(submit(ApiCommand.Create(withUint256Id.toRef, arg))) {
        case Left(Error.Preprocessing(_: Error.Preprocessing.TypeMismatch)) =>
      }
    }
  }

  "Engine.computeInterfaceView" should {
    "version a view carrying a Uint256 with minUint256" in {
      inside(
        newEngine()
          .computeInterfaceView(
            withUint256Id,
            withUint256Arg(Value.ValueUint256(u42)),
            interfaceId,
          )
          .consume(lookupHandler())
      ) { case Right(Versioned(version, view)) =>
        version shouldBe SerializationVersion.minUint256
        view shouldBe Value.ValueRecord(None, ImmArray(None -> Value.ValueUint256(u42)))
      }
    }
  }
}
