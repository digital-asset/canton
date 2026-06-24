// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package engine

import com.digitalasset.canton.logging.SuppressingLogging
import com.digitalasset.daml.lf.command.{ApiCommand, ApiCommands}
import com.digitalasset.daml.lf.crypto.Hash
import com.digitalasset.daml.lf.data.{ImmArray, Ref, Time}
import com.digitalasset.daml.lf.interpretation.{Error as IE, InterpretationConfig}
import com.digitalasset.daml.lf.language.LanguageVersion
import com.digitalasset.daml.lf.testing.parser.Implicits.SyntaxHelper
import com.digitalasset.daml.lf.testing.parser.ParserParameters
import com.digitalasset.daml.lf.transaction.{ExternalCallResult, Node}
import com.digitalasset.daml.lf.value.{ContractIdVersion, Value}
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class ExternalCallEngineTest extends AnyWordSpec with Matchers with Inside with SuppressingLogging {

  implicit private val parserParameters: ParserParameters[this.type] =
    ParserParameters(
      defaultPackageId = Ref.PackageId.assertFromString("-pkg-"),
      languageVersion = LanguageVersion.v2_dev,
    )

  private val pkgId = parserParameters.defaultPackageId
  private val pkg = p"""
    metadata ( '-pkg-' : '1.0.0' )

    module M {
      record @serializable T = { party: Party };

      template (this: T) = {
        precondition True;
        signatories Cons @Party [M:T {party} this] (Nil @Party);
        observers Nil @Party;

        choice Call (self) (arg: Unit) : Text,
          controllers Cons @Party [M:T {party} this] (Nil @Party)
          to EXTERNAL_CALL "ext" "fun" "0a0b" "c0ff";
      };
    }
  """

  def newEngine(config: EngineConfig = Engine.DevConfig): Engine = {
    val engine = new Engine(config, loggerFactory)
    engine.preloadPackage(pkgId, pkg).consume() shouldBe Right(())
    engine
  }

  def submit(engine: Engine) =
    engine.submit(
      submitters = Set(alice),
      readAs = Set.empty,
      cmds = ApiCommands(ImmArray(command), let, "external-call-engine-test"),
      participantId = participantId,
      submissionSeed = submissionSeed,
      contractIdVersion = ContractIdVersion.V1,
      interpretationConfig = InterpretationConfig.Dev,
      prefetchKeys = Seq.empty,
    )

  private val alice = Ref.Party.assertFromString("Alice")
  private val participantId = Ref.ParticipantId.assertFromString("participant")
  private val submissionSeed = Hash.hashPrivateKey("ExternalCallEngineTest")
  private val let = Time.Timestamp.now()
  private val templateId = Ref.Identifier(pkgId, Ref.QualifiedName.assertFromString("M:T"))
  private val command = ApiCommand.CreateAndExercise(
    templateId.toRef,
    Value.ValueRecord(None, ImmArray(None -> Value.ValueParty(alice))),
    Ref.ChoiceName.assertFromString("Call"),
    Value.ValueUnit,
  )

  "Engine.submit" should {
    "emit ResultNeedExternalCall with the expected payload and continuation" in {
      val result = submit(newEngine())

      inside(result) {
        case ResultNeedExternalCall(extensionId, functionId, configHash, input, resume) =>
          extensionId shouldBe "ext"
          functionId shouldBe "fun"
          configHash shouldBe "0a0b"
          input shouldBe "c0ff"

          inside(resume(Right("beef")).consume()) { case Right((tx, _)) =>
            val exerciseNodes = tx.nodes.collect { case (_, exercise: Node.Exercise) => exercise }
            exerciseNodes should have size 1
            exerciseNodes.head.externalCallResults shouldBe ImmArray(
              ExternalCallResult(
                extensionId = "ext",
                functionId = "fun",
                config = data.Bytes.assertFromString("0a0b"),
                input = data.Bytes.assertFromString("c0ff"),
                output = data.Bytes.assertFromString("beef"),
              )
            )
          }
      }
    }

    "surface external call failures through the engine continuation" in {
      val result = submit(newEngine())

      inside(result) { case ResultNeedExternalCall(_, _, _, _, resume) =>
        inside(
          resume(Left(ResultNeedExternalCall.Error("upstream unavailable"))).consume()
        ) {
          case Left(
                err @ Error.Interpretation(
                  Error.Interpretation.DamlException(
                    IE.ExternalCall(
                      IE.ExternalCall.ExecutionFailed(
                        "ext",
                        "fun",
                        IE.ExternalCall.ExecutionFailed.CallFailed(message),
                      )
                    )
                  ),
                  _,
                )
              ) =>
            message shouldBe "upstream unavailable"
            err.message should include("External call execution failed")
            err.message should include("extensionId=ext")
            err.message should include("functionId=fun")
        }
      }
    }
  }
}
