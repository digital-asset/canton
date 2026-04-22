// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package engine

import com.digitalasset.canton.logging.SuppressingLogging
import com.digitalasset.daml.lf.command.{ApiCommand, ApiCommands}
import com.digitalasset.daml.lf.crypto.Hash
import com.digitalasset.daml.lf.data.{ImmArray, Ref, Time}
import com.digitalasset.daml.lf.engine.ResultNeedExternalCall
import com.digitalasset.daml.lf.language.LanguageVersion
import com.digitalasset.daml.lf.testing.parser.Implicits.SyntaxHelper
import com.digitalasset.daml.lf.testing.parser.ParserParameters
import com.digitalasset.daml.lf.transaction.{ExternalCallResult, Node}
import com.digitalasset.daml.lf.value.{ContractIdVersion, Value}
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class ExternalCallEngineTest
    extends AnyWordSpec
    with Matchers
    with Inside
    with SuppressingLogging {

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

  private val engine = Engine.DevEngine(loggerFactory)
  engine.preloadPackage(pkgId, pkg).consume() shouldBe Right(())

  private val alice = Ref.Party.assertFromString("Alice")
  private val participantId = Ref.ParticipantId.assertFromString("participant")
  private val submissionSeed = Hash.hashPrivateKey("ExternalCallEngineTest")
  private val let = Time.Timestamp.now()
  private val templateId = Ref.Identifier(pkgId, Ref.QualifiedName.assertFromString("M:T"))

  "Engine.submit" should {
    "emit ResultNeedExternalCall with the expected payload and continuation" in {
      val command = ApiCommand.CreateAndExercise(
        templateId.toRef,
        Value.ValueRecord(None, ImmArray(None -> Value.ValueParty(alice))),
        Ref.ChoiceName.assertFromString("Call"),
        Value.ValueUnit,
      )

      val result = engine.submit(
        submitters = Set(alice),
        readAs = Set.empty,
        cmds = ApiCommands(ImmArray(command), let, "external-call-engine-test"),
        participantId = participantId,
        submissionSeed = submissionSeed,
        contractIdVersion = ContractIdVersion.V1,
        contractStateMode = transaction.NextGenContractStateMachine.Mode.devDefault,
        prefetchKeys = Seq.empty,
      )

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
  }
}
