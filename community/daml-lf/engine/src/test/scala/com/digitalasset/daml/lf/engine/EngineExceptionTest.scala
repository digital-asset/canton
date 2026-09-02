// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package engine

import com.digitalasset.canton.logging.SuppressingLogging
import com.digitalasset.daml.lf.command.{ApiCommand, ApiCommands}
import com.digitalasset.daml.lf.crypto.Hash
import com.digitalasset.daml.lf.data.{Bytes, ImmArray, Ref, Time}
import com.digitalasset.daml.lf.engine.Result.lookupHandler
import com.digitalasset.daml.lf.interpretation.{Error as IE, InterpretationConfig}
import com.digitalasset.daml.lf.language.Ast.TTyCon
import com.digitalasset.daml.lf.language.LanguageVersion
import com.digitalasset.daml.lf.speedy.SValue.SAny
import com.digitalasset.daml.lf.speedy.compiler.Compiler
import com.digitalasset.daml.lf.speedy.{MachineLogger, SValue}
import com.digitalasset.daml.lf.stablepackages.StablePackages
import com.digitalasset.daml.lf.testing.parser.Implicits.SyntaxHelper
import com.digitalasset.daml.lf.testing.parser.ParserParameters
import com.digitalasset.daml.lf.transaction.test.TransactionBuilder
import com.digitalasset.daml.lf.transaction.{
  CreationTime,
  FatContractInstance,
  Node,
  SerializationVersion,
}
import com.digitalasset.daml.lf.value.{ContractIdVersion, Value}
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.collection.immutable.ArraySeq

/** Tests for engine-level exception-to-FailureStatus conversion (Engine.computeFailureStatus). */
class EngineExceptionTest extends AnyWordSpec with Matchers with Inside with SuppressingLogging {

  implicit private val parserParameters: ParserParameters[this.type] =
    ParserParameters(
      defaultPackageId = Ref.PackageId.assertFromString("-exception-test-pkg-"),
      languageVersion = LanguageVersion.stagingLfVersion,
    )

  private val pkgId = parserParameters.defaultPackageId

  private val pkg = p"""
    metadata ( 'exception-test-pkg' : '1.0.0' )
    module M {
      record @serializable E1 = { } ;
      exception E1 = { message \(e: M:E1) -> "E1" } ;

      record @serializable E2 = { } ;
      exception E2 = { message \(e: M:E2) -> throw @Text @M:E1 (M:E1 {}) } ;

      record @serializable View = { } ;
      interface (this: I) = {
        viewtype M:View;
      };

      record @serializable T = { party: Party, valid: Bool } ;
      template (this: T) = {
        precondition M:T {valid} this;
        signatories Cons @Party [M:T {party} this] (Nil @Party);
        observers Nil @Party;

        choice FailingChoice (self) (arg: Unit) : Unit,
          controllers Cons @Party [M:T {party} this] (Nil @Party)
          to throw @(Update Unit) @M:E1 (M:E1 {});

        implements M:I {
          view = throw @M:View @M:E1 (M:E1 {});
        };
      };
    }
  """

  private val stablePkgs = StablePackages.stablePackages.packagesMap
  private val allPkgs = stablePkgs + (pkgId -> pkg)

  private val compiledPackage = PureCompiledPackages.assertBuild(allPkgs, Compiler.Config.Dev)

  private val e1TyCon = Ref.TypeConId(pkgId, Ref.QualifiedName.assertFromString("M:E1"))
  private val e1RecordValue = SValue.SRecord(e1TyCon, ImmArray.Empty, ArraySeq.empty)
  private val e1Value = SAny(TTyCon(e1TyCon), e1RecordValue)
  private val e2TyCon = Ref.TypeConId(pkgId, Ref.QualifiedName.assertFromString("M:E2"))
  private val e2RecordValue = SValue.SRecord(e2TyCon, ImmArray.Empty, ArraySeq.empty)
  private val e2Value = SAny(TTyCon(e2TyCon), e2RecordValue)

  private val alice = Ref.Party.assertFromString("Alice")
  private val participantId = Ref.ParticipantId.assertFromString("participant")
  private val submissionSeed = Hash.hashPrivateKey("EngineExceptionTest")
  private val let = Time.Timestamp.now()
  private val templateId = Ref.Identifier(pkgId, Ref.QualifiedName.assertFromString("M:T"))
  private val interfaceId = Ref.Identifier(pkgId, Ref.QualifiedName.assertFromString("M:I"))

  private def newEngine(): Engine = {
    val engine = new Engine(Engine.DevConfig, loggerFactory)
    engine.preloadPackage(pkgId, pkg).consume(lookupHandler(pkgs = allPkgs)) shouldBe Right(())
    engine
  }

  private def command(choiceName: String) = ApiCommand.CreateAndExercise(
    templateId.toRef,
    Value.ValueRecord(
      None,
      ImmArray(None -> Value.ValueParty(alice), None -> Value.ValueBool(true)),
    ),
    Ref.ChoiceName.assertFromString(choiceName),
    Value.ValueUnit,
  )

  private def submit(engine: Engine, choiceName: String = "FailingChoice") =
    engine.submit(
      submitters = Set(alice),
      readAs = Set.empty,
      cmds = ApiCommands(ImmArray(command(choiceName)), let, "exception-test"),
      participantId = participantId,
      submissionSeed = submissionSeed,
      contractIdVersion = ContractIdVersion.V1,
      interpretationConfig = InterpretationConfig.Dev,
      prefetchKeys = Seq.empty,
    )

  "Engine.computeFailureStatus" should {

    "convert an unhandled exception to FailureStatus using the exception message" in {
      inside(
        Engine
          .computeFailureStatus(
            excp = e1Value,
            compiledPackages = compiledPackage,
            machineLogger = MachineLogger.Dummy,
            iterationsBetweenInterruptions = Long.MaxValue,
            detailMsg = None,
          )
          .consume(lookupHandler(pkgs = allPkgs))
      ) { case Right(IE.FailureStatus(errorId, _, msg, _)) =>
        errorId shouldBe "UNHANDLED_EXCEPTION/M:E1"
        msg shouldBe "E1"
      }
    }

    "use a fallback message when the message function itself throws" in {
      inside(
        Engine
          .computeFailureStatus(
            excp = e2Value,
            compiledPackages = compiledPackage,
            machineLogger = MachineLogger.Dummy,
            iterationsBetweenInterruptions = Long.MaxValue,
            detailMsg = None,
          )
          .consume(lookupHandler(pkgs = allPkgs))
      ) { case Right(IE.FailureStatus(errorId, _, msg, _)) =>
        errorId shouldBe "UNHANDLED_EXCEPTION/M:E2"
        msg shouldBe "<Failed to calculate message as M:E1 was thrown during conversion>"
      }
    }

    "produce a FailureStatus when provided with detailMsg" in {
      val expectedTrace = "test trace: exercise details"
      inside(
        Engine
          .computeFailureStatus(
            excp = e1Value,
            compiledPackages = compiledPackage,
            machineLogger = MachineLogger.Dummy,
            iterationsBetweenInterruptions = Long.MaxValue,
            detailMsg = Some(expectedTrace),
          )
          .consume(lookupHandler(pkgs = allPkgs))
      ) { case Right(IE.FailureStatus(errorId, _, msg, _)) =>
        errorId shouldBe "UNHANDLED_EXCEPTION/M:E1"
        msg shouldBe "E1"
      }
    }

    "produce a FailureStatus when the message function throws and detailMsg is provided" in {
      val expectedTrace = "test trace: nested exercise details"
      inside(
        Engine
          .computeFailureStatus(
            excp = e2Value,
            compiledPackages = compiledPackage,
            machineLogger = MachineLogger.Dummy,
            iterationsBetweenInterruptions = Long.MaxValue,
            detailMsg = Some(expectedTrace),
          )
          .consume(lookupHandler(pkgs = allPkgs))
      ) { case Right(IE.FailureStatus(errorId, _, msg, _)) =>
        errorId shouldBe "UNHANDLED_EXCEPTION/M:E2"
        msg shouldBe "<Failed to calculate message as M:E1 was thrown during conversion>"
      }
    }
  }

  "Engine.submit" should {
    "preserve transaction trace when a choice throws an assertion failure" in {
      val engine = newEngine()
      inside(submit(engine).consume(lookupHandler(pkgs = allPkgs))) {
        case Left(
              Error.Interpretation(
                Error.Interpretation.DamlException(
                  IE.FailureStatus(errorId, _, msg, _)
                ),
                transactionTrace,
              )
            ) =>
          errorId shouldBe "UNHANDLED_EXCEPTION/M:E1"
          msg shouldBe "E1"
          transactionTrace shouldBe defined
          transactionTrace.get should include("in choice")
          transactionTrace.get should include("M:T:FailingChoice")
      }
    }
  }

  "Engine.computeInterfaceView" should {
    "convert an unhandled view exception to FailureStatus" in {
      val engine = newEngine()
      val argument = Value.ValueRecord(
        None,
        ImmArray(None -> Value.ValueParty(alice), None -> Value.ValueBool(true)),
      )

      inside(
        engine
          .computeInterfaceView(templateId, argument, interfaceId)
          .consume(lookupHandler(pkgs = allPkgs))
      ) {
        case Left(
              Error.Interpretation(
                Error.Interpretation.DamlException(IE.FailureStatus(errorId, _, _, _)),
                None,
              )
            ) =>
          errorId shouldBe "UNHANDLED_EXCEPTION/M:E1"
      }
    }
  }

  "Engine.validateContractInstance" should {
    "return a structured error when the template precondition fails" in {
      val engine = newEngine()
      val argument = Value.ValueRecord(
        None,
        ImmArray(None -> Value.ValueParty(alice), None -> Value.ValueBool(false)),
      )
      val contractInstance = FatContractInstance.fromCreateNode(
        Node.Create(
          coid = TransactionBuilder.newCid,
          packageName = Ref.PackageName.assertFromString("exception-test-pkg"),
          templateId = templateId,
          arg = argument,
          signatories = Set(alice),
          stakeholders = Set(alice),
          keyOpt = None,
          version = SerializationVersion.minVersion,
        ),
        CreationTime.CreatedAt(let),
        Bytes.Empty,
      )

      inside(
        engine
          .validateContractInstance(
            contractInstance,
            pkgId,
            identity,
            Hash.HashingMethod.Legacy,
            _ => true,
          )
          .consume(lookupHandler(pkgs = allPkgs))
      ) {
        case Right(
              Left(
                IE.TemplatePreconditionViolated(templateIdFound, _, _)
              )
            ) =>
          templateIdFound shouldBe Ref.TypeConId(pkgId, templateId.qualifiedName)
      }
    }
  }
}
