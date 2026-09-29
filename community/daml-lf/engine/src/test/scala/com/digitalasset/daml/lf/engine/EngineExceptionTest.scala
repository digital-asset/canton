// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package engine

import com.digitalasset.canton.logging.SuppressingLogging
import com.digitalasset.daml.lf.command.{ApiCommand, ApiCommands}
import com.digitalasset.daml.lf.crypto.Hash
import com.digitalasset.daml.lf.data.{Bytes, ImmArray, Ref, Time}
import com.digitalasset.daml.lf.engine.Result.lookupHandler
import com.digitalasset.daml.lf.interpretation.{ExecutionMode, InterpretationConfig, Error as IE}
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

trait EngineExceptionTestFixture extends Matchers with SuppressingLogging {

  protected def executionMode: ExecutionMode = ExecutionMode.UpdateMachine

  implicit private val parserParameters: ParserParameters[this.type] =
    ParserParameters(
      defaultPackageId = Ref.PackageId.assertFromString("-exception-test-pkg-"),
      languageVersion = LanguageVersion.stagingLfVersion,
    )

  protected val pkgId = parserParameters.defaultPackageId

  protected val pkg = p"""
    metadata ( 'exception-test-pkg' : '1.0.0' )
    module M {
      record @serializable E1 = { } ;
      exception E1 = { message \(e: M:E1) -> "E1" } ;

      record @serializable E2 = { } ;
      exception E2 = { message \(e: M:E2) -> throw @Text @M:E1 (M:E1 {}) } ;

      record @serializable E3 = { } ;
      // comparing functions raises NonComparableValues, a non-exception interpretation error
      exception E3 = { message \(e: M:E3) -> case (EQUAL @(Unit -> Unit) (\(x: Unit) -> x) (\(x: Unit) -> x)) of True -> "unreachable" | False -> "unreachable" } ;

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

        choice @nonConsuming Middle (self) (arg: Unit) : Unit,
          controllers Cons @Party [M:T {party} this] (Nil @Party)
          to try @Unit (exercise @M:T Throwing self ())
             catch e -> None @(Update Unit);

        choice @nonConsuming Throwing (self) (arg: Unit) : Unit,
          controllers Cons @Party [M:T {party} this] (Nil @Party)
          to throw @(Update Unit) @M:E1 (M:E1 {});

        choice @nonConsuming CatchThenError (self) (arg: Unit) : Bool,
          controllers Cons @Party [M:T {party} this] (Nil @Party)
          to ubind
                _:Unit <- try @Unit (exercise @M:T Throwing self ()) catch e -> Some @(Update Unit) (upure @Unit ())
              // comparing functions raises NonComparableValues, a non-exception interpretation error
              in upure @Bool (EQUAL @(Unit -> Unit) (\(x: Unit) -> x) (\(x: Unit) -> x));

        implements M:I {
          view = throw @M:View @M:E1 (M:E1 {});
        };
      };
    }
  """

  private val stablePkgs = StablePackages.stablePackages.packagesMap
  protected val allPkgs = stablePkgs + (pkgId -> pkg)

  protected lazy val compiledPackage =
    PureCompiledPackages.assertBuild(allPkgs, Compiler.Config.Dev)

  private def exceptionValue(name: String) = {
    val tyCon = Ref.TypeConId(pkgId, Ref.QualifiedName.assertFromString(name))
    SAny(TTyCon(tyCon), SValue.SRecord(tyCon, ImmArray.Empty, ArraySeq.empty))
  }

  protected val e1Value = exceptionValue("M:E1")
  protected val e2Value = exceptionValue("M:E2")
  protected val e3Value = exceptionValue("M:E3")

  protected val alice = Ref.Party.assertFromString("Alice")
  protected val participantId = Ref.ParticipantId.assertFromString("participant")
  protected val submissionSeed = Hash.hashPrivateKey("EngineExceptionTest")
  protected val let = Time.Timestamp.now()
  protected val templateId = Ref.Identifier(pkgId, Ref.QualifiedName.assertFromString("M:T"))
  protected val interfaceId = Ref.Identifier(pkgId, Ref.QualifiedName.assertFromString("M:I"))

  protected def newEngine(): Engine = {
    val engine = new Engine(Engine.DevConfig.copy(executionMode = executionMode), loggerFactory)
    engine.preloadPackage(pkgId, pkg).consume(lookupHandler(pkgs = allPkgs)) shouldBe Right(())
    engine
  }

  protected def command(choiceName: String) = ApiCommand.CreateAndExercise(
    templateId.toRef,
    Value.ValueRecord(
      None,
      ImmArray(None -> Value.ValueParty(alice), None -> Value.ValueBool(true)),
    ),
    Ref.ChoiceName.assertFromString(choiceName),
    Value.ValueUnit,
  )

  protected def submit(engine: Engine, choiceName: String = "FailingChoice") =
    engine.submit(
      submitters = Set(alice),
      cmds = ApiCommands(ImmArray(command(choiceName)), let, "exception-test"),
      participantId = participantId,
      submissionSeed = submissionSeed,
      contractIdVersion = ContractIdVersion.V1,
      interpretationConfig = InterpretationConfig.Dev,
      prefetchKeys = Seq.empty,
    )
}

class EngineExceptionTestUpdateMachine extends EngineExceptionTest(ExecutionMode.UpdateMachine)
class EngineExceptionTestConductor extends EngineExceptionTest(ExecutionMode.Conductor)

abstract class EngineExceptionTest(override protected val executionMode: ExecutionMode)
    extends AnyWordSpec
    with Inside
    with EngineExceptionTestFixture {

  "Engine.submit" should {
    "preserve the transaction trace when a choice throws" in {
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

    "start the transaction trace at the exercise where the exception was thrown" in {
      // E1 is thrown inside the nested exercise `Throwing`, then unwinds through the
      // non-catching try-catch in the enclosing exercise `Middle`. The throw-site trace
      // is captured before unwinding, so it starts at `Throwing`, not at `Middle`.
      val engine = newEngine()
      inside(submit(engine, "Middle").consume(lookupHandler(pkgs = allPkgs))) {
        case Left(
              Error.Interpretation(
                Error.Interpretation.DamlException(IE.FailureStatus(errorId, _, _, _)),
                Some(trace),
              )
            ) =>
          errorId shouldBe "UNHANDLED_EXCEPTION/M:E1"
          trace should include("M:T:Throwing")
          trace.indexOf("M:T:Throwing") should be < trace.indexOf("M:T:Middle")
      }
    }

    "not reuse a caught exception's throw site for a later error" in {
      // Regression: E1 is thrown in `Throwing` and caught, then a normal interpretation error
      // (comparing functions -> NonComparableValues) is raised. The trace must report the current
      // location `CatchThenError`, not the caught throw site `Throwing`.
      val engine = newEngine()
      inside(submit(engine, "CatchThenError").consume(lookupHandler(pkgs = allPkgs))) {
        case Left(
              Error.Interpretation(
                Error.Interpretation.DamlException(IE.NonComparableValues),
                Some(trace),
              )
            ) =>
          trace should include("M:T:CatchThenError")
          trace should not include "M:T:Throwing"
      }
    }
  }
}

class EngineFailureStatusTest extends AnyWordSpec with Inside with EngineExceptionTestFixture {

  private def computeFailureStatus(excp: SAny, detailMsg: Option[String]) =
    Engine
      .computeFailureStatus(
        excp = excp,
        compiledPackages = compiledPackage,
        machineLogger = MachineLogger.Dummy,
        iterationsBetweenInterruptions = Long.MaxValue,
        detailMsg = detailMsg,
      )
      .consume(lookupHandler(pkgs = allPkgs))

  "Engine.computeFailureStatus" should {

    "convert an unhandled exception to FailureStatus using the exception message" in {
      inside(computeFailureStatus(e1Value, detailMsg = None)) {
        case Right(IE.FailureStatus(errorId, _, msg, _)) =>
          errorId shouldBe "UNHANDLED_EXCEPTION/M:E1"
          msg shouldBe "E1"
      }
    }

    "use a fallback message when the message function itself throws" in {
      inside(computeFailureStatus(e2Value, detailMsg = None)) {
        case Right(IE.FailureStatus(errorId, _, msg, _)) =>
          errorId shouldBe "UNHANDLED_EXCEPTION/M:E2"
          msg shouldBe "<Failed to calculate message as M:E1 was thrown during conversion>"
      }
    }

    "attach detailMsg when the message function raises an interpretation error" in {
      val expectedTrace = "test trace: exercise details"
      computeFailureStatus(e3Value, detailMsg = Some(expectedTrace)) shouldBe Left(
        Error.Interpretation(
          Error.Interpretation.DamlException(IE.NonComparableValues),
          Some(expectedTrace),
        )
      )
    }

    "omit the transaction trace when no detailMsg is provided" in {
      computeFailureStatus(e3Value, detailMsg = None) shouldBe Left(
        Error.Interpretation(
          Error.Interpretation.DamlException(IE.NonComparableValues),
          None,
        )
      )
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
