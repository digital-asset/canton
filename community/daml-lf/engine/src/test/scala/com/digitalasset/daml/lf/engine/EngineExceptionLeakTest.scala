// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package engine

import com.digitalasset.canton.logging.SuppressingLogging
import com.digitalasset.daml.lf.command.{ApiCommand, ApiCommands}
import com.digitalasset.daml.lf.crypto.Hash
import com.digitalasset.daml.lf.data.{Bytes, ImmArray, Ref, Time}
import com.digitalasset.daml.lf.engine.Result.lookupHandler
import com.digitalasset.daml.lf.interpretation.{ExecutionMode, InterpretationConfig}
import com.digitalasset.daml.lf.language.{Ast, LanguageVersion}
import com.digitalasset.daml.lf.stablepackages.StablePackages
import com.digitalasset.daml.lf.testing.parser.Implicits.SyntaxHelper
import com.digitalasset.daml.lf.testing.parser.ParserParameters
import com.digitalasset.daml.lf.transaction.test.TransactionBuilder
import com.digitalasset.daml.lf.transaction.{FatContractInstance, SerializationVersion}
import com.digitalasset.daml.lf.validation.Validation
import com.digitalasset.daml.lf.value.Value.ContractId
import com.digitalasset.daml.lf.value.{ContractIdVersion, Value}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

class EngineExceptionLeakTestUpdateMachine
    extends EngineExceptionLeakTest(ExecutionMode.UpdateMachine)
class EngineExceptionLeakTestConductor extends EngineExceptionLeakTest(ExecutionMode.Conductor)

/** Checks that an unexpected exception raised while interpreting a well-typed program does not leak
  * out of the engine.
  *
  * The exception is injected with the experimental builtin `INTERNAL_ERROR`, as a well-typed
  * program cannot otherwise reach an internal error.
  */
abstract class EngineExceptionLeakTest(executionMode: ExecutionMode)
    extends AnyWordSpec
    with Matchers
    with SuppressingLogging {

  import EngineExceptionLeakTest.*

  implicit private val parserParameters: ParserParameters[this.type] =
    ParserParameters(
      defaultPackageId = Ref.PackageId.assertFromString("-leak-test-pkg-"),
      languageVersion = LanguageVersion.stagingLfVersion,
    )

  private val pkgId = parserParameters.defaultPackageId

  private val pkg = p"""
    metadata ( 'leak-test-pkg' : '1.0.0' )
    module M {
      val boom : forall (a: *) . Unit -> a =
        /\ (a: *). experimental INTERNAL_ERROR (Unit -> a);

      record @serializable Pre = { party: Party } ;
      template (this: Pre) = {
        precondition M:boom @Bool ();
        signatories Cons @Party [M:Pre {party} this] (Nil @Party);
        observers Nil @Party;
      };

      record @serializable Sig = { party: Party } ;
      template (this: Sig) = {
        precondition True;
        signatories (M:boom @(List Party) ());
        observers Nil @Party;
      };

      record @serializable Body = { party: Party } ;
      template (this: Body) = {
        precondition True;
        signatories Cons @Party [M:Body {party} this] (Nil @Party);
        observers Nil @Party;

        choice @nonConsuming Go (self) (arg: Unit) : Unit,
          controllers Cons @Party [M:Body {party} this] (Nil @Party)
          to upure @Unit (M:boom @Unit ());
      };

      record @serializable Target = { party: Party } ;
      template (this: Target) = {
        precondition True;
        signatories Cons @Party [M:Target {party} this] (Nil @Party);
        observers Nil @Party;
      };

      record @serializable Keyed = { party: Party } ;
      template (this: Keyed) = {
        precondition True;
        signatories Cons @Party [M:Keyed {party} this] (Nil @Party);
        observers Nil @Party;
        key @Party (M:Keyed {party} this) (\(p: Party) -> Cons @Party [p] (Nil @Party));
      };

      record @serializable View = { } ;
      interface (this: I) = {
        viewtype M:View;
      };

      // Each choice fails right after the answer to the question it asks.
      record @serializable Fetcher = { party: Party } ;
      template (this: Fetcher) = {
        precondition True;
        signatories Cons @Party [M:Fetcher {party} this] (Nil @Party);
        observers Nil @Party;

        choice @nonConsuming AfterContract (self) (arg: ContractId M:Target) : Unit,
          controllers Cons @Party [M:Fetcher {party} this] (Nil @Party)
          to ubind _: M:Target <- fetch_template @M:Target arg
             in upure @Unit (M:boom @Unit ());

        choice @nonConsuming AfterKey (self) (arg: Party) : Unit,
          controllers Cons @Party [M:Fetcher {party} this] (Nil @Party)
          to ubind _: Option (List ('5aee9b21b8e9a4c4975b5f4c4198e6e6e8469df49e2010820e792f393db870f4':DA.Types:Tuple2 (ContractId M:Keyed) M:Keyed))
               <- query_n_by_key @M:Keyed 1 arg
             in upure @Unit (M:boom @Unit ());

        choice @nonConsuming AfterInterface (self) (arg: ContractId M:I) : Unit,
          controllers Cons @Party [M:Fetcher {party} this] (Nil @Party)
          to ubind _: M:I <- fetch_interface @M:I arg
             in upure @Unit (M:boom @Unit ());
      };
    }
  """

  // Implements the interface of `pkg` and is left unloaded until the engine asks for it.
  private object ImplPackage {
    implicit val parserParameters: ParserParameters[this.type] =
      ParserParameters(
        defaultPackageId = Ref.PackageId.assertFromString("-leak-impl-pkg-"),
        languageVersion = LanguageVersion.stagingLfVersion,
      )

    val pkgId = parserParameters.defaultPackageId

    val pkg = p"""
      metadata ( 'leak-impl-pkg' : '1.0.0' )
      module N {
        record @serializable Impl = { party: Party } ;
        template (this: Impl) = {
          precondition True;
          signatories Cons @Party [N:Impl {party} this] (Nil @Party);
          observers Nil @Party;
          implements '-leak-test-pkg-':M:I {
            view = '-leak-test-pkg-':M:View {};
          };
        };
      }
    """
  }

  private val allPkgs: Map[Ref.PackageId, Ast.Package] =
    StablePackages.stablePackages.packagesMap + (pkgId -> pkg)

  private val alice = Ref.Party.assertFromString("Alice")
  private val participantId = Ref.ParticipantId.assertFromString("participant")
  private val submissionSeed = Hash.hashPrivateKey("EngineExceptionLeakTest")
  private val let = Time.Timestamp.now()

  private def templateId(name: String) =
    Ref.Identifier(pkgId, Ref.QualifiedName.assertFromString(s"M:$name"))

  private def cid(name: String): ContractId =
    Value.ContractId.V1.assertBuild(
      Hash.hashPrivateKey(s"EngineExceptionLeakTest.$name"),
      Bytes.assertFromString("00"),
    )

  // Contract arguments carry neither record type ids nor field labels.
  private val partyRecord =
    Value.ValueRecord(None, ImmArray(None -> Value.ValueParty(alice)))

  private def create(template: String) =
    ApiCommand.Create(templateId(template).toRef, partyRecord)

  private def exercise(template: String, choice: String, arg: Value) =
    ApiCommand.CreateAndExercise(
      templateId(template).toRef,
      partyRecord,
      Ref.ChoiceName.assertFromString(choice),
      arg,
    )

  private def newEngine(
      iterationsBetweenInterruptions: Long = Engine.DevConfig.iterationsBetweenInterruptions
  ): Engine = {
    val engine = new Engine(
      Engine.DevConfig.copy(
        executionMode = executionMode,
        iterationsBetweenInterruptions = iterationsBetweenInterruptions,
      ),
      loggerFactory,
    )
    engine.preloadPackage(pkgId, pkg).consume(lookupHandler(pkgs = allPkgs)) shouldBe Right(())
    engine
  }

  private val engine = newEngine()
  // Every iteration is interrupted, so each interruption is answered before the failure.
  private val interruptedEngine = newEngine(iterationsBetweenInterruptions = 1)
  // Only `pkg` is loaded: the engine has to ask for `ImplPackage`.
  private val unloadedEngine = newEngine()

  // The ledger knows both packages even though the engine has not loaded `ImplPackage` yet.
  private val packageMap = Map(
    pkgId -> (pkg.pkgName -> pkg.pkgVersion),
    ImplPackage.pkgId -> (ImplPackage.pkg.pkgName -> ImplPackage.pkg.pkgVersion),
  )

  private def submit(engine: Engine, command: ApiCommand) =
    engine.submit(
      packageMap = packageMap,
      packagePreference = packageMap.keySet,
      submitters = Set(alice),
      cmds = ApiCommands(ImmArray(command), let, "leak-test"),
      participantId = participantId,
      submissionSeed = submissionSeed,
      contractIdVersion = ContractIdVersion.V1,
      interpretationConfig = InterpretationConfig.Dev,
      prefetchKeys = Seq.empty,
    )

  // Steps the result by hand, so that a leak is attributed to the answer that triggered it.
  private def run[A](result: => Result[A], handler: Result.Handler): Run = {
    val needs = Vector.newBuilder[Result.Need[?]]

    @tailrec def go(step: Try[Result.Step[A]]): Either[Throwable, Option[Error]] =
      step match {
        case Failure(exception) => Left(exception)
        case Success(Result.Step.Pure(_)) => Right(None)
        case Success(Result.Step.Error(error)) => Right(Some(error))
        case Success(im: Result.Step.Impure[x, A]) =>
          needs += im.fx
          handler(im.fx) match {
            case Left(error) => Right(Some(error))
            case Right(answer) => go(Try(im.resume(answer)))
          }
      }

    val outcome = go(Try(result.start))
    Run(needs.result(), outcome)
  }

  // `asked` guards against a scenario that never reaches the callback it claims to cover.
  private def assertNoLeak(run: Run, asked: Option[Class[?]]) = {
    val needNames = run.needs.map(_.productPrefix).mkString(", ")
    asked.foreach { need =>
      withClue(s"the engine never asked ${need.getSimpleName}, it asked $needNames: ") {
        run.needs.exists(need.isInstance) shouldBe true
      }
    }
    run.outcome match {
      case Left(exception) => fail(s"leaked after $needNames: $exception", exception)
      case Right(None) => fail("the exception was not raised: the engine succeeded")
      case Right(Some(_)) => succeed
    }
  }

  // Submits the command, answers the engine's questions, and asserts that nothing leaks.
  private def check(
      engine: Engine,
      command: ApiCommand,
      contracts: Map[Value.ContractId, FatContractInstance] = Map.empty,
      extraPkgs: Map[Ref.PackageId, Ast.Package] = Map.empty,
      asked: Option[Class[?]],
  ) =
    assertNoLeak(
      run(submit(engine, command), lookupHandler(pcs = contracts, pkgs = allPkgs ++ extraPkgs)),
      asked,
    )

  private val targetCid = cid("target")
  private val implCid = cid("impl")

  private def contractInstance(
      packageName: Ref.PackageName,
      template: Ref.Identifier,
      contractId: Value.ContractId,
  ) =
    TransactionBuilder.fatContractInstanceWithDummyDefaults(
      version = SerializationVersion.VDev,
      packageName = packageName,
      template = template,
      arg = partyRecord,
      contractId = contractId,
      signatories = List(alice),
    )

  "The test packages" should {
    "be well typed (sanity check)" in {
      Validation.checkPackages(
        allPkgs + (ImplPackage.pkgId -> ImplPackage.pkg)
      ) shouldBe Right(())
    }
  }

  "Engine.submit" should {

    // The failure is raised during interpretation, with and without interruptions before it.
    for {
      (when, testedEngine, asked) <- Seq[(String, Engine, Option[Class[?]])](
        ("", engine, None),
        (", after an interruption", interruptedEngine, Some(classOf[Result.Need.Interruption])),
      )
      (position, command) <- Seq(
        "a precondition" -> create("Pre"),
        "a signatories expression" -> create("Sig"),
        "a choice body" -> exercise("Body", "Go", Value.ValueUnit),
      )
    } s"not leak an exception raised in $position$when" in {
      check(testedEngine, command, asked = asked)
    }

    "not leak an exception raised after answering Need.Contract" in {
      val command = exercise("Fetcher", "AfterContract", Value.ValueContractId(targetCid))
      val target = contractInstance(pkg.pkgName, templateId("Target"), targetCid)
      check(
        engine,
        command,
        contracts = Map(targetCid -> target),
        asked = Some(classOf[Result.Need.Contract]),
      )
    }

    // The caller answers that there is no contract for the key.
    "not leak an exception raised after answering Need.Key" in {
      val command = exercise("Fetcher", "AfterKey", Value.ValueParty(alice))
      check(engine, command, asked = Some(classOf[Result.Need.Key]))
    }

    // The contract is of a template from a package that is not loaded yet.
    "not leak an exception raised after answering Need.Package" in {
      val command = exercise("Fetcher", "AfterInterface", Value.ValueContractId(implCid))
      val implTemplate =
        Ref.Identifier(ImplPackage.pkgId, Ref.QualifiedName.assertFromString("N:Impl"))
      val contracts =
        Map(implCid -> contractInstance(ImplPackage.pkg.pkgName, implTemplate, implCid))
      val extraPkgs = Map(ImplPackage.pkgId -> ImplPackage.pkg)
      check(
        unloadedEngine,
        command,
        contracts = contracts,
        extraPkgs = extraPkgs,
        asked = Some(classOf[Result.Need.Package]),
      )
    }
  }
}

object EngineExceptionLeakTest {
  private case class Run(needs: Vector[Result.Need[?]], outcome: Either[Throwable, Option[Error]])
}
