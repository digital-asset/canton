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
import com.digitalasset.daml.lf.stablepackages.StablePackages
import com.digitalasset.daml.lf.testing.parser.Implicits.SyntaxHelper
import com.digitalasset.daml.lf.testing.parser.ParserParameters
import com.digitalasset.daml.lf.value.{ContractIdVersion, Value}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.util.concurrent.{CountDownLatch, TimeoutException}
import scala.concurrent.duration.*
import scala.concurrent.{Await, Promise}
import scala.util.Try

object TimeLimit {

  // Prevents a hang if a loop never ends, because of a bug in the test or in the engine.
  def withTimeout[A](timeout: FiniteDuration)(body: => A): Either[TimeoutException, A] = {
    val result = Promise[A]()
    val thread = new Thread(() => { val _ = result.complete(Try(body)) })
    thread.setDaemon(true)
    thread.start()
    try Right(Await.result(result.future, timeout))
    catch { case timeoutException: TimeoutException => Left(timeoutException) }
  }
}

// Sanity check of the test infrastructure only. It does not test the engine.
class TimeLimitTest extends AnyWordSpec with Matchers {

  "TimeLimit.withTimeout" should {
    "return the result when the body returns in time" in {
      TimeLimit.withTimeout(10.seconds)(42) shouldBe Right(42)
    }

    "return Left when the body does not return in time" in {
      TimeLimit.withTimeout(100.millis)(new CountDownLatch(1).await()) shouldBe a[Left[?, ?]]
    }
  }
}

class EngineInterruptionTestUpdateMachine
    extends EngineInterruptionTest(ExecutionMode.UpdateMachine)
class EngineInterruptionTestConductor extends EngineInterruptionTest(ExecutionMode.Conductor)

abstract class EngineInterruptionTest(executionMode: ExecutionMode)
    extends AnyWordSpec
    with Matchers
    with SuppressingLogging {

  implicit private val parserParameters: ParserParameters[this.type] =
    ParserParameters(
      defaultPackageId = Ref.PackageId.assertFromString("-interruption-test-pkg-"),
      languageVersion = LanguageVersion.stagingLfVersion,
    )

  private val pkgId = parserParameters.defaultPackageId

  private val pkg = p"""
    metadata ( 'interruption-test-pkg' : '1.0.0' )
    module M {
      val spin : Int64 -> Int64 = \(n: Int64) ->
        case (EQUAL @Int64 n 0) of
            True -> 0
          | _ -> M:spin (SUB_INT64 n 1);

      val forever : Int64 -> Int64 = \(n: Int64) -> M:forever (ADD_INT64 n 1);

      record @serializable Exercising = { party: Party } ;
      template (this: Exercising) = {
        precondition True;
        signatories Cons @Party [M:Exercising {party} this] (Nil @Party);
        observers Nil @Party;

        choice @nonConsuming SpinChoice (self) (arg: Unit) : Unit,
          controllers Cons @Party [M:Exercising {party} this] (Nil @Party)
          to ubind _: Int64 <- upure @Int64 (M:spin 1000) in upure @Unit ();

        choice @nonConsuming ForeverChoice (self) (arg: Unit) : Unit,
          controllers Cons @Party [M:Exercising {party} this] (Nil @Party)
          to ubind _: Int64 <- upure @Int64 (M:forever 0) in upure @Unit ();
      };

      record @serializable SlowSignatory = { party: Party } ;
      template (this: SlowSignatory) = {
        precondition True;
        signatories
          case (EQUAL @Int64 (M:spin 1000) 0) of
              True -> Cons @Party [M:SlowSignatory {party} this] (Nil @Party)
            | False -> Nil @Party;
        observers Nil @Party;
      };

      record @serializable ForeverSignatory = { party: Party } ;
      template (this: ForeverSignatory) = {
        precondition True;
        signatories
          case (EQUAL @Int64 (M:forever 0) 0) of
              True -> Cons @Party [M:ForeverSignatory {party} this] (Nil @Party)
            | False -> Nil @Party;
        observers Nil @Party;
      };
    }
  """

  private val allPkgs = StablePackages.stablePackages.packagesMap + (pkgId -> pkg)

  private val alice = Ref.Party.assertFromString("Alice")

  private def templateId(name: String) =
    Ref.Identifier(pkgId, Ref.QualifiedName.assertFromString(s"M:$name"))

  private val aliceRecord = Value.ValueRecord(None, ImmArray(None -> Value.ValueParty(alice)))

  private def exercise(choiceName: String) =
    ApiCommand.CreateAndExercise(
      templateId("Exercising").toRef,
      aliceRecord,
      Ref.ChoiceName.assertFromString(choiceName),
      Value.ValueUnit,
    )

  private val createSlowSignatory =
    ApiCommand.Create(templateId("SlowSignatory").toRef, aliceRecord)

  private val createForeverSignatory =
    ApiCommand.Create(templateId("ForeverSignatory").toRef, aliceRecord)

  private val stopMarker = Error.Interpretation(
    Error.Interpretation.Internal("EngineInterruptionTest", "stopped by the probe", None),
    None,
  )

  private final class InterruptionProbe(stopAfter: Option[Int]) {
    var count = 0
    var abortedTrace: Option[String] = None

    private val fallback = lookupHandler(pkgs = allPkgs)

    val handler: Result.Handler = new Result.Handler {
      def apply[X](need: Result.Need[X]): Either[Error, X] =
        need match {
          case Result.Need.Interruption(abort) =>
            count += 1
            if (stopAfter.exists(count >= _)) {
              abortedTrace = abort()
              Left(stopMarker)
            } else Right(())
          case other => fallback(other)
        }
    }
  }

  private def newEngine(mode: ExecutionMode): Engine = {
    val engine = new Engine(
      Engine.DevConfig.copy(executionMode = mode, iterationsBetweenInterruptions = 10),
      loggerFactory,
    )
    engine.preloadPackage(pkgId, pkg).consume(lookupHandler(pkgs = allPkgs)) shouldBe Right(())
    engine
  }

  private def submit(engine: Engine, command: ApiCommand, probe: InterruptionProbe) =
    engine
      .submit(
        submitters = Set(alice),
        cmds = ApiCommands(ImmArray(command), Time.Timestamp.Epoch, "interruption-test"),
        participantId = Ref.ParticipantId.assertFromString("participant"),
        submissionSeed = Hash.hashPrivateKey("EngineInterruptionTest"),
        contractIdVersion = ContractIdVersion.V1,
        interpretationConfig = InterpretationConfig.Dev,
        prefetchKeys = Seq.empty,
      )
      .consume(probe.handler)

  private val finiteScenarios = List[(String, ApiCommand, Option[String])](
    ("a slow choice body", exercise("SpinChoice"), Some("M:Exercising:SpinChoice")),
    ("a slow signatories expression", createSlowSignatory, None),
  )

  private val infiniteScenarios = List[(String, ApiCommand)](
    ("an infinite choice body", exercise("ForeverChoice")),
    ("an infinite signatories expression", createForeverSignatory),
  )

  "Engine.submit" should {
    val engine = newEngine(executionMode)

    finiteScenarios.foreach { case (name, command, expectedTrace) =>
      s"ask the host for interruptions during $name" in {
        val probe = new InterruptionProbe(stopAfter = None)
        submit(engine, command, probe) shouldBe a[Right[?, ?]]
        probe.count should be > 0
      }

      s"stop when the host aborts during $name" in {
        val probe = new InterruptionProbe(stopAfter = Some(1))
        submit(engine, command, probe) shouldBe Left(stopMarker)
        probe.abortedTrace shouldBe defined
        expectedTrace.foreach(trace => probe.abortedTrace.get should include(trace))
      }

      s"give the same transaction as the update machine for $name}" in {
        val baseline = newEngine(ExecutionMode.UpdateMachine)
        val actual = submit(engine, command, new InterruptionProbe(stopAfter = None))
        val expected = submit(baseline, command, new InterruptionProbe(stopAfter = None))
        actual.map(_._1.transaction) shouldBe expected.map(_._1.transaction)
      }
    }

    infiniteScenarios.foreach { case (name, command) =>
      s"stop after many interruptions without a stack overflow for $name" in {
        val probe = new InterruptionProbe(stopAfter = Some(10000))
        TimeLimit.withTimeout(1.second)(submit(engine, command, probe)) shouldBe Right(
          Left(stopMarker)
        )
        probe.count shouldBe 10000
      }
    }
  }
}
