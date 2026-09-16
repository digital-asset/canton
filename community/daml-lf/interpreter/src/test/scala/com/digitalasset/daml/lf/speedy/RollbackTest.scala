// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package speedy

import com.digitalasset.canton.logging.SuppressingLogging
import com.digitalasset.daml.lf.data.Ref.Party
import com.digitalasset.daml.lf.data.{ImmArray, Ref}
import com.digitalasset.daml.lf.interpretation.Error as IE
import com.digitalasset.daml.lf.speedy.SValue.*
import com.digitalasset.daml.lf.speedy.TestPkg.*
import com.digitalasset.daml.lf.testing.parser.Implicits.SyntaxHelper
import com.digitalasset.daml.lf.testing.parser.ParserParameters
import com.digitalasset.daml.lf.transaction.{Node, NodeId, SubmittedTransaction}
import com.digitalasset.daml.lf.value.Value.{ValueInt64, ValueRecord}
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks

import scala.collection.immutable.ArraySeq

// Dual-execution (UpdateMachine and TransactionConductor), dual-config (Legacy and Default)
// version of the rollback-shape tests.
abstract class RollbackTestBase
    extends AnyFreeSpec
    with CmdFlowRunner
    with Matchers
    with TableDrivenPropertyChecks
    with SuppressingLogging {

  import RollbackTest.*

  // Legacy (protocol v3.4) allows an effect (create/exercise) inside a rolled-back scope.
  // Default (protocol v3.5+) forbids it and crashes with EffectfulRollback instead.
  protected def legacy: Boolean

  private[this] val interpretationConfig: interpretation.InterpretationConfig =
    if (legacy) interpretation.InterpretationConfig.Legacy
    else interpretation.InterpretationConfig.Default

  // The only scenarios with an effect inside a rollback: uncatchable under Default.
  private[this] val effectfulRollbackChoices =
    Set("Create3ThrowAndCatch", "Create3ThrowAndOuterCatch", "Exer2")

  private[this] implicit val defaultParserParameters: ParserParameters[RollbackTestBase.this.type] =
    ParserParameters.default

  private[this] val alice = Party.assertFromString("Alice")

  val pkgs: PureCompiledPackages = SpeedyTestLib.typeAndCompile(
    p"""
  metadata ( 'pkg' : '1.0.0' )

  module M {

    record @serializable MyException = { message: Text } ;
    exception MyException = {
      message \(e: M:MyException) -> M:MyException {message} e
    };

    record @serializable T1 = { party: Party, info: Int64 } ;
    template (record : T1) = {
      precondition True;
      signatories Cons @Party [M:T1 {party} record] (Nil @Party);
      observers Nil @Party;
      choice Ch1 (self) (i : Unit) : Unit,
        controllers Cons @Party [M:T1 {party} record] (Nil @Party)
        to
          ubind
            x1: ContractId M:T1 <- create @M:T1 M:T1 { party = M:T1 {party} record, info = 400 };
            x2: ContractId M:T1 <- create @M:T1 M:T1 { party = M:T1 {party} record, info = 500 }
          in upure @Unit ();
      choice Ch2 (self) (i : Unit) : Unit,
        controllers Cons @Party [M:T1 {party} record] (Nil @Party)
        to
          ubind
            x1: ContractId M:T1 <- create @M:T1 M:T1 { party = M:T1 {party} record, info = 400 };
            u: Unit <- throw @(Update Unit) @M:MyException (M:MyException {message = "oops"});
            x2: ContractId M:T1 <- create @M:T1 M:T1 { party = M:T1 {party} record, info = 500 }
          in upure @Unit ();
    };

    val create0 : Party -> Update Unit = \(party: Party) ->
        upure @Unit ();

    val create1 : Party -> Update Unit = \(party: Party) ->
        ubind
          x1: ContractId M:T1 <- create @M:T1 M:T1 { party = party, info = 100 }
        in upure @Unit ();

    val create2 : Party -> Update Unit = \(party: Party) ->
        ubind
          x1: ContractId M:T1 <- create @M:T1 M:T1 { party = party, info = 100 };
          x2: ContractId M:T1 <- create @M:T1 M:T1 { party = party, info = 200 }
        in upure @Unit ();

    val create3 : Party -> Update Unit = \(party: Party) ->
        ubind
          x1: ContractId M:T1 <- create @M:T1 M:T1 { party = party, info = 100 };
          x2: ContractId M:T1 <- create @M:T1 M:T1 { party = party, info = 200 };
          x3: ContractId M:T1 <- create @M:T1 M:T1 { party = party, info = 300 }
        in upure @Unit ();

    val create3nested : Party -> Update Unit = \(party: Party) ->
        ubind
          u1: Unit <-
            ubind
              x1: ContractId M:T1 <- create @M:T1 M:T1 { party = party, info = 100 };
              x2: ContractId M:T1 <- create @M:T1 M:T1 { party = party, info = 200 }
            in upure @Unit ();
          x3: ContractId M:T1 <- create @M:T1 M:T1 { party = party, info = 300 }
        in upure @Unit ();

    val create3catchNoThrow : Party -> Update Unit = \(party: Party) ->
        ubind
          u1: Unit <-
            try @Unit
              ubind
                x1: ContractId M:T1 <- create @M:T1 M:T1 { party = party, info = 100 };
                x2: ContractId M:T1 <- create @M:T1 M:T1 { party = party, info = 200 }
              in upure @Unit ()
            catch e -> Some @(Update Unit) (upure @Unit ())
          ;
          x3: ContractId M:T1 <- create @M:T1 M:T1 { party = party, info = 300 }
        in upure @Unit ();

    val create3throwAndCatch : Party -> Update Unit = \(party: Party) ->
        ubind
          u1: Unit <-
            try @Unit
              ubind
                x1: ContractId M:T1 <- create @M:T1 M:T1 { party = party, info = 100 };
                x2: ContractId M:T1 <- create @M:T1 M:T1 { party = party, info = 200 }
              in throw @(Update Unit) @M:MyException (M:MyException {message = "oops"})
            catch e -> Some @(Update Unit) (upure @Unit ())
          ;
          x3: ContractId M:T1 <- create @M:T1 M:T1 { party = party, info = 300 }
        in upure @Unit ();

    val create3throwAndOuterCatch : Party -> Update Unit = \(party: Party) ->
        ubind
          u1: Unit <-
            try @Unit
              try @Unit
                ubind
                  x1: ContractId M:T1 <- create @M:T1 M:T1 { party = party, info = 100 };
                  x2: ContractId M:T1 <- create @M:T1 M:T1 { party = party, info = 200 }
                in throw @(Update Unit) @M:MyException (M:MyException {message = "oops"})
              catch e -> None @(Update Unit)
            catch e -> Some @(Update Unit) (upure @Unit ())
          ;
          x3: ContractId M:T1 <- create @M:T1 M:T1 { party = party, info = 300 }
        in upure @Unit ();


    val exer1 : Party -> Update Unit = \(party: Party) ->
        ubind
          x1: ContractId M:T1 <- create @M:T1 M:T1 { party = party, info = 100 };

          u: Unit <-
            try @Unit
              ubind
                u: Unit <- exercise @M:T1 Ch1 x1 ();
                x2: ContractId M:T1 <- create @M:T1 M:T1 { party = party, info = 200 }
              in upure @Unit ()
            catch e -> Some @(Update Unit) (upure @Unit ());

          x3: ContractId M:T1 <- create @M:T1 M:T1 { party = party, info = 300 }
        in upure @Unit ();


    val exer2 : Party -> Update Unit = \(party: Party) ->
        ubind
          x1: ContractId M:T1 <- create @M:T1 M:T1 { party = party, info = 100 };

          u: Unit <-
            try @Unit
              ubind
                u: Unit <- exercise @M:T1 Ch2 x1 ();
                x2: ContractId M:T1 <- create @M:T1 M:T1 { party = party, info = 200 }
              in upure @Unit ()
            catch e -> Some @(Update Unit) (upure @Unit ());

          x3: ContractId M:T1 <- create @M:T1 M:T1 { party = party, info = 300 }
        in upure @Unit ();

    record @serializable Test = { party: Party };
    template (this: Test) = {
      precondition True;
      signatories Cons @Party [M:Test {party} this] Nil @Party;
      observers Nil @Party;
      choice Create0 (self) (u: Unit) : Unit,
        controllers Cons @Party [M:Test {party} this] Nil @Party
        to M:create0 (M:Test {party} this);
      choice Create1 (self) (u: Unit) : Unit,
        controllers Cons @Party [M:Test {party} this] Nil @Party
        to M:create1 (M:Test {party} this);
      choice Create2 (self) (u: Unit) : Unit,
        controllers Cons @Party [M:Test {party} this] Nil @Party
        to M:create2 (M:Test {party} this);
      choice Create3 (self) (u: Unit) : Unit,
        controllers Cons @Party [M:Test {party} this] Nil @Party
        to M:create3 (M:Test {party} this);
      choice Create3Nested (self) (u: Unit) : Unit,
        controllers Cons @Party [M:Test {party} this] Nil @Party
        to M:create3nested (M:Test {party} this);
      choice Create3CatchNoThrow (self) (u: Unit) : Unit,
        controllers Cons @Party [M:Test {party} this] Nil @Party
        to M:create3catchNoThrow (M:Test {party} this);
      choice Create3ThrowAndCatch (self) (u: Unit) : Unit,
        controllers Cons @Party [M:Test {party} this] Nil @Party
        to M:create3throwAndCatch (M:Test {party} this);
      choice Create3ThrowAndOuterCatch (self) (u: Unit) : Unit,
        controllers Cons @Party [M:Test {party} this] Nil @Party
        to M:create3throwAndOuterCatch (M:Test {party} this);
      choice Exer1 (self) (u: Unit) : Unit,
        controllers Cons @Party [M:Test {party} this] Nil @Party
        to M:exer1 (M:Test {party} this);
      choice Exer2 (self) (u: Unit) : Unit,
        controllers Cons @Party [M:Test {party} this] Nil @Party
        to M:exer2 (M:Test {party} this);
    };

   }
  """,
    cmdMode = cmdMode,
  )

  private[this] val testTemplateId =
    Ref.Identifier.assertFromString(s"${defaultParserParameters.defaultPackageId}:M:Test")
  private[this] val testPayload = SRecord(
    testTemplateId,
    ImmArray(Ref.Name.assertFromString("party")),
    ArraySeq(SParty(alice)),
  )

  val testCases = Table[String, List[Tree]](
    ("choice", "expected-number-of-contracts"),
    ("Create0", Nil),
    ("Create1", List(C(100))),
    ("Create2", List(C(100), C(200))),
    ("Create3", List(C(100), C(200), C(300))),
    ("Create3Nested", List(C(100), C(200), C(300))),
    ("Create3CatchNoThrow", List(C(100), C(200), C(300))),
    ("Create3ThrowAndCatch", List[Tree](R(List(C(100), C(200))), C(300))),
    ("Create3ThrowAndOuterCatch", List[Tree](R(List(C(100), C(200))), C(300))),
    ("Exer1", List[Tree](C(100), X(List(C(400), C(500))), C(200), C(300))),
    ("Exer2", List[Tree](C(100), R(List(X(List(C(400))))), C(300))),
  )

  forEvery(testCases) { (choiceName: String, expected: List[Tree]) =>
    val description =
      if (!legacy && effectfulRollbackChoices(choiceName))
        s"$choiceName, expected to crash with EffectfulRollback"
      else
        s"$choiceName, contracts expected: $expected"
    description in {
      val (result, _) = runCmdFlow[SubmittedTransaction](
        pkgs = pkgs,
        setup = CmdFlow.submit(Command.Create(testTemplateId, testPayload)),
        test = cid =>
          for {
            _ <- CmdFlow.submit(
              Command.ExerciseTemplate(
                testTemplateId,
                asSCid(cid),
                Ref.ChoiceName.assertFromString(choiceName),
                SUnit,
              )
            )
            tx <- CmdFlow.commit
          } yield tx,
        parties = Set(alice),
        packageResolution = Map.empty,
        interpretationConfig = interpretationConfig,
      )
      if (!legacy && effectfulRollbackChoices(choiceName)) {
        result match {
          case Left(SError.InterpretationError(IE.EffectfulRollback(_))) => succeed
          case other => fail(s"expected EffectfulRollback, got: $other")
        }
      } else {
        result match {
          case Right(tx) =>
            // The transaction has a root Create (the Test wrapper contract) and a root Exercise
            // (the wrapper choice itself). Skip both, inspect only what the choice body produced.
            val exerciseNode = tx.roots.toList
              .map(tx.nodes)
              .collectFirst { case node: Node.Exercise => node }
              .getOrElse(fail(s"expected an Exercise root, got: ${tx.roots.map(tx.nodes)}"))
            exerciseNode.children.toList
              .flatMap(nid => RollbackTest.treeOf(tx, nid)) shouldBe expected
          case Left(err) => fail(err.toString)
        }
      }
    }
  }
}

class RollbackTestWithUpdateMachineLegacy
    extends RollbackTestBase
    with CmdFlowRunnerWithUpdateMachine {
  override protected def legacy: Boolean = true
}
class RollbackTestWithUpdateMachineKey
    extends RollbackTestBase
    with CmdFlowRunnerWithUpdateMachine {
  override protected def legacy: Boolean = false
}
class RollbackTestWithTransactionConductorLegacy
    extends RollbackTestBase
    with CmdFlowRunnerWithTransactionConductor {
  override protected def legacy: Boolean = true
}
class RollbackTestWithTransactionConductorKey
    extends RollbackTestBase
    with CmdFlowRunnerWithTransactionConductor {
  override protected def legacy: Boolean = false
}

object RollbackTest {

  sealed trait Tree // minimal transaction tree, for purposes of writing test expectation
  final case class C(x: Long) extends Tree // Create Node
  final case class X(x: List[Tree]) extends Tree // Exercise Node
  final case class R(x: List[Tree]) extends Tree // Rollback Node

  def treeOf(tx: SubmittedTransaction, nid: NodeId): List[Tree] =
    tx.nodes(nid) match {
      case create: Node.Create =>
        create.arg match {
          case ValueRecord(_, ImmArray(_, (None, ValueInt64(n)))) =>
            List(C(n))
          case _ =>
            sys.error(s"unexpected create.arg: ${create.arg}")
        }
      case _: Node.LeafOnlyAction =>
        Nil
      case node: Node.Exercise =>
        List(X(node.children.toList.flatMap(nid => treeOf(tx, nid))))
      case node: Node.Rollback =>
        List(R(node.children.toList.flatMap(nid => treeOf(tx, nid))))
    }

  def shapeOfTransaction(tx: SubmittedTransaction): List[Tree] =
    tx.roots.toList.flatMap(nid => treeOf(tx, nid))

}
