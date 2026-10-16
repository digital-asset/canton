// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package engine

import com.digitalasset.canton.logging.SuppressingLogging
import com.digitalasset.daml.lf.command.ReplayCommand
import com.digitalasset.daml.lf.crypto.SValueHash
import com.digitalasset.daml.lf.data.*
import com.digitalasset.daml.lf.data.Ref.*
import com.digitalasset.daml.lf.engine.Result.lookupHandler
import com.digitalasset.daml.lf.interpretation.{ExecutionMode, InterpretationConfig}
import com.digitalasset.daml.lf.language.Ast.*
import com.digitalasset.daml.lf.language.LanguageVersion
import com.digitalasset.daml.lf.speedy.SValue
import com.digitalasset.daml.lf.stablepackages.StablePackages
import com.digitalasset.daml.lf.testing.parser.Implicits.SyntaxHelper
import com.digitalasset.daml.lf.testing.parser.ParserParameters
import com.digitalasset.daml.lf.transaction.test.TransactionBuilder
import com.digitalasset.daml.lf.transaction.{
  FatContractInstance,
  GlobalKey,
  GlobalKeyWithMaintainers,
  Node,
  NodeId,
  SerializationVersion,
  SubmittedTransaction,
  Transaction,
}
import com.digitalasset.daml.lf.value.ContractIdVersion
import com.digitalasset.daml.lf.value.Value.*
import org.scalatest.EitherValues
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.wordspec.AnyWordSpec

import scala.language.implicitConversions

class ReinterpretTestUpdateMachine extends ReinterpretTest(ExecutionMode.UpdateMachine)
class ReinterpretTestConductor extends ReinterpretTest(ExecutionMode.Conductor)

abstract class ReinterpretTest(
    executionMode: ExecutionMode
) extends AnyWordSpec
    with Matchers
    with TableDrivenPropertyChecks
    with EitherValues
    with SuppressingLogging {

  import ReinterpretTest.*

  private[this] val version = SerializationVersion.assign(hasKey = true)

  private def hash(s: String) = crypto.Hash.hashPrivateKey(s)

  private val party = Party.assertFromString("Party")

  implicit private val parserParameters: ParserParameters[this.type] =
    ParserParameters(
      defaultPackageId = PackageId.assertFromString("-reinterpret-test-pkg-"),
      languageVersion = LanguageVersion.stagingLfVersion,
    )

  private val pkgId = parserParameters.defaultPackageId

  private val pkg = p"""
    metadata ( 'reinterpret-test-pkg' : '1.0.0' )
    module M {
      record @serializable E1 = { } ;
      exception E1 = { message \(e: M:E1) -> "E1" } ;

      record @serializable View = { } ;
      interface (this: I) = {
        viewtype M:View;
        method getCtrls: List Party;
        choice @nonConsuming IHello (self) (arg: Unit) : Unit,
          controllers (call_method @M:I getCtrls this)
          to upure @Unit ();
        choice @nonConsuming IThrow (self) (arg: Unit) : Unit,
          controllers (call_method @M:I getCtrls this)
          to throw @(Update Unit) @M:E1 (M:E1 {});
      };

      record @serializable T = { p: Party } ;
      template (this: T) = {
        precondition True;
        signatories Cons @Party [M:T {p} this] (Nil @Party);
        observers Nil @Party;

        choice @nonConsuming Hello (self) (arg: Unit) : Unit,
          controllers Cons @Party [M:T {p} this] (Nil @Party)
          to upure @Unit ();

        choice @nonConsuming Throw (self) (arg: Unit) : Unit,
          controllers Cons @Party [M:T {p} this] (Nil @Party)
          to throw @(Update Unit) @M:E1 (M:E1 {});

        choice @nonConsuming Nested (self) (arg: Unit) : Unit,
          controllers Cons @Party [M:T {p} this] (Nil @Party)
          to exercise @M:T Throw self ();

        choice @nonConsuming NestedCatch (self) (arg: Unit) : Unit,
          controllers Cons @Party [M:T {p} this] (Nil @Party)
          to try @Unit (exercise @M:T Throw self ())
             catch e -> Some @(Update Unit) (upure @Unit ());

        choice @nonConsuming BadOrd (self) (arg: Unit) : Bool,
          controllers Cons @Party [M:T {p} this] (Nil @Party)
          to upure @Bool (EQUAL @(Unit -> Unit) (\(x: Unit) -> x) (\(x: Unit) -> x));

        implements M:I {
          view = M:View {};
          method getCtrls = Cons @Party [M:T {p} this] (Nil @Party);
        };
        key @Party (M:T {p} this) (\(p: Party) -> Cons @Party [p] (Nil @Party));
      };
    }
  """

  private val allPackages = StablePackages.stablePackages.packagesMap + (pkgId -> pkg)

  private val templateId = Identifier(pkgId, "M:T")
  private val interfaceId = Identifier(pkgId, "M:I")
  private val cid = toContractId("M:T:1")

  private val globalKey = GlobalKey(
    templateId,
    pkg.pkgName,
    ValueParty(party),
    SValueHash.assertHashContractKey(
      pkg.pkgName,
      templateId.qualifiedName,
      SValue.SParty(party),
    ),
  )

  private val keyedContract: FatContractInstance =
    TransactionBuilder.fatContractInstanceWithDummyDefaults(
      version = version,
      contractId = cid,
      packageName = pkg.pkgName,
      template = templateId,
      arg = ValueRecord(None, ImmArray((None, ValueParty(party)))),
      signatories = List(party),
      contractKeyWithMaintainers = Some(GlobalKeyWithMaintainers(globalKey, Set(party))),
    )

  private val defaultContracts: Map[ContractId, FatContractInstance] =
    Map(cid -> keyedContract)

  private val keyLookup: PartialFunction[GlobalKey, Vector[FatContractInstance]] =
    Map(globalKey -> Vector(keyedContract))

  private def freshEngine =
    new Engine(Engine.DevConfig.copy(executionMode = executionMode), loggerFactory)

  private val engine = freshEngine

  def Top(xs: Shape*) = Shape.Top(xs.toList)
  def Exercise(xs: Shape*) = Shape.Exercise(xs.toList)
  def Rollback(xs: Shape*) = Shape.Rollback(xs.toList)
  def Create() = Shape.Create()

  val submitters = Set(party)
  val time = Time.Timestamp.now()
  val seed = hash("ReinterpretTests")
  val contractIdVersion = ContractIdVersion.V1

  private def reinterpretCommand(theCommand: ReplayCommand): Either[Error, SubmittedTransaction] = {
    val res = engine
      .reinterpret(
        submitters = submitters,
        command = theCommand,
        nodeSeed = Some(seed),
        preparationTime = time,
        ledgerEffectiveTime = time,
        contractIdVersion = contractIdVersion,
        interpretationConfig = InterpretationConfig.Dev,
        packageResolution = Map(pkg.pkgName -> pkgId),
      )
      .consume(lookupHandler(pcs = defaultContracts, pkgs = allPackages, keys = keyLookup))
    res match {
      case Right((tx, _)) => Right(tx)
      case Left(e) => Left(e)
    }
  }

  private def exercise(choice: String) =
    ReplayCommand.Exercise(templateId, None, cid, choice, ValueUnit)

  private def exerciseByInterface(choice: String) =
    ReplayCommand.Exercise(templateId, Some(interfaceId), cid, choice, ValueUnit)

  private def exerciseByKey(choice: String) =
    ReplayCommand.ExerciseByKey(templateId, ValueParty(party), choice, ValueUnit)

  private def shapeOf(command: ReplayCommand) = {
    val Right(tx) = reinterpretCommand(command)
    Shape.ofTransaction(tx.transaction)
  }

  "Reinterpretation" should {

    "handle exercise by template" in {
      shapeOf(exercise("Hello")) shouldBe Top(Exercise())
      shapeOf(exercise("Throw")) shouldBe Top(Rollback(Exercise()))
    }

    "handle nested exercises by template" in {
      shapeOf(exercise("Nested")) shouldBe Top(Rollback(Exercise(Exercise())))
      shapeOf(exercise("NestedCatch")) shouldBe Top(Exercise(Rollback(Exercise())))
    }

    "handle exercise by interface" in {
      shapeOf(exerciseByInterface("IHello")) shouldBe Top(Exercise())
      shapeOf(exerciseByInterface("IThrow")) shouldBe Top(Rollback(Exercise()))
    }

    "handle exercise by key" in {
      shapeOf(exerciseByKey("Hello")) shouldBe Top(Exercise())
      shapeOf(exerciseByKey("Throw")) shouldBe Top(Rollback(Exercise()))
    }

    "still fail for an uncatchable exception" in {
      val Left(err) = reinterpretCommand(exercise("BadOrd"))
      assert(err.message.contains("Error: functions are not comparable"))
    }
  }

  "exercise by interface pull interfaceId package " in {
    val templatePkgId = Ref.PackageId.assertFromString("-template-package-")
    val templateId = Ref.Identifier(templatePkgId, "A:T")
    val interfacePkgId = Ref.PackageId.assertFromString("-interface-package-")
    val interfaceId = Ref.Identifier(interfacePkgId, "B:I")

    val testCases = Table(
      "cmd" -> "packgeIds",
      ReplayCommand.Create(templateId, ValueUnit) -> List(templatePkgId),
      ReplayCommand.Exercise(templateId, None, toContractId("cid"), "myChoice", ValueUnit) -> List(
        templatePkgId
      ),
      ReplayCommand.Exercise(
        templateId,
        Some(interfaceId),
        toContractId("cid"),
        "myChoice",
        ValueUnit,
      ) -> List(templatePkgId, interfacePkgId),
      ReplayCommand.ExerciseByKey(templateId, ValueUnit, "MyChoice", ValueUnit) -> List(
        templatePkgId
      ),
      ReplayCommand.Fetch(templateId, None, toContractId("cid")) -> List(templatePkgId),
      ReplayCommand.Fetch(templateId, Some(interfaceId), toContractId("cid")) ->
        List(templatePkgId, interfacePkgId),
      ReplayCommand.FetchByKey(templateId, ValueUnit) -> List(templatePkgId),
    )

    forEvery(testCases) { (cmd, pkgIds) =>
      val emptyPackage =
        Package(
          Map.empty,
          Set.empty,
          LanguageVersion.latestStableLfVersion,
          PackageMetadata(
            PackageName.assertFromString("foo"),
            PackageVersion.assertFromString("0.0.0"),
            None,
          ),
          GeneratedImports(
            reason = "package made in com.digitalasset.daml.lf.engine.ReinterpretTest",
            pkgIds = Set.empty,
          ),
        )
      var queriedPackageIds = Set.empty[Ref.PackageId]
      val trackPackageQueries: PartialFunction[Ref.PackageId, Package] = { pkgId =>
        queriedPackageIds = queriedPackageIds + pkgId
        emptyPackage
      }
      freshEngine
        .reinterpret(
          submitters = submitters,
          command = cmd,
          nodeSeed = Some(seed),
          preparationTime = time,
          ledgerEffectiveTime = time,
          contractIdVersion = contractIdVersion,
          interpretationConfig = InterpretationConfig.Default,
        )
        .consume(lookupHandler(pkgs = trackPackageQueries))
      pkgIds.toSet shouldBe queriedPackageIds
    }
  }
}

object ReinterpretTest {

  private implicit def qualifiedNameStr(s: String): QualifiedName =
    QualifiedName.assertFromString(s)

  private implicit def toName(s: String): Name =
    Name.assertFromString(s)

  private val dummySuffix = Bytes.assertFromString("00")

  private def toContractId(s: String): ContractId =
    ContractId.V1.assertBuild(crypto.Hash.hashPrivateKey(s), dummySuffix)

  sealed trait Shape
  object Shape {

    final case class Top(xs: List[Shape])
    final case class Exercise(x: List[Shape]) extends Shape
    final case class Rollback(x: List[Shape]) extends Shape
    final case class Create() extends Shape

    def ofTransaction(tx: Transaction): Top = {
      def ofNid(nid: NodeId): Shape =
        tx.nodes(nid) match {
          case node: Node.Exercise => Exercise(node.children.toList.map(ofNid))
          case node: Node.Rollback => Rollback(node.children.toList.map(ofNid))
          case _: Node.Create => Create()
          case _ => sys.error(s"Shape.ofTransaction, unexpected tx node")
        }
      Top(tx.roots.toList.map(nid => ofNid(nid)))
    }
  }

}
