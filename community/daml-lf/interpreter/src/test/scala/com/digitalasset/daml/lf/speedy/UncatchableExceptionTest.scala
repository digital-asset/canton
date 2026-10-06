// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package speedy

import com.digitalasset.canton.logging.{NamedLoggingContext, SuppressingLogging}
import com.digitalasset.daml.lf.crypto.{Hash, SValueHash}
import com.digitalasset.daml.lf.data.Ref.Party
import com.digitalasset.daml.lf.data.{FrontStack, ImmArray, Ref}
import com.digitalasset.daml.lf.language.Ast.Package
import com.digitalasset.daml.lf.language.{LanguageVersion, PackageInterface}
import com.digitalasset.daml.lf.speedy.SValue.{SContractId, SParty, SUnit}
import com.digitalasset.daml.lf.speedy.SpeedyTestLib.typeAndCompile
import com.digitalasset.daml.lf.speedy.TestPkg.*
import com.digitalasset.daml.lf.testing.parser.Implicits.SyntaxHelper
import com.digitalasset.daml.lf.testing.parser.ParserParameters
import com.digitalasset.daml.lf.transaction.test.TransactionBuilder
import com.digitalasset.daml.lf.transaction.{GlobalKeyWithMaintainers, SerializationVersion}
import com.digitalasset.daml.lf.validation.Validation
import com.digitalasset.daml.lf.value.Value
import org.scalatest.Inside
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks

import scala.collection.immutable.ArraySeq

/** Test support for writing transactions as data: an LF interpreter for a small `Action` language,
  * where the placement of exception handlers is an input of the test rather than generated code.
  *
  * An `Action` is one of:
  *   - `Do kind`: a ledger operation, implemented by the `doUpdate` function supplied by the user
  *   - `Throw`: an ordinary, catchable user exception
  *   - `Catch a`: runs `a` inside a try/catch whose handler swallows any exception
  *   - `Child a`: runs `a` in a nested exercise of the `Run` choice of the same contract
  *
  * The LF definitions are meant to be embedded in module `Mod` of a test package; see
  * [[definitions]] and [[runChoice]].
  */
private object Actions {

  val throwMessage = "Throw"

  /** LF definitions to embed in module `Mod`: the `Action` and `RunArg` types, `doUpdate`, and the
    * interpreter `run`. `doUpdateBody` is an LF lambda of type `ContractId Unit -> keyType ->
    * kindType -> Update Unit`; `exceptionType` needs a `Text` field `message`.
    */
  def definitions(
      testTemplate: String,
      kindType: String,
      keyType: String,
      exceptionType: String,
      doUpdateBody: String,
  ): String =
    s"""
       variant @serializable Action =
         Do: $kindType |
         Throw: Unit |
         Catch: Mod:Action |
         Child: Mod:Action;

       record @serializable RunArg = { cid: ContractId Unit, key: $keyType, action: Mod:Action };

       val doUpdate : ContractId Unit -> $keyType -> $kindType -> Update Unit = $doUpdateBody;

       val run : ContractId $testTemplate -> Mod:RunArg -> Mod:Action -> Update Unit =
         \\(self: ContractId $testTemplate) -> \\(env: Mod:RunArg) -> \\(a: Mod:Action) ->
           case a of
             Mod:Action:Do k -> Mod:doUpdate (Mod:RunArg {cid} env) (Mod:RunArg {key} env) k |
             Mod:Action:Throw u -> throw @(Update Unit) @$exceptionType ($exceptionType { message = "$throwMessage" }) |
             Mod:Action:Catch b -> try @Unit (Mod:run self env b) catch e -> Some @(Update Unit) (upure @Unit ()) |
             Mod:Action:Child b -> exercise @$testTemplate Run self (Mod:RunArg { cid = Mod:RunArg {cid} env, key = Mod:RunArg {key} env, action = b });
       """

  /** The choice `Run` to add to the template `testTemplate` passed to [[definitions]]. */
  def runChoice(controllers: String): String =
    s"""choice @nonConsuming Run (self) (env: Mod:RunArg): Unit, controllers $controllers, observers (Nil @Party) to Mod:run self env (Mod:RunArg {action} env);"""

  /** Action expressions, as LF source. `kind` is an expression of the type `kindType` above. */
  def perform(kind: String): String = s"Mod:Action:Do ($kind)"
  val throws: String = "Mod:Action:Throw ()"
  def catchAround(action: String): String = s"Mod:Action:Catch ($action)"
  def child(action: String): String = s"Mod:Action:Child ($action)"

  /** The argument of the `Run` choice. */
  def runArg(
      pkgId: Ref.PackageId,
      cid: SValue.SContractId,
      key: SValue,
      action: SValue,
  ): SValue.SRecord =
    SValue.SRecord(
      Ref.Identifier(pkgId, Ref.QualifiedName.assertFromString("Mod:RunArg")),
      ImmArray("cid", "key", "action").map(Ref.Name.assertFromString),
      ArraySeq(cid, key, action),
    )

  /** Evaluates a pure LF expression to a value. */
  def eval[P](
      pkgs: CompiledPackages,
      parserParameters: ParserParameters[P],
      src: String,
  )(implicit loggingContext: NamedLoggingContext): SValue =
    Speedy.Machine.runPureExpr(e"$src" (parserParameters), pkgs) match {
      case Right(value) => value
      case Left(err) => sys.error(s"cannot evaluate `$src`: $err")
    }

  def evalRecord[P](
      pkgs: CompiledPackages,
      parserParameters: ParserParameters[P],
      src: String,
  )(implicit loggingContext: NamedLoggingContext): SValue.SRecord =
    eval(pkgs, parserParameters, src) match {
      case record: SValue.SRecord => record
      case other => sys.error(s"`$src` evaluated to $other, expected a record")
    }
}

// TEST_EVIDENCE: Integrity: Exceptions, throw/catch.
//
// Dual-execution version (UpdateMachine and TransactionConductor) of the "uncatchable
// exceptions" and "metadata exceptions" tests originally in ExceptionTest.scala.
abstract class UncatchableExceptionTestBase
    extends AnyFreeSpec
    with CmdFlowRunner
    with Inside
    with Matchers
    with TableDrivenPropertyChecks
    with SuppressingLogging {

  private val stablePackages =
    com.digitalasset.daml.lf.stablepackages.StablePackages.stablePackages

  private val alice = Party.assertFromString("Alice")

  implicit val defaultParserParameters: ParserParameters[this.type] = ParserParameters.default
  val defaultPackageId = defaultParserParameters.defaultPackageId

  protected val UnhandledExceptionError: UnhandledExceptionError

  "uncatchable exceptions" - {
    val pkg: Package = p"""
        metadata ( 'pkg' : '1.0.0' )

        module M {

          record @serializable MyUnit = {};

          record @serializable E = { msg: Text } ;
          exception E = { message \(e: M:E) -> M:E {msg} e };

          record @serializable T = { party: Party, viewFails: Bool };

          interface (this: I) = {
            viewtype M:MyUnit;
            method parties: List Party;
            choice @nonConsuming Noop (self) (u: Unit) : Unit,
              controllers (call_method @M:I parties this),
              observers Nil @Party
              to upure @Unit ();
            choice @nonConsuming BodyCrash (self) (u: Unit) : Unit,
              controllers (call_method @M:I parties this),
              observers Nil @Party
              to upure @Unit (throw @Unit @M:E (M:E { msg = "E" }));
            choice @nonConsuming ControllersCrash (self) (u: Unit) : Unit,
              controllers throw @(List Party) @M:E (M:E { msg = "E" }),
              observers Nil @Party
              to upure @Unit ();
            choice @nonConsuming ObserversCrash (self) (u: Unit) : Unit,
              controllers (call_method @M:I parties this),
              observers throw @(List Party) @M:E (M:E { msg = "E" })
              to upure @Unit ();
          };

          template (this: T) = {
            precondition True;
            signatories Cons @Party [M:T {party} this] Nil @Party;
            observers Nil @Party;
            choice @nonConsuming BodyCrash (self) (u: Unit) : Unit,
              controllers Cons @Party [M:T {party} this] Nil @Party,
              observers Nil @Party
              to upure @Unit (throw @Unit @M:E (M:E { msg = "E" }));
            choice @nonConsuming ControllersCrash (self) (u: Unit) : Unit,
              controllers throw @(List Party) @M:E (M:E { msg = "E" }),
              observers Nil @Party
              to upure @Unit ();
            choice @nonConsuming ObserversCrash (self) (u: Unit) : Unit,
              controllers Cons @Party [M:T {party} this] Nil @Party,
              observers throw @(List Party) @M:E (M:E { msg = "E" })
              to upure @Unit ();
            implements M:I {
              view = case (M:T {viewFails} this) of
                  False -> M:MyUnit {}
                | True -> throw @M:MyUnit @M:E (M:E { msg = "E" });
               method parties = Cons @Party [M:T {party} this] Nil @Party;
            };
          };

          val runTest : M:T -> (ContractId M:T -> Update Unit) -> Update Unit =
            \(payload: M:T) -> \(action: ContractId M:T -> Update Unit) ->
              ubind
                cid : ContractId M:T <- create @M:T payload
              in try @Unit (action cid) catch e -> Some @(Update Unit) (upure @Unit ());

          record @serializable Test = { party: Party };

          template (this: Test) = {
            precondition True;
            signatories Cons @Party [M:Test {party} this] Nil @Party;
            observers Nil @Party;
            choice TemplateBodyCrash (self) (u: Unit) : Unit,
              controllers Cons @Party [M:Test {party} this] Nil @Party,
              observers Nil @Party
              to M:runTest (M:T {party = M:Test {party} this, viewFails = False}) (\(cid: ContractId M:T) -> exercise @M:T BodyCrash cid ());
            choice TemplateControllersCrash (self) (u: Unit) : Unit,
              controllers Cons @Party [M:Test {party} this] Nil @Party,
              observers Nil @Party
              to M:runTest (M:T {party = M:Test {party} this, viewFails = False}) (\(cid: ContractId M:T) -> exercise @M:T ControllersCrash cid ());
            choice TemplateObserversCrash (self) (u: Unit) : Unit,
              controllers Cons @Party [M:Test {party} this] Nil @Party,
              observers Nil @Party
              to M:runTest (M:T {party = M:Test {party} this, viewFails = False}) (\(cid: ContractId M:T) -> exercise @M:T ObserversCrash cid ());
            choice InterfaceBodyCrash (self) (u: Unit) : Unit,
              controllers Cons @Party [M:Test {party} this] Nil @Party,
              observers Nil @Party
              to M:runTest (M:T {party = M:Test {party} this, viewFails = False}) (\(cid: ContractId M:T) -> exercise_interface @M:I BodyCrash (COERCE_CONTRACT_ID @M:T @M:I cid) ());
            choice InterfaceControllersCrash (self) (u: Unit) : Unit,
              controllers Cons @Party [M:Test {party} this] Nil @Party,
              observers Nil @Party
              to M:runTest (M:T {party = M:Test {party} this, viewFails = False}) (\(cid: ContractId M:T) -> exercise_interface @M:I ControllersCrash (COERCE_CONTRACT_ID @M:T @M:I cid) ());
            choice InterfaceObserversCrash (self) (u: Unit) : Unit,
              controllers Cons @Party [M:Test {party} this] Nil @Party,
              observers Nil @Party
              to M:runTest (M:T {party = M:Test {party} this, viewFails = False}) (\(cid: ContractId M:T) -> exercise_interface @M:I ObserversCrash (COERCE_CONTRACT_ID @M:T @M:I cid) ());
          };
        }
  """

    val pkgs: PureCompiledPackages = typeAndCompile(pkg, cmdMode = cmdMode)

    val testTemplate = Ref.Identifier.assertFromString(s"$defaultPackageId:M:Test")
    val testPayload = SValue.SRecord(
      testTemplate,
      ImmArray(Ref.Name.assertFromString("party")),
      ArraySeq(SParty(alice)),
    )

    val testCases = Table[String, Ref.ChoiceName](
      ("description", "choiceName"),
      "exception thrown by the evaluation of the choice body during exercise by template can be caught" ->
        Ref.ChoiceName.assertFromString("TemplateBodyCrash"),
      "exception thrown by the evaluation of the choice controllers during exercise by template cannot be caught" ->
        Ref.ChoiceName.assertFromString("TemplateControllersCrash"),
      "exception thrown by the evaluation of the choice observers during exercise by template cannot be caught" ->
        Ref.ChoiceName.assertFromString("TemplateObserversCrash"),
      "exception thrown by the evaluation of the choice body during exercise by interface can be caught" ->
        Ref.ChoiceName.assertFromString("InterfaceBodyCrash"),
      "exception thrown by the evaluation of the choice controllers during exercise by interface cannot be caught" ->
        Ref.ChoiceName.assertFromString("InterfaceControllersCrash"),
      "exception thrown by the evaluation of the choice observers during exercise by interface cannot be caught" ->
        Ref.ChoiceName.assertFromString("InterfaceObserversCrash"),
    )

    forEvery(testCases) { (description, choiceName) =>
      description in {
        val (result, _) = runCmdFlow(
          pkgs = pkgs,
          setup = CmdFlow.submit(Command.Create(testTemplate, testPayload)),
          test = cid =>
            CmdFlow.submit(
              Command.ExerciseTemplate(testTemplate, asSCid(cid), choiceName, SValue.SUnit)
            ),
          parties = Set(alice),
          packageResolution = Map(pkg.pkgName -> defaultParserParameters.defaultPackageId),
        )
        if (description.contains("can be caught"))
          result shouldBe Right(SUnit)
        else if (description.contains("cannot be caught"))
          inside(result) { case Left(UnhandledExceptionError(_)) =>
          }
        else
          sys.error("the description should contains \"can be caught\" or \"cannot be caught\"")
      }
    }
  }

  // Exceptions thrown when computing the metadata of a contract
  {
    val parserParameters =
      defaultParserParameters.copy(languageVersion = LanguageVersion.defaultLfVersion)

    // An interface, a key type, an exception and a party shared by the packages below.
    val commonDefsPkgId = Ref.PackageId.assertFromString("-common-defs-v1-")
    val commonDefsPkg =
      p"""metadata ( '-common-defs-' : '1.0.0' )
          module Mod {
            record @serializable MyUnit = {};
            interface (this : Iface) = {
              viewtype Mod:MyUnit;

              method myChoiceControllers : List Party;
              method myChoiceObservers : List Party;

              choice @nonConsuming MyChoice (self) (u: Unit): Text
                  , controllers (call_method @Mod:Iface myChoiceControllers this)
                  , observers (call_method @Mod:Iface myChoiceObservers this)
                  to upure @Text "MyChoice was called";
            };

            // Selects which metadata expression of the -fail-defs- V2 template throws.
            enum @serializable Fail = NoFailure | Precondition | Signatories | Observers | Key | Maintainers | ChoiceControllers | ChoiceObservers;
            record @serializable FKey = { label: Text, maintainers: List Party, fail: Mod:Fail };

            record @serializable Ex = { message: Text } ;
            exception Ex = {
              message \(e: Mod:Ex) -> Mod:Ex {message} e
            };

            val mkParty : Text -> Party = \(t:Text) -> case TEXT_TO_PARTY t of None -> ERROR @Party "none" | Some x -> x;
            val alice : Party = Mod:mkParty "Alice";
          }
      """ (parserParameters.copy(defaultPackageId = commonDefsPkgId))

    val failDefsPkgName = Ref.PackageName.assertFromString("-fail-defs-")

    // Template T whose metadata throws as selected by its `fail` field if `checkFail`, else never.
    //
    // For example, with `checkFail = true` and the payload `T { p = Alice, fail = Signatories }`,
    // the generated template reads (simplified, `Fail:X` stands for the `Fail` enum constructor X):
    //
    //   template (this: T) = {
    //     precondition (case fail of Fail:Precondition -> throw Ex "Precondition" | _ -> True);
    //     signatories  (case fail of Fail:Signatories -> throw Ex "Signatories" | _ -> [p]);  // throws here
    //     observers    (case fail of Fail:Observers -> throw Ex "Observers" | _ -> []);
    //     implements Iface {
    //       method myChoiceControllers = (case fail of Fail:ChoiceControllers -> throw ... | _ -> [p]);
    //       method myChoiceObservers   = (case fail of Fail:ChoiceObservers -> throw ... | _ -> []);
    //     };
    //     key @FKey (case fail of Fail:Key -> throw ... | _ -> FKey {label, maintainers = [p], fail})
    //                (\k -> case k.fail of Fail:Maintainers -> throw ... | _ -> k.maintainers);
    //   };
    //
    // With `checkFail = false` every `case` is replaced by its non-throwing branch, so the
    // template never throws whatever `fail` is.
    def failTemplateDefinition(checkFail: Boolean): String = {
      val c = commonDefsPkgId
      val keyType = s"'$c':Mod:FKey"
      val partyList = "Cons @Party [Mod:T {p} this] (Nil @Party)"
      val keyValue =
        s"""$keyType { label = "test-key", maintainers = ($partyList), fail = Mod:T {fail} this }"""

      def throwing(failure: String, resultType: String): String =
        s"""throw @$resultType @'$c':Mod:Ex ('$c':Mod:Ex {message = "$failure"})"""

      def guarded(failure: String, resultType: String, valid: String): String =
        if (checkFail)
          s"(case Mod:T {fail} this of '$c':Mod:Fail:$failure -> ${throwing(failure, resultType)} | _ -> $valid)"
        else s"($valid)"

      val maintainers =
        if (checkFail) {
          val maintainersThrow = throwing("Maintainers", "(List Party)")
          s"""(\\(k: $keyType) -> case $keyType {fail} k of '$c':Mod:Fail:Maintainers -> $maintainersThrow | _ -> $keyType {maintainers} k)"""
        } else s"(\\(k: $keyType) -> $keyType {maintainers} k)"

      s"""
         record @serializable T = { p: Party, fail: '$c':Mod:Fail };
         template (this: T) = {
           precondition ${guarded("Precondition", "Bool", "True")};
           signatories ${guarded("Signatories", "(List Party)", partyList)};
           observers ${guarded("Observers", "(List Party)", "Nil @Party")};

           choice @nonConsuming SomeChoice (self) (u: Unit): Text
             , controllers (Cons @Party [Mod:T {p} this] (Nil @Party))
             , observers (Nil @Party)
             to upure @Text "SomeChoice was called";

           implements '$c':Mod:Iface {
             view = '$c':Mod:MyUnit {};
             method myChoiceControllers = ${guarded(
          "ChoiceControllers",
          "(List Party)",
          partyList,
        )};
             method myChoiceObservers = ${guarded("ChoiceObservers", "(List Party)", "Nil @Party")};
           };

           key @$keyType ${guarded("Key", keyType, keyValue)} $maintainers;
         };"""
    }

    val failDefsV1PkgId = Ref.PackageId.assertFromString("-fail-defs-v1-id-")
    val failDefsV1Pkg =
      p"""metadata ( '$failDefsPkgName' : '1.0.0' )
          module Mod {
            ${failTemplateDefinition(checkFail = false)}
          }
      """ (parserParameters.copy(defaultPackageId = failDefsV1PkgId))

    val failDefsV2PkgId = Ref.PackageId.assertFromString("-fail-defs-v2-id-")
    val failDefsV2Pkg =
      p"""metadata ( '$failDefsPkgName' : '2.0.0' )
          module Mod {
            ${failTemplateDefinition(checkFail = true)}
          }
      """ (parserParameters.copy(defaultPackageId = failDefsV2PkgId))

    val failV2Template = s"'$failDefsV2PkgId':Mod:T"
    val ifaceType = s"'$commonDefsPkgId':Mod:Iface"
    val failKeyType = s"'$commonDefsPkgId':Mod:FKey"
    val tuple2Type = {
      import stablePackages.Tuple2
      s"'${Tuple2.packageId}':${Tuple2.qualifiedName}"
    }

    // The operations selectable by the Action interpreter, as bodies of the `doUpdate` cases.
    def coerceCid(target: String) = s"(COERCE_CONTRACT_ID @Unit @$target cid)"
    val updateKindBodies = List(
      "Create" ->
        s"ubind _: ContractId $failV2Template <- create @$failV2Template ($failV2Template { p = '$commonDefsPkgId':Mod:alice, fail = $failKeyType {fail} key }) in upure @Unit ()",
      "Exercise" ->
        s"ubind _: Text <- exercise @$failV2Template SomeChoice ${coerceCid(failV2Template)} () in upure @Unit ()",
      "ExerciseByKey" ->
        s"ubind _: Text <- exercise_by_key @$failV2Template SomeChoice key () in upure @Unit ()",
      "Fetch" ->
        s"ubind _: $failV2Template <- fetch_template @$failV2Template ${coerceCid(failV2Template)} in upure @Unit ()",
      "FetchByInterface" ->
        s"ubind _: $ifaceType <- fetch_interface @$ifaceType ${coerceCid(ifaceType)} in upure @Unit ()",
      "FetchByKey" ->
        s"ubind _: $tuple2Type (ContractId $failV2Template) $failV2Template <- fetch_by_key @$failV2Template key in upure @Unit ()",
      "QueryNByKey" ->
        s"ubind _: Option (List ($tuple2Type (ContractId $failV2Template) $failV2Template)) <- query_n_by_key @$failV2Template 1 key in upure @Unit ()",
      "ExerciseByInterface" ->
        s"ubind _: Text <- exercise_interface @$ifaceType MyChoice ${coerceCid(ifaceType)} () in upure @Unit ()",
    )
    val updateKinds: List[String] = updateKindBodies.map(_._1)
    val updateKindNames: String = updateKinds.mkString(" | ")

    val doUpdateCases: String =
      updateKindBodies.map { case (k, body) => s"Mod:UpdateKind:$k -> $body" }.mkString(" | ")
    val doUpdateBody: String =
      s"""\\(cid: ContractId Unit) -> \\(key: $failKeyType) -> \\(kind: Mod:UpdateKind) -> case kind of $doUpdateCases"""

    val actionDefinitions: String = Actions.definitions(
      testTemplate = "Mod:Test",
      kindType = "Mod:UpdateKind",
      keyType = failKeyType,
      exceptionType = s"'$commonDefsPkgId':Mod:Ex",
      doUpdateBody = doUpdateBody,
    )

    val testTemplateDefinition: String =
      s"""
         |  record @serializable Test = { party: Party };
         |  template (this: Test) = {
         |    precondition True;
         |    signatories Cons @Party [Mod:Test {party} this] (Nil @Party);
         |    observers (Nil @Party);
         |    ${Actions.runChoice("Cons @Party [Mod:Test {party} this] (Nil @Party)")}
         |  };
      """.stripMargin

    val metadataTestsPkgId = Ref.PackageId.assertFromString("-metadata-tests-id-")
    val metadataTestsParserParams = parserParameters.copy(defaultPackageId = metadataTestsPkgId)
    val metadataTestsPkg =
      p"""metadata ( '-metadata-tests-' : '1.0.0' )
          module Mod {
            $testTemplateDefinition
            enum @serializable UpdateKind = $updateKindNames;
            $actionDefinitions
          }
    """ (metadataTestsParserParams)

    // Command.FetchByKey/QueryNByKey compile to definitions that construct the stable Tuple2
    // record internally, so the stable Tuple2 package must be part of the package map even
    // though no DAML source below references it syntactically.
    val allPackages: Map[Ref.PackageId, Package] = Map(
      stablePackages.Tuple2.packageId -> stablePackages.packagesMap(
        stablePackages.Tuple2.packageId
      ),
      commonDefsPkgId -> commonDefsPkg,
      failDefsV1PkgId -> failDefsV1Pkg,
      failDefsV2PkgId -> failDefsV2Pkg,
      metadataTestsPkgId -> metadataTestsPkg,
    )
    // The Action interpreter is hand-written LF, so typecheck it.
    Validation.unsafeCheckPackages(PackageInterface(allPackages), allPackages)
    val compiledPackages: PureCompiledPackages =
      PureCompiledPackages.assertBuild(
        allPackages,
        Compiler.Config.Default.copy(cmdMode = cmdMode),
      )

    sealed trait ContractOrigin {
      def description: String
    }
    case object Global extends ContractOrigin {
      override def description: String = "global contract"
    }
    case object Local extends ContractOrigin {
      override def description: String = "local contract"
    }

    val contractOrigins: List[ContractOrigin] = List(Global, Local)

    val testTemplateId = Ref.Identifier.assertFromString(s"$metadataTestsPkgId:Mod:Test")
    val testPayload = SValue.SRecord(
      testTemplateId,
      ImmArray(Ref.Name.assertFromString("party")),
      ArraySeq(SParty(alice)),
    )

    // The transaction is data (see Actions): a sequence of nested exercises of the Run choice of
    // a Test contract, where a handler can be placed around any of them.
    "metadata exceptions and exception handlers" - {
      val noFailure = "NoFailure"
      val templateLevelFailures =
        List("Precondition", "Signatories", "Observers", "Key", "Maintainers")
      val choiceLevelFailures = List("ChoiceControllers", "ChoiceObservers")

      def relevantFailures(kind: String): List[String] =
        if (kind == "ExerciseByInterface") templateLevelFailures ++ choiceLevelFailures
        else templateLevelFailures

      // Where the handler sits relative to the risky action, and whether it is around it.
      val shapes = List[(String, String => String, Boolean)](
        ("bare", a => a, false),
        ("catch in the same exercise body", a => Actions.child(Actions.catchAround(a)), true),
        ("catch in the parent exercise", a => Actions.catchAround(Actions.child(a)), true),
      )

      val v1TemplateId = Ref.Identifier.assertFromString(s"$failDefsV1PkgId:Mod:T")
      val commonAlice = s"'$commonDefsPkgId':Mod:alice"
      val packageResolution = Map(
        failDefsPkgName -> failDefsV2PkgId,
        metadataTestsPkg.pkgName -> metadataTestsPkgId,
      )

      // A V1 contract whose payload and key both carry `fail`.
      def payloadAndKey(failure: String): (SValue.SRecord, SValue.SRecord) = {
        val failValue = s"'$commonDefsPkgId':Mod:Fail:$failure"
        val payload = Actions.evalRecord(
          compiledPackages,
          metadataTestsParserParams,
          s"'$failDefsV1PkgId':Mod:T { p = $commonAlice, fail = $failValue }",
        )
        val key = Actions.evalRecord(
          compiledPackages,
          metadataTestsParserParams,
          s"""'$commonDefsPkgId':Mod:FKey { label = "test-key", maintainers = Cons @Party [$commonAlice] (Nil @Party), fail = $failValue }""",
        )
        (payload, key)
      }

      def runCommand(
          testCid: SValue,
          targetCid: SValue.SContractId,
          key: SValue,
          action: SValue,
      ): Command =
        Command.ExerciseTemplate(
          testTemplateId,
          asSCid(testCid),
          n"Run",
          Actions.runArg(metadataTestsPkgId, targetCid, key, action),
        )

      // Runs `action` against a V1 contract with the given `failure` that gets upgraded to V2.
      def runAction(
          origin: ContractOrigin,
          failure: String,
          action: String,
      ): Either[Throwable, SValue] = {
        val (payload, key) = payloadAndKey(failure)
        val actionValue = Actions.eval(compiledPackages, metadataTestsParserParams, action)
        val setupTest = CmdFlow.submit(Command.Create(testTemplateId, testPayload))
        origin match {
          case Global =>
            val cid = Value.ContractId.V1(Hash.hashPrivateKey("abc"))
            val globalKey = GlobalKeyWithMaintainers(
              templateId = v1TemplateId,
              value = key.toNormalizedValue,
              valueHash = SValueHash.assertHashContractKey(
                packageName = failDefsPkgName,
                templateName = v1TemplateId.qualifiedName,
                key = key,
              ),
              maintainers = Set(alice),
              packageName = failDefsPkgName,
            )
            val globalContract = TransactionBuilder.fatContractInstanceWithDummyDefaults(
              version = SerializationVersion.StableVersions.max,
              packageName = failDefsPkgName,
              template = v1TemplateId,
              arg = payload.toNormalizedValue,
              signatories = List(alice),
              observers = List.empty,
              contractKeyWithMaintainers = Some(globalKey),
              contractId = cid,
            )
            val (result, _) = runCmdFlow[SValue](
              pkgs = compiledPackages,
              setup = setupTest,
              test =
                testCid => CmdFlow.submit(runCommand(testCid, SContractId(cid), key, actionValue)),
              parties = Set(alice),
              packageResolution = packageResolution,
              getContract = Map(globalContract.contractId -> globalContract),
              getKeys = Map(globalKey.globalKey -> Vector(globalContract)),
            )
            result
          case Local =>
            val (result, _) = runCmdFlow[SValue](
              pkgs = compiledPackages,
              setup = for {
                testCid <- setupTest
                localCid <- CmdFlow.submit(Command.Create(v1TemplateId, payload))
              } yield SValue.SList(FrontStack(testCid, localCid)),
              test = {
                case SValue.SList(ids) =>
                  ids.iterator.toList match {
                    case List(testCid, localCid) =>
                      CmdFlow.submit(runCommand(testCid, asSCid(localCid), key, actionValue))
                    case other => sys.error(s"unexpected setup result: $other")
                  }
                case other => sys.error(s"unexpected setup result: $other")
              },
              parties = Set(alice),
              packageResolution = packageResolution,
            )
            result
        }
      }

      // Only exceptions thrown while creating a contract can be caught.
      "metadata exceptions" - {
        for (kind <- updateKinds) {
          kind - {
            for (failure <- relevantFailures(kind)) {
              failure - {
                for (origin <- contractOrigins) {
                  origin.description - {
                    for ((shapeName, shape, handled) <- shapes) {
                      shapeName in {
                        val action = shape(Actions.perform(s"Mod:UpdateKind:$kind"))
                        val result = runAction(origin, failure, action)
                        if (kind == "Create" && handled)
                          result shouldBe Right(SUnit)
                        else
                          inside(result) { case Left(UnhandledExceptionError(msg)) =>
                            msg should include(failure)
                          }
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }

      "valid metadata (control)" - {
        for (kind <- updateKinds) {
          kind - {
            for (origin <- contractOrigins) {
              origin.description - {
                for ((shapeName, shape, _) <- shapes) {
                  shapeName in {
                    val action = shape(Actions.perform(s"Mod:UpdateKind:$kind"))
                    runAction(origin, noFailure, action) shouldBe Right(SUnit)
                  }
                }
              }
            }
          }
        }
      }

      "user exceptions (control)" - {
        "are not caught without a handler" in {
          inside(runAction(Global, noFailure, Actions.throws)) {
            case Left(UnhandledExceptionError(msg)) =>
              msg shouldBe Actions.throwMessage
          }
        }
        "are caught by a handler around them" in {
          runAction(Global, noFailure, Actions.catchAround(Actions.throws)) shouldBe Right(SUnit)
        }
        "are caught by a handler in the same exercise body" in {
          runAction(
            Global,
            noFailure,
            Actions.child(Actions.catchAround(Actions.throws)),
          ) shouldBe Right(SUnit)
        }
        "are caught by a handler in the parent exercise" in {
          runAction(
            Global,
            noFailure,
            Actions.catchAround(Actions.child(Actions.throws)),
          ) shouldBe Right(SUnit)
        }
      }
    }
  }

  // The expression evaluating to the key maintainers function is closed (`this` is not in scope),
  // so a field of the payload cannot select a throw there. A dedicated template always throws.
  "exception in the key maintainers expression" - {
    val pkg: Package = p"""
        metadata ( 'maintainers-expr' : '1.0.0' )

        module M {
          record @serializable E = { msg: Text };
          exception E = { message \(e: M:E) -> M:E {msg} e };

          record @serializable Bad = { party: Party };
          template (this: Bad) = {
            precondition True;
            signatories Cons @Party [M:Bad {party} this] Nil @Party;
            observers Nil @Party;
            key @Party (M:Bad {party} this) (throw @(Party -> List Party) @M:E (M:E { msg = "MaintainersExpr" }));
          };

          record @serializable Test = { party: Party };
          template (this: Test) = {
            precondition True;
            signatories Cons @Party [M:Test {party} this] Nil @Party;
            observers Nil @Party;
            choice @nonConsuming Bare (self) (u: Unit) : Unit,
              controllers Cons @Party [M:Test {party} this] Nil @Party,
              observers Nil @Party
              to ubind cid : ContractId M:Bad <- create @M:Bad (M:Bad {party = M:Test {party} this}) in upure @Unit ();
            choice @nonConsuming Caught (self) (u: Unit) : Unit,
              controllers Cons @Party [M:Test {party} this] Nil @Party,
              observers Nil @Party
              to try @Unit (ubind cid : ContractId M:Bad <- create @M:Bad (M:Bad {party = M:Test {party} this}) in upure @Unit ())
                 catch e -> Some @(Update Unit) (upure @Unit ());
          };
        }
    """

    val pkgs: PureCompiledPackages = typeAndCompile(pkg, cmdMode = cmdMode)

    val testTemplate = Ref.Identifier.assertFromString(s"$defaultPackageId:M:Test")
    val testPayload = SValue.SRecord(
      testTemplate,
      ImmArray(Ref.Name.assertFromString("party")),
      ArraySeq(SParty(alice)),
    )

    def run(choice: String): Either[Throwable, SValue] =
      runCmdFlow(
        pkgs = pkgs,
        setup = CmdFlow.submit(Command.Create(testTemplate, testPayload)),
        test = cid =>
          CmdFlow.submit(
            Command.ExerciseTemplate(
              testTemplate,
              asSCid(cid),
              Ref.ChoiceName.assertFromString(choice),
              SValue.SUnit,
            )
          ),
        parties = Set(alice),
        packageResolution = Map(pkg.pkgName -> defaultParserParameters.defaultPackageId),
      )._1

    "bare" in {
      inside(run("Bare")) { case Left(UnhandledExceptionError(msg)) =>
        msg should include("MaintainersExpr")
      }
    }

    "caught by a handler around the create" in {
      run("Caught") shouldBe Right(SUnit)
    }
  }
}

class UncatchableExceptionTestWithUpdateMachine
    extends UncatchableExceptionTestBase
    with CmdFlowRunnerWithUpdateMachine

class UncatchableExceptionTestWithTransactionConductor
    extends UncatchableExceptionTestBase
    with CmdFlowRunnerWithTransactionConductor
