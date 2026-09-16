// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package speedy

import com.digitalasset.canton.logging.SuppressingLogging
import com.digitalasset.daml.lf.crypto.{Hash, SValueHash}
import com.digitalasset.daml.lf.data.Ref.Party
import com.digitalasset.daml.lf.data.{FrontStack, ImmArray, Ref}
import com.digitalasset.daml.lf.language.Ast.Package
import com.digitalasset.daml.lf.language.LanguageVersion
import com.digitalasset.daml.lf.speedy.SValue.{SContractId, SParty, SUnit}
import com.digitalasset.daml.lf.speedy.SpeedyTestLib.typeAndCompile
import com.digitalasset.daml.lf.speedy.TestPkg.*
import com.digitalasset.daml.lf.testing.parser.Implicits.SyntaxHelper
import com.digitalasset.daml.lf.testing.parser.ParserParameters
import com.digitalasset.daml.lf.transaction.test.TransactionBuilder
import com.digitalasset.daml.lf.transaction.{GlobalKeyWithMaintainers, SerializationVersion}
import com.digitalasset.daml.lf.value.Value
import org.scalatest.Inside
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks

import scala.collection.immutable.ArraySeq

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

  // Section testing exceptions thrown when computing the metadata of a contract
  {
    val parserParameters =
      defaultParserParameters.copy(languageVersion = LanguageVersion.defaultLfVersion)

    // A package that defines an interface, a key type, an exception, and a party to be used by
    // the packages defined below.
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

            record @serializable Key = { label: Text, maintainers: List Party };

            record @serializable Ex = { message: Text } ;
            exception Ex = {
              message \(e: Mod:Ex) -> Mod:Ex {message} e
            };

            val mkParty : Text -> Party = \(t:Text) -> case TEXT_TO_PARTY t of None -> ERROR @Party "none" | Some x -> x;
            val alice : Party = Mod:mkParty "Alice";
          }
      """ (parserParameters.copy(defaultPackageId = commonDefsPkgId))

    /** An abstract class whose [[templateDefinition]] method generates LF code that defines a
      * template named [[templateName]]. The class is meant to be extended by concrete case objects
      * which override one the metadata's expressions with an expression that throws an exception.
      */
    abstract class TemplateGenerator(val templateName: String) {
      def precondition = """True"""
      def signatories = s"""Cons @Party [Mod:$templateName {p} this] (Nil @Party)"""
      def observers = """Nil @Party"""
      def key =
        s"""
           |  '$commonDefsPkgId':Mod:Key {
           |    label = "test-key",
           |    maintainers = (Cons @Party [Mod:$templateName {p} this] (Nil @Party))
           |  }""".stripMargin
      def choiceControllers = s"""Cons @Party [Mod:$templateName {p} this] (Nil @Party)"""
      def choiceObservers = """Nil @Party"""

      def maintainers =
        s"""\\(key: '$commonDefsPkgId':Mod:Key) -> ('$commonDefsPkgId':Mod:Key {maintainers} key)"""

      def templateDefinition: String =
        s"""
           |  record @serializable $templateName = { p: Party };
           |  template (this: $templateName) = {
           |    precondition $precondition;
           |    signatories $signatories;
           |    observers $observers;
           |
           |    choice @nonConsuming SomeChoice (self) (u: Unit): Text
           |      , controllers (Cons @Party [Mod:$templateName {p} this] (Nil @Party))
           |      , observers (Nil @Party)
           |      to upure @Text "SomeChoice was called";
           |
           |    implements '$commonDefsPkgId':Mod:Iface {
           |      view = '$commonDefsPkgId':Mod:MyUnit {};
           |      method myChoiceControllers = $choiceControllers;
           |      method myChoiceObservers = $choiceObservers;
           |    };
           |
           |    key @'$commonDefsPkgId':Mod:Key ($key) ($maintainers);
           |  };""".stripMargin
    }

    case class ValidMetadata(override val templateName: String)
        extends TemplateGenerator(templateName)

    case object FailingPrecondition extends TemplateGenerator("Precondition") {
      override def precondition =
        s"""throw @Bool @'$commonDefsPkgId':Mod:Ex ('$commonDefsPkgId':Mod:Ex {message = "Precondition"})"""
    }
    case object FailingSignatories extends TemplateGenerator("Signatories") {
      override def signatories =
        s"""throw @(List Party) @'$commonDefsPkgId':Mod:Ex ('$commonDefsPkgId':Mod:Ex {message = "Signatories"})"""
    }
    case object FailingObservers extends TemplateGenerator("Observers") {
      override def observers =
        s"""throw @(List Party) @'$commonDefsPkgId':Mod:Ex ('$commonDefsPkgId':Mod:Ex {message = "Observers"})"""
    }
    case object FailingKey extends TemplateGenerator("Key") {
      override def key =
        s"""throw @'$commonDefsPkgId':Mod:Key @'$commonDefsPkgId':Mod:Ex ('$commonDefsPkgId':Mod:Ex {message = "Key"})"""
    }
    case object FailingMaintainers extends TemplateGenerator("Maintainers") {
      override def maintainers =
        s"""throw @('$commonDefsPkgId':Mod:Key -> List Party) @'$commonDefsPkgId':Mod:Ex ('$commonDefsPkgId':Mod:Ex {message = "Maintainers"})"""
    }
    case object FailingMaintainersBody extends TemplateGenerator("MaintainersBody") {
      override def maintainers =
        s"""\\(key: '$commonDefsPkgId':Mod:Key) -> throw @(List Party) @'$commonDefsPkgId':Mod:Ex ('$commonDefsPkgId':Mod:Ex {message = "MaintainersBody"})"""
    }
    case object FailingChoiceControllers extends TemplateGenerator("ChoiceControllers") {
      override def choiceControllers =
        s"""throw @(List Party) @'$commonDefsPkgId':Mod:Ex ('$commonDefsPkgId':Mod:Ex {message = "ChoiceControllers"})"""
    }
    case object FailingChoiceObservers extends TemplateGenerator("ChoiceObservers") {
      override def choiceObservers =
        s"""throw @(List Party) @'$commonDefsPkgId':Mod:Ex ('$commonDefsPkgId':Mod:Ex {message = "ChoiceObservers"})"""
    }

    val templateDefsPkgName = Ref.PackageName.assertFromString("-template-defs-")

    /** A package that defines templates called Precondition, Signatories, ... whose metadata should
      * evaluate without throwing exceptions.
      */
    val templateDefsV1PkgId = Ref.PackageId.assertFromString("-template-defs-v1-id-")
    val templateDefsV1ParserParams = parserParameters.copy(defaultPackageId = templateDefsV1PkgId)
    val templateDefsV1Pkg =
      p"""metadata ( '$templateDefsPkgName' : '1.0.0' )
          module Mod {
            ${ValidMetadata("Precondition").templateDefinition}
            ${ValidMetadata("Signatories").templateDefinition}
            ${ValidMetadata("Observers").templateDefinition}
            ${ValidMetadata("Key").templateDefinition}
            ${ValidMetadata("Maintainers").templateDefinition}
            ${ValidMetadata("MaintainersBody").templateDefinition}
            ${ValidMetadata("ChoiceControllers").templateDefinition}
            ${ValidMetadata("ChoiceObservers").templateDefinition}
          }
      """ (templateDefsV1ParserParams)

    /** Version 2 of the package above. It upgrades the previously defined templates such that:
      *   - the precondition in the Precondition template is changed to throw an exception
      *   - the signatories in the Signatories template is changed to throw an exception
      *   - etc.
      */
    val templateDefsV2PkgId = Ref.PackageId.assertFromString("-template-defs-v2-id-")
    val templateDefsV2ParserParams = parserParameters.copy(defaultPackageId = templateDefsV2PkgId)
    val templateDefsV2Pkg =
      p"""metadata ( '$templateDefsPkgName' : '2.0.0' )
          module Mod {
            ${FailingPrecondition.templateDefinition}
            ${FailingSignatories.templateDefinition}
            ${FailingObservers.templateDefinition}
            ${FailingKey.templateDefinition}
            ${FailingMaintainers.templateDefinition}
            ${FailingMaintainersBody.templateDefinition}
            ${FailingChoiceControllers.templateDefinition}
            ${FailingChoiceObservers.templateDefinition}
          }
      """ (templateDefsV2ParserParams)

    // A choice on the Test template below: creates a $templateName instance, catching the
    // exception thrown by its metadata, if any.
    def createAndCatchErrorChoice(pkgId: Ref.PackageId, templateName: String): String = {
      val tplQualifiedName = s"'$pkgId':Mod:$templateName"
      s"""
         |    choice CreateAndCatchError$templateName (self) (u: Unit): Unit,
         |      controllers Cons @Party [Mod:Test {party} this] (Nil @Party),
         |      observers (Nil @Party)
         |      to try @Unit
         |           (ubind _: (ContractId $tplQualifiedName) <-
         |              create @$tplQualifiedName ($tplQualifiedName { p = '$commonDefsPkgId':Mod:alice })
         |            in upure @Unit ())
         |         catch
         |           e -> Some @(Update Unit) (upure @Unit ());""".stripMargin
    }

    val testTemplateDefinition: String =
      s"""
         |  record @serializable Test = { party: Party };
         |  template (this: Test) = {
         |    precondition True;
         |    signatories Cons @Party [Mod:Test {party} this] (Nil @Party);
         |    observers (Nil @Party);
         |${createAndCatchErrorChoice(templateDefsV2PkgId, "Precondition")}
         |${createAndCatchErrorChoice(templateDefsV2PkgId, "Signatories")}
         |${createAndCatchErrorChoice(templateDefsV2PkgId, "Observers")}
         |${createAndCatchErrorChoice(templateDefsV2PkgId, "Key")}
         |${createAndCatchErrorChoice(templateDefsV2PkgId, "Maintainers")}
         |${createAndCatchErrorChoice(templateDefsV2PkgId, "MaintainersBody")}
         |  };
      """.stripMargin

    val metadataTestsPkgId = Ref.PackageId.assertFromString("-metadata-tests-id-")
    val metadataTestsParserParams = parserParameters.copy(defaultPackageId = metadataTestsPkgId)
    val metadataTestsPkg =
      p"""metadata ( '-metadata-tests-' : '1.0.0' )
          module Mod {
            $testTemplateDefinition
          }
    """ (metadataTestsParserParams)

    // Command.FetchByKey/QueryNByKey compile to definitions that construct the stable Tuple2
    // record internally, so the stable Tuple2 package must be part of the package map even
    // though no DAML source below references it syntactically.
    val compiledPackages: PureCompiledPackages =
      PureCompiledPackages.assertBuild(
        Map(
          stablePackages.Tuple2.packageId -> stablePackages.packagesMap(
            stablePackages.Tuple2.packageId
          ),
          commonDefsPkgId -> commonDefsPkg,
          templateDefsV1PkgId -> templateDefsV1Pkg,
          templateDefsV2PkgId -> templateDefsV2Pkg,
          metadataTestsPkgId -> metadataTestsPkg,
        ),
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

    val failingTemplateMetadataTemplates: List[String] = List(
      FailingPrecondition.templateName,
      FailingSignatories.templateName,
      FailingObservers.templateName,
      FailingKey.templateName,
      FailingMaintainers.templateName,
      FailingMaintainersBody.templateName,
    )

    val failingChoiceMetadataTemplates: List[String] = List(
      FailingChoiceControllers.templateName,
      FailingChoiceObservers.templateName,
    )

    val testTemplateId = Ref.Identifier.assertFromString(s"$metadataTestsPkgId:Mod:Test")
    val testPayload = SValue.SRecord(
      testTemplateId,
      ImmArray(Ref.Name.assertFromString("party")),
      ArraySeq(SParty(alice)),
    )

    s"metadata exceptions can be caught when creating a contract" - {
      for (templateName <- failingTemplateMetadataTemplates) {
        templateName in {
          val (result, _) = runCmdFlow(
            pkgs = compiledPackages,
            setup = CmdFlow.submit(Command.Create(testTemplateId, testPayload)),
            test = cid =>
              CmdFlow.submit(
                Command.ExerciseTemplate(
                  testTemplateId,
                  asSCid(cid),
                  Ref.ChoiceName.assertFromString(s"CreateAndCatchError$templateName"),
                  SValue.SUnit,
                )
              ),
            parties = Set(alice),
            packageResolution = Map.empty,
          )
          result shouldBe Right(SUnit)
        }
      }
    }

    s"metadata exceptions cannot be caught" - {
      val ifaceId = Ref.Identifier.assertFromString(s"$commonDefsPkgId:Mod:Iface")
      val keyBasedPrefixes =
        Set("exerciseByKeyAndCatchError", "fetchByKeyAndCatchError", "lookUpByKeyAndCatchError")

      // Builds the Command corresponding to a given test-kind prefix: the "risky" action that
      // should fail to have its metadata exception caught.
      def riskyCommand(prefix: String, v2TemplateId: Ref.Identifier, arg: SValue): Command =
        prefix match {
          case "exerciseAndCatchError" =>
            Command.ExerciseTemplate(v2TemplateId, asSCid(arg), n"SomeChoice", SValue.SUnit)
          case "exerciseByKeyAndCatchError" =>
            Command.ExerciseByKey(v2TemplateId, arg, n"SomeChoice", SValue.SUnit)
          case "fetchAndCatchError" =>
            Command.FetchTemplate(v2TemplateId, asSCid(arg))
          case "fetchByInterfaceAndCatchError" =>
            Command.FetchInterface(ifaceId, asSCid(arg))
          case "fetchByKeyAndCatchError" =>
            Command.FetchByKey(v2TemplateId, arg)
          case "lookUpByKeyAndCatchError" =>
            Command.QueryNByKey(v2TemplateId, SValue.SInt64(1L), arg)
          case "exerciseByInterfaceAndCatchError" =>
            Command.ExerciseInterface(ifaceId, asSCid(arg), n"MyChoice", SValue.SUnit)
        }

      // A test case is a LF test method prefix and a list of relevant template names to test.
      val testCases = List[(String, List[String])](
        "exerciseAndCatchError" -> failingTemplateMetadataTemplates,
        "exerciseByKeyAndCatchError" -> failingTemplateMetadataTemplates,
        "fetchAndCatchError" -> failingTemplateMetadataTemplates,
        "fetchByInterfaceAndCatchError" -> failingTemplateMetadataTemplates,
        "fetchByKeyAndCatchError" -> failingTemplateMetadataTemplates,
        "lookUpByKeyAndCatchError" -> failingTemplateMetadataTemplates,
        "exerciseByInterfaceAndCatchError" ->
          (failingTemplateMetadataTemplates ++ failingChoiceMetadataTemplates),
      )

      for ((prefix, relevantTemplates) <- testCases) {
        prefix - {
          for (templateName <- relevantTemplates) {
            templateName - {
              val v1TemplateId =
                Ref.Identifier.assertFromString(s"$templateDefsV1PkgId:Mod:$templateName")
              val v2TemplateId =
                Ref.Identifier.assertFromString(s"$templateDefsV2PkgId:Mod:$templateName")
              val v1Payload = SValue.SRecord(
                v1TemplateId,
                ImmArray(Ref.Name.assertFromString("p")),
                ArraySeq(SParty(alice)),
              )
              val cid = Value.ContractId.V1(Hash.hashPrivateKey("abc"))
              val key = SValue.SRecord(
                Ref.Identifier.assertFromString(s"$commonDefsPkgId:Mod:Key"),
                ImmArray(
                  Ref.Name.assertFromString("label"),
                  Ref.Name.assertFromString("maintainers"),
                ),
                ArraySeq(
                  SValue.SText("test-key"),
                  SValue.SList(FrontStack(SValue.SParty(alice))),
                ),
              )
              val globalKey = GlobalKeyWithMaintainers(
                templateId = v1TemplateId,
                value = key.toNormalizedValue,
                valueHash = SValueHash.assertHashContractKey(
                  packageName = templateDefsPkgName,
                  templateName = v1TemplateId.qualifiedName,
                  key = key,
                ),
                maintainers = Set(alice),
                packageName = templateDefsPkgName,
              )
              val globalContract = TransactionBuilder.fatContractInstanceWithDummyDefaults(
                version = SerializationVersion.StableVersions.max,
                packageName = templateDefsV1Pkg.pkgName,
                template = v1TemplateId,
                arg = Value.ValueRecord(None, ImmArray(None -> Value.ValueParty(alice))),
                signatories = List(alice),
                observers = List.empty,
                contractKeyWithMaintainers = Some(globalKey),
                contractId = cid,
              )

              for (origin <- contractOrigins) {
                origin.description in {
                  val (result, _) = origin match {
                    case Global =>
                      runCmdFlow(
                        pkgs = compiledPackages,
                        test = _ =>
                          CmdFlow.submit(
                            riskyCommand(
                              prefix,
                              v2TemplateId,
                              if (keyBasedPrefixes(prefix)) key else SContractId(cid),
                            )
                          ),
                        parties = Set(alice),
                        packageResolution = Map(templateDefsPkgName -> templateDefsV2PkgId),
                        getContract = Map(globalContract.contractId -> globalContract),
                        getKeys = Map(globalKey.globalKey -> Vector(globalContract)),
                      )
                    case Local =>
                      runCmdFlow(
                        pkgs = compiledPackages,
                        setup = CmdFlow.submit(Command.Create(v1TemplateId, v1Payload)),
                        test = createdCid =>
                          CmdFlow.submit(
                            riskyCommand(
                              prefix,
                              v2TemplateId,
                              if (keyBasedPrefixes(prefix)) key else createdCid,
                            )
                          ),
                        parties = Set(alice),
                        packageResolution = Map(templateDefsPkgName -> templateDefsV2PkgId),
                      )
                  }
                  inside(result) { case Left(UnhandledExceptionError(msg)) =>
                    msg should include(templateName)
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

class UncatchableExceptionTestWithUpdateMachine
    extends UncatchableExceptionTestBase
    with CmdFlowRunnerWithUpdateMachine

class UncatchableExceptionTestWithTransactionConductor
    extends UncatchableExceptionTestBase
    with CmdFlowRunnerWithTransactionConductor
