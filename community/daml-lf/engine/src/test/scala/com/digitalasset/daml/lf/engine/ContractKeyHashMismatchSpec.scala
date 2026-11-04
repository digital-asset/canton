// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package engine

import com.digitalasset.canton.logging.SuppressingLogging
import com.digitalasset.daml.lf.command.{ApiCommand, ApiCommands}
import com.digitalasset.daml.lf.crypto.{Hash, SValueHash}
import com.digitalasset.daml.lf.data.{Bytes, ImmArray, Ref, Time}
import com.digitalasset.daml.lf.engine.Error.Interpretation as IErr
import com.digitalasset.daml.lf.engine.Result.lookupHandler
import com.digitalasset.daml.lf.interpretation.{InterpretationConfig, Error as IE}
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
  SerializationVersion,
}
import com.digitalasset.daml.lf.value.{ContractIdVersion, Value}
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class ContractKeyHashMismatchSpec
    extends AnyWordSpec
    with Matchers
    with Inside
    with SuppressingLogging {

  implicit private val parserParameters: ParserParameters[this.type] =
    ParserParameters(
      defaultPackageId = Ref.PackageId.assertFromString("-key-mismatch-test-pkg-"),
      languageVersion = LanguageVersion.devLfVersion,
    )

  private val pkgId = parserParameters.defaultPackageId

  private val tuple2 = {
    val id = StablePackages.stablePackages.Tuple2
    s"'${id.packageId}':${id.qualifiedName}"
  }

  private val pkg =
    p"""
    metadata ( 'key-mismatch-test-pkg' : '1.0.0' )
    module M {
      record @serializable View = { };
      interface (this: I) = {
        viewtype M:View;
        method getCtrls: List Party;
        choice @nonConsuming ITouch (self) (arg: Unit) : Unit,
          controllers (call_method @M:I getCtrls this)
          to upure @Unit ();
      };

      record @serializable T = { sig1: Party, sig2: Party };
      template (this: T) = {
        precondition True;
        signatories Cons @Party [M:T {sig1} this, M:T {sig2} this] (Nil @Party);
        observers Nil @Party;
        choice @nonConsuming Touch (self) (arg: Unit) : Unit,
          controllers Cons @Party [M:T {sig1} this] (Nil @Party)
          to upure @Unit ();
        implements M:I {
          view = M:View {};
          method getCtrls = Cons @Party [M:T {sig1} this] (Nil @Party);
        };
        key @Party (M:T {sig1} this) (\ (p: Party) -> Cons @Party [p] (Nil @Party));
      };

      record @serializable Helper = { p: Party };
      template (this: Helper) = {
        precondition True;
        signatories Cons @Party [M:Helper {p} this] (Nil @Party);
        observers Nil @Party;
        choice @nonConsuming FetchById (self) (cid: ContractId M:T) : Unit,
          controllers Cons @Party [M:Helper {p} this] (Nil @Party)
          to ubind _: M:T <- fetch_template @M:T cid in upure @Unit ();
        choice @nonConsuming FetchByInterface (self) (cid: ContractId M:I) : Unit,
          controllers Cons @Party [M:Helper {p} this] (Nil @Party)
          to ubind _: M:I <- fetch_interface @M:I cid in upure @Unit ();
        choice @nonConsuming FetchByKey (self) (k: Party) : Unit,
          controllers Cons @Party [M:Helper {p} this] (Nil @Party)
          to ubind _: $tuple2 (ContractId M:T) M:T <- fetch_by_key @M:T k
             in upure @Unit ();
        choice @nonConsuming LookupByKey (self) (k: Party) : Unit,
          controllers Cons @Party [M:Helper {p} this] (Nil @Party)
          to ubind _: Option (List ($tuple2 (ContractId M:T) M:T)) <- query_n_by_key @M:T 1 k
             in upure @Unit ();
      };
    }
  """

  private val allPkgs = StablePackages.stablePackages.packagesMap + (pkgId -> pkg)

  private val alice = Ref.Party.assertFromString("Alice")
  private val bob = Ref.Party.assertFromString("Bob")
  private val participantId = Ref.ParticipantId.assertFromString("participant")
  private val submissionSeed = Hash.hashPrivateKey("ContractKeyMismatchSpec")
  private val let = Time.Timestamp.now()

  private def id(name: String) = Ref.Identifier(pkgId, Ref.QualifiedName.assertFromString(name))

  private val templateId = id("M:T")
  private val interfaceId = id("M:I")
  private val helperId = id("M:Helper")
  private val cid =
    Value.ContractId.V1.assertBuild(Hash.hashPrivateKey("keyed"), Bytes.assertFromString("00"))

  private def keyWithMaintainers(
      key: Ref.Party,
      hashedKey: Ref.Party,
      maintainers: Set[Ref.Party],
  ) =
    GlobalKeyWithMaintainers(
      GlobalKey(
        templateId = templateId,
        packageName = pkg.pkgName,
        key = Value.ValueParty(key),
        hash = SValueHash.assertHashContractKey(
          pkg.pkgName,
          templateId.qualifiedName,
          SValue.SParty(hashedKey),
        ),
      ),
      maintainers,
    )

  private val correctKey = keyWithMaintainers(alice, alice, Set(alice))
  // Same key value and maintainers, but a wrong hash.
  private val wrongHashKey = keyWithMaintainers(alice, bob, Set(alice))
  // Correct hash and maintainers, but a wrong key value.
  private val wrongValueKey = keyWithMaintainers(bob, alice, Set(alice))
  // Correct hash and key value, but wrong maintainers (still a subset of the signatories).
  private val wrongMaintainersKey = keyWithMaintainers(alice, alice, Set(bob))

  private def choice(name: String) = Ref.ChoiceName.assertFromString(name)

  private def helperCommand(choiceName: String, arg: Value) =
    ApiCommand.CreateAndExercise(
      helperId.toRef,
      Value.ValueRecord(None, ImmArray(None -> Value.ValueParty(alice))),
      choice(choiceName),
      arg,
    )

  private val operations = List[(String, ApiCommand)](
    "exercise-by-id" ->
      ApiCommand.Exercise(templateId.toRef, cid, choice("Touch"), Value.ValueUnit),
    "exercise-by-key" ->
      ApiCommand.ExerciseByKey(
        templateId.toRef,
        Value.ValueParty(alice),
        choice("Touch"),
        Value.ValueUnit,
      ),
    "exercise-by-interface" ->
      ApiCommand.Exercise(interfaceId.toRef, cid, choice("ITouch"), Value.ValueUnit),
    "fetch-by-id" -> helperCommand("FetchById", Value.ValueContractId(cid)),
    "fetch-by-interface" -> helperCommand("FetchByInterface", Value.ValueContractId(cid)),
    "fetch-by-key" -> helperCommand("FetchByKey", Value.ValueParty(alice)),
    "lookup-by-key" -> helperCommand("LookupByKey", Value.ValueParty(alice)),
  )

  private val packageMap = Map(pkgId -> (pkg.pkgName -> pkg.pkgVersion))

  private def newEngine(): Engine = {
    val engine = new Engine(Engine.DevConfig, loggerFactory)
    engine.preloadPackage(pkgId, pkg).consume(lookupHandler(pkgs = allPkgs)) shouldBe Right(())
    engine
  }

  private def contract(declaredKey: GlobalKeyWithMaintainers): FatContractInstance =
    TransactionBuilder.fatContractInstanceWithDummyDefaults(
      version = SerializationVersion.minContractKeys,
      contractId = cid,
      packageName = pkg.pkgName,
      template = templateId,
      arg = Value.ValueRecord(
        None,
        ImmArray(None -> Value.ValueParty(alice), None -> Value.ValueParty(bob)),
      ),
      signatories = List(alice, bob),
      contractKeyWithMaintainers = Some(declaredKey),
    )

  private def run(
      cmd: ApiCommand,
      declaredKey: GlobalKeyWithMaintainers,
  ) = {
    val inst = contract(declaredKey)
    newEngine()
      .submit(
        packageMap = packageMap,
        packagePreference = packageMap.keySet,
        submitters = Set(alice),
        cmds = ApiCommands(ImmArray(cmd), let, "key-mismatch-test"),
        participantId = participantId,
        submissionSeed = submissionSeed,
        contractIdVersion = ContractIdVersion.V1,
        interpretationConfig = InterpretationConfig.Dev,
        prefetchKeys = Seq.empty,
      )
      // The key handler always returns the contract under test, whatever the key being asked.
      // This lets the declared key of the contract differ from the key used for the lookup.
      .consume(
        lookupHandler(
          pcs = Map(cid -> inst),
          pkgs = allPkgs,
          keys = { case _ =>
            Vector(inst)
          },
        )
      )
  }

  private def expectValidationFailed(
      cmd: ApiCommand,
      declaredKey: GlobalKeyWithMaintainers,
      expectedMsg: String,
  ) =
    inside(run(cmd, declaredKey)) {
      case Left(
            IErr(IErr.DamlException(IE.Upgrade(validationFailed: IE.Upgrade.ValidationFailed)), _)
          ) =>
        validationFailed.coid shouldBe cid
        validationFailed.originalKeyOpt shouldBe Some(declaredKey)
        validationFailed.recomputedKeyOpt shouldBe Some(correctKey)
        validationFailed.msg should include(expectedMsg)
    }

  operations.foreach { case (operation, cmd) =>
    operation should {

      "accept a correct key" in {
        inside(run(cmd, correctKey))
      }

      "reject a wrong key hash" in {
        inside(run(cmd, wrongHashKey)) {
          case Left(
                IErr(
                  IErr.DamlException(
                    IE.Upgrade(error: IE.Upgrade.ValidationFailed)
                  ),
                  _,
                )
              ) =>
            error.coid shouldBe cid
            error.originalKeyOpt.map(_.globalKey.hash) shouldBe Some(wrongHashKey.globalKey.hash)
            error.recomputedKeyOpt.map(_.globalKey.hash) shouldBe Some(correctKey.globalKey.hash)
        }
      }

      "reject a wrong key value" in {
        expectValidationFailed(
          cmd,
          wrongValueKey,
          "key value mismatch",
        )
      }

      "reject wrong key maintainers" in {
        expectValidationFailed(
          cmd,
          wrongMaintainersKey,
          "key maintainers mismatch",
        )
      }
    }
  }
}
