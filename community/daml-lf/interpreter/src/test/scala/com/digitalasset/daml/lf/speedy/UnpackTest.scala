// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package speedy

import com.digitalasset.canton.logging.SuppressingLogging
import com.digitalasset.daml.lf.data.*
import com.digitalasset.daml.lf.interpretation.Error as IE
import com.digitalasset.daml.lf.language.{Ast, LanguageVersion}
import com.digitalasset.daml.lf.speedy.SValue.{SValue as _, *}
import com.digitalasset.daml.lf.testing.parser.Implicits.SyntaxHelper
import com.digitalasset.daml.lf.testing.parser.ParserParameters
import com.digitalasset.daml.lf.transaction.SerializationVersion
import com.digitalasset.daml.lf.transaction.test.TransactionBuilder
import com.digitalasset.daml.lf.value.Value
import org.scalatest.Inside
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.immutable.ArraySeq
import scala.util.{Failure, Success}

// unpack_template behaves like fetch_template but skips the activeness and authorization checks.
class UnpackTest extends AnyFreeSpec with Matchers with Inside with SuppressingLogging {

  import EvalHelpers.*

  val alice = Ref.Party.assertFromString("Alice")
  val bob = Ref.Party.assertFromString("Bob")

  val pkgId = Ref.PackageId.assertFromString("-unpack-test-package-")
  implicit val parserParameters: ParserParameters[Any] =
    ParserParameters(defaultPackageId = pkgId, languageVersion = LanguageVersion.v2_dev)

  val pkg =
    p"""metadata ( '-unpack-test-' : '1.0.0' )
        module Mod {
          record @serializable MyUnit = {};
          interface (this: Iface) = {
            viewtype Mod:MyUnit;
          };

          record @serializable Secret = { p: Party, secret: Text };
          template (this: Secret) = {
            precondition True;
            signatories Cons @Party [Mod:Secret {p} this] (Nil @Party);
            observers Nil @Party;
            choice Burn (self) (arg: Unit): Unit,
              controllers Cons @Party [Mod:Secret {p} this] (Nil @Party)
              to upure @Unit ();
            implements Mod:Iface { view = Mod:MyUnit {}; };
          };

          record @serializable Other = { p: Party };
          template (this: Other) = {
            precondition True;
            signatories Cons @Party [Mod:Other {p} this] (Nil @Party);
            observers Nil @Party;
          };
        }
      """

  val compiledPackages = PureCompiledPackages.assertBuild(Map(pkgId -> pkg), Compiler.Config.Dev)

  val Ast.TTyCon(secretTplId) = t"Mod:Secret"
  val Ast.TTyCon(otherTplId) = t"Mod:Other"

  val cid = Value.ContractId.V1(crypto.Hash.hashPrivateKey("unpack-test"))
  val secretPayload = Value.ValueRecord(
    None,
    ImmArray(None -> Value.ValueParty(alice), None -> Value.ValueText("42")),
  )
  val contract = TransactionBuilder.fatContractInstanceWithDummyDefaults(
    version = SerializationVersion.StableVersions.max,
    packageName = pkg.pkgName,
    template = secretTplId,
    arg = secretPayload,
    signatories = List(alice),
    contractId = cid,
  )

  // A contract of a different template, used to test unpack against the wrong type / interface.
  val otherCid = Value.ContractId.V1(crypto.Hash.hashPrivateKey("unpack-test-other"))
  val otherPayload = Value.ValueRecord(None, ImmArray(None -> Value.ValueParty(alice)))
  val otherContract = TransactionBuilder.fatContractInstanceWithDummyDefaults(
    version = SerializationVersion.StableVersions.max,
    packageName = pkg.pkgName,
    template = otherTplId,
    arg = otherPayload,
    signatories = List(alice),
    contractId = otherCid,
  )

  // A contract ID never disclosed to the machine.
  val unknownCid = Value.ContractId.V1(crypto.Hash.hashPrivateKey("unpack-test-unknown"))

  "unpack_template" - {
    "should return the payload of an active, visible contract" in {
      inside(
        evalApp(
          e"\(cid: ContractId Mod:Secret) -> unpack_template @Mod:Secret cid",
          ArraySeq(SContractId(cid), SToken),
          getContract = Map(contract.contractId -> contract),
          compiledPackages = compiledPackages,
          committers = Set(alice),
        )
      ) { case Success(Right(SRecord(`secretTplId`, _, ArraySeq(SParty(`alice`), SText("42"))))) =>
        succeed
      }
    }

    "should return the payload of a contract archived earlier in the same transaction" in {
      inside(
        evalApp(
          e"""\(cid: ContractId Mod:Secret) ->
                ubind _: Unit <- exercise @Mod:Secret Burn cid ()
                in unpack_template @Mod:Secret cid""",
          ArraySeq(SContractId(cid), SToken),
          getContract = Map(contract.contractId -> contract),
          compiledPackages = compiledPackages,
          committers = Set(alice),
        )
      ) { case Success(Right(SRecord(`secretTplId`, _, ArraySeq(SParty(`alice`), SText("42"))))) =>
        succeed
      }
    }

    "should succeed for a committer that is not a stakeholder" in {
      inside(
        evalApp(
          e"\(cid: ContractId Mod:Secret) -> unpack_template @Mod:Secret cid",
          ArraySeq(SContractId(cid), SToken),
          getContract = Map(contract.contractId -> contract),
          compiledPackages = compiledPackages,
          committers = Set(bob),
        )
      ) { case Success(Right(SRecord(`secretTplId`, _, ArraySeq(SParty(`alice`), SText("42"))))) =>
        succeed
      }
    }

    "should return the payload of a contract created earlier in the same transaction" in {
      inside(
        evalApp(
          e"""\(p: Party) ->
                ubind cid: ContractId Mod:Secret <- create @Mod:Secret Mod:Secret { p = p, secret = "42" }
                in unpack_template @Mod:Secret cid""",
          ArraySeq(SParty(alice), SToken),
          compiledPackages = compiledPackages,
          committers = Set(alice),
        )
      ) { case Success(Right(SRecord(`secretTplId`, _, ArraySeq(SParty(`alice`), SText("42"))))) =>
        succeed
      }
    }

    "should fail when the contract is of the wrong template" in {
      inside(
        evalApp(
          e"\(cid: ContractId Mod:Secret) -> unpack_template @Mod:Secret cid",
          ArraySeq(SContractId(otherCid), SToken),
          getContract = Map(otherContract.contractId -> otherContract),
          compiledPackages = compiledPackages,
          committers = Set(alice),
        )
      ) { case Success(Left(SError.InterpretationError(error))) =>
        error shouldBe a[IE.WronglyTypedContract]
      }
    }

    "should fail when the contract is unknown" in {
      inside(
        evalApp(
          e"\(cid: ContractId Mod:Secret) -> unpack_template @Mod:Secret cid",
          ArraySeq(SContractId(unknownCid), SToken),
          getContract = Map.empty,
          compiledPackages = compiledPackages,
          committers = Set(alice),
        )
      ) { case Failure(SpeedyTestLib.UnknownContract(`unknownCid`)) =>
        succeed
      }
    }
  }

  "unpack_interface" - {
    "should return the payload of an active, visible contract" in {
      inside(
        evalApp(
          e"\(cid: ContractId Mod:Iface) -> unpack_interface @Mod:Iface cid",
          ArraySeq(SContractId(cid), SToken),
          packageResolution = Map(pkg.pkgName -> pkgId),
          getContract = Map(contract.contractId -> contract),
          compiledPackages = compiledPackages,
          committers = Set(alice),
        )
      ) {
        case Success(
              Right(
                SAny(
                  Ast.TTyCon(`secretTplId`),
                  SRecord(`secretTplId`, _, ArraySeq(SParty(`alice`), SText("42"))),
                )
              )
            ) =>
          succeed
      }
    }

    "should return the payload of a contract archived earlier in the same transaction" in {
      inside(
        evalApp(
          e"""\(cid: ContractId Mod:Secret) ->
                ubind _: Unit <- exercise @Mod:Secret Burn cid ()
                in unpack_interface @Mod:Iface (COERCE_CONTRACT_ID @Mod:Secret @Mod:Iface cid)""",
          ArraySeq(SContractId(cid), SToken),
          packageResolution = Map(pkg.pkgName -> pkgId),
          getContract = Map(contract.contractId -> contract),
          compiledPackages = compiledPackages,
          committers = Set(alice),
        )
      ) {
        case Success(
              Right(
                SAny(
                  Ast.TTyCon(`secretTplId`),
                  SRecord(`secretTplId`, _, ArraySeq(SParty(`alice`), SText("42"))),
                )
              )
            ) =>
          succeed
      }
    }

    "should succeed for a committer that is not a stakeholder" in {
      inside(
        evalApp(
          e"\(cid: ContractId Mod:Iface) -> unpack_interface @Mod:Iface cid",
          ArraySeq(SContractId(cid), SToken),
          packageResolution = Map(pkg.pkgName -> pkgId),
          getContract = Map(contract.contractId -> contract),
          compiledPackages = compiledPackages,
          committers = Set(bob),
        )
      ) {
        case Success(
              Right(
                SAny(
                  Ast.TTyCon(`secretTplId`),
                  SRecord(`secretTplId`, _, ArraySeq(SParty(`alice`), SText("42"))),
                )
              )
            ) =>
          succeed
      }
    }

    "should return the payload of a contract created earlier in the same transaction" in {
      inside(
        evalApp(
          e"""\(p: Party) ->
                ubind cid: ContractId Mod:Secret <- create @Mod:Secret Mod:Secret { p = p, secret = "42" }
                in unpack_interface @Mod:Iface (COERCE_CONTRACT_ID @Mod:Secret @Mod:Iface cid)""",
          ArraySeq(SParty(alice), SToken),
          packageResolution = Map(pkg.pkgName -> pkgId),
          compiledPackages = compiledPackages,
          committers = Set(alice),
        )
      ) {
        case Success(
              Right(
                SAny(
                  Ast.TTyCon(`secretTplId`),
                  SRecord(`secretTplId`, _, ArraySeq(SParty(`alice`), SText("42"))),
                )
              )
            ) =>
          succeed
      }
    }

    "should fail when the contract does not implement the interface" in {
      inside(
        evalApp(
          e"\(cid: ContractId Mod:Iface) -> unpack_interface @Mod:Iface cid",
          ArraySeq(SContractId(otherCid), SToken),
          packageResolution = Map(pkg.pkgName -> pkgId),
          getContract = Map(otherContract.contractId -> otherContract),
          compiledPackages = compiledPackages,
          committers = Set(alice),
        )
      ) { case Success(Left(SError.InterpretationError(error))) =>
        error shouldBe a[IE.ContractDoesNotImplementInterface]
      }
    }

    "should fail when the contract's package name cannot be resolved" in {
      inside(
        evalApp(
          e"\(cid: ContractId Mod:Iface) -> unpack_interface @Mod:Iface cid",
          ArraySeq(SContractId(cid), SToken),
          getContract = Map(contract.contractId -> contract),
          compiledPackages = compiledPackages,
          committers = Set(alice),
        )
      ) { case Success(Left(SError.InterpretationError(error))) =>
        error shouldBe a[IE.UnresolvedPackageName]
      }
    }

    "should fail when the contract is unknown" in {
      inside(
        evalApp(
          e"\(cid: ContractId Mod:Iface) -> unpack_interface @Mod:Iface cid",
          ArraySeq(SContractId(unknownCid), SToken),
          packageResolution = Map(pkg.pkgName -> pkgId),
          getContract = Map.empty,
          compiledPackages = compiledPackages,
          committers = Set(alice),
        )
      ) { case Failure(SpeedyTestLib.UnknownContract(`unknownCid`)) =>
        succeed
      }
    }
  }
}
