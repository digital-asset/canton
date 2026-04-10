// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package engine

import com.digitalasset.canton.logging.SuppressingLogging
import com.digitalasset.daml.lf.archive.DarDecoder
import com.digitalasset.daml.lf.command.ApiCommand
import com.digitalasset.daml.lf.data.Ref.{
  Identifier,
  Name,
  PackageId,
  Party,
  QualifiedName,
  TypeConId,
}
import com.digitalasset.daml.lf.data.{Bytes, ImmArray, Ref, Time}
import com.digitalasset.daml.lf.language.Ast.Package
import com.digitalasset.daml.lf.language.LanguageVersion
import com.digitalasset.daml.lf.speedy.{InitialSeeding, SValue}
import com.digitalasset.daml.lf.transaction.{
  NextGenContractStateMachine => ContractStateMachine,
  FatContractInstance,
  GlobalKey,
  GlobalKeyWithMaintainers,
  SerializationVersion,
}
import com.digitalasset.daml.lf.value.{ContractIdVersion, Value}
import com.digitalasset.daml.lf.value.Value.{
  ContractId,
  ValueContractId,
  ValueInt64,
  ValueParty,
  ValueRecord,
}
import com.digitalasset.daml.lf.crypto.SValueHash
import com.digitalasset.daml.lf.stablepackages.StablePackagesV2
import com.digitalasset.daml.lf.transaction.test.TransactionBuilder

import org.scalatest.EitherValues
import org.scalatest.Inside.inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.wordspec.AnyWordSpec

import java.util.zip.ZipInputStream
import scala.collection.immutable.ArraySeq
import scala.language.implicitConversions

//  TODO(#30398)
//   review those test when queryNKey is implemented.
//   For now those do not make a lot of sens as the lookupKey function use
//   to emulate the indexer do not return all the keys.
abstract class ContractKeySpecV2 extends ContractKeySpec(LanguageVersion.Major.V2)

@SuppressWarnings(
  Array(
    "org.wartremover.warts.Any",
    "org.wartremover.warts.Serializable",
    "org.wartremover.warts.Product",
  )
)
class ContractKeySpec(majorLanguageVersion: LanguageVersion.Major)
    extends AnyWordSpec
    with Matchers
    with TableDrivenPropertyChecks
    with EitherValues
    with SuppressingLogging {

  import ContractKeySpec._

  private[this] val version = SerializationVersion.minContractKeys
  private[this] val contractIdVersion = ContractIdVersion.V2

  private[this] val suffixLenientEngine = Engine.DevEngine(loggerFactory)
  private[this] val compiledPackages = ConcurrentCompiledPackages(
    suffixLenientEngine.config.getCompilerConfig
  )
  private[this] val preprocessor = preprocessing.Preprocessor.forTesting(compiledPackages, loggerFactory)

  private def loadAndAddPackage(resource: String): (PackageId, Package, Map[PackageId, Package]) = {
    val stream = getClass.getClassLoader.getResourceAsStream(resource)
    val packages = DarDecoder.readArchive(resource, new ZipInputStream(stream)).toOption.get
    val (mainPkgId, mainPkg) = packages.main
    assert(
      compiledPackages.addPackage(mainPkgId, mainPkg).consume(pkgs = packages.all.toMap).isRight
    )
    (mainPkgId, mainPkg, packages.all.toMap)
  }

  private val (basicTestsPkgId, basicTestsPkg, allPackages) = loadAndAddPackage(
    s"BasicTests-v${majorLanguageVersion.pretty}dev.dar"
  )

  val basicTestsSignatures = language.PackageInterface(Map(basicTestsPkgId -> basicTestsPkg))

  val withKeyTemplate = "BasicTests:WithKey"
  val BasicTests_WithKey = Identifier(basicTestsPkgId, withKeyTemplate)
  val withKeyContractInst: FatContractInstance =
    TransactionBuilder.fatContractInstanceWithDummyDefaults(
      version = version,
      contractId = toContractId("BasicTests:WithKey:1"),
      packageName = basicTestsPkg.pkgName,
      template = TypeConId(basicTestsPkgId, withKeyTemplate),
      arg = ValueRecord(
        Some(BasicTests_WithKey),
        ImmArray(
          (Some[Ref.Name]("p"), ValueParty(alice)),
          (Some[Ref.Name]("k"), ValueInt64(42)),
        ),
      ),
      signatories = List(alice),
    )

  val defaultContracts: Map[ContractId, FatContractInstance] =
    Map(
      toContractId("BasicTests:Simple:1") ->
        TransactionBuilder.fatContractInstanceWithDummyDefaults(
          version = version,
          packageName = basicTestsPkg.pkgName,
          template = TypeConId(basicTestsPkgId, "BasicTests:Simple"),
          arg = ValueRecord(
            Some(Identifier(basicTestsPkgId, "BasicTests:Simple")),
            ImmArray((Some[Name]("p"), ValueParty(party))),
          ),
          signatories = List(party),
        ),
      toContractId("BasicTests:CallablePayout:1") ->
        TransactionBuilder.fatContractInstanceWithDummyDefaults(
          version = version,
          packageName = basicTestsPkg.pkgName,
          template = TypeConId(basicTestsPkgId, "BasicTests:CallablePayout"),
          arg = ValueRecord(
            Some(Identifier(basicTestsPkgId, "BasicTests:CallablePayout")),
            ImmArray(
              (Some[Ref.Name]("giver"), ValueParty(alice)),
              (Some[Ref.Name]("receiver"), ValueParty(bob)),
            ),
          ),
          signatories = List(alice),
        ),
      withKeyContractInst.contractId -> withKeyContractInst,
    )

  val defaultKey = {
    val sKey = SValue.SRecord(
      StablePackagesV2.Tuple2,
      ImmArray(Ref.Name.assertFromString("_1"), Ref.Name.assertFromString("_2")),
      ArraySeq(SValue.SParty(alice), SValue.SInt64(42)),
    )
    Map(
      GlobalKey.assertBuild(
        TypeConId(basicTestsPkgId, withKeyTemplate),
        basicTestsPkg.pkgName,
        sKey.toNormalizedValue,
        SValueHash.assertHashContractKey(
          basicTestsPkg.pkgName,
          Identifier(basicTestsPkgId, "BasicTests:WithKey").qualifiedName,
          sKey,
        ),
      )
        ->
          Vector(withKeyContractInst)
    )
  }

  private[this] val lookupKey
      : PartialFunction[GlobalKey, Vector[FatContractInstance]] = {
    case
        GlobalKey(
          BasicTests_WithKey,
          ValueRecord(_, ImmArray((_, ValueParty(`alice`)), (_, ValueInt64(42)))),
        ) =>
      Vector(withKeyContractInst)
  }

  "contract key" should {
    val now = Time.Timestamp.now()
    val submissionSeed = crypto.Hash.hashPrivateKey("contract key")
    val txSeed = crypto.Hash.deriveTransactionSeed(submissionSeed, participant, now)

    // TEST_EVIDENCE: Integrity: contract keys should be evaluated only when executing create
    "be evaluated only when executing create" in {
      val templateId =
        Identifier(basicTestsPkgId, "BasicTests:ComputeContractKeyWhenExecutingCreate")
      val createArg =
        ValueRecord(
          Some(templateId),
          ImmArray((Some[Name]("owner"), ValueParty(alice))),
        )
      val exerciseArg =
        ValueRecord(
          Some(Identifier(basicTestsPkgId, "BasicTests:DontExecuteCreate")),
          ImmArray.Empty,
        )

      val submitters = Set(alice)

      val Right(cmds) = preprocessor
        .preprocessApiCommands(
          Map.empty,
          ImmArray(
            ApiCommand
              .CreateAndExercise(templateId.toRef, createArg, "DontExecuteCreate", exerciseArg)
          ),
        )
        .consume(pkgs = allPackages, keys = lookupKey)

      val result = suffixLenientEngine
        .interpretCommands(
          validating = false,
          submitters = submitters,
          readAs = Set.empty,
          commands = cmds,
          ledgerTime = now,
          preparationTime = now,
          seeding = InitialSeeding.TransactionSeed(txSeed),
          contractIdVersion = contractIdVersion,
          contractStateMode = ContractStateMachine.Mode.devDefault,
        )
        .consume(pkgs = allPackages, keys = lookupKey)
      result shouldBe a[Right[_, _]]
    }

    // TEST_EVIDENCE: Integrity: contract keys should be evaluated after ensure clause
    "be evaluated after ensure clause" in {
      val templateId =
        Identifier(basicTestsPkgId, "BasicTests:ComputeContractKeyAfterEnsureClause")
      val createArg =
        ValueRecord(
          Some(templateId),
          ImmArray((Some[Name]("owner"), ValueParty(alice))),
        )

      val submitters = Set(alice)

      val Right(cmds) = preprocessor
        .preprocessApiCommands(Map.empty, ImmArray(ApiCommand.Create(templateId.toRef, createArg)))
        .consume(pkgs = allPackages, keys = lookupKey)
      val result = suffixLenientEngine
        .interpretCommands(
          validating = false,
          submitters = submitters,
          readAs = Set.empty,
          commands = cmds,
          ledgerTime = now,
          preparationTime = now,
          seeding = InitialSeeding.TransactionSeed(txSeed),
          contractIdVersion = contractIdVersion,
          contractStateMode = ContractStateMachine.Mode.devDefault,
        )
        .consume(pkgs = allPackages, keys = lookupKey)
      result shouldBe a[Left[_, _]]
      val Left(err) = result
      err.message should not include ("Boom")
      err.message should include("Template precondition violated")
    }

    // TEST_EVIDENCE: Integrity: contract keys must have a non-empty set of maintainers
    "not be create if has an empty set of maintainer" in {
      val templateId =
        Identifier(basicTestsPkgId, "BasicTests:NoMaintainer")
      val createArg =
        ValueRecord(
          Some(templateId),
          ImmArray((Some[Name]("sig"), ValueParty(alice))),
        )

      val submitters = Set(alice)

      val Right(cmds) = preprocessor
        .preprocessApiCommands(Map.empty, ImmArray(ApiCommand.Create(templateId.toRef, createArg)))
        .consume(pkgs = allPackages, keys = lookupKey)
      val result = suffixLenientEngine
        .interpretCommands(
          validating = false,
          submitters = submitters,
          readAs = Set.empty,
          commands = cmds,
          ledgerTime = now,
          preparationTime = now,
          seeding = InitialSeeding.TransactionSeed(txSeed),
          contractIdVersion = contractIdVersion,
          contractStateMode = ContractStateMachine.Mode.devDefault,
        )
        .consume(pkgs = allPackages, keys = lookupKey)

      inside(result) { case Left(err) =>
        err.message should include(
          "Update failed due to a contract key with an empty set of maintainers"
        )
      }
    }

    // Note that we provide no stability for multi key semantics so
    // these tests serve only as an indication of the current behavior
    // but can be changed freely.
    "multi keys" should {
      import com.digitalasset.daml.lf.language.{LanguageVersion => LV}
      val engine = new Engine(
        EngineConfig(
          allowedLanguageVersions = LV.allLfVersionsRange,
          forbidLocalContractIds = true,
        ),
        loggerFactory,
      )
      val (multiKeysPkgId, multiKeysPkg, allMultiKeysPkgs) =
        loadAndAddPackage(s"MultiKeys-v${majorLanguageVersion.pretty}dev.dar")
      val keyedId = Identifier(multiKeysPkgId, "MultiKeys:Keyed")
      val opsId = Identifier(multiKeysPkgId, "MultiKeys:KeyOperations")
      val let = Time.Timestamp.now()
      val submissionSeed = hash("multikeys")
      val seeding = Engine.initialSeeding(submissionSeed, participant, let)
      val sKey = SValue.SParty(party)

      val cid1 = toContractId("1")
      val cid2 = toContractId("2")
      def keyedInst(cid: ContractId) = TransactionBuilder.fatContractInstanceWithDummyDefaults(
        version = version,
        contractId = cid,
        packageName = multiKeysPkg.pkgName,
        template = TypeConId(multiKeysPkgId, "MultiKeys:Keyed"),
        arg = ValueRecord(None, ImmArray((None, ValueParty(party)))),
        signatories = List(party),
        contractKeyWithMaintainers = Some(
          GlobalKeyWithMaintainers(
            GlobalKey.assertBuild(
              templateId = TypeConId(multiKeysPkgId, "MultiKeys:Keyed"),
              packageName = multiKeysPkg.pkgName,
              key = sKey.toNormalizedValue,
              SValueHash.assertHashContractKey(
                multiKeysPkg.pkgName,
                "MultiKeys:Keyed",
                sKey,
              ),
            ),
            Set(party),
          )
        ),
      )
      val contracts =
        List(keyedInst(cid1), keyedInst(cid2)).map(inst => inst.contractId -> inst).toMap
      val lookupKey: PartialFunction[GlobalKey, Vector[FatContractInstance]] = {
        case GlobalKey(`keyedId`, ValueParty(`party`)) =>
          Vector(keyedInst(cid1))
      }

      def run(
          engine: Engine,
          choice: String,
          argument: Value,
          contractStateMode: ContractStateMachine.Mode,
      ) = {
        val cmd = ApiCommand.CreateAndExercise(
          opsId.toRef,
          ValueRecord(None, ImmArray((None, ValueParty(party)))),
          choice,
          argument,
        )
        val Right(cmds) = preprocessor
          .preprocessApiCommands(Map.empty, ImmArray(cmd))
          .consume(contracts, pkgs = allMultiKeysPkgs, keys = lookupKey)
        engine
          .interpretCommands(
            validating = false,
            submitters = Set(party),
            readAs = Set.empty,
            commands = cmds,
            ledgerTime = let,
            preparationTime = let,
            seeding = seeding,
            contractIdVersion = contractIdVersion,
            contractStateMode = contractStateMode,
          )
          .consume(contracts, pkgs = allMultiKeysPkgs, keys = lookupKey)
      }

      val emptyRecord = ValueRecord(None, ImmArray.Empty)
      // The cid returned by a fetchByKey at the beginning
      val keyResultCid = ValueRecord(None, ImmArray((None, ValueContractId(cid1))))
      // The cid not returned by a fetchByKey at the beginning
      val nonKeyResultCid = ValueRecord(None, ImmArray((None, ValueContractId(cid2))))
      val twoCids =
        ValueRecord(None, ImmArray((None, ValueContractId(cid1)), (None, ValueContractId(cid2))))
      val createOverwritesLocal = ("CreateOverwritesLocal", emptyRecord)
      val createOverwritesUnknownGlobal = ("CreateOverwritesUnknownGlobal", emptyRecord)
      val createOverwritesKnownGlobal = ("CreateOverwritesKnownGlobal", emptyRecord)
      val fetchDoesNotOverwriteGlobal = ("FetchDoesNotOverwriteGlobal", nonKeyResultCid)
      val fetchDoesNotOverwriteLocal = ("FetchDoesNotOverwriteLocal", keyResultCid)
      val localArchiveOverwritesUnknownGlobal = ("LocalArchiveOverwritesUnknownGlobal", emptyRecord)
      val localArchiveOverwritesKnownGlobal = ("LocalArchiveOverwritesKnownGlobal", emptyRecord)
      val globalArchiveOverwritesUnknownGlobal = ("GlobalArchiveOverwritesUnknownGlobal", twoCids)
      val globalArchiveOverwritesKnownGlobal1 = ("GlobalArchiveOverwritesKnownGlobal1", twoCids)
      val globalArchiveOverwritesKnownGlobal2 = ("GlobalArchiveOverwritesKnownGlobal2", twoCids)
      val rollbackCreateNonRollbackFetchByKey = ("RollbackCreateNonRollbackFetchByKey", emptyRecord)
      val rollbackFetchByKeyRollbackCreateNonRollbackFetchByKey =
        ("RollbackFetchByKeyRollbackCreateNonRollbackFetchByKey", emptyRecord)
      val rollbackFetchByKeyNonRollbackCreate = ("RollbackFetchByKeyNonRollbackCreate", emptyRecord)
      val rollbackFetchNonRollbackCreate = ("RollbackFetchNonRollbackCreate", keyResultCid)
      val rollbackGlobalArchiveNonRollbackCreate =
        ("RollbackGlobalArchiveNonRollbackCreate", keyResultCid)
      val rollbackCreateNonRollbackGlobalArchive =
        ("RollbackCreateNonRollbackGlobalArchive", keyResultCid)
      val rollbackGlobalArchiveUpdates =
        ("RollbackGlobalArchiveUpdates", twoCids)
      val rollbackGlobalArchivedLookup =
        ("RollbackGlobalArchivedLookup", keyResultCid)
      val rollbackGlobalArchivedCreate =
        ("RollbackGlobalArchivedCreate", keyResultCid)

      // regression tests for https://github.com/digital-asset/daml/issues/14171
      val rollbackExerciseCreateFetchByKey =
        ("RollbackExerciseCreateFetchByKey", keyResultCid)
      val rollbackExerciseCreateLookup =
        ("RollbackExerciseCreateLookup", keyResultCid)

      val allCases = Table(
        ("choice", "argument"),
        createOverwritesLocal,
        createOverwritesUnknownGlobal,
        createOverwritesKnownGlobal,
        fetchDoesNotOverwriteGlobal,
        fetchDoesNotOverwriteLocal,
        localArchiveOverwritesUnknownGlobal,
        localArchiveOverwritesKnownGlobal,
        globalArchiveOverwritesUnknownGlobal,
        globalArchiveOverwritesKnownGlobal1,
        globalArchiveOverwritesKnownGlobal2,
        rollbackCreateNonRollbackFetchByKey,
        rollbackFetchByKeyRollbackCreateNonRollbackFetchByKey,
        rollbackFetchByKeyNonRollbackCreate,
        rollbackFetchNonRollbackCreate,
        rollbackGlobalArchiveNonRollbackCreate,
        rollbackCreateNonRollbackGlobalArchive,
        rollbackGlobalArchiveUpdates,
        rollbackGlobalArchivedLookup,
        rollbackGlobalArchivedCreate,
        rollbackExerciseCreateFetchByKey,
        rollbackExerciseCreateLookup,
      )

      val nonUckFailures = Set(
        "RollbackExerciseCreateLookup",
        "RollbackExerciseCreateFetchByKey",
      )

      // TEST_EVIDENCE: Integrity: contract key behaviour (non-unique mode)
      "non-uck mode" in {
        val contractStateMode = ContractStateMachine.Mode.NUCK
        forEvery(allCases) { case (name, arg) =>
          if (nonUckFailures.contains(name)) {
            run(engine, name, arg, contractStateMode) shouldBe a[Left[_, _]]
          } else {
            run(engine, name, arg, contractStateMode) shouldBe a[Right[_, _]]
          }
        }
      }
    }
  }
}

object ContractKeySpec {

  private def hash(s: String) = crypto.Hash.hashPrivateKey(s)
  private def participant = Ref.ParticipantId.assertFromString("participant")

  private val party = Party.assertFromString("Party")
  private val alice = Party.assertFromString("Alice")
  private val bob = Party.assertFromString("Bob")

  private implicit def qualifiedNameStr(s: String): QualifiedName =
    QualifiedName.assertFromString(s)

  private implicit def toName(s: String): Name =
    Name.assertFromString(s)

  private val dummySuffix = Bytes.assertFromString("00")

  private def toContractId(s: String): ContractId =
    ContractId.V1.assertBuild(crypto.Hash.hashPrivateKey(s), dummySuffix)

}
