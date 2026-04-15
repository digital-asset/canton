// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package speedy

import com.digitalasset.daml.lf.crypto.SValueHash
import com.digitalasset.daml.lf.data.Ref.{PackageId, PackageName, Party, TypeConId}
import com.digitalasset.daml.lf.data.{FrontStack, ImmArray, Ref}
import com.digitalasset.daml.lf.interpretation.Error as IE
import com.digitalasset.daml.lf.language.Ast.*
import com.digitalasset.daml.lf.language.LanguageVersion
import com.digitalasset.daml.lf.ledger.FailedAuthorization
import com.digitalasset.daml.lf.ledger.FailedAuthorization.*
import com.digitalasset.daml.lf.speedy.SError.*
import com.digitalasset.daml.lf.speedy.SExpr.*
import com.digitalasset.daml.lf.speedy.SValue.*
import com.digitalasset.daml.lf.testing.parser.Implicits.SyntaxHelper
import com.digitalasset.daml.lf.transaction.test.TransactionBuilder
import com.digitalasset.daml.lf.transaction.{
  NextGenContractStateMachine as ContractStateMachine,
  FatContractInstance,
  GlobalKey,
  GlobalKeyWithMaintainers,
  SerializationVersion,
}
import com.digitalasset.daml.lf.value.Value
import com.digitalasset.daml.lf.value.Value.{ValueParty, ValueRecord}
import org.scalatest.Inside
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.immutable.ArraySeq
import scala.util.{Success, Try}
import com.digitalasset.canton.logging.SuppressingLogging

class AuthorizationTest_V2Dev
    extends AuthorizationTest(LanguageVersion.v2_dev, withKey = true)
class AuthorizationTest_V23
    extends AuthorizationTest(LanguageVersion.v2_3, withKey = true)

/** Tests for authorization failure evaluation ordering.
  *
  * These tests were extracted from EvaluationOrderTest where they were removed
  * when the AuthorizationCheckerLogger (a dummy that always succeeds) was introduced.
  * Here we use the DefaultAuthorizationChecker (the real authorization checker) so that
  * actual FailedAuthorization errors are produced.
  *
  * NOTE: Many values and helpers are duplicated from EvaluationOrderTest because
  * the originals are private[this] and cannot be accessed from a separate file.
  * Do NOT modify EvaluationOrderTest to change visibility.
  */
abstract class AuthorizationTest(languageVersion: LanguageVersion, withKey: Boolean)
    extends AnyFreeSpec
    with Matchers
    with Inside
    with SuppressingLogging {

  private val testPkg = new TestPkg(withKey, languageVersion)
  import testPkg._

  private[this] val Helper: TypeConId = t"Test:Helper" match {
    case TTyCon(tycon) => tycon
    case _ => sys.error("unexpected error")
  }

  private[this] val cId: Value.ContractId =
    Value.ContractId.V1(crypto.Hash.hashPrivateKey("test"))

  private[this] val helperCId: Value.ContractId =
    Value.ContractId.V1(crypto.Hash.hashPrivateKey("Helper"))

  private[this] val emptyNestedValue = Value.ValueRecord(None, ImmArray.empty)

  private[this] val keySValue = SRecord(
    TKey,
    ImmArray("maintainers", "optCid", "nested").map(Ref.Name.assertFromString),
    ArraySeq(
      SList(FrontStack(SParty(alice))),
      SOptional(None),
      SRecord(Nested, ImmArray(Ref.Name.assertFromString("f")), ArraySeq(SOptional(None))),
    ),
  )

  private[this] val keyValue = Value.ValueRecord(
    None,
    ImmArray(
      None -> Value.ValueList(FrontStack(Value.ValueParty(alice))),
      None -> Value.ValueNone,
      None -> emptyNestedValue,
    ),
  )

  private[this] val normalizedKeyValue = Value.ValueRecord(
    None,
    ImmArray(
      None -> Value.ValueList(FrontStack(Value.ValueParty(alice))),
      None -> Value.ValueNone,
      None -> Value.ValueRecord(None, ImmArray()),
    ),
  )

  private val testTxVersion: SerializationVersion = serializationVersion

  // NOTE: duplicated from EvaluationOrderTest (private[this] there)
  private[this] def buildContract(
      observer: Party,
      contractId: Value.ContractId,
  ): FatContractInstance =
    TransactionBuilder.fatContractInstanceWithDummyDefaults(
      testTxVersion,
      packageName = pkg.pkgName,
      template = T,
      arg = Value.ValueRecord(
        None,
        ImmArray(
          None -> Value.ValueParty(alice),
          None -> Value.ValueParty(observer),
          None -> Value.ValueTrue,
          None -> keyValue,
          None -> emptyNestedValue,
        ),
      ),
      signatories = List(alice),
      observers = List(observer),
      contractKeyWithMaintainers = Option.when(withKey)(
        GlobalKeyWithMaintainers(
          GlobalKey
            .assertBuild(
              templateId = T,
              packageName = pkg.pkgName,
              key = normalizedKeyValue,
              keyHash =
                SValueHash.assertHashContractKey(pkg.pkgName, T.qualifiedName, keySValue),
            ),
          Set(alice),
        )
      ),
      contractId = contractId,
    )

  private[this] val iface_contract = TransactionBuilder.fatContractInstanceWithDummyDefaults(
    testTxVersion,
    packageName = pkg.pkgName,
    template = Human,
    arg = Value.ValueRecord(
      None,
      ImmArray(
        None -> Value.ValueParty(alice),
        None -> Value.ValueParty(bob),
        None -> Value.ValueParty(alice),
        None -> Value.ValueTrue,
        None -> keyValue,
        None -> emptyNestedValue,
      ),
    ),
    signatories = List(alice),
    observers = List(bob),
    contractKeyWithMaintainers = Option.when(withKey)(
      GlobalKeyWithMaintainers(
        GlobalKey.assertBuild(
          templateId = Human,
          packageName = pkg.pkgName,
          key = normalizedKeyValue,
          keyHash = SValueHash.assertHashContractKey(pkg.pkgName, Human.qualifiedName, keySValue),
        ),
        Set(alice),
      )
    ),
    contractId = cId,
  )

  private[this] val helper = TransactionBuilder.fatContractInstanceWithDummyDefaults(
    testTxVersion,
    packageName = pkg.pkgName,
    template = Helper,
    arg = ValueRecord(
      None,
      ImmArray(None -> ValueParty(alice), None -> ValueParty(charlie)),
    ),
    signatories = List(alice),
    contractId = helperCId,
  )

  private[this] val getContract = { val c = buildContract(bob, cId); Map(c.contractId -> c) }
  private[this] val getIfaceContract = Map(iface_contract.contractId -> iface_contract)
  private[this] val getHelper = Map(helper.contractId -> helper)

  private[this] val getKeys = Map(
    GlobalKey.assertBuild(
      templateId = T,
      packageName = pkg.pkgName,
      key = keyValue,
      keyHash = SValueHash.assertHashContractKey(pkg.pkgName, T.qualifiedName, keySValue),
    ) -> Vector(cId)
  )

  private[this] val seed = crypto.Hash.hashPrivateKey("seed")

  private def evalUpdateApp(
      pkgs: CompiledPackages,
      e: Expr,
      args: ArraySeq[SValue],
      parties: Set[Party],
      readAs: Set[Party] = Set.empty,
      packageResolution: Map[PackageName, PackageId] = packageNameMap,
      getContract: PartialFunction[Value.ContractId, FatContractInstance] = PartialFunction.empty,
      getKeys: PartialFunction[GlobalKey, Vector[FatContractInstance]] = PartialFunction.empty,
  ): Try[Either[SError, SValue]] = {
    val se = pkgs.compiler.unsafeCompile(e)
    val machine = Speedy.Machine
      .fromUpdateSExpr(
        compiledPackages = pkgs,
        transactionSeed = seed,
        updateSE = if (args.isEmpty) se else SEApp(se, args),
        committers = parties,
        logger = MachineLogger(),
        readAs = readAs,
        mode =
          if (withKey) ContractStateMachine.Mode.NUCK
          else ContractStateMachine.Mode.NoKey,
        packageResolution = packageResolution,
      )
    Try(
      SpeedyTestLib.run(
        machine,
        getContract = getContract,
        getKeys = getKeys,
      )
    )
  }

  private def mapKeys[K, V, R](getKeys: Map[K, Vector[V]], getContract: V => R): Map[K, Vector[R]] =
    getKeys.view.mapValues(_.map(getContract)).toMap

  // ---------------------------------------------------------------------------
  // Authorization failure tests
  // ---------------------------------------------------------------------------

  "authorization failures" - {

    "create" - {

      // TEST_EVIDENCE: Integrity: Evaluation order of create with authorization failure
      "authorization failure" in {
        val res = evalUpdateApp(
          pkgs,
          e"""\(sig : Party) (obs : Party) ->
            Test:create M:T { signatory = sig, observer = obs, precondition = True, key = M:toKey sig, nested = M:buildNested 0 }
       """,
          ArraySeq(SParty(alice), SParty(bob)),
          Set(bob),
        )
        inside(res) {
          case Success(
                Left(
                  SErrorDamlException(
                    IE.FailedAuthorization(
                      _,
                      CreateMissingAuthorization(T, _, authorizingParties, requiredParties),
                    )
                  )
                )
              ) =>
            authorizingParties shouldBe Set(bob)
            requiredParties shouldBe Set(alice)
        }
      }
    }

    "create_interface" - {

      // TEST_EVIDENCE: Integrity: Evaluation order of create_interface with authorization failure
      "authorization failure" in {
        val res = evalUpdateApp(
          pkgs,
          e"""\(sig : Party) (obs : Party) ->
            Test:create_interface M:Human { person = sig, obs = obs, ctrl = sig, precond = True, key = M:toKey sig, nested = M:buildNested 0}
       """,
          ArraySeq(SParty(alice), SParty(bob)),
          Set(bob),
        )
        inside(res) {
          case Success(
                Left(
                  SErrorDamlException(
                    IE.FailedAuthorization(
                      _,
                      CreateMissingAuthorization(Human, _, authorizingParties, requiredParties),
                    )
                  )
                )
              ) =>
            authorizingParties shouldBe Set(bob)
            requiredParties shouldBe Set(alice)
        }
      }
    }

    "exercise" - {

      "a non-cached global contract" - {

        // TEST_EVIDENCE: Integrity: Evaluation order of exercise of a non-cached global contract with failure authorization
        "authorization failure" in {
          val res = evalUpdateApp(
            pkgs,
            e"""\(exercisingParty : Party) (cId: ContractId M:T) -> Test:exercise_by_id exercisingParty cId (M:Either:Left @Int64 @Int64 0)""",
            ArraySeq(SParty(charlie), SContractId(cId)),
            Set(alice, charlie),
            getContract = getContract,
          )

          inside(res) {
            case Success(
                  Left(
                    SErrorDamlException(
                      IE.FailedAuthorization(
                        _,
                        ExerciseMissingAuthorization(
                          T,
                          "Choice",
                          _,
                          authorizingParties,
                          requiredParties,
                        ),
                      )
                    )
                  )
                ) =>
              authorizingParties shouldBe Set(charlie)
              requiredParties shouldBe Set(alice)
          }
        }
      }

      "a cached global contract" - {

        // TEST_EVIDENCE: Integrity: Evaluation order of exercise of cached global contract with failure authorization
        "authorization failure" in {
          val res = evalUpdateApp(
            pkgs,
            e"""\(exercisingParty : Party) (cId: ContractId M:T) ->
           ubind x: M:T <- fetch_template @M:T cId
           in  Test:exercise_by_id exercisingParty cId (M:Either:Left @Int64 @Int64 0)""",
            ArraySeq(SParty(charlie), SContractId(cId)),
            Set(alice, charlie),
            getContract = getContract,
          )

          inside(res) {
            case Success(
                  Left(
                    SErrorDamlException(
                      IE.FailedAuthorization(
                        _,
                        ExerciseMissingAuthorization(
                          T,
                          "Choice",
                          _,
                          authorizingParties,
                          requiredParties,
                        ),
                      )
                    )
                  )
                ) =>
              authorizingParties shouldBe Set(charlie)
              requiredParties shouldBe Set(alice)
          }
        }
      }

      "a local contract" - {

        // TEST_EVIDENCE: Integrity: Evaluation order of exercise of a local contract with failure authorization
        "authorization failure" in {
          val res = evalUpdateApp(
            pkgs,
            e"""\(sig: Party) (obs : Party) (exercisingParty : Party) ->
              ubind cId: ContractId M:T <- create @M:T M:T { signatory = sig, observer = obs, precondition = True, key = M:toKey sig, nested = M:buildNested 0 }
              in
                Test:exercise_by_id exercisingParty cId (M:Either:Left @Int64 @Int64 0)
              """,
            ArraySeq(SParty(alice), SParty(bob), SParty(charlie)),
            Set(alice, charlie),
            getContract = getContract,
          )

          inside(res) {
            case Success(
                  Left(
                    SErrorDamlException(
                      IE.FailedAuthorization(
                        _,
                        ExerciseMissingAuthorization(
                          T,
                          "Choice",
                          _,
                          authorizingParties,
                          requiredParties,
                        ),
                      )
                    )
                  )
                ) =>
              authorizingParties shouldBe Set(charlie)
              requiredParties shouldBe Set(alice)
          }
        }
      }
    }

    if (withKey) "exercise_by_key" - {

      "a non-cached global contract" - {

        // TEST_EVIDENCE: Integrity: Evaluation order of exercise_by_key of a non-cached global contract with failure authorization
        "authorization failure" in {
          val res = evalUpdateApp(
            pkgs,
            e"""\(exercisingParty : Party) (sig: Party) -> Test:exercise_by_key exercisingParty (Test:someParty sig) Test:noCid 0 (M:Either:Left @Int64 @Int64 0)""",
            ArraySeq(SParty(charlie), SParty(alice)),
            Set(alice, charlie),
            getContract = getContract,
            getKeys = mapKeys(getKeys, getContract),
          )

          inside(res) {
            case Success(
                  Left(
                    SErrorDamlException(
                      IE.FailedAuthorization(
                        _,
                        ExerciseMissingAuthorization(
                          T,
                          "Choice",
                          _,
                          authorizingParties,
                          requiredParties,
                        ),
                      )
                    )
                  )
                ) =>
              authorizingParties shouldBe Set(charlie)
              requiredParties shouldBe Set(alice)
          }
        }
      }

      "a cached global contract" - {

        // TEST_EVIDENCE: Integrity: Evaluation order of exercise_by_key of cached global contract with failure authorization
        "authorization failure" in {
          val res = evalUpdateApp(
            pkgs,
            e"""\(exercisingParty : Party) (cId: ContractId M:T) (sig: Party) ->
           ubind x: M:T <- fetch_template @M:T cId
           in Test:exercise_by_key exercisingParty (Test:someParty sig) Test:noCid 0 (M:Either:Left @Int64 @Int64 0)""",
            ArraySeq(SParty(charlie), SContractId(cId), SParty(alice)),
            Set(alice, charlie),
            getContract = getContract,
            getKeys = mapKeys(getKeys, getContract),
          )

          inside(res) {
            case Success(
                  Left(
                    SErrorDamlException(
                      IE.FailedAuthorization(
                        _,
                        ExerciseMissingAuthorization(
                          T,
                          "Choice",
                          _,
                          authorizingParties,
                          requiredParties,
                        ),
                      )
                    )
                  )
                ) =>
              authorizingParties shouldBe Set(charlie)
              requiredParties shouldBe Set(alice)
          }
        }
      }

      "a local contract" - {

        // TEST_EVIDENCE: Integrity: Evaluation order of exercise_by_key of a local contract with failure authorization
        "authorization failure" in {
          val res = evalUpdateApp(
            pkgs,
            e"""\(sig: Party) (obs : Party) (exercisingParty : Party) ->
              ubind cId: ContractId M:T <- create @M:T M:T { signatory = sig, observer = obs, precondition = True, key = M:toKey sig, nested = M:buildNested 0 }
              in
                Test:exercise_by_key exercisingParty (Test:someParty sig) Test:noCid 0 (M:Either:Left @Int64 @Int64 0)
              """,
            ArraySeq(SParty(alice), SParty(bob), SParty(charlie)),
            Set(alice, charlie),
          )

          inside(res) {
            case Success(
                  Left(
                    SErrorDamlException(
                      IE.FailedAuthorization(
                        _,
                        ExerciseMissingAuthorization(
                          T,
                          "Choice",
                          _,
                          authorizingParties,
                          requiredParties,
                        ),
                      )
                    )
                  )
                ) =>
              authorizingParties shouldBe Set(charlie)
              requiredParties shouldBe Set(alice)
          }
        }
      }
    }

    List("exercise_interface", "exercise_interface_with_guard").foreach { testCase =>

      testCase - {

        "a non-cached global contract" - {

          // TEST_EVIDENCE: Integrity: Evaluation order of exercise_interface of a non-cached global contract with failed authorization
          "authorization failure" in {
            val res = evalUpdateApp(
              pkgs = pkgs,
              e =
                e"""\(exercisingParty : Party) (cId: ContractId M:Human) -> Test:$testCase exercisingParty cId""",
              args = ArraySeq(SParty(charlie), SContractId(cId)),
              parties = Set(charlie),
              readAs = Set(alice),
              getContract = getIfaceContract,
            )

            inside(res) {
              case Success(
                    Left(
                      SErrorDamlException(
                        IE.FailedAuthorization(
                          _,
                          ExerciseMissingAuthorization(
                            Human,
                            "Nap",
                            _,
                            authorizingParties,
                            requiredParties,
                          ),
                        )
                      )
                    )
                  ) =>
                authorizingParties shouldBe Set(charlie)
                requiredParties shouldBe Set(alice)
            }
          }
        }

        "a cached global contract" - {

          // TEST_EVIDENCE: Integrity: Evaluation order of exercise by interface of cached global contract with failed authorization
          "authorization failure" in {
            val res = evalUpdateApp(
              pkgs,
              e"""\(exercisingParty : Party) (cId: ContractId M:Human) ->
           ubind x: M:Human <- fetch_template @M:Human cId
           in  Test:$testCase exercisingParty cId""",
              ArraySeq(SParty(bob), SContractId(cId)),
              Set(bob),
              getContract = getIfaceContract,
            )

            inside(res) {
              case Success(
                    Left(
                      SErrorDamlException(
                        IE.FailedAuthorization(
                          _,
                          ExerciseMissingAuthorization(
                            Human,
                            "Nap",
                            _,
                            authorizingParties,
                            requiredParties,
                          ),
                        )
                      )
                    )
                  ) =>
                authorizingParties shouldBe Set(bob)
                requiredParties shouldBe Set(alice)
            }
          }
        }

        "a local contract" - {

          // TEST_EVIDENCE: Integrity: Evaluation order of exercise_interface of a local contract with failed authorization
          "authorization failure" in {
            val res = evalUpdateApp(
              pkgs,
              e"""\(exercisingParty : Party) (other : Party)->
              ubind cId: ContractId M:Human <- create @M:Human M:Human {person = exercisingParty, obs = other, ctrl = other, precond = True, key = M:toKey exercisingParty, nested = M:buildNested 0} in
                Test:$testCase exercisingParty cId
              """,
              ArraySeq(SParty(alice), SParty(bob)),
              Set(alice),
            )

            inside(res) {
              case Success(
                    Left(
                      SErrorDamlException(
                        IE.FailedAuthorization(
                          _,
                          FailedAuthorization.ExerciseMissingAuthorization(
                            Human,
                            "Nap",
                            None,
                            authorizingParties,
                            requiredParties,
                          ),
                        )
                      )
                    )
                  ) =>
                authorizingParties shouldBe Set(alice)
                requiredParties shouldBe Set(bob)
            }
          }
        }
      }
    }

    "fetch" - {

      "a non-cached global contract" - {

        // TEST_EVIDENCE: Integrity: Evaluation order of fetch of a non-cached global contract with failure authorization
        "authorization failure" in {
          val res = evalUpdateApp(
            pkgs,
            e"""Test:fetch_by_id""",
            ArraySeq(SParty(charlie), SContractId(cId)),
            Set(alice, charlie),
            getContract = getContract,
          )

          inside(res) {
            case Success(
                  Left(
                    SErrorDamlException(
                      IE.FailedAuthorization(
                        _,
                        FetchMissingAuthorization(T, _, stakeholders, authorizingParties),
                      )
                    )
                  )
                ) =>
              stakeholders shouldBe Set(alice, bob)
              authorizingParties shouldBe Set(charlie)
          }
        }
      }

      "a cached global contract" - {

        // TEST_EVIDENCE: Integrity: Evaluation order of fetch of cached global contract with failure authorization
        "authorization failure" in {
          val res = evalUpdateApp(
            pkgs,
            e"""\(fetchingParty: Party) (cId: ContractId M:T) ->
           ubind x: M:T <- fetch_template @M:T cId
           in Test:fetch_by_id fetchingParty cId""",
            ArraySeq(SParty(charlie), SContractId(cId)),
            Set(alice, charlie),
            getContract = getContract,
          )

          inside(res) {
            case Success(
                  Left(
                    SErrorDamlException(
                      IE.FailedAuthorization(
                        _,
                        FetchMissingAuthorization(T, _, stakeholders, authorizingParties),
                      )
                    )
                  )
                ) =>
              stakeholders shouldBe Set(alice, bob)
              authorizingParties shouldBe Set(charlie)
          }
        }
      }

      "a local contract" - {

        // TEST_EVIDENCE: Integrity: Evaluation order of fetch of a local contract with failure authorization
        "authorization failure" in {
          val res = evalUpdateApp(
            pkgs,
            e"""\(sig: Party) (obs : Party) (fetchingParty: Party) ->
              ubind cId: ContractId M:T <- create @M:T M:T { signatory = sig, observer = obs, precondition = True, key = M:toKey sig, nested = M:buildNested 0 }
              in Test:fetch_by_id fetchingParty cId""",
            ArraySeq(SParty(alice), SParty(bob), SParty(charlie)),
            Set(alice, charlie),
            getContract = getContract,
          )

          inside(res) {
            case Success(
                  Left(
                    SErrorDamlException(
                      IE.FailedAuthorization(
                        _,
                        FetchMissingAuthorization(T, _, stakeholders, authorizingParties),
                      )
                    )
                  )
                ) =>
              stakeholders shouldBe Set(alice, bob)
              authorizingParties shouldBe Set(charlie)
          }
        }
      }
    }

    if (withKey) "fetch_by_key" - {

      "a non-cached global contract" - {

        // TEST_EVIDENCE: Integrity: Evaluation order of fetch_by_key of a non-cached global contract with authorization failure
        "authorization failure" in {
          val res = evalUpdateApp(
            pkgs = pkgs,
            e =
              e"""\(fetchingParty:Party) (sig: Party) -> Test:fetch_by_key fetchingParty (Test:someParty sig) Test:noCid 0""",
            args = ArraySeq(SParty(charlie), SParty(alice)),
            parties = Set(alice, charlie),
            getContract = getContract,
            getKeys = mapKeys(getKeys, getContract),
          )
          inside(res) {
            case Success(
                  Left(
                    SErrorDamlException(
                      IE.FailedAuthorization(
                        _,
                        FetchMissingAuthorization(T, _, stakeholders, authorizingParties),
                      )
                    )
                  )
                ) =>
              stakeholders shouldBe Set(alice, bob)
              authorizingParties shouldBe Set(charlie)
          }
        }
      }

      "a cached global contract" - {

        // TEST_EVIDENCE: Integrity: Evaluation order of fetch_by_key of a cached global contract with authorization failure
        "authorization failure" in {
          val res = evalUpdateApp(
            pkgs,
            e"""\(fetchingParty:Party) (sig: Party) (cId: ContractId M:T) ->
           ubind x: M:T <- fetch_template @M:T cId
            in Test:fetch_by_key fetchingParty (Test:someParty sig) Test:noCid 0""",
            ArraySeq(SParty(charlie), SParty(alice), SContractId(cId)),
            Set(alice, charlie),
            getContract = getContract,
            getKeys = mapKeys(getKeys, getContract),
          )
          inside(res) {
            case Success(
                  Left(
                    SErrorDamlException(
                      IE.FailedAuthorization(
                        _,
                        FetchMissingAuthorization(T, _, stakeholders, authorizingParties),
                      )
                    )
                  )
                ) =>
              stakeholders shouldBe Set(alice, bob)
              authorizingParties shouldBe Set(charlie)
          }
        }
      }

      "a local contract" - {

        // TEST_EVIDENCE: Integrity: Evaluation order of fetch_by_key of a local contract with authorization failure
        "authorization failure" in {
          val res = evalUpdateApp(
            pkgs = pkgs,
            e = e"""\(helperCId: ContractId Test:Helper) (sig : Party) (fetchingParty: Party) ->
         ubind x: ContractId M:T <- exercise @Test:Helper CreateNonvisibleKey helperCId ()
         in Test:fetch_by_key fetchingParty (Test:someParty sig) Test:noCid 0""",
            args = ArraySeq(SContractId(helperCId), SParty(alice), SParty(charlie)),
            parties = Set(charlie),
            readAs = Set(alice),
            getContract = getHelper,
          )
          inside(res) {
            case Success(
                  Left(
                    SErrorDamlException(
                      IE.FailedAuthorization(
                        _,
                        FetchMissingAuthorization(T, _, stakeholders, authorizingParties),
                      )
                    )
                  )
                ) =>
              stakeholders shouldBe Set(alice)
              authorizingParties shouldBe Set(charlie)
          }
        }
      }
    }

    if (withKey) "lookup_by_key" - {

      "a non-cached global contract" - {

        // TEST_EVIDENCE: Integrity: Evaluation order of lookup_by_key of a non-cached global contract with authorization failure
        "authorization failure" in {
          val res = evalUpdateApp(
            pkgs,
            e"""\(lookingParty:Party) (sig: Party) -> Test:lookup_by_key lookingParty (Test:someParty sig) Test:noCid 0""",
            ArraySeq(SParty(charlie), SParty(alice)),
            Set(alice, charlie),
            getContract = getContract,
            getKeys = mapKeys(getKeys, getContract),
          )
          inside(res) {
            case Success(
                  Left(
                    SErrorDamlException(
                      IE.FailedAuthorization(
                        _,
                        LookupByKeyMissingAuthorization(T, _, maintainers, authorizingParties),
                      )
                    )
                  )
                ) =>
              authorizingParties shouldBe Set(charlie)
              maintainers shouldBe Set(alice)
          }
        }
      }

      "a cached global contract" - {

        // TEST_EVIDENCE: Integrity: Evaluation order of lookup_by_key of a cached global contract with authorization failure
        "authorization failure" in {
          val res = evalUpdateApp(
            pkgs,
            e"""\(lookingParty:Party) (sig: Party) (cId: ContractId M:T) ->
           ubind x: M:T <- fetch_template @M:T cId
            in Test:lookup_by_key lookingParty (Test:someParty sig) Test:noCid 0""",
            ArraySeq(SParty(charlie), SParty(alice), SContractId(cId)),
            Set(alice, charlie),
            getContract = getContract,
            getKeys = mapKeys(getKeys, getContract),
          )
          inside(res) {
            case Success(
                  Left(
                    SErrorDamlException(
                      IE.FailedAuthorization(
                        _,
                        LookupByKeyMissingAuthorization(T, _, maintainers, authorizingParties),
                      )
                    )
                  )
                ) =>
              authorizingParties shouldBe Set(charlie)
              maintainers shouldBe Set(alice)
          }
        }
      }

      "a local contract" - {

        // TEST_EVIDENCE: Integrity: Evaluation order of lookup_by_key of a local contract with failure authorization
        "authorization failure" in {
          val res = evalUpdateApp(
            pkgs,
            e"""\(sig: Party) (obs : Party) (lookingParty: Party) ->
              ubind cId: ContractId M:T <- create @M:T M:T { signatory = sig, observer = obs, precondition = True, key = M:toKey sig, nested = M:buildNested 0 }
             in Test:lookup_by_key lookingParty (Test:someParty sig) Test:noCid 0""",
            ArraySeq(SParty(alice), SParty(bob), SParty(charlie)),
            Set(alice, charlie),
          )

          inside(res) {
            case Success(
                  Left(
                    SErrorDamlException(
                      IE.FailedAuthorization(
                        _,
                        LookupByKeyMissingAuthorization(T, _, maintainers, authorizingParties),
                      )
                    )
                  )
                ) =>
              authorizingParties shouldBe Set(charlie)
              maintainers shouldBe Set(alice)
          }
        }
      }
    }
  }
}
