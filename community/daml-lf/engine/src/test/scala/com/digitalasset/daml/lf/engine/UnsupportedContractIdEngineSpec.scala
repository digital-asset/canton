// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package engine

import com.daml.logging.LoggingContext
import com.digitalasset.canton.logging.SuppressingLogging
import com.digitalasset.daml.lf.crypto.Hash
import com.digitalasset.daml.lf.data.{ImmArray, Ref, Time}
import com.digitalasset.daml.lf.interpretation
import com.digitalasset.daml.lf.interpretation.InterpretationConfig
import com.digitalasset.daml.lf.speedy.{InitialSeeding, SValue}
import com.digitalasset.daml.lf.transaction.{
  NeedKeyProgression,
  NextGenContractStateMachine as ContractStateMachine,
}
import com.digitalasset.daml.lf.value.ContractIdVersion
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class UnsupportedContractIdEngineSpec extends AnyWordSpec with Matchers with SuppressingLogging {

  implicit val logContext: LoggingContext = LoggingContext.ForTesting

  private val helpers =
    new EngineTestHelpers(ContractIdVersion.V1, "BasicTests-keys.dar", loggerFactory)
  import helpers.*

  private val seed = hash("UnsupportedContractIdEngineSpec")
  private val now = Time.Timestamp.now()
  private val withKeyTemplateId = Ref.Identifier(basicTestsPkgId, "BasicTests:WithKey")
  private val withKeySKey = mkSValuePair(SValue.SParty(alice), SValue.SInt64(42))

  /** Drive a program past the needs that can be answered automatically (packages, prefetch,
    * interruptions), stopping at the first contract/key/external-call suspension (or the result).
    */
  private def driveAuxiliary[A](step: Result.Step[A]): Result.Step[A] =
    step match {
      case im: Result.Step.Impure[x, A] =>
        im.fx match {
          case Result.Need.Package(pkgId) => driveAuxiliary(im.resume(lookupPackage.lift(pkgId)))
          case Result.Need.Prefetch(_, _) => driveAuxiliary(im.resume(()))
          case Result.Need.Interruption(_) => driveAuxiliary(im.resume(()))
          case _ => step
        }
      case other => other
    }

  private def runFetchTemplate(coid: com.digitalasset.daml.lf.value.Value.ContractId) = {
    val templateId = Ref.Identifier(basicTestsPkgId, "BasicTests:Simple")
    val cmds = ImmArray(speedy.Command.FetchTemplate(templateId, SValue.SContractId(coid)))
    suffixLenientEngine.interpretCommands(
      validating = false,
      submitters = Set(party),
      readAs = Set.empty,
      commands = cmds,
      ledgerTime = now,
      preparationTime = now,
      seeding = InitialSeeding.TransactionSeed(seed),
      contractIdVersion = ContractIdVersion.V1,
      interpretationConfig = InterpretationConfig.Default.copy(
        contractStateMode = ContractStateMachine.Mode.NoKey
      ),
    )
  }

  private def runFetchByKey() = {
    val cmds = ImmArray(speedy.Command.FetchByKey(withKeyTemplateId, withKeySKey))
    suffixLenientEngine.interpretCommands(
      validating = false,
      submitters = Set(alice),
      readAs = Set.empty,
      commands = cmds,
      ledgerTime = now,
      preparationTime = now,
      seeding = InitialSeeding.TransactionSeed(seed),
      contractIdVersion = ContractIdVersion.V1,
      interpretationConfig = InterpretationConfig.Default.copy(
        contractStateMode = ContractStateMachine.Mode.Key
      ),
    )
  }

  "Engine" should {

    "return UnsupportedContractId Error when NeedContract receives UnsupportedContractIdVersion" in {
      // Use a CID that is not in defaultContracts so the engine asks for it.
      val coid = toContractId("BasicTests:Simple:99")
      val result = runFetchTemplate(coid)

      driveAuxiliary(result.start) match {
        case im: Result.Step.Impure[x, ?] =>
          im.fx match {
            case Result.Need.Contract(_) =>
              im.resume(Result.Need.Contract.UnsupportedIdVersion) match {
                case Result.Step.Error(err) =>
                  err shouldBe Error.Interpretation(
                    Error.Interpretation.DamlException(
                      interpretation.Error.UnsupportedContractId(coid)
                    ),
                    None,
                  )
                case other => fail(s"expected an Error step, got $other")
              }
            case other => fail(s"expected a NeedContract, got $other")
          }
        case other => fail(s"expected a NeedContract suspension, got $other")
      }
    }

    "return UnsupportedContractId Error when a NeedKey response contains only UnsupportedContractIdVersion" in {
      val coid = toContractId("BasicTests:WithKey:unsupported")
      val result = runFetchByKey()

      driveAuxiliary(result.start) match {
        case im: Result.Step.Impure[x, ?] =>
          im.fx match {
            case Result.Need.Key(_, _, _) =>
              im.resume(
                Result.Need.Key.Response(
                  Vector(Result.Need.Key.Response.UnsupportedContractIdVersion(coid)),
                  NeedKeyProgression.Finished,
                )
              ) match {
                case Result.Step.Error(err) =>
                  err shouldBe Error.Interpretation(
                    Error.Interpretation.DamlException(
                      interpretation.Error.UnsupportedContractId(coid)
                    ),
                    None,
                  )
                case other => fail(s"expected an Error step, got $other")
              }
            case other => fail(s"expected a NeedKey, got $other")
          }
        case other => fail(s"expected a NeedKey suspension, got $other")
      }
    }

    "defer unsupported overflow entries when first NeedKey entry is supported" in {
      val coid = toContractId("BasicTests:WithKey:unsupported")
      val result = runFetchByKey()

      driveAuxiliary(result.start) match {
        case im: Result.Step.Impure[x, ?] =>
          im.fx match {
            case Result.Need.Key(_, _, _) =>
              val continued = driveAuxiliary(
                im.resume(
                  Result.Need.Key.Response(
                    Vector(
                      Result.Need.Key.Response.AuthenticableFatContractInstance(
                        withKeyContractInst,
                        Hash.HashingMethod.TypedNormalForm,
                        _ => true,
                      ),
                      Result.Need.Key.Response.UnsupportedContractIdVersion(coid),
                    ),
                    NeedKeyProgression.Finished,
                  )
                )
              )
              continued match {
                case im2: Result.Step.Impure[y, ?] =>
                  im2.fx match {
                    case Result.Need.Contract(coid2) =>
                      coid2 shouldBe withKeyContractInst.contractId
                    case other => fail(s"expected a NeedContract, got $other")
                  }
                case other => fail(s"expected a NeedContract suspension, got $other")
              }
            case other => fail(s"expected a NeedKey, got $other")
          }
        case other => fail(s"expected a NeedKey suspension, got $other")
      }
    }
  }
}
