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
import org.scalatest.Inside
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class UnsupportedContractIdEngineSpec
    extends AnyWordSpec
    with Matchers
    with Inside
    with SuppressingLogging {

  implicit val logContext: LoggingContext = LoggingContext.ForTesting

  private val helpers =
    new EngineTestHelpers(ContractIdVersion.V1, "BasicTests-keys.dar", loggerFactory)
  import helpers.*

  private val seed = hash("UnsupportedContractIdEngineSpec")
  private val now = Time.Timestamp.now()
  private val withKeyTemplateId = Ref.Identifier(basicTestsPkgId, "BasicTests:WithKey")
  private val withKeySKey = mkSValuePair(SValue.SParty(alice), SValue.SInt64(42))

  private def driveAuxiliary[A](result: Result[A]): Result[A] = result match {
    case ResultNeedPackage(pkgId, resume) => driveAuxiliary(resume(lookupPackage.lift(pkgId)))
    case ResultPrefetch(_, _, r) => driveAuxiliary(r())
    case ResultInterruption(continue, _) => driveAuxiliary(continue())
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
        contractStateMode = ContractStateMachine.Mode.NUCK
      ),
    )
  }

  "Engine" should {

    "return UnsupportedContractId Error when ResultNeedContract receives UnsupportedContractIdVersion" in {
      // Use a CID that is not in defaultContracts so the engine asks for it.
      val coid = toContractId("BasicTests:Simple:99")
      val result = runFetchTemplate(coid)

      inside(driveAuxiliary(result)) { case ResultNeedContract(_, resume) =>
        inside(resume(ResultNeedContract.Response.UnsupportedContractIdVersion)) {
          case ResultError(err) =>
            err shouldBe Error.Interpretation(
              Error.Interpretation.DamlException(
                interpretation.Error.UnsupportedContractId(coid)
              ),
              None,
            )
        }
      }
    }

    "return UnsupportedContractId Error when a ResultNeedKey response contains only UnsupportedContractIdVersion" in {
      val coid = toContractId("BasicTests:WithKey:unsupported")
      val result = runFetchByKey()

      inside(driveAuxiliary(result)) { case ResultNeedKey(_, _, _, resume) =>
        inside(
          resume(
            ResultNeedKey.Response(
              Vector(ResultNeedKey.Response.UnsupportedContractIdVersion(coid)),
              NeedKeyProgression.Finished,
            )
          )
        ) { case ResultError(err) =>
          err shouldBe Error.Interpretation(
            Error.Interpretation.DamlException(
              interpretation.Error.UnsupportedContractId(coid)
            ),
            None,
          )
        }
      }
    }

    "defer unsupported overflow entries when first NeedKey entry is supported" in {
      val coid = toContractId("BasicTests:WithKey:unsupported")
      val result = runFetchByKey()

      inside(driveAuxiliary(result)) { case ResultNeedKey(_, _, _, resume) =>
        val continued = driveAuxiliary(
          resume(
            ResultNeedKey.Response(
              Vector(
                ResultNeedKey.Response.AuthenticableFatContractInstance(
                  withKeyContractInst,
                  Hash.HashingMethod.TypedNormalForm,
                  _ => true,
                ),
                ResultNeedKey.Response.UnsupportedContractIdVersion(coid),
              ),
              NeedKeyProgression.Finished,
            )
          )
        )
        inside(continued) { case ResultNeedContract(coid, _) =>
          coid shouldBe withKeyContractInst.contractId
        }
      }
    }
  }
}
