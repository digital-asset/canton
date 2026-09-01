// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.conflictdetection

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.participant.sync.SyncServiceError.SyncServiceAlarm
import com.digitalasset.canton.protocol.{ExampleTransactionFactory, LfContractId, RequestId}
import org.scalatest.wordspec.AnyWordSpec

/** Tests the `crashAfterFailedValidation` participant parameter at its only crash site,
  * [[CommitSet.createForTransaction]].
  *
  * ==Why the flag exists==
  *
  * A participant that detects a protocol violation which a correct node cannot produce has two
  * options: report a `SyncServiceAlarm` and carry on, or additionally fail fast. Canton
  * historically did the latter. The SG-16 "Availability Security" work is progressively removing
  * those crashes, because a node that dies on remote-controlled input hands the peer that supplied
  * it a denial-of-service lever.
  *
  * That trade-off is not yet the right default. Genuine attacks are rare, whereas a participant
  * that hits one of these checks has in practice been misconfigured or has hit a bug -- and there,
  * crashing is what an operator wants, because it surfaces the problem instead of letting the node
  * continue on state that may already have diverged from the rest of the network. The flag exists
  * so that fail-fast behaviour is retained until SG-16 is officially opened, at which point both
  * the flag and the crash it gates are meant to be removed.
  *
  * The flag gates only the crash. The alarm is reported either way, which is what lets tests
  * observe the detection without a node dying: production defaults to `true`, while
  * `ConfigTransforms.setCrashAfterFailedValidation(false)` in `heavyTestDefaults` turns it off
  * across integration tests.
  *
  * ==Why this test exists==
  *
  * Two reasons, both structural rather than incidental.
  *
  * First, the flag replaced an older parameter, `commitAfterFailedActivenessCheck`, whose meaning
  * was the opposite: it named the escape hatch rather than the behaviour, defaulted to `false`, and
  * `false` was the branch that crashed. Renaming it therefore had to invert every use. A purely
  * textual rename would have left `if (!crashAfterFailedValidation)` in place and silently turned
  * production from crashing to not crashing -- a change of behaviour that no type error and no
  * existing assertion would have caught. Pinning both polarities here makes that class of mistake
  * fail loudly.
  *
  * Second, and more durably: because the flag is `true` in production and `false` throughout the
  * test defaults, the crashing branch is a code path that the integration suites do not normally
  * execute. Anything gated on this flag is therefore under-covered by construction. This test is
  * the counterweight -- it drives both branches directly, so the throw is exercised on every run
  * rather than only in the one integration test that opts back into it (See
  * `LedgerConsistencyIntegrationTest`, which configures one participant each way).
  *
  * The exception message is asserted verbatim because that integration test matches it by regular
  * expression through `SyncServiceSynchronizerDisconnect.UnrecoverableException`; changing the
  * wording here breaks it there.
  *
  * Note that the transaction protocol is currently the only path honouring the flag. Reassignments
  * ignore the activeness result in phase 7 altogether; see TODO(#34870).
  */
class CrashAfterFailedValidationTest extends AnyWordSpec with BaseTest {

  import ConflictDetectionHelpers.*

  private val coid00: LfContractId = ExampleTransactionFactory.suffixedId(0, 0)
  private val requestId: RequestId = RequestId(CantonTimestamp.Epoch)

  private implicit val loggingContext: ErrorLoggingContext =
    ErrorLoggingContext.forClass(loggerFactory, getClass)

  private def createForTransaction(
      activenessResult: ActivenessResult,
      crashAfterFailedValidation: Boolean,
  ): CommitSet =
    CommitSet.createForTransaction(
      activenessResult = activenessResult,
      requestId = requestId,
      consumedInputsOfHostedParties = Map.empty,
      transient = Map.empty,
      createdContracts = Map.empty,
      crashAfterFailedValidation = crashAfterFailedValidation,
      hostedOnboardingPartiesO = None,
    )

  "createForTransaction" when {
    "the activeness check succeeded" should {
      "not alarm, whatever the flag" in {
        forAll(Seq(true, false)) { crashAfterFailedValidation =>
          createForTransaction(mkActivenessResult(), crashAfterFailedValidation) shouldBe
            CommitSet.empty
        }
      }
    }

    "the activeness check failed" should {
      val failed = mkActivenessResult(locked = Set(coid00))

      "alarm and crash if crashAfterFailedValidation is set" in {
        loggerFactory.assertLogs(
          a[RuntimeException] shouldBe thrownBy {
            createForTransaction(failed, crashAfterFailedValidation = true)
          },
          _.shouldBeCantonError(
            SyncServiceAlarm.code,
            _ shouldBe s"Request $requestId with failed activeness check is approved.",
          ),
        )
      }

      "alarm but commit if crashAfterFailedValidation is not set" in {
        loggerFactory.assertLogs(
          createForTransaction(failed, crashAfterFailedValidation = false) shouldBe CommitSet.empty,
          _.shouldBeCantonError(
            SyncServiceAlarm.code,
            _ shouldBe s"Request $requestId with failed activeness check is approved.",
          ),
        )
      }
    }
  }
}
