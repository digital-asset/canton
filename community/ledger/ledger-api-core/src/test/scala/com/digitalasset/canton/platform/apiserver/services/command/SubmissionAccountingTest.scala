// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services.command

import com.digitalasset.canton.config.CantonRequireTypes.String255
import com.digitalasset.canton.ledger.participant.state.ChangeId
import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.platform.apiserver.services.command.TrafficAccountSelection.*
import com.digitalasset.canton.tea.TrafficEnforcementErrors.FatalFailure
import com.digitalasset.canton.{BaseTest, LfPartyId}
import com.digitalasset.daml.lf.data.Ref
import org.scalatest.wordspec.AnyWordSpec

class SubmissionAccountingTest extends AnyWordSpec with BaseTest {

  private val adminParty = LfPartyId.assertFromString("participantAdmin")
  private val alice = LfPartyId.assertFromString("Alice")
  private val bob = LfPartyId.assertFromString("Bob")

  "SubmissionAccounting" should {

    "select the account by the fallback rules" in {
      val cases = Seq[(Option[String], Set[LfPartyId], TrafficAccountSelection)](
        (Some("acc"), Set(alice, bob), Explicit("acc")),
        (Some("acc"), Set(adminParty), Explicit("acc")),
        (None, Set(alice), FallbackParty(alice)),
        (None, Set(adminParty), AdminFallback(adminParty)),
        (None, Set(alice, bob), NoAccount),
        (None, Set(adminParty, alice), NoAccount),
      )

      forAll(cases) { case (accountId, actAs, expected) =>
        SubmissionAccounting
          .forSubmitter(accountId.map(String255.tryCreate(_)), actAs, adminParty)
          .selection shouldBe expected
      }
    }

    "require a missing submission id" in {
      implicit val errorLoggingContext: ErrorLoggingContext =
        ErrorLoggingContext.fromTracedLogger(logger)
      val changeId = ChangeId(
        Ref.UserId.assertFromString("user"),
        Ref.CommandId.assertFromString("command"),
        Set(alice),
      )
      val accounting = SubmissionAccounting(FallbackParty(alice))
      val submissionId = Ref.SubmissionId.assertFromString("submission")

      // Without submission id.
      loggerFactory.assertLogs(
        SubmissionAccountingInfo
          .create(changeId, None, accounting)
          .left
          .value
          .code
          .id shouldBe FatalFailure.id,
        _.shouldBeCantonErrorCode(FatalFailure),
      )

      // With submission id.
      SubmissionAccountingInfo.create(changeId, Some(submissionId), accounting) shouldBe Right(
        SubmissionAccountingInfo(changeId, submissionId, accounting)
      )
    }
  }
}
