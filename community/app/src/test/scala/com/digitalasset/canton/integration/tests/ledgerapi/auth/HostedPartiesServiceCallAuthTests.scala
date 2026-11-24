// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.auth

import com.daml.ledger.api.v2.state_service.{HostedPartiesFilter, HostedParty}
import com.daml.test.evidence.scalatest.ScalaTestSupport.Implicits.*
import com.digitalasset.canton.integration.TestConsoleEnvironment

import java.util.UUID
import scala.concurrent.Future

trait HostedPartiesServiceCallAuthTests extends PublicServiceCallAuthTests {

  private val otherParty = s"otherParty-${UUID.randomUUID()}"

  override protected def prerequisiteParties: List[String] =
    otherParty :: super.prerequisiteParties

  private def getOtherPartyId(implicit env: TestConsoleEnvironment): String =
    getPartyId(otherParty)

  protected def hostedParties(
      context: ServiceCallContext,
      filter: Option[HostedPartiesFilter],
  )(implicit env: TestConsoleEnvironment): Future[Seq[HostedParty]]

  // TODO(#38113): Use the default selection once callers without readable parties are supported
  override def serviceCall(
      context: ServiceCallContext
  )(implicit env: TestConsoleEnvironment): Future[Any] =
    hostedParties(
      context,
      Some(
        HostedPartiesFilter(
          partyFilter = Seq.empty,
          allParticipants = false,
          synchronizerIdFilter = Seq.empty,
        )
      ),
    )

  serviceCallName should {
    "honor supplied filters without requiring party read access" taggedAs securityAsset
      .setHappyCase(
        "Supplied filters are public, including an empty filter selecting all parties"
      ) in { implicit env =>
      import env.*
      val (_, withoutRights) = createUserByAdmin(randomUserId()).futureValue
      def assertFiltersArePublic(context: ServiceCallContext): Unit = {
        val explicit = hostedParties(
          context,
          Some(
            HostedPartiesFilter(
              partyFilter = Seq(getOtherPartyId),
              allParticipants = false,
              synchronizerIdFilter = Seq.empty,
            )
          ),
        ).futureValue
        explicit.map(_.party) shouldBe Seq(getOtherPartyId)
        val all = hostedParties(
          context,
          Some(
            HostedPartiesFilter(
              partyFilter = Seq.empty,
              allParticipants = false,
              synchronizerIdFilter = Seq.empty,
            )
          ),
        ).futureValue
        all.map(_.party) should contain allElementsOf Seq(getPartyId(randomParty), getOtherPartyId)
      }
      assertFiltersArePublic(withoutRights)
      assertFiltersArePublic(canReadAsRandomParty)
    }
  }
}
