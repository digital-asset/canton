// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.upgrading

import com.daml.ledger.javaapi.data.codegen.UnknownTrailingFieldPolicy
import com.digitalasset.canton.damltests.upgrade.v1.java as v1
import com.digitalasset.canton.damltests.upgrade.v1.java.upgrade.Upgrading
import com.digitalasset.canton.damltests.upgrade.v2.java as v2
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseH2}
import com.digitalasset.canton.integration.tests.upgrading.UpgradingBaseTest.{UpgradeV1, UpgradeV2}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.topology.Party

import java.util.Optional
import scala.jdk.CollectionConverters.{CollectionHasAsScala, SeqHasAsJava}

class ValueDecoderUpgradingGrpcIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment {

  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))

  @volatile private var alice: Party = _

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1_S1M1.withSetup { implicit env =>
      import env.*

      participant1.synchronizers.connect_local(sequencer1, alias = daName)

      alice = participant1.parties.testing.enable("alice")
      participant1.dars.upload(UpgradeV1)
      participant1.dars.upload(UpgradeV2)

    }

  "GRPC Client" should {

    "be able to decode Upgrade V2 GRPC payload into V1 with policy Ignore" in { implicit env =>
      import env.*

      val issuer = alice.toLf
      val owner = alice.toLf
      val field = 1337
      val more = List("extra data").asJava

      participant1.ledger_api.javaapi.commands.submit(
        Seq(alice),
        new v2.upgrade.Upgrading(
          issuer,
          owner,
          field,
          Optional.of(more),
        ).create.commands.asScala.toSeq,
      )

      val v1ContractIgnore = participant1.ledger_api.javaapi.state.acs
        .await(v1.upgrade.Upgrading.COMPANION, UnknownTrailingFieldPolicy.IGNORE)(alice)

      v1ContractIgnore.data shouldBe new Upgrading(issuer, owner, field) withClue (
        "Decoded contract should match submitted contract (ignoring extra V2 fields)",
      )

      intercept[IllegalArgumentException](
        participant1.ledger_api.javaapi.state.acs
          .await(v1.upgrade.Upgrading.COMPANION, UnknownTrailingFieldPolicy.STRICT)(alice)
      ).getMessage shouldBe "Unexpected non-empty 1 fields were received and Strict policy is used"
    }
  }
}
