// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.modelbased

import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.participant.ledger.api.client.JavaDecodeUtil
import com.digitalasset.canton.testing.modelbased.universal.java.universal.{
  NonConsumingChoice,
  SomeContractId,
  TxAction,
  Universal,
  UniversalWithKey,
  txaction,
}
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.canton.version.ProtocolVersion

import java.util as ju
import scala.jdk.CollectionConverters.*

class BasicNuckIntegrationTest extends CommunityIntegrationTest with SharedEnvironment {

  private var alice: PartyId = _

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1_S1M1
      .withSetup { implicit env =>
        import env.*
        participant1.synchronizers.connect_local(sequencer1, alias = daName)
        participant1.dars.upload(BaseTest.UniversalPath)
        alice = participant1.parties.enable("alice")
      }

  def buildUniversalWithKey(
      signatories: Seq[PartyId],
      observers: Seq[PartyId],
      contractId: Long,
      keyId: Long,
      maintainers: Seq[PartyId],
  ): UniversalWithKey =
    new UniversalWithKey(
      signatories.map(_.toProtoPrimitive).asJava,
      observers.map(_.toProtoPrimitive).asJava,
      contractId,
      keyId,
      maintainers.map(_.toProtoPrimitive).asJava,
    )

  def buildUniversal(
      signatories: Seq[PartyId],
      observers: Seq[PartyId],
  ): Universal =
    new Universal(
      signatories.map(_.toProtoPrimitive).asJava,
      observers.map(_.toProtoPrimitive).asJava,
    )

  def buildNonConsumingChoice(
      controllers: Seq[PartyId],
      subTransaction: Seq[TxAction],
      env: Map[java.lang.Long, SomeContractId] = Map.empty,
      choiceObservers: Seq[PartyId] = Seq.empty,
  ): NonConsumingChoice =
    new NonConsumingChoice(
      new ju.HashMap[java.lang.Long, SomeContractId](env.asJava),
      controllers.map(_.toProtoPrimitive).asJava,
      choiceObservers.map(_.toProtoPrimitive).asJava,
      subTransaction.asJava,
    )

  def buildQueryByKey(
      keyId: Long,
      maintainers: Seq[PartyId],
      exhaustive: Boolean,
      expectedContractIds: Seq[Long],
  ): txaction.QueryByKey =
    new txaction.QueryByKey(
      keyId,
      maintainers.map(_.toProtoPrimitive).asJava,
      exhaustive,
      expectedContractIds.map(java.lang.Long.valueOf).asJava,
    )

  "End to end" when {

    "Create universal" onlyRunWithOrGreaterThan ProtocolVersion.v35 in { implicit env =>
      import env.*

      val key = 1L

      val testIds = Seq(1L, 2L, 3L, 4L, 5L)

      val universalWithKeys = testIds.map { id =>
        buildUniversalWithKey(Seq(alice), Seq.empty, id, key, Seq(alice))
      }

      universalWithKeys.map { u =>
        val tx = participant1.ledger_api.javaapi.commands.submit(
          Seq(alice),
          u.create().commands().asScala.toSeq,
        )
        JavaDecodeUtil.decodeAllCreated(UniversalWithKey.COMPANION)(tx).loneElement.id
      }

      val universalTx = participant1.ledger_api.javaapi.commands.submit(
        Seq(alice),
        buildUniversal(Seq(alice), Seq.empty).create().commands().asScala.toSeq,
      )
      val uCid = JavaDecodeUtil.decodeAllCreated(Universal.COMPANION)(universalTx).loneElement.id

      val queryByKey =
        buildQueryByKey(key, Seq(alice), exhaustive = false, expectedContractIds = testIds.reverse)

      val uChoice = buildNonConsumingChoice(Seq(alice), Seq(queryByKey))

      participant1.ledger_api.javaapi.commands.submit(
        Seq(alice),
        uCid.exerciseNonConsumingChoice(uChoice).commands().asScala.toSeq,
      )

    }
  }

}
