// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.security

import com.digitalasset.canton.crypto.CryptoPureApi
import com.digitalasset.canton.damltests.java.universal.UniversalContract
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.participant.ledger.api.client.JavaDecodeUtil
import com.digitalasset.canton.synchronizer.sequencer.HasProgrammableSequencer
import com.digitalasset.canton.util.MaliciousParticipantNode
import com.digitalasset.canton.version.ProtocolVersion

import java.util.concurrent.atomic.AtomicReference
import scala.jdk.CollectionConverters.*

class MissingSubviewVettingIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with HasProgrammableSequencer
    with HasCycleUtils
    with SecurityTestHelpers {

  private var maliciousP1: MaliciousParticipantNode = _

  private lazy val pureCryptoRef: AtomicReference[CryptoPureApi] = new AtomicReference()
  override def pureCrypto: CryptoPureApi = pureCryptoRef.get()

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1.withSetup { implicit env =>
      import env.*

      participants.all.synchronizers.connect_local(sequencer1, alias = daName)
      participant1.dars.upload(CantonTestsPath)

      maliciousP1 = MaliciousParticipantNode(
        participant1,
        daId,
        testedProtocolVersion,
        timeouts,
        loggerFactory,
      )
      pureCryptoRef.set(participant1.crypto.pureCrypto)
    }

  "A transaction that contains a unvetted subview" should {

    "be rejected" onlyRunWithOrGreaterThan ProtocolVersion.v36 in { implicit env =>
      import env.*

      val alice = env.participant1.parties.enable("Alice")
      val bob = env.participant2.parties.enable("Bob")

      val createCmd = new UniversalContract(
        List.empty.asJava,
        List(alice.toProtoPrimitive).asJava,
        List.empty.asJava,
        List(alice.toProtoPrimitive).asJava,
      ).create.commands.overridePackageId(UniversalContract.PACKAGE_ID).asScala.toSeq

      val cid = JavaDecodeUtil
        .decodeAllCreated(UniversalContract.COMPANION)(
          participant1.ledger_api.javaapi.commands.submit(
            Seq(alice),
            createCmd,
          )
        )
        .loneElement
        .id

      val replaceCmd = cid
        .exerciseReplace(
          List.empty.asJava,
          List(alice.toProtoPrimitive).asJava,
          List(bob.toProtoPrimitive).asJava,
          List(alice.toProtoPrimitive).asJava,
          List.empty.asJava,
        )
        .commands()
        .overridePackageId(UniversalContract.PACKAGE_ID)
        .asScala
        .toSeq

      val (_, events) =
        loggerFactory.assertLoggedWarningsAndErrorsSeq(
          trackingLedgerEvents(participants.all, Seq.empty)(
            maliciousP1.submitJavaApiCommand(Seq(alice), replaceCmd).futureValueUS
          ),
          assertion = LogEntry.assertLogSeq(
            Seq(
              (
                { l =>
                  l.loggerName should include("/participant=participant1/")
                  l.warningMessage should include regex raw"(?s)LOCAL_VERDICT_FAILED_MODEL_CONFORMANCE_CHECK.*Rejected transaction due to a failed model conformance check: UnvettedPackages.*PAR::participant2"
                },
                "participant1 check failure",
              ),
              (
                { l =>
                  l.loggerName should include("/participant=participant2/")
                  l.warningMessage should include regex raw"(?s)LOCAL_VERDICT_FAILED_MODEL_CONFORMANCE_CHECK.*Rejected transaction due to a failed model conformance check:.*MissingPackage"
                },
                "participant2 check failure",
              ),
            )
          ),
        )
      events.assertNoTransactions()
      succeed
    }
  }
}
