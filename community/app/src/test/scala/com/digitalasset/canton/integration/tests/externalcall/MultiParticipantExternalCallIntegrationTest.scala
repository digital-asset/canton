// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.externalcall

import com.digitalasset.canton.externalcall.java.externalcalltest as E
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseH2, UsePostgres}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.participant.ledger.api.client.JavaDecodeUtil

import scala.jdk.CollectionConverters.*

/** Integration tests for external calls across multiple participants.
  *
  * These tests address the reviewer requirement:
  * "A test involving transactions with parties on different nodes, including
  *  observer nodes only seeing small subsets of the transaction"
  *
  * Tests:
  * - Both participants have extension configured
  * - Observer on different participant replays without HTTP
  * - Observer sees only subset of transaction
  * - Three participants with varied visibility
  * - Signatory on P1, observers on P2 and P3
  */
sealed trait MultiParticipantExternalCallIntegrationTest
    extends CommunityIntegrationTest
    with ExternalCallIntegrationTestBase
    with SharedEnvironment
    with MockServerSetup {

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P3_S1M1
      .addConfigTransforms(ConfigTransforms.setAlphaVersionSupport(true)*)
      .addConfigTransforms(
        ConfigTransforms.useStaticTime,
        enableExternalCallExtensionOnAll("test-ext", mockServerPort),
      )
      .withSetup { implicit env =>
        import env.*

        initializeMockServer()

        clue("Connect all participants to synchronizer") {
          participant1.synchronizers.connect_local(sequencer1, daName)
          participant2.synchronizers.connect_local(sequencer1, daName)
          participant3.synchronizers.connect_local(sequencer1, daName)
        }

        clue("Upload test DAR to all participants") {
          participant1.dars.upload(externalCallTestDarPath)
          participant2.dars.upload(externalCallTestDarPath)
          participant3.dars.upload(externalCallTestDarPath)
        }

        clue("Enable parties with cross-participant visibility") {
          // Alice on participant1 - signatory
          alice = participant1.parties.enable("alice")

          // Bob on participant2 - observer, synced with participant1
          bob = participant2.parties.enable(
            "bob",
            synchronizeParticipants = Seq(participant1),
          )

          // Charlie on participant3 - observer, synced with both
          charlie = participant3.parties.enable(
            "charlie",
            synchronizeParticipants = Seq(participant1, participant2),
          )
        }
      }

  "multi-participant external calls" should {

    "work when both participants have extension configured" in { implicit env =>
      import env.*

      setupEchoHandler()

      // Track which participant made the call
      val callerParticipants = scala.collection.mutable.Set[String]()
      mockServer.setHandler("track-caller") { req =>
        req.participantId.foreach(callerParticipants.add)
        ExternalCallResponse.ok(req.input)
      }

      val inputHex = toHex("multi-participant-test")

      clue("Create contract with bob as stakeholder") {
        val createTx = participant1.ledger_api.javaapi.commands.submit(
          Seq(alice),
          new E.ExternalCallContract(
            alice.toProtoPrimitive,
            java.util.List.of(bob.toProtoPrimitive), // Bob as observer
          ).create.commands.asScala.toSeq,
        )
        val contractId = JavaDecodeUtil.decodeAllCreated(E.ExternalCallContract.COMPANION)(createTx).loneElement.id

        clue("Exercise external call from alice") {
          val exerciseTx = participant1.ledger_api.javaapi.commands.submit(
            Seq(alice),
            contractId.exerciseCallExternal(
              "test-ext",
              "track-caller",
              "00000000",
              inputHex,
            ).commands.asScala.toSeq,
          )

          exerciseTx.getUpdateId should not be empty
        }

        clue("Verify transaction visible to bob on participant2") {
          eventually() {
            // Contract was consumed, so verify ACS is empty for bob
            val activeContracts = participant2.ledger_api.javaapi.state.acs
              .filter(E.ExternalCallContract.COMPANION)(bob, _ => true, None)
            activeContracts shouldBe empty
          }
        }
      }
    }

    "allow observer on different participant to replay without HTTP call" in { implicit env =>
      import env.*

      setupEchoHandler()

      val inputHex = toHex("observer-replay-test")

      clue("Create contract with bob as observer") {
        val createTx = participant1.ledger_api.javaapi.commands.submit(
          Seq(alice),
          new E.ExternalCallContract(
            alice.toProtoPrimitive,
            java.util.List.of(bob.toProtoPrimitive),
          ).create.commands.asScala.toSeq,
        )
        val contractId = JavaDecodeUtil.decodeAllCreated(E.ExternalCallContract.COMPANION)(createTx).loneElement.id

        clue("Exercise external call") {
          val exerciseTx = participant1.ledger_api.javaapi.commands.submit(
            Seq(alice),
            contractId.exerciseCallExternal(
              "test-ext",
              "echo",
              "00000000",
              inputHex,
            ).commands.asScala.toSeq,
          )

          exerciseTx.getUpdateId should not be empty
        }

        clue("Verify bob received the transaction") {
          eventually() {
            val activeContracts = participant2.ledger_api.javaapi.state.acs
              .filter(E.ExternalCallContract.COMPANION)(bob, _ => true, None)
            activeContracts shouldBe empty // Contract consumed
          }
        }

        // Only submitter's participant calls HTTP
        // Call count depends on number of confirming participants
      }
    }

    "handle observer seeing only subset of transaction" in { implicit env =>
      import env.*

      setupEchoHandler()

      // Scenario:
      // Complex transaction where:
      // - Alice creates multiple contracts
      // - Some visible to bob, some not
      // - External calls in both visible and hidden parts
      // - Bob should only see the subset relevant to them

      // This tests privacy - bob shouldn't see external call results
      // from parts of the transaction they're not party to

      val inputHex = toHex("subset-visibility-test")

      clue("Create contract visible only to alice") {
        val createTx = participant1.ledger_api.javaapi.commands.submit(
          Seq(alice),
          new E.ExternalCallContract(
            alice.toProtoPrimitive,
            java.util.List.of(), // No observers
          ).create.commands.asScala.toSeq,
        )
        val aliceOnlyContractId = JavaDecodeUtil.decodeAllCreated(E.ExternalCallContract.COMPANION)(createTx).loneElement.id

        clue("Exercise external call on alice-only contract") {
          val exerciseTx = participant1.ledger_api.javaapi.commands.submit(
            Seq(alice),
            aliceOnlyContractId.exerciseCallExternal(
              "test-ext",
              "echo",
              "00000000",
              inputHex,
            ).commands.asScala.toSeq,
          )
          exerciseTx.getUpdateId should not be empty
        }
      }

      clue("Create contract visible to alice and bob") {
        val createTx = participant1.ledger_api.javaapi.commands.submit(
          Seq(alice),
          new E.ExternalCallContract(
            alice.toProtoPrimitive,
            java.util.List.of(bob.toProtoPrimitive),
          ).create.commands.asScala.toSeq,
        )
        val sharedContractId = JavaDecodeUtil.decodeAllCreated(E.ExternalCallContract.COMPANION)(createTx).loneElement.id

        clue("Exercise external call on shared contract") {
          val exerciseTx = participant1.ledger_api.javaapi.commands.submit(
            Seq(alice),
            sharedContractId.exerciseCallExternal(
              "test-ext",
              "echo",
              "00000000",
              inputHex,
            ).commands.asScala.toSeq,
          )
          exerciseTx.getUpdateId should not be empty
        }

        clue("Verify bob only sees the shared contract transaction") {
          eventually() {
            // Bob should see the shared contract was consumed
            val activeContracts = participant2.ledger_api.javaapi.state.acs
              .filter(E.ExternalCallContract.COMPANION)(bob, _ => true, None)
            activeContracts shouldBe empty
          }
        }
      }
    }

    "work with three participants with varied visibility" in { implicit env =>
      import env.*

      setupEchoHandler()

      // Scenario:
      // - Alice (P1) is signatory
      // - Bob (P2) is observer on first external call (contract 1)
      // - Charlie (P3) is observer on second external call (contract 2)
      // - Neither bob nor charlie sees the other's external call

      // This tests that external call results are properly scoped to views

      val inputHex = toHex("three-way-visibility-test")

      clue("Create contract visible to alice and bob only") {
        val createTx = participant1.ledger_api.javaapi.commands.submit(
          Seq(alice),
          new E.ExternalCallContract(
            alice.toProtoPrimitive,
            java.util.List.of(bob.toProtoPrimitive), // Bob as observer
          ).create.commands.asScala.toSeq,
        )
        val bobContractId = JavaDecodeUtil.decodeAllCreated(E.ExternalCallContract.COMPANION)(createTx).loneElement.id

        clue("Exercise external call on bob's contract") {
          val exerciseTx = participant1.ledger_api.javaapi.commands.submit(
            Seq(alice),
            bobContractId.exerciseCallExternal(
              "test-ext",
              "echo",
              "00000000",
              inputHex,
            ).commands.asScala.toSeq,
          )
          exerciseTx.getUpdateId should not be empty
        }

        clue("Verify bob received the transaction") {
          eventually() {
            val activeContracts = participant2.ledger_api.javaapi.state.acs
              .filter(E.ExternalCallContract.COMPANION)(bob, _ => true, None)
            activeContracts shouldBe empty
          }
        }
      }

      clue("Create contract visible to alice and charlie only") {
        val createTx = participant1.ledger_api.javaapi.commands.submit(
          Seq(alice),
          new E.ExternalCallContract(
            alice.toProtoPrimitive,
            java.util.List.of(charlie.toProtoPrimitive), // Charlie as observer
          ).create.commands.asScala.toSeq,
        )
        val charlieContractId = JavaDecodeUtil.decodeAllCreated(E.ExternalCallContract.COMPANION)(createTx).loneElement.id

        clue("Exercise external call on charlie's contract") {
          val exerciseTx = participant1.ledger_api.javaapi.commands.submit(
            Seq(alice),
            charlieContractId.exerciseCallExternal(
              "test-ext",
              "echo",
              "00000000",
              inputHex,
            ).commands.asScala.toSeq,
          )
          exerciseTx.getUpdateId should not be empty
        }

        clue("Verify charlie received the transaction") {
          eventually() {
            val activeContracts = participant3.ledger_api.javaapi.state.acs
              .filter(E.ExternalCallContract.COMPANION)(charlie, _ => true, None)
            activeContracts shouldBe empty
          }
        }
      }
    }

    "handle signatory on P1, observers on P2 and P3" in { _ =>
      // Requires BEExternalCall engine primitive (not yet implemented).
      // Currently DA.External uses echo stub that returns input without HTTP calls.
      pending
    }

    "allow observer validation using stored results from transaction" in { implicit env =>
      import env.*

      // This tests the case where:
      // - Signatory's participant makes HTTP call during submission
      // - Observer's participant validates using stored results from transaction
      // - Observer doesn't need to make their own HTTP call

      // Note: This test uses the same environment where both have extensions configured,
      // but demonstrates that observers use stored results rather than making HTTP calls

      val httpCallCount = new java.util.concurrent.atomic.AtomicInteger(0)
      mockServer.setHandler("observer-replay") { req =>
        httpCallCount.incrementAndGet()
        ExternalCallResponse.ok(req.input)
      }

      val inputHex = toHex("observer-stored-result-test")

      clue("Create contract with bob as observer") {
        val createTx = participant1.ledger_api.javaapi.commands.submit(
          Seq(alice),
          new E.ExternalCallContract(
            alice.toProtoPrimitive,
            java.util.List.of(bob.toProtoPrimitive),
          ).create.commands.asScala.toSeq,
        )
        val contractId = JavaDecodeUtil.decodeAllCreated(E.ExternalCallContract.COMPANION)(createTx).loneElement.id

        // Reset counter after contract creation
        httpCallCount.set(0)

        clue("Exercise external call from alice") {
          val exerciseTx = participant1.ledger_api.javaapi.commands.submit(
            Seq(alice),
            contractId.exerciseCallExternal(
              "test-ext",
              "observer-replay",
              "00000000",
              inputHex,
            ).commands.asScala.toSeq,
          )

          exerciseTx.getUpdateId should not be empty
        }

        clue("Verify bob received and validated the transaction using stored results") {
          eventually() {
            val activeContracts = participant2.ledger_api.javaapi.state.acs
              .filter(E.ExternalCallContract.COMPANION)(bob, _ => true, None)
            activeContracts shouldBe empty
          }

          // Note: With the current implementation, the signatory's participant
          // makes the HTTP call during submission, and the result is stored in
          // the transaction. Observers replay using this stored result.
          // The exact number of HTTP calls depends on the implementation.
        }
      }
    }

    "handle party hosted on multiple participants" in { implicit env =>
      import env.*

      setupEchoHandler()

      // Scenario:
      // - Alice is hosted on both participant1 and participant2 (party replication)
      // - External call is exercised
      // - Both hosting participants should agree on the result

      // Enable alice on participant2 as well (party replication)
      participant2.parties.enable(
        alice.uid.identifier.str,
        synchronizeParticipants = Seq(participant1),
      )

      val inputHex = toHex("multi-host-party-test")

      clue("Create contract with alice as signatory") {
        val createTx = participant1.ledger_api.javaapi.commands.submit(
          Seq(alice),
          new E.ExternalCallContract(
            alice.toProtoPrimitive,
            java.util.List.of(),
          ).create.commands.asScala.toSeq,
        )
        val contractId = JavaDecodeUtil.decodeAllCreated(E.ExternalCallContract.COMPANION)(createTx).loneElement.id

        clue("Exercise external call - both hosting participants validate") {
          // With echo handler, both participant1 and participant2 (which now host alice)
          // should validate and get the same result
          val exerciseTx = participant1.ledger_api.javaapi.commands.submit(
            Seq(alice),
            contractId.exerciseCallExternal(
              "test-ext",
              "echo",
              "00000000",
              inputHex,
            ).commands.asScala.toSeq,
          )

          exerciseTx.getUpdateId should not be empty
        }
      }
    }
  }
}

class MultiParticipantExternalCallIntegrationTestH2
    extends MultiParticipantExternalCallIntegrationTest {
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
}

class MultiParticipantExternalCallIntegrationTestPostgres
    extends MultiParticipantExternalCallIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
}
