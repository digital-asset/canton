// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.multisynchronizer

import com.daml.ledger.api.v2.transaction_filter.TransactionShape.TRANSACTION_SHAPE_LEDGER_EFFECTS
import com.daml.ledger.javaapi.data.ExercisedEvent
import com.digitalasset.canton.damltestslf23.java.basickeys.{BasicKey, KeyOps}
import com.digitalasset.canton.integration.util.TestUtils.damlSet
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.ledger.error.groups.CommandExecutionErrors
import com.digitalasset.canton.participant.ledger.api.client.JavaDecodeUtil
import com.digitalasset.canton.protocol.LfContractId
import com.digitalasset.canton.topology.PartyId

import scala.jdk.CollectionConverters.*
import scala.jdk.OptionConverters.*

/** Checks the interaction of contract keys with multiple synchronizers: a key lookup on one
  * synchronizer for a keyed contract that currently is assigned to another synchronizer.
  */
final class AutomaticReassignmentContractKeysIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment {

  override def environmentDefinition: EnvironmentDefinition = EnvironmentDefinition.P1_S1M1_S1M1
    .addConfigTransforms(
      ConfigTransforms.enableMultiSynchronizerTopologyFeatureFlag,
      ConfigTransforms.disableAutomaticAssignment,
    )
    .withSetup { implicit env =>
      import env.*
      participant1.synchronizers.connect_local(sequencer1, alias = daName)
      participant1.synchronizers.connect_local(sequencer2, alias = acmeName)
      participant1.dars.upload(CantonTestsLF23Path, synchronizerId = daId)
      participant1.dars.upload(CantonTestsLF23Path, synchronizerId = acmeId)
    }

  "Automatic reassignment" should {
    /*
    Scenario:
    - Create BasicKey on acme
    - Create KeyOps on da
    - Exercise KeyOps.Lookup on da
    - Both contracts should be assigned to da afterwards
     */
    "reassign a keyed contract looked up by key" in { implicit env =>
      import env.*

      val alice = participant1.parties.enable("alice", synchronizer = daName)
      participant1.parties.enable("alice", synchronizer = acmeName)

      val aliceSet = damlSet(Set(alice.toProtoPrimitive))

      // Create BasicKey on acme
      val basicKey = JavaDecodeUtil
        .decodeAllCreated(BasicKey.COMPANION)(
          participant1.ledger_api.javaapi.commands.submit(
            actAs = Seq(alice),
            commands =
              new BasicKey(aliceSet, damlSet(Set.empty[String])).create.commands.asScala.toSeq,
            synchronizerId = Some(acmeId),
          )
        )
        .loneElement

      // Create KeyOps on da
      val keyOps = JavaDecodeUtil
        .decodeAllCreated(KeyOps.COMPANION)(
          participant1.ledger_api.javaapi.commands.submit(
            actAs = Seq(alice),
            commands = new KeyOps(aliceSet, alice.toProtoPrimitive).create.commands.asScala.toSeq,
            synchronizerId = Some(daId),
          )
        )
        .loneElement

      def synchronizerOf(contractId: String): String =
        participant1.ledger_api.state.acs
          .active_contracts_of_party(alice)
          .filter(_.createdEvent.value.contractId == contractId)
          .loneElement
          .synchronizerId

      synchronizerOf(basicKey.id.contractId) shouldBe acmeId.logical.toProtoPrimitive
      synchronizerOf(keyOps.id.contractId) shouldBe daId.logical.toProtoPrimitive

      // Exercise KeyOps.Lookup (succeeds iff no exception is thrown)
      val lookupTx = participant1.ledger_api.javaapi.commands.submit(
        actAs = Seq(alice),
        commands = keyOps.id.exerciseLookup(alice.toProtoPrimitive).commands.asScala.toSeq,
        synchronizerId = Some(daId),
      )

      lookupTx.getSynchronizerId shouldBe daId.logical.toProtoPrimitive

      // Both contracts are on da
      synchronizerOf(basicKey.id.contractId) shouldBe daId.logical.toProtoPrimitive
      synchronizerOf(keyOps.id.contractId) shouldBe daId.logical.toProtoPrimitive
    }

    /*
    Scenario:
    - Create BasicKey on da
    - Create KeyOps on da
    - Unassign BasicKey from da to acme (no assignment, so the contract is in flight)
    - Exercise KeyOps.Lookup on da: it succeeds, but the lookup returns None
    - BasicKey stays in flight, KeyOps stays assigned to da
     */
    "not reassign a keyed contract looked up by key while it is in-flight" in { implicit env =>
      import env.*

      val (bob, basicKey, keyOps) = createInFlightSetup("bob")

      // Exercise KeyOps.Lookup (succeeds iff no exception is thrown).
      // Ledger effects shape, so that the exercised event (and its result) is included.
      val lookupTx = participant1.ledger_api.javaapi.commands.submit(
        actAs = Seq(bob),
        commands = keyOps.id.exerciseLookup(bob.toProtoPrimitive).commands.asScala.toSeq,
        synchronizerId = Some(daId),
        transactionShape = TRANSACTION_SHAPE_LEDGER_EFFECTS,
      )

      lookupTx.getSynchronizerId shouldBe daId.logical.toProtoPrimitive

      // Result of `lookupByKey`: the in-flight BasicKey is not visible, so the lookup yields None
      val lookupResult = lookupTx.getEvents.asScala
        .collect { case e: ExercisedEvent if e.getChoice == "Lookup" => e }
        .loneElement
        .getExerciseResult
        .asOptional()
        .toScala
        .value
        .getValue
        .toScala
        .map(_.asContractId().toScala.value.getValue)

      lookupResult shouldBe None

      //  BasicKey is in flight, so it is not active anywhere and the key lookup resolves to
      //  `None` (asserted above). BasicKey is then not an input contract of the transaction, and
      //  the router has no reason to reassign it.
      //  Even if it were an input, the router cannot reassign an in-flight contract: the
      //  assignment to acme has to complete first, which is blocked by the
      //  enable-automatic-assignment==false setting.
      activeSynchronizerOf(basicKey.id.contractId, bob) shouldBe None
      participant1.ledger_api.state.acs
        .incomplete_unassigned_of_party(bob)
        .map(_.contractId) should contain(basicKey.id.contractId)
      activeSynchronizerOf(keyOps.id.contractId, bob) shouldBe Some(daId.logical.toProtoPrimitive)
    }

    /*
    Scenario:
    - Create BasicKey on da
    - Create KeyOps on da
    - Unassign BasicKey from da to acme (no assignment, so the contract is in flight)
    - Exercise KeyOps.Fetch on da: it is rejected with CONTRACT_KEY_NOT_FOUND
    - BasicKey stays in flight, KeyOps stays on da
     */
    "not reassign a keyed contract fetched by key while it is in-flight" in { implicit env =>
      import env.*

      val (charlie, basicKey, keyOps) = createInFlightSetup("charlie")

      // Exercise KeyOps.Fetch: the submission is rejected during interpretation with
      //   NOT_FOUND/CONTRACT_KEY_NOT_FOUND: dependency error: couldn't find key: GlobalKey(...BasicKey...)
      // The in-flight BasicKey is not active on any synchronizer, so key resolution does not
      // succeed. `fetchByKey` requires the key to resolve and aborts the interpretation.
      assertThrowsAndLogsCommandFailures(
        participant1.ledger_api.javaapi.commands.submit(
          actAs = Seq(charlie),
          commands = keyOps.id.exerciseFetch(charlie.toProtoPrimitive).commands.asScala.toSeq,
          synchronizerId = Some(daId),
        ),
        _.shouldBeCommandFailure(
          CommandExecutionErrors.Interpreter.LookupErrors.ContractKeyNotFound
        ),
      )

      activeSynchronizerOf(basicKey.id.contractId, charlie) shouldBe None
      participant1.ledger_api.state.acs
        .incomplete_unassigned_of_party(charlie)
        .map(_.contractId) should contain(basicKey.id.contractId)
      activeSynchronizerOf(keyOps.id.contractId, charlie) shouldBe Some(
        daId.logical.toProtoPrimitive
      )
    }
  }

  private def activeSynchronizerOf(contractId: String, party: PartyId)(implicit
      env: FixtureParam
  ): Option[String] =
    env.participant1.ledger_api.state.acs
      .active_contracts_of_party(party)
      .filter(_.createdEvent.value.contractId == contractId)
      .map(_.synchronizerId)
      .headOption

  private def createInFlightSetup(
      partyName: String
  )(implicit env: FixtureParam): (PartyId, BasicKey.Contract, KeyOps.Contract) = {
    import env.*

    val party = participant1.parties.enable(partyName, synchronizer = daName)
    participant1.parties.enable(partyName, synchronizer = acmeName)

    val partySet = damlSet(Set(party.toProtoPrimitive))

    // Create BasicKey on da
    val basicKey = JavaDecodeUtil
      .decodeAllCreated(BasicKey.COMPANION)(
        participant1.ledger_api.javaapi.commands.submit(
          actAs = Seq(party),
          commands =
            new BasicKey(partySet, damlSet(Set.empty[String])).create.commands.asScala.toSeq,
          synchronizerId = Some(daId),
        )
      )
      .loneElement

    // Create KeyOps on da
    val keyOps = JavaDecodeUtil
      .decodeAllCreated(KeyOps.COMPANION)(
        participant1.ledger_api.javaapi.commands.submit(
          actAs = Seq(party),
          commands = new KeyOps(partySet, party.toProtoPrimitive).create.commands.asScala.toSeq,
          synchronizerId = Some(daId),
        )
      )
      .loneElement

    activeSynchronizerOf(basicKey.id.contractId, party) shouldBe Some(daId.logical.toProtoPrimitive)
    activeSynchronizerOf(keyOps.id.contractId, party) shouldBe Some(daId.logical.toProtoPrimitive)

    // Unassign BasicKey from da to acme. With the enable-automatic-assignment==false setting
    // the contract stays in flight during the rest of the test.
    participant1.ledger_api.commands.submit_unassign(
      party,
      Seq(LfContractId.assertFromString(basicKey.id.contractId)),
      daId,
      acmeId,
    )

    activeSynchronizerOf(basicKey.id.contractId, party) shouldBe None
    participant1.ledger_api.state.acs
      .incomplete_unassigned_of_party(party)
      .map(_.contractId) should contain(basicKey.id.contractId)
    (party, basicKey, keyOps)
  }
}
