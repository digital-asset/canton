// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests

import com.daml.ledger.api.v2.commands.Command
import com.daml.ledger.api.v2.transaction.Transaction
import com.digitalasset.canton.ProtoDeserializationError.InvariantViolation
import com.digitalasset.canton.config
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.console.{CommandFailure, InstanceReference}
import com.digitalasset.canton.damltests.java.universal.UniversalContract
import com.digitalasset.canton.error.MediatorError
import com.digitalasset.canton.integration.bootstrap.{
  NetworkBootstrapper,
  NetworkTopologyDescription,
}
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UsePostgres}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.protocol.LocalRejectError
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.canton.version.ProtocolVersion
import monocle.macros.syntax.lens.*

import scala.jdk.CollectionConverters.*

/** Simple test to validate that collection size limits configured in the `SynchronizerLimits` are
  * checked by participants and mediators while processing a submission. This test uses the
  * `maxActAs` limit as an example, but the same mechanism is used for all collection size limits.
  */
sealed trait CollectionSizeValidationIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment {

  private var extraParties: Seq[PartyId] = _
  private val nbExtraParties = 2

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2S1M1_Config
      .withNetworkBootstrap { implicit env =>
        import env.*

        val defaultSsp = EnvironmentDefinition.defaultStaticSynchronizerParameters

        new NetworkBootstrapper(
          NetworkTopologyDescription(
            daName,
            synchronizerOwners = Seq[InstanceReference](sequencer1, mediator1),
            synchronizerThreshold = PositiveInt.one,
            sequencers = Seq(sequencer1),
            mediators = Seq(mediator1),
            overrideStaticSynchronizerParameters =
              Option.when(testedProtocolVersion >= ProtocolVersion.boundsCheck)(
                defaultSsp
                  .focus(_.synchronizerLimits.transactionProtocolLimits.maxActAs)
                  .replace(PositiveInt.two)
              ),
          )
        )
      }
      .withSetup { implicit env =>
        import env.*

        participants.all.synchronizers.connect_local(sequencer1, alias = daName)
        participants.all.dars.upload(CantonTestsPath)

        extraParties =
          (1 to nbExtraParties).map(i => participant1.parties.enable(s"extra-party-$i"))
      }

  "synchronizer-wide collection limits are checked" in { implicit env =>
    import env.*

    val cmd = new UniversalContract(
      Seq(participant1.adminParty).map(_.toProtoPrimitive).asJava,
      List.empty.asJava,
      List.empty.asJava,
      Seq(participant1.adminParty).map(_.toProtoPrimitive).asJava,
    ).createAnd
      .exerciseArchive()
      .commands
      .asScala
      .toSeq
      .map(c => Command.fromJavaProto(c.toProtoCommand))

    def submitCommand: Transaction = participant1.ledger_api.commands
      .submit(
        actAs = List(participant1.adminParty) ++ extraParties,
        cmd,
        optTimeout = Some(config.NonNegativeDuration.ofSeconds(5)),
      )

    clue("submit command") {
      if (testedProtocolVersion >= ProtocolVersion.boundsCheck) {
        val invariantViolation = InvariantViolation(
          Some("act_as"),
          s"repeated field has ${nbExtraParties + 1} elements, exceeding the maximum of $nbExtraParties",
        )

        loggerFactory.assertThrowsAndLogsSeq[CommandFailure](
          submitCommand,
          LogEntry.assertLogSeq(
            mustContainWithClue = Seq(
              (
                _.shouldBeCantonError(
                  MediatorError.MalformedMessage.code,
                  _ should include(invariantViolation.toString),
                ),
                "Mediator detects invalid message",
              ),
              (
                _.warningMessage should (include(
                  "Decryption error: SymmetricDecryptError"
                ) and include(invariantViolation.message)),
                "Participant cannot decrypt message",
              ),
              (
                _.shouldBeCantonError(
                  LocalRejectError.MalformedRejects.Payloads.code,
                  _ should (include(
                    "Rejected transaction due to malformed payload within views"
                  ) and include(invariantViolation.message)),
                ),
                "Participant rejects locally",
              ),
              (
                _.shouldBeCantonError(
                  MediatorError.InvalidMessage.code,
                  _ should include("unknown request id"),
                ),
                "Mediator does not know locally rejected request",
              ),
              (_.errorMessage should include("DEADLINE_EXCEEDED"), "Command fails with timeout"),
            )
          ),
        )
      } else {
        // Bounds are not checked in this protocol version, so the command should succeed
        submitCommand shouldBe a[Transaction]
      }
    }
  }
}

final class CollectionSizeValidationIntegrationTestPostgres
    extends CollectionSizeValidationIntegrationTest {
  registerPlugin(new UsePostgres(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
}
