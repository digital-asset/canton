// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.security

import com.digitalasset.canton.damltests.java.universal.UniversalContract
import com.digitalasset.canton.integration.plugins.{UseBftSequencer, UseH2}
import com.digitalasset.canton.integration.util.TestSubmissionService.CommandsWithMetadata
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.logging.SuppressionRule
import com.digitalasset.canton.participant.protocol.ParallelMessageDispatcher
import com.digitalasset.canton.protocol.messages.DefaultOpenEnvelope
import com.digitalasset.canton.sequencing.protocol.{
  MediatorGroupRecipient,
  MemberRecipient,
  Recipient,
  Recipients,
  RecipientsTree,
}
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.canton.util.MaliciousParticipantNode
import org.slf4j.event.Level

import scala.jdk.CollectionConverters.*

/** A malicious participant addresses the confirmation request and the root hash messages to the
  * mediator node directly, without naming a mediator group. The sequencer delivers such a batch.
  * The mediator must raise an alarm and keep processing subsequent requests.
  */
trait RootHashMessageWithoutMediatorGroupIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment {

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1_S1M1

  "a mediator" should {
    "keep processing requests after a request addressed to the mediator node without a mediator group" in {
      implicit env =>
        import env.*

        participants.local.synchronizers.connect_local(sequencer1, daName)
        participants.all.dars.upload(CantonTestsPath)

        val alice = participant1.parties.enable("Alice")

        val maliciousParticipant1 = MaliciousParticipantNode(
          participant1,
          daId,
          testedProtocolVersion,
          testedTransactionLimits,
          timeouts,
          loggerFactory,
        )

        def addressMediatorNodeDirectly(envelope: DefaultOpenEnvelope): DefaultOpenEnvelope = {
          def patchTree(tree: RecipientsTree): RecipientsTree =
            RecipientsTree(
              tree.recipientGroup.map {
                case _: MediatorGroupRecipient => MemberRecipient(mediator1.id): Recipient
                case other => other
              },
              tree.children.map(patchTree),
            )
          envelope.copy(recipients = Recipients(envelope.recipients.trees.map(patchTree)))
        }

        def createCommand(maintainer: PartyId) =
          new UniversalContract(
            List(maintainer.toProtoPrimitive).asJava,
            List.empty.asJava,
            List.empty.asJava,
            List.empty.asJava,
          ).create.commands.asScala.toSeq

        loggerFactory.assertEventuallyLogsSeq(
          (SuppressionRule.LoggerNameContains("DefaultVerdictSender") ||
            SuppressionRule.LoggerNameContains("ConfirmationRequestAndResponseProcessor") ||
            SuppressionRule.forLogger[ParallelMessageDispatcher]) &&
            SuppressionRule.Level(Level.WARN)
        )(
          maliciousParticipant1
            .submitCommand(
              CommandsWithMetadata(
                createCommand(alice).map(c =>
                  com.daml.ledger.api.v2.commands.Command.fromJavaProto(c.toProtoCommand)
                ),
                Seq(alice),
                ledgerTime = environment.now.toLf,
              ),
              envelopeInterceptor = addressMediatorNodeDirectly,
            )
            .futureValueUS,
          entries => {
            forExactly(1, entries)(
              _.warningMessage should (include("MEDIATOR_RECEIVED_MALFORMED_MESSAGE") and
                include("without a mediator group recipient"))
            )
            forAtLeast(1, entries)(
              _.warningMessage should include("No valid root hash message in batch")
            )
            forEvery(entries)(
              _.warningMessage should (include("MEDIATOR_RECEIVED_MALFORMED_MESSAGE") or
                include("SYNC_SERVICE_ALARM"))
            )
          },
        )

        participant1.ledger_api.javaapi.commands.submit(Seq(alice), createCommand(alice))
    }
  }
}

class RootHashMessageWithoutMediatorGroupIntegrationTestH2
    extends RootHashMessageWithoutMediatorGroupIntegrationTest {
  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(new UseBftSequencer(loggerFactory))
}
