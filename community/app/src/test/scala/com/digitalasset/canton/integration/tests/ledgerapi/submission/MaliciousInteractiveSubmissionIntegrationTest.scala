// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.ledgerapi.submission

import com.daml.ledger.api.v2.interactive.interactive_submission_service.PreparedTransaction
import com.digitalasset.canton.HasExecutionContext
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.console.LocalParticipantReference
import com.digitalasset.canton.crypto.{Hash, Signature, SigningKeyUsage}
import com.digitalasset.canton.examples.java.cycle as M
import com.digitalasset.canton.integration.*
import com.digitalasset.canton.integration.plugins.UseProgrammableSequencer
import com.digitalasset.canton.integration.util.TestSubmissionService.CommandsWithMetadata
import com.digitalasset.canton.ledger.participant.state.SubmitterInfo.ExternallySignedSubmission
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.logging.SuppressionRule.LevelAndAbove
import com.digitalasset.canton.platform.apiserver.SeedService.WeakRandom
import com.digitalasset.canton.protocol.LfHash
import com.digitalasset.canton.protocol.LocalRejectError.MalformedRejects.MalformedRequest
import com.digitalasset.canton.synchronizer.sequencer.HasProgrammableSequencer
import com.digitalasset.canton.topology.{ExternalParty, PartyId}
import com.digitalasset.canton.util.MaliciousParticipantNode
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.daml.lf.data.{ImmArray, Time}
import com.digitalasset.daml.lf.transaction.{NodeId, SubmittedTransaction}
import io.grpc.Status
import monocle.macros.syntax.lens.*
import org.scalatest.Assertion
import org.slf4j.event.Level

import java.util.UUID

// TODO(#32963): Review this test once malicious participant node has better external signing support.
final class MaliciousInteractiveSubmissionIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with BaseInteractiveSubmissionTest
    with HasProgrammableSequencer
    with HasExecutionContext
    with HasCycleUtils {

  private var aliceE: ExternalParty = _

  override def environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P1_S1M1
      .withSetup { implicit env =>
        import env.*
        participants.all.synchronizers.connect_local(sequencer1, alias = daName)
        participants.all.dars.upload(CantonExamplesPath)
        participants.all.dars.upload(CantonTestsPath)

        aliceE = cpn.parties.testing.external.enable(
          "Alice",
          // Use 3 keys but start with a threshold of 1
          keysCount = PositiveInt.three,
          keysThreshold = PositiveInt.one,
        )
      }
      .addConfigTransform(ConfigTransforms.enableInteractiveSubmissionTransforms)
      .addConfigTransform(
        ConfigTransforms
          .updateAllParticipantConfigs_(
            // Disable in this suite so we can perform phase 3 assertions
            _.focus(_.ledgerApi.interactiveSubmissionService.enforceSingleRootNode)
              .replace(false)
          )
      )

  registerPlugin(new UseProgrammableSequencer(this.getClass.toString, loggerFactory))

  override def cpn(implicit env: TestConsoleEnvironment): LocalParticipantReference =
    env.participant1

  def prepareTx(
      command: com.daml.ledger.javaapi.data.Command,
      party: ExternalParty,
      mutation: PreparedTransaction => PreparedTransaction,
  )(implicit
      env: TestConsoleEnvironment
  ): (Signature, PreparedTransaction) = {
    import env.*

    val preparedTransaction = cpn.ledger_api.javaapi.interactive_submission
      .prepare(
        Seq(party.partyId),
        Seq(command),
        hashingSchemeVersion = testedHashingSchemeVersion.toLedgerApiProto,
      )
      .getPreparedTransaction

    val mutatedPreparedTransaction = mutation(preparedTransaction)

    val signature = signProtoPreparedTransaction(
      mutatedPreparedTransaction,
      party.fingerprint,
    )(executionContext, env)

    (signature, mutatedPreparedTransaction)
  }

  def decodeTx(
      preparedTx: PreparedTransaction
  ): (SubmittedTransaction, ImmArray[(NodeId, LfHash)]) = {
    val decoded = decodeProtoPreparedTransaction(preparedTx).impoverish
    (decoded.transaction, decoded.transactionMeta.optNodeSeeds.value)
  }

  def maliciousSubmit(
      mutation: PreparedTransaction => PreparedTransaction = identity,
      signatureMutation: Hash => Map[PartyId, Seq[Signature]] = _ => Map.empty,
  )(implicit
      env: TestConsoleEnvironment
  ): com.google.rpc.status.Status = {
    import env.*

    val maliciousCpn = MaliciousParticipantNode(
      cpn,
      daId,
      testedProtocolVersion,
      timeouts,
      loggerFactory,
    )

    val ledgerEnd = participant1.ledger_api.state.end()

    val cycle =
      new M.Cycle(
        UUID.randomUUID().toString,
        aliceE.toProtoPrimitive,
      ).create.commands.loneElement

    val (signature, preparedTx) =
      prepareTx(cycle, aliceE, mutation)

    val decoded = decodeProtoPreparedTransaction(preparedTx)(parallelExecutionContext)
    val decodedTx = decoded.impoverish.transaction
    val decodedNodeSeeds = decoded.impoverish.transactionMeta.optNodeSeeds.value
    val hash = decoded.computeHash(testedHashingSchemeVersion, testedProtocolVersion).value

    val preparationTime = Time.Timestamp(preparedTx.getMetadata.preparationTime)
    val transactionUUID = UUID.fromString(preparedTx.getMetadata.transactionUuid)

    val commandsWithMetadata = CommandsWithMetadata(
      commands = Seq(cycle).map(c =>
        com.daml.ledger.api.v2.commands.Command.fromJavaProto(c.toProtoCommand)
      ),
      actAs = Seq(aliceE),
      submissionSeed = WeakRandom.nextSeed(), // does not matter as node seeds are re-written
      ledgerTime = Time.Timestamp(preparedTx.getMetadata.preparationTime),
      commandId = preparedTx.getMetadata.getSubmitterInfo.commandId,
      transactionUuid = transactionUUID,
    )

    val signatures = Map(aliceE.partyId -> Seq(signature)) ++ signatureMutation(hash)

    /** The transaction created by the submitted command will use a different engine submission seed
      * that will in turn result in the transaction nodes having different discriminators (if the
      * contract details are identical the suffix will be the same). This will cause a model
      * conformance failure if a new contract is created as the view core-inputs will have different
      * contract-ids.
      */
    maliciousCpn
      .submitCommand(
        commandsWithMetadata,
        transactionInterceptor = (_, meta) =>
          (
            decodedTx,
            meta.copy(
              nodeSeeds = decodedNodeSeeds,
              preparationTime = preparationTime,
            ),
          ),
        submitterInfoInterceptor = _.copy(
          externallySignedSubmission = Some(
            ExternallySignedSubmission(
              version = testedHashingSchemeVersion,
              signatures = signatures,
              transactionUUID = transactionUUID,
              mediatorGroup = NonNegativeInt.zero,
              maxRecordTime = None,
            )
          )
        ),
      )
      .futureValueUS
      .value

    val completion = findCompletion(commandsWithMetadata.submissionId, ledgerEnd, aliceE, cpn)
    completion.status.value

  }

  val malformedRequestLogAssertion: Seq[(LogEntry => Assertion, String)] = Seq(
    (
      _.warningMessage should (include regex s"${MalformedRequest.id}.*with a view that is not correctly authenticated"),
      "Expected malformed request log",
    )
  )

  def invalidSignaturesLogAssertion(
      valid: Int,
      invalid: Int,
      expectedValid: Int,
  ): Seq[(LogEntry => Assertion, String)] = Seq(
    (
      {
        _.warningMessage should include(
          s"Received $valid valid signatures from distinct keys ($invalid invalid), but expected at least $expectedValid valid for ${aliceE.partyId}"
        )
      },
      "Expected invalid signature log",
    )
  )

  "Interactive submission in phase 3" should {

    "pass for the identity mutation" onlyRunWithOrGreaterThan ProtocolVersion.v35 in {
      implicit env =>
        maliciousSubmit(identity).code shouldBe Status.Code.OK.value()
    }

    "fail if the wrong synchronizer ID format is used" onlyRunWithOrGreaterThan ProtocolVersion.v35 in {
      implicit env =>
        import env.*

        val wrongSyncId =
          if (testedProtocolVersion <= ProtocolVersion.v34)
            synchronizer1Id
          else
            synchronizer1Id.logical

        val mutation = (tx: PreparedTransaction) =>
          tx.update(_.metadata.synchronizerId := wrongSyncId.toProtoPrimitive)

        loggerFactory.assertEventuallyLogsSeq(LevelAndAbove(Level.WARN))(
          maliciousSubmit(mutation).code shouldBe Status.Code.INVALID_ARGUMENT.value(),
          LogEntry.assertLogSeq(
            malformedRequestLogAssertion ++ invalidSignaturesLogAssertion(
              valid = 0,
              invalid = 1,
              expectedValid = 1,
            )
          ),
        )
    }

    "fail if providing more signatures than registered keys" onlyRunWithOrGreaterThan ProtocolVersion.v35 in {
      implicit env =>
        import env.*

        val signatureMutation: Hash => Map[PartyId, Seq[Signature]] = { hash =>
          Map(
            aliceE.partyId -> (global_secret.sign(hash.unwrap, aliceE, useAllKeys = true) ++ Seq(
              // Add one more
              global_secret.sign(hash.unwrap, aliceE.fingerprint, SigningKeyUsage.ProtocolOnly)
            ))
          )
        }

        loggerFactory.assertEventuallyLogsSeq(LevelAndAbove(Level.WARN))(
          maliciousSubmit(signatureMutation =
            signatureMutation
          ).code shouldBe Status.Code.INVALID_ARGUMENT.value(),
          LogEntry.assertLogSeq(
            malformedRequestLogAssertion ++ Seq(
              (
                _.warningMessage should include(
                  "4 external signatures were provided, which is more than the number of registered signing keys (3)"
                ),
                "expected too many signatures error",
              )
            )
          ),
        )

    }

  }

}
