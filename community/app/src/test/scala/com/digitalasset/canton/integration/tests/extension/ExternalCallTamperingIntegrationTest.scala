// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.integration.tests.extension

import com.daml.ledger.api.v2.commands.Command
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.CryptoPureApi
import com.digitalasset.canton.data.{
  GenTransactionTree,
  MerkleSeq,
  MerkleTree,
  TransactionView,
  ViewParticipantData,
}
import com.digitalasset.canton.extcall.java as M
import com.digitalasset.canton.integration.plugins.{UseExtensionService, UseH2}
import com.digitalasset.canton.integration.util.TestSubmissionService.CommandsWithMetadata
import com.digitalasset.canton.integration.util.{
  EntitySyntax,
  PartiesAllocator,
  TestSubmissionService,
}
import com.digitalasset.canton.integration.{
  CommunityIntegrationTest,
  ConfigTransforms,
  EnvironmentDefinition,
  SharedEnvironment,
}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.SuppressingLogger.LogEntryOptionality
import com.digitalasset.canton.logging.{LogEntry, SuppressingLogger}
import com.digitalasset.canton.participant.ledger.api.client.JavaDecodeUtil
import com.digitalasset.canton.participant.protocol.validation.ExternalCallValidationError
import com.digitalasset.canton.platform.execution.{ExternalCallHandler, ExternalCallMode}
import com.digitalasset.canton.protocol.LocalRejectError.MalformedRejects.ModelConformance
import com.digitalasset.canton.topology.PartyId
import com.digitalasset.canton.topology.transaction.ParticipantPermission
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.MaliciousParticipantNode
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.daml.lf.data.Bytes
import com.digitalasset.daml.lf.engine.Result
import org.scalatest.Assertion

import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.duration.DurationInt

/** End-to-end coverage for a malicious participant that tampers with the recorded external-call
  * results before sending the confirmation request. Honest participants recompute or replay the
  * results during validation and reject the request. External-call wire data exists from protocol
  * version 36 onwards, so the tests are gated on v36. The test package targets LF 2.4-staging,
  * which lies outside the participant's default (stable) LF range, so dev version support is
  * enabled on the participants.
  */
class ExternalCallTamperingIntegrationTest
    extends CommunityIntegrationTest
    with SharedEnvironment
    with EntitySyntax {

  override val loggerFactory: SuppressingLogger =
    SuppressingLogger(getClass, pollTimeout = 10.seconds)

  private val extensionService = new UseExtensionService(loggerFactory)

  registerPlugin(new UseH2(loggerFactory))
  registerPlugin(extensionService)

  // Read from various threads, hence an AtomicReference.
  private lazy val pureCryptoRef: AtomicReference[CryptoPureApi] = new AtomicReference()
  private def pureCrypto: CryptoPureApi = pureCryptoRef.get()

  private var maliciousP1: MaliciousParticipantNode = _
  private var owner: PartyId = _
  private var tester: M.externalcalltest.ExternalCallTester.Contract = _

  /** Records the extension service's default output at submission, mirroring what an honest
    * submitter would record. The tampering happens later, on the transaction tree.
    */
  private val recordingHandler: ExternalCallHandler = new ExternalCallHandler {
    override def handleExternalCall(
        extensionId: String,
        functionId: String,
        configHash: String,
        input: String,
        mode: ExternalCallMode,
    )(implicit
        tc: TraceContext
    ): FutureUnlessShutdown[Either[Result.Need.ExternalCall.Error, String]] =
      FutureUnlessShutdown.pure(Right(UseExtensionService.defaultResponseHex))
  }

  override lazy val environmentDefinition: EnvironmentDefinition =
    EnvironmentDefinition.P2_S1M1
      .addConfigTransforms(ConfigTransforms.setDevVersionSupport(true)*)
      .withSetup { implicit env =>
        import env.*

        participants.all.foreach(_.synchronizers.connect_local(sequencer1, alias = daName))
        pureCryptoRef.set(sequencer1.crypto.pureCrypto)

        if (testedProtocolVersion >= ProtocolVersion.v36) {
          participants.all.dars.upload(BaseTest.ExternalCallTestPath)

          PartiesAllocator(Set(participant1, participant2))(
            newParties = Seq("owner" -> participant1),
            targetTopology = Map(
              "owner" -> Map(
                daId -> (PositiveInt.one, Set(
                  participant1.id -> ParticipantPermission.Submission,
                  participant2.id -> ParticipantPermission.Confirmation,
                ))
              )
            ),
          )
          owner = "owner".toPartyId(participant1)

          tester = JavaDecodeUtil
            .decodeAllCreated(M.externalcalltest.ExternalCallTester.COMPANION)(
              participant1.ledger_api.javaapi.commands.submit(
                Seq(owner),
                Seq(
                  new M.externalcalltest.ExternalCallTester(
                    owner.toProtoPrimitive
                  ).create.commands.loneElement
                ),
              )
            )
            .loneElement

          maliciousP1 = MaliciousParticipantNode(
            participant1,
            daId,
            testedProtocolVersion,
            timeouts,
            loggerFactory,
            testSubmissionServiceOverrideO = Some(
              TestSubmissionService(
                participant1,
                checkAuthorization = false,
                enableLfDev = true,
                externalCallHandler = recordingHandler,
              )
            ),
          )
        }
      }

  private def callExtensionCommand: Command =
    Command.fromJavaProto(
      tester.id
        .exerciseCallExtension(extensionService.extensionId, "test-function", "00ff", "deadbeef")
        .commands
        .loneElement
        .toProtoCommand
    )

  /** Optic onto every root view's recorded external-call results. */
  private def externalCallResultsOfRootViews =
    GenTransactionTree.Optics.rootViewsUnsafe
      .andThen(MerkleSeq.Optics.toSeq[TransactionView](pureCrypto, testedProtocolVersion))
      .andThen(MerkleTree.Optics.unblindedSeq[TransactionView])
      .andThen(TransactionView.Optics.viewParticipantDataUnsafe)
      .andThen(MerkleTree.Optics.unblinded[ViewParticipantData])
      .andThen(ViewParticipantData.Optics.externalCallResultsUnsafe)

  /** Optic onto the first root view's recorded external-call results, so that a single view can be
    * tampered with while the other is left intact.
    */
  private def externalCallResultsOfFirstRootView =
    GenTransactionTree.Optics.rootViewsUnsafe
      .andThen(MerkleSeq.Optics.toSeq[TransactionView](pureCrypto, testedProtocolVersion))
      .index(0)
      .andThen(MerkleTree.Optics.unblinded[TransactionView])
      .andThen(TransactionView.Optics.viewParticipantDataUnsafe)
      .andThen(MerkleTree.Optics.unblinded[ViewParticipantData])
      .andThen(ViewParticipantData.Optics.externalCallResultsUnsafe)

  "a malicious participant that drops the recorded external-call results" should {
    "be rejected by a failed model conformance check" onlyRunWithOrGreaterThan ProtocolVersion.v36 in {
      implicit env =>
        import env.*
        extensionService.reset()

        val command = CommandsWithMetadata(Seq(callExtensionCommand), actAs = Seq(owner))

        def modelConformanceReplayMissing(member: String)(entry: LogEntry): Assertion = {
          entry.shouldBeCantonErrorCode(ModelConformance)
          entry.message should include(
            "Rejected transaction due to a failed model conformance check"
          )
          entry.message should include("ExternalCallReplayMissing")
          // The missing key is described by its identity and size-only payload fields, never the
          // payload bytes themselves (config "00ff" = 2 bytes, input "deadbeef" = 4 bytes).
          entry.message should include(s"""extension id = "${extensionService.extensionId}"""")
          entry.message should include("""function id = "test-function"""")
          entry.message should include("""config bytes = "2 bytes"""")
          entry.message should include("""input bytes = "4 bytes"""")
          entry.loggerName should include(member)
        }

        loggerFactory.assertLogsUnorderedOptional(
          maliciousP1
            .submitCommand(
              command,
              transactionTreeInterceptor = externalCallResultsOfRootViews.modify(_ => Seq.empty),
            )
            .futureValueUS
            .valueOrFail("malicious submission"),
          LogEntryOptionality.Required -> modelConformanceReplayMissing("participant1"),
          LogEntryOptionality.Required -> modelConformanceReplayMissing("participant2"),
        )

        // Re-interpretation replays recorded results instead of contacting the service, and the
        // stripped views leave the external-call check nothing to re-validate, so the service is
        // never contacted.
        extensionService.observedCalls shouldBe empty

        assertPingSucceeds(participant1, participant2)
    }
  }

  "a malicious participant that records disagreeing external-call results" should {
    "be rejected with a disagreement alarm on every confirming participant" onlyRunWithOrGreaterThan ProtocolVersion.v36 in {
      implicit env =>
        import env.*
        extensionService.reset()

        // Two identical external-call exercises in one command yield two root views that share a
        // single external-call key, each recording the same honest output.
        val command = CommandsWithMetadata(
          Seq(callExtensionCommand, callExtensionCommand),
          actAs = Seq(owner),
        )

        // Rewrite the first root view's recorded output to a different value so that the two
        // visible occurrences of the same key disagree ("c0ffee" recorded honestly, "beef"
        // tampered in), which the confirming participants detect and reject.
        val tamperFirstRootViewOutput: GenTransactionTree => GenTransactionTree =
          externalCallResultsOfFirstRootView.modify(
            _.map(result =>
              result.copy(result = result.result.copy(output = Bytes.assertFromString("beef")))
            )
          )

        // The disagreement is described by the call's identity, the sorted output byte sizes
        // ("beef" = 2 bytes, "c0ffee" = 3 bytes, never the output bytes themselves), and the two
        // occurrences that recorded the disagreeing outputs. The two root views hold one exercise
        // each, so both occurrences are at exercise 0, call 0; the tampered view is at position L.
        val expectedInconsistency =
          """Inconsistency(
            |  extensionId = test-extension,
            |  functionId = test-function,
            |  config = 2 bytes,
            |  input = 4 bytes,
            |  outputByteSizes = Seq(2, 3),
            |  occurrences = Seq(ExternalCallOccurrence(viewPosition = ViewPosition(L), exerciseIndex = 0, callIndex = 0), ExternalCallOccurrence(viewPosition = ViewPosition(R), exerciseIndex = 0, callIndex = 0))
            |)""".stripMargin

        // The consistency check alarms (WARN) once per request on every participant receiving the
        // views (both confirm here) and, on the strength of the same disagreement, rejects both
        // root views recording the key (each an INFO-level
        // LOCAL_VERDICT_EXTERNAL_CALL_RESULT_DISAGREEMENT verdict, below this WARN suppressor). The
        // alarm carries the full description, including both occurrences, so both confirmers
        // rejecting proves the request cannot be committed.
        def disagreementAlarm(member: String)(entry: LogEntry): Assertion =
          entry.shouldBeCantonError(
            ExternalCallValidationError.ExternalCallResultDisagreementAlarm,
            messageAssertion =
              _ shouldBe s"Observed inconsistent external call results: $expectedInconsistency",
            contextAssertion = _.keySet should contain("requestId"),
            loggerAssertion = _ should include(member),
          )

        loggerFactory.assertLogsUnorderedOptional(
          maliciousP1
            .submitCommand(command, transactionTreeInterceptor = tamperFirstRootViewOutput)
            .futureValueUS
            .valueOrFail("malicious submission"),
          LogEntryOptionality.Required -> disagreementAlarm("participant1"),
          LogEntryOptionality.Required -> disagreementAlarm("participant2"),
        )

        // The disagreement is decided from the two recorded outputs alone; the disagreeing key is
        // excluded from re-validation, so the extension service is never contacted.
        extensionService.observedCalls shouldBe empty

        assertPingSucceeds(participant1, participant2)
    }
  }
}
