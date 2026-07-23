// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.validation

import cats.data.EitherT
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.data.{
  CantonTimestamp,
  ExternalCallKey,
  ParticipantTransactionView,
  PathRollbackContextFactory,
  TransactionView,
  ViewConfirmationParameters,
  ViewParticipantData,
  ViewPosition,
}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.lifecycle.FutureUnlessShutdownImpl.*
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.participant.protocol.LedgerEffectAbsolutizer.ViewAbsoluteLedgerEffect
import com.digitalasset.canton.participant.protocol.conflictdetection.ConflictDetectionHelpers
import com.digitalasset.canton.participant.protocol.validation.ExternalCallConsistencyChecker.{
  ExternalCallOccurrence,
  Inconsistency,
}
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.protocol.ExampleTransactionFactory.{
  signatory,
  submitter,
  submittingParticipant,
}
import com.digitalasset.canton.protocol.messages.{
  ConfirmationResponse,
  LocalAbstain,
  LocalApprove,
  LocalReject,
}
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{
  BaseTestWordSpec,
  HasExecutionContext,
  LfPartyId,
  ProtocolVersionChecksAnyWordSpec,
}
import com.digitalasset.daml.lf.data.Bytes

import java.util.concurrent.ConcurrentLinkedQueue
import scala.jdk.CollectionConverters.*

/** Basic coverage for the external-call protocol integration: the [[ExternalCallCheck]] outcomes
  * and their translation into confirmation responses. Completing the coverage is a tracked
  * post-merge task.
  */
final class ExternalCallProtocolIntegrationTest
    extends BaseTestWordSpec
    with HasExecutionContext
    with ProtocolVersionChecksAnyWordSpec
    with ExternalCallValidationTestUtil {

  protected val factory: ExampleTransactionFactory = new ExampleTransactionFactory()()

  private val requestId: RequestId = RequestId(CantonTimestamp.Epoch)

  private val confirmers: Set[LfPartyId] = Set(submitter, signatory)

  private val externalCallKey: ExternalCallKey =
    ExternalCallKey.fromResult(externalCallResult)

  private val leftViewPosition: ViewPosition =
    ViewPosition(
      List(ViewPosition.MerkleSeqIndex(List(ViewPosition.MerkleSeqIndex.Direction.Left)))
    )
  private val rightViewPosition: ViewPosition =
    ViewPosition(
      List(ViewPosition.MerkleSeqIndex(List(ViewPosition.MerkleSeqIndex.Direction.Right)))
    )

  private final class RecordingExternalCallValidator(
      results: Map[ExternalCallKey, ExternalCallValidator.Result]
  ) extends ExternalCallValidator {
    private val observedKeys: ConcurrentLinkedQueue[ExternalCallKey] =
      new ConcurrentLinkedQueue[ExternalCallKey]

    def observed: Seq[ExternalCallKey] = observedKeys.asScala.toSeq

    override def validate(
        key: ExternalCallKey,
        recordedOutput: Bytes,
    )(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[ExternalCallValidator.Result] = {
      observedKeys.add(key)
      FutureUnlessShutdown.pure(results.getOrElse(key, ExternalCallValidator.Matched))
    }
  }

  private val identityTopologySnapshot: TopologySnapshot = {
    val snapshot = mock[TopologySnapshot]
    when(snapshot.canConfirm(any[ParticipantId], any[Set[LfPartyId]])(anyTraceContext))
      .thenAnswer { (_: ParticipantId, parties: Set[LfPartyId]) =>
        FutureUnlessShutdown.pure(parties)
      }
    snapshot
  }

  private def hostingOnly(hostedParties: Set[LfPartyId]): TopologySnapshot = {
    val snapshot = mock[TopologySnapshot]
    when(snapshot.canConfirm(any[ParticipantId], any[Set[LfPartyId]])(anyTraceContext))
      .thenAnswer { (_: ParticipantId, parties: Set[LfPartyId]) =>
        FutureUnlessShutdown.pure(parties.intersect(hostedParties))
      }
    snapshot
  }

  private def externalCallCheck(validator: ExternalCallValidator): ExternalCallCheck =
    new ExternalCallCheck(
      submittingParticipant,
      validator,
      PositiveInt.tryCreate(8),
      loggerFactory,
    )

  private def withConfirmers(
      view: TransactionView,
      confirmers: Set[LfPartyId],
  ): TransactionView = {
    val confirmationParameters = ViewConfirmationParameters.create(
      informees = confirmers.map(_ -> NonNegativeInt.one).toMap,
      threshold = NonNegativeInt.tryCreate(confirmers.size),
    )
    TransactionView.Optics.viewCommonDataUnsafe
      .modify(commonData =>
        commonData.tryUnwrap.copy(viewConfirmationParameters = confirmationParameters)
      )(view)
  }

  private def views(
      leftResults: Seq[ViewParticipantData.ViewExternalCallResult],
      rightResults: Seq[ViewParticipantData.ViewExternalCallResult] = Seq.empty,
  ): Map[ViewPosition, ParticipantTransactionView] = {
    val example = factory.MultipleRoots
    Map(
      leftViewPosition -> participantView(
        withExternalCallResults(withConfirmers(example.rootViews(4), confirmers), leftResults)
      ),
      rightViewPosition -> participantView(
        withExternalCallResults(withConfirmers(example.rootViews(5), confirmers), rightResults)
      ),
    )
  }

  private def runCheck(
      validator: ExternalCallValidator,
      views: Map[ViewPosition, ParticipantTransactionView],
      runValidation: Boolean = true,
      topologySnapshot: TopologySnapshot = identityTopologySnapshot,
  ): Map[ViewPosition, ExternalCallCheck.Result] =
    externalCallCheck(validator)
      .check(requestId, views, topologySnapshot, runValidation)
      .futureValueUS

  private def assertDisagreementAlarm[A](within: => A): A =
    loggerFactory.assertLogs(
      within,
      (logEntry: LogEntry) => {
        logEntry.shouldBeCantonErrorCode(
          ExternalCallValidationError.ExternalCallResultDisagreementAlarm
        )
        logEntry.mdc should contain("requestId" -> requestId.toString)
      },
    )

  "ExternalCallCheck" should {
    "return no results when no view records external-call results" in {
      val validator = new RecordingExternalCallValidator(Map.empty)

      runCheck(validator, views(Seq.empty)) shouldBe Map.empty
      validator.observed shouldBe empty
    }

    "pass when the recorded results agree and re-validation matches" onlyRunWithOrGreaterThan ProtocolVersion.dev in {
      val validator = new RecordingExternalCallValidator(Map.empty)
      val result = runCheck(
        validator,
        views(
          leftResults = Seq(externalCallViewResult(0, externalCallResult, Set(submitter))),
          rightResults = Seq(externalCallViewResult(1, externalCallResult, Set(submitter))),
        ),
      )

      result shouldBe Map(
        leftViewPosition -> ExternalCallCheck.Passed,
        rightViewPosition -> ExternalCallCheck.Passed,
      )
      // Re-validated once for the shared semantic key.
      validator.observed shouldBe Seq(externalCallKey)
    }

    "reject and alarm when recorded results disagree across the request" onlyRunWithOrGreaterThan ProtocolVersion.dev in {
      val validator = new RecordingExternalCallValidator(Map.empty)
      val result = assertDisagreementAlarm {
        runCheck(
          validator,
          views(
            leftResults = Seq(externalCallViewResult(0, externalCallResult, Set(submitter))),
            rightResults = Seq(externalCallViewResult(1, otherExternalCallResult, Set(submitter))),
          ),
        )
      }

      val expectedInconsistency = Inconsistency(
        externalCallKey,
        outputs = Set(externalCallResult.output, otherExternalCallResult.output),
        occurrences = Set(
          ExternalCallOccurrence(leftViewPosition, NonNegativeInt.zero, NonNegativeInt.zero),
          ExternalCallOccurrence(rightViewPosition, NonNegativeInt.one, NonNegativeInt.zero),
        ),
      )
      // Both views record the disagreeing call, so both are rejected.
      result shouldBe Map(
        leftViewPosition -> ExternalCallCheck.Rejected(expectedInconsistency.description),
        rightViewPosition -> ExternalCallCheck.Rejected(expectedInconsistency.description),
      )
      // Disagreeing results are not re-validated.
      validator.observed shouldBe empty
    }

    "reject and alarm when re-validation returns a different output" onlyRunWithOrGreaterThan ProtocolVersion.dev in {
      val validator = new RecordingExternalCallValidator(
        Map(
          externalCallKey -> ExternalCallValidator.Mismatched(
            computedOutput = otherExternalCallResult.output,
            recordedOutput = externalCallResult.output,
          )
        )
      )
      val result = assertDisagreementAlarm {
        runCheck(
          validator,
          views(leftResults = Seq(externalCallViewResult(0, externalCallResult, Set(submitter)))),
        )
      }

      val expectedInconsistency = Inconsistency(
        externalCallKey,
        outputs = Set(otherExternalCallResult.output, externalCallResult.output),
        occurrences =
          Set(ExternalCallOccurrence(leftViewPosition, NonNegativeInt.zero, NonNegativeInt.zero)),
      )
      result shouldBe Map(
        leftViewPosition -> ExternalCallCheck.Rejected(expectedInconsistency.description)
      )
      validator.observed shouldBe Seq(externalCallKey)
    }

    "report a recorded result that cannot be re-validated" onlyRunWithOrGreaterThan ProtocolVersion.dev in {
      val validator = new RecordingExternalCallValidator(
        Map(
          externalCallKey -> ExternalCallValidator.UnableToValidate(
            "extension service is not configured"
          )
        )
      )
      val result = runCheck(
        validator,
        views(leftResults = Seq(externalCallViewResult(0, externalCallResult, Set(submitter)))),
      )

      result shouldBe Map(
        leftViewPosition -> ExternalCallCheck.CannotValidate(
          "extension service is not configured"
        )
      )
    }

    "skip re-validation of consistent results when instructed" onlyRunWithOrGreaterThan ProtocolVersion.dev in {
      val validator = new RecordingExternalCallValidator(Map.empty)
      val result = runCheck(
        validator,
        views(
          leftResults = Seq(externalCallViewResult(0, externalCallResult, Set(submitter))),
          rightResults = Seq(externalCallViewResult(1, externalCallResult, Set(submitter))),
        ),
        runValidation = false,
      )

      result shouldBe Map(
        leftViewPosition -> ExternalCallCheck.Passed,
        rightViewPosition -> ExternalCallCheck.Passed,
      )
      validator.observed shouldBe empty
    }

    "reject every view recording a mismatched call" onlyRunWithOrGreaterThan ProtocolVersion.dev in {
      val validator = new RecordingExternalCallValidator(
        Map(
          externalCallKey -> ExternalCallValidator.Mismatched(
            computedOutput = otherExternalCallResult.output,
            recordedOutput = externalCallResult.output,
          )
        )
      )
      val result = assertDisagreementAlarm {
        runCheck(
          validator,
          views(
            leftResults = Seq(externalCallViewResult(0, externalCallResult, Set(submitter))),
            rightResults = Seq(externalCallViewResult(1, externalCallResult, Set(submitter))),
          ),
        )
      }

      val expectedInconsistency = Inconsistency(
        externalCallKey,
        outputs = Set(otherExternalCallResult.output, externalCallResult.output),
        occurrences = Set(
          ExternalCallOccurrence(leftViewPosition, NonNegativeInt.zero, NonNegativeInt.zero),
          ExternalCallOccurrence(rightViewPosition, NonNegativeInt.one, NonNegativeInt.zero),
        ),
      )
      // The mismatched call is recorded in both views, so both are rejected, after a single
      // validator invocation for the shared semantic key.
      result shouldBe Map(
        leftViewPosition -> ExternalCallCheck.Rejected(expectedInconsistency.description),
        rightViewPosition -> ExternalCallCheck.Rejected(expectedInconsistency.description),
      )
      validator.observed shouldBe Seq(externalCallKey)
    }

    "isolate re-validation failures to the views recording the call" onlyRunWithOrGreaterThan ProtocolVersion.dev in {
      val independentCall = externalCallResult.copy(functionId = "other-function")
      val validator = new RecordingExternalCallValidator(
        Map(
          externalCallKey -> ExternalCallValidator.Mismatched(
            computedOutput = otherExternalCallResult.output,
            recordedOutput = externalCallResult.output,
          )
        )
      )
      val result = assertDisagreementAlarm {
        runCheck(
          validator,
          views(
            leftResults = Seq(externalCallViewResult(0, externalCallResult, Set(submitter))),
            rightResults = Seq(externalCallViewResult(1, independentCall, Set(submitter))),
          ),
        )
      }

      // Only the view recording the mismatched call is rejected; the view recording the
      // independently validated call passes.
      val expectedInconsistency = Inconsistency(
        externalCallKey,
        outputs = Set(otherExternalCallResult.output, externalCallResult.output),
        occurrences =
          Set(ExternalCallOccurrence(leftViewPosition, NonNegativeInt.zero, NonNegativeInt.zero)),
      )
      result(leftViewPosition) shouldBe
        ExternalCallCheck.Rejected(expectedInconsistency.description)
      result(rightViewPosition) shouldBe ExternalCallCheck.Passed
      validator.observed should contain theSameElementsAs
        Seq(externalCallKey, ExternalCallKey.fromResult(independentCall))
    }

    "re-validate calls unaffected by a disagreement" onlyRunWithOrGreaterThan ProtocolVersion.dev in {
      val independentCall = externalCallResult.copy(functionId = "other-function")
      val independentKey = ExternalCallKey.fromResult(independentCall)
      val validator = new RecordingExternalCallValidator(
        Map(
          independentKey -> ExternalCallValidator.Mismatched(
            computedOutput = otherExternalCallResult.output,
            recordedOutput = independentCall.output,
          )
        )
      )
      val result = loggerFactory.assertLogs(
        runCheck(
          validator,
          views(
            leftResults = Seq(externalCallViewResult(0, externalCallResult, Set(submitter))),
            rightResults = Seq(
              externalCallViewResult(1, otherExternalCallResult, Set(submitter)),
              externalCallViewResult(2, independentCall, Set(submitter)),
            ),
          ),
        ),
        // One alarm for the cross-view disagreement, one for the re-validation mismatch.
        _.shouldBeCantonErrorCode(
          ExternalCallValidationError.ExternalCallResultDisagreementAlarm
        ),
        _.shouldBeCantonErrorCode(
          ExternalCallValidationError.ExternalCallResultDisagreementAlarm
        ),
      )

      // The disagreeing key is not re-validated, but the independent call still is: its
      // mismatch rejects the recording view. Both views record the disagreeing call, so both
      // are rejected; the right view's rejection reports the disagreement, which takes
      // precedence over its re-validation mismatch.
      validator.observed shouldBe Seq(independentKey)
      val expectedDisagreement = Inconsistency(
        externalCallKey,
        outputs = Set(externalCallResult.output, otherExternalCallResult.output),
        occurrences = Set(
          ExternalCallOccurrence(leftViewPosition, NonNegativeInt.zero, NonNegativeInt.zero),
          ExternalCallOccurrence(rightViewPosition, NonNegativeInt.one, NonNegativeInt.zero),
        ),
      )
      result shouldBe Map(
        leftViewPosition -> ExternalCallCheck.Rejected(expectedDisagreement.description),
        rightViewPosition -> ExternalCallCheck.Rejected(expectedDisagreement.description),
      )
    }

    "skip re-validation when no checking party is hosted" onlyRunWithOrGreaterThan ProtocolVersion.dev in {
      val validator = new RecordingExternalCallValidator(Map.empty)
      val result = runCheck(
        validator,
        views(leftResults = Seq(externalCallViewResult(0, externalCallResult, Set(submitter)))),
        topologySnapshot = hostingOnly(Set.empty),
      )

      // The participant hosts no checking party of the call, so re-validation is not its
      // responsibility; the view passes from this check's perspective.
      result shouldBe Map(leftViewPosition -> ExternalCallCheck.Passed)
      validator.observed shouldBe empty
    }

    "skip re-validation when the checking parties are not confirmers of the recording view" onlyRunWithOrGreaterThan ProtocolVersion.dev in {
      val validator = new RecordingExternalCallValidator(Map.empty)
      val result = runCheck(
        validator,
        views(
          leftResults = Seq(
            externalCallViewResult(
              0,
              externalCallResult,
              Set(ExampleTransactionFactory.observer),
            )
          )
        ),
      )

      // The observer checks the call but does not confirm the view, so hosting it does not
      // make this participant responsible for re-validation.
      result shouldBe Map(leftViewPosition -> ExternalCallCheck.Passed)
      validator.observed shouldBe empty
    }

    "attach a mismatch to every view recording the key when the gate passes on one" onlyRunWithOrGreaterThan ProtocolVersion.dev in {
      val validator = new RecordingExternalCallValidator(
        Map(
          externalCallKey -> ExternalCallValidator.Mismatched(
            computedOutput = otherExternalCallResult.output,
            recordedOutput = externalCallResult.output,
          )
        )
      )
      val result = assertDisagreementAlarm {
        runCheck(
          validator,
          views(
            leftResults = Seq(externalCallViewResult(0, externalCallResult, Set(submitter))),
            rightResults = Seq(
              externalCallViewResult(
                1,
                externalCallResult,
                Set(ExampleTransactionFactory.observer),
              )
            ),
          ),
          topologySnapshot = hostingOnly(Set(submitter)),
        )
      }

      // The gate passes only for the left view's occurrence, so the call is re-validated
      // (once); the mismatch nevertheless rejects every view recording the call, and its
      // reported occurrences include the view whose occurrence did not pass the gate.
      validator.observed shouldBe Seq(externalCallKey)
      val expectedInconsistency = Inconsistency(
        externalCallKey,
        outputs = Set(otherExternalCallResult.output, externalCallResult.output),
        occurrences = Set(
          ExternalCallOccurrence(leftViewPosition, NonNegativeInt.zero, NonNegativeInt.zero),
          ExternalCallOccurrence(rightViewPosition, NonNegativeInt.one, NonNegativeInt.zero),
        ),
      )
      result shouldBe Map(
        leftViewPosition -> ExternalCallCheck.Rejected(expectedInconsistency.description),
        rightViewPosition -> ExternalCallCheck.Rejected(expectedInconsistency.description),
      )
    }

    "reject disagreements regardless of hosting" onlyRunWithOrGreaterThan ProtocolVersion.dev in {
      val validator = new RecordingExternalCallValidator(Map.empty)
      val result = assertDisagreementAlarm {
        runCheck(
          validator,
          views(
            leftResults = Seq(externalCallViewResult(0, externalCallResult, Set(submitter))),
            rightResults = Seq(externalCallViewResult(1, otherExternalCallResult, Set(submitter))),
          ),
          topologySnapshot = hostingOnly(Set.empty),
        )
      }

      // The re-validation gate does not apply to the consistency check: a visible disagreement
      // is local evidence of ambiguous recorded data, alarmed and rejected also by participants
      // hosting none of the checking parties.
      val expectedInconsistency = Inconsistency(
        externalCallKey,
        outputs = Set(externalCallResult.output, otherExternalCallResult.output),
        occurrences = Set(
          ExternalCallOccurrence(leftViewPosition, NonNegativeInt.zero, NonNegativeInt.zero),
          ExternalCallOccurrence(rightViewPosition, NonNegativeInt.one, NonNegativeInt.zero),
        ),
      )
      result shouldBe Map(
        leftViewPosition -> ExternalCallCheck.Rejected(expectedInconsistency.description),
        rightViewPosition -> ExternalCallCheck.Rejected(expectedInconsistency.description),
      )
      validator.observed shouldBe empty
    }

    "describe each view's rejection by a call recorded in that view" onlyRunWithOrGreaterThan ProtocolVersion.dev in {
      val leftCall = externalCallResult.copy(functionId = "left-function")
      val rightCall = externalCallResult.copy(functionId = "right-function")
      val leftKey = ExternalCallKey.fromResult(leftCall)
      val rightKey = ExternalCallKey.fromResult(rightCall)
      val validator = new RecordingExternalCallValidator(
        Map(
          leftKey -> ExternalCallValidator.Mismatched(
            computedOutput = otherExternalCallResult.output,
            recordedOutput = leftCall.output,
          ),
          rightKey -> ExternalCallValidator.Mismatched(
            computedOutput = otherExternalCallResult.output,
            recordedOutput = rightCall.output,
          ),
        )
      )
      val result = loggerFactory.assertLogs(
        runCheck(
          validator,
          views(
            leftResults = Seq(externalCallViewResult(0, leftCall, Set(submitter))),
            rightResults = Seq(externalCallViewResult(1, rightCall, Set(submitter))),
          ),
        ),
        _.shouldBeCantonErrorCode(
          ExternalCallValidationError.ExternalCallResultDisagreementAlarm
        ),
        _.shouldBeCantonErrorCode(
          ExternalCallValidationError.ExternalCallResultDisagreementAlarm
        ),
      )

      // Each view's rejection describes the mismatched call recorded in that view, not the
      // globally first mismatch.
      val leftInconsistency = Inconsistency(
        leftKey,
        outputs = Set(otherExternalCallResult.output, leftCall.output),
        occurrences =
          Set(ExternalCallOccurrence(leftViewPosition, NonNegativeInt.zero, NonNegativeInt.zero)),
      )
      val rightInconsistency = Inconsistency(
        rightKey,
        outputs = Set(otherExternalCallResult.output, rightCall.output),
        occurrences =
          Set(ExternalCallOccurrence(rightViewPosition, NonNegativeInt.one, NonNegativeInt.zero)),
      )
      result shouldBe Map(
        leftViewPosition -> ExternalCallCheck.Rejected(leftInconsistency.description),
        rightViewPosition -> ExternalCallCheck.Rejected(rightInconsistency.description),
      )
    }

    "prefer a mismatch over an unvalidatable result within a view" onlyRunWithOrGreaterThan ProtocolVersion.dev in {
      val mismatchedCall = externalCallResult.copy(functionId = "mismatched-function")
      val unvalidatableCall = externalCallResult.copy(functionId = "unvalidatable-function")
      val mismatchedKey = ExternalCallKey.fromResult(mismatchedCall)
      val validator = new RecordingExternalCallValidator(
        Map(
          mismatchedKey -> ExternalCallValidator.Mismatched(
            computedOutput = otherExternalCallResult.output,
            recordedOutput = mismatchedCall.output,
          ),
          ExternalCallKey.fromResult(unvalidatableCall) ->
            ExternalCallValidator.UnableToValidate("extension service is not configured"),
        )
      )
      val result = assertDisagreementAlarm {
        runCheck(
          validator,
          views(
            leftResults = Seq(
              externalCallViewResult(0, mismatchedCall, Set(submitter)),
              externalCallViewResult(1, unvalidatableCall, Set(submitter)),
            ),
            rightResults = Seq(externalCallViewResult(2, unvalidatableCall, Set(submitter))),
          ),
        )
      }

      // Within the left view, the mismatch takes precedence over the unvalidatable result;
      // the right view, recording only the unvalidatable call, abstains independently.
      val expectedInconsistency = Inconsistency(
        mismatchedKey,
        outputs = Set(otherExternalCallResult.output, mismatchedCall.output),
        occurrences =
          Set(ExternalCallOccurrence(leftViewPosition, NonNegativeInt.zero, NonNegativeInt.zero)),
      )
      result shouldBe Map(
        leftViewPosition -> ExternalCallCheck.Rejected(expectedInconsistency.description),
        rightViewPosition ->
          ExternalCallCheck.CannotValidate("extension service is not configured"),
      )
    }
  }

  private lazy val responsesFactory: TransactionConfirmationResponsesFactory =
    new TransactionConfirmationResponsesFactory(
      submittingParticipant,
      factory.psid,
      loggerFactory,
    )

  private def createResponses(
      checkResult: Map[ViewPosition, ExternalCallCheck.Result],
      timeValidationResultE: Either[TimeValidator.TimeCheckFailure, Unit] = Right(()),
      authorizationResult: Map[ViewPosition, String] = Map.empty,
  ): Seq[ConfirmationResponse] = {
    val example = factory.MultipleRoots
    val viewValidationResults = Map(
      leftViewPosition -> validationResult(withConfirmers(example.rootViews(4), confirmers)),
      rightViewPosition -> validationResult(withConfirmers(example.rootViews(5), confirmers)),
    )
    val defaultModelConformance = ModelConformanceChecker.Result(
      example.updateId,
      WellFormedTransaction.checkOrThrow(
        example.versionedSuffixedTransaction,
        example.metadata,
        WellFormedTransaction.WithSuffixesAndMerged,
        PathRollbackContextFactory,
      ),
      unmergedTransactionsWithoutTopLevelRollbackNodes = Seq.empty,
    )

    val transactionValidationResult = TransactionValidationResult(
      updateId = defaultModelConformance.updateId,
      submitterMetadataO = None,
      workflowIdO = None,
      contractConsistencyResultE = Right(()),
      authenticationResult = Map.empty,
      authorizationResult = authorizationResult,
      modelConformanceResultET =
        EitherT.rightT[FutureUnlessShutdown, ModelConformanceChecker.ErrorWithSubTransaction[
          ViewAbsoluteLedgerEffect
        ]](defaultModelConformance),
      internalConsistencyResultET = EitherT
        .rightT[FutureUnlessShutdown, InternalConsistencyChecker.ErrorWithInternalConsistencyCheck](
          ()
        ),
      externalCallCheckResultF = FutureUnlessShutdown.pure(checkResult),
      consumedInputsOfHostedParties = Map.empty,
      witnessed = Map.empty,
      createdContracts = Map.empty,
      transient = Map.empty,
      activenessResult = ConflictDetectionHelpers.mkActivenessResult(),
      viewValidationResults = viewValidationResults,
      timeValidationResultE = timeValidationResultE,
      hostedWitnesses = Set.empty,
      replayCheckResult = None,
      validatedExternalTransactionHash = None,
      commitAfterFailedActivenessCheck = false,
    )

    responsesFactory
      .createConfirmationResponses(
        requestId,
        Seq.empty,
        transactionValidationResult,
        identityTopologySnapshot,
      )
      .futureValueUS
      .value
      .responses
  }

  "TransactionConfirmationResponsesFactory" should {
    "reject exactly the views with a rejected result and approve the others" in {
      val responses = createResponses(
        Map(leftViewPosition -> ExternalCallCheck.Rejected("disagreeing external call"))
      )

      responses should have size 2
      val leftResponse = responses.find(_.viewPositionO.contains(leftViewPosition)).value
      inside(leftResponse) { case ConfirmationResponse(_, reject: LocalReject, parties) =>
        reject.isMalformed shouldBe false
        reject.reason.message shouldBe
          "LOCAL_VERDICT_EXTERNAL_CALL_RESULT_DISAGREEMENT(8,0): " +
          "Rejected transaction due to inconsistent external call results: " +
          "disagreeing external call"
        parties shouldBe confirmers
      }
      // The view without external-call results receives no verdict from the check.
      val rightResponse = responses.find(_.viewPositionO.contains(rightViewPosition)).value
      inside(rightResponse) { case ConfirmationResponse(_, LocalApprove(), parties) =>
        parties shouldBe confirmers
      }
    }

    "abstain instead of approving exactly the views that cannot be validated" in {
      val responses = createResponses(
        Map(
          leftViewPosition ->
            ExternalCallCheck.CannotValidate("extension service is not configured")
        )
      )

      responses should have size 2
      val leftResponse = responses.find(_.viewPositionO.contains(leftViewPosition)).value
      inside(leftResponse) { case ConfirmationResponse(_, abstain: LocalAbstain, parties) =>
        abstain.reason.message shouldBe
          "CANNOT_PERFORM_ALL_VALIDATIONS(9,0): " +
          "Cannot perform all validations: extension service is not configured"
        parties shouldBe confirmers
      }
      val rightResponse = responses.find(_.viewPositionO.contains(rightViewPosition)).value
      inside(rightResponse) { case ConfirmationResponse(_, LocalApprove(), parties) =>
        parties shouldBe confirmers
      }
    }

    "prefer a rejection from an earlier validation suite over the abstention" in {
      val ledgerTime = CantonTimestamp.Epoch
      val recordTime = CantonTimestamp.Epoch.plusSeconds(10)
      val maxDelta = NonNegativeFiniteDuration.tryOfSeconds(1)
      val responses = createResponses(
        Map(
          leftViewPosition ->
            ExternalCallCheck.CannotValidate("extension service is not configured"),
          rightViewPosition ->
            ExternalCallCheck.CannotValidate("extension service is not configured"),
        ),
        timeValidationResultE = Left(
          TimeValidator.LedgerTimeRecordTimeDeltaTooLargeError(ledgerTime, recordTime, maxDelta)
        ),
      )

      responses should have size 2
      forEvery(responses) { response =>
        inside(response) { case ConfirmationResponse(_, reject: LocalReject, parties) =>
          reject.isMalformed shouldBe false
          reject.reason.message shouldBe
            "LOCAL_VERDICT_LEDGER_TIME_OUT_OF_BOUND(2,0): Rejected transaction as delta of " +
            "the ledger time and the record time exceed the time tolerance " +
            s"ledgerTime=$ledgerTime, recordTime=$recordTime, maxDelta=$maxDelta"
          parties shouldBe confirmers
        }
      }
    }

    "prefer a rejection from a later validation suite over the abstention" in {
      val responses = loggerFactory.assertLogs(
        createResponses(
          Map(
            leftViewPosition ->
              ExternalCallCheck.CannotValidate("extension service is not configured"),
            rightViewPosition ->
              ExternalCallCheck.CannotValidate("extension service is not configured"),
          ),
          authorizationResult = Map(leftViewPosition -> "no authorization"),
        ),
        _.shouldBeCantonErrorCode(LocalRejectError.MalformedRejects.MalformedRequest),
      )

      responses should have size 2
      val leftResponse = responses.find(_.viewPositionO.contains(leftViewPosition)).value
      inside(leftResponse) { case ConfirmationResponse(_, reject: LocalReject, parties) =>
        reject.isMalformed shouldBe true
        // Malformed rejections carry a redacted reason on the wire.
        reject.reason.message shouldBe
          "An error occurred. Please contact the operator and inquire about the request " +
          "<no-correlation-id> with tid <no-tid>"
        parties shouldBe empty
      }
      val rightResponse = responses.find(_.viewPositionO.contains(rightViewPosition)).value
      inside(rightResponse) { case ConfirmationResponse(_, abstain: LocalAbstain, parties) =>
        abstain.reason.message shouldBe
          "CANNOT_PERFORM_ALL_VALIDATIONS(9,0): " +
          "Cannot perform all validations: extension service is not configured"
        parties shouldBe confirmers
      }
    }

    "approve when the check passed" in {
      val responses = createResponses(
        Map(
          leftViewPosition -> ExternalCallCheck.Passed,
          rightViewPosition -> ExternalCallCheck.Passed,
        )
      )

      responses should have size 2
      forEvery(responses) { response =>
        inside(response) { case ConfirmationResponse(_, LocalApprove(), parties) =>
          parties shouldBe confirmers
        }
      }
    }
  }
}
