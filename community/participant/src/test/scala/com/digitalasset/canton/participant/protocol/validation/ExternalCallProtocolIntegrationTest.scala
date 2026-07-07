// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.validation

import cats.data.EitherT
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.data.{
  CantonTimestamp,
  ParticipantTransactionView,
  PathRollbackContextFactory,
  TransactionView,
  ViewConfirmationParameters,
  ViewParticipantData,
  ViewPosition,
}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.participant.protocol.LedgerEffectAbsolutizer.ViewAbsoluteLedgerEffect
import com.digitalasset.canton.participant.protocol.conflictdetection.ConflictDetectionHelpers
import com.digitalasset.canton.participant.protocol.validation.ExternalCallConsistencyChecker.{
  ExternalCallOccurrence,
  Inconsistency,
}
import com.digitalasset.canton.participant.util.DAMLe
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
import com.digitalasset.canton.topology.{ParticipantId, SynchronizerId, UniqueIdentifier}
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{BaseTestWordSpec, HasExecutionContext, LfPartyId}
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
    with ExternalCallValidationTestUtil {

  protected val factory: ExampleTransactionFactory =
    new ExampleTransactionFactory(versionOverride = Some(ProtocolVersion.dev))(
      psid = SynchronizerId(
        UniqueIdentifier.tryFromProtoPrimitive("example::default")
      ).toPhysical.copy(protocolVersion = ProtocolVersion.dev)
    )

  private val requestId: RequestId = RequestId(CantonTimestamp.Epoch)

  private val confirmers: Set[LfPartyId] = Set(submitter, signatory)

  private val externalCallKey: DAMLe.ExternalCallKey =
    DAMLe.ExternalCallKey.fromResult(externalCallResult)

  private val leftViewPosition: ViewPosition =
    ViewPosition(
      List(ViewPosition.MerkleSeqIndex(List(ViewPosition.MerkleSeqIndex.Direction.Left)))
    )
  private val rightViewPosition: ViewPosition =
    ViewPosition(
      List(ViewPosition.MerkleSeqIndex(List(ViewPosition.MerkleSeqIndex.Direction.Right)))
    )

  private final class RecordingExternalCallValidator(
      results: Map[DAMLe.ExternalCallKey, ExternalCallValidator.Result]
  ) extends ExternalCallValidator {
    private val observedKeys: ConcurrentLinkedQueue[DAMLe.ExternalCallKey] =
      new ConcurrentLinkedQueue[DAMLe.ExternalCallKey]

    def observed: Seq[DAMLe.ExternalCallKey] = observedKeys.asScala.toSeq

    override def validate(
        key: DAMLe.ExternalCallKey,
        recordedOutput: Bytes,
    )(implicit
        traceContext: TraceContext
    ): FutureUnlessShutdown[ExternalCallValidator.Result] = {
      observedKeys.add(key)
      FutureUnlessShutdown.pure(results.getOrElse(key, ExternalCallValidator.Matched))
    }
  }

  private def externalCallCheck(validator: ExternalCallValidator): ExternalCallCheck =
    new ExternalCallCheck(validator, PositiveInt.tryCreate(8), loggerFactory)

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
  ): ExternalCallCheck.Result =
    externalCallCheck(validator).check(requestId, views, runValidation).futureValueUS

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
    "pass when no view records external-call results" in {
      val validator = new RecordingExternalCallValidator(Map.empty)

      runCheck(validator, views(Seq.empty)) shouldBe ExternalCallCheck.Passed
      validator.observed shouldBe empty
    }

    "pass when the recorded results agree and re-validation matches" in {
      val validator = new RecordingExternalCallValidator(Map.empty)
      val result = runCheck(
        validator,
        views(
          leftResults = Seq(externalCallViewResult(0, externalCallResult, Set(submitter))),
          rightResults = Seq(externalCallViewResult(1, externalCallResult, Set(submitter))),
        ),
      )

      result shouldBe ExternalCallCheck.Passed
      // Re-validated once for the shared semantic key.
      validator.observed shouldBe Seq(externalCallKey)
    }

    "reject and alarm when recorded results disagree across the request" in {
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
      result shouldBe ExternalCallCheck.Rejected(expectedInconsistency.description)
      // Disagreeing results are not re-validated.
      validator.observed shouldBe empty
    }

    "reject when re-validation returns a different output" in {
      val validator = new RecordingExternalCallValidator(
        Map(
          externalCallKey -> ExternalCallValidator.Mismatched(
            computedOutput = otherExternalCallResult.output,
            recordedOutput = externalCallResult.output,
          )
        )
      )
      val result = runCheck(
        validator,
        views(leftResults = Seq(externalCallViewResult(0, externalCallResult, Set(submitter)))),
      )

      val expectedInconsistency = Inconsistency(
        externalCallKey,
        outputs = Set(otherExternalCallResult.output, externalCallResult.output),
        occurrences =
          Set(ExternalCallOccurrence(leftViewPosition, NonNegativeInt.zero, NonNegativeInt.zero)),
      )
      result shouldBe ExternalCallCheck.Rejected(expectedInconsistency.description)
      validator.observed shouldBe Seq(externalCallKey)
    }

    "report a recorded result that cannot be re-validated" in {
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

      result shouldBe ExternalCallCheck.CannotValidate(
        "extension service is not configured"
      )
    }

    "skip re-validation of consistent results when instructed" in {
      val validator = new RecordingExternalCallValidator(Map.empty)
      val result = runCheck(
        validator,
        views(
          leftResults = Seq(externalCallViewResult(0, externalCallResult, Set(submitter))),
          rightResults = Seq(externalCallViewResult(1, externalCallResult, Set(submitter))),
        ),
        runValidation = false,
      )

      result shouldBe ExternalCallCheck.Passed
      validator.observed shouldBe empty
    }
  }

  private lazy val responsesFactory: TransactionConfirmationResponsesFactory =
    new TransactionConfirmationResponsesFactory(
      submittingParticipant,
      factory.psid,
      loggerFactory,
    )

  private val identityTopologySnapshot: TopologySnapshot = {
    val snapshot = mock[TopologySnapshot]
    when(snapshot.canConfirm(any[ParticipantId], any[Set[LfPartyId]])(anyTraceContext))
      .thenAnswer { (_: ParticipantId, parties: Set[LfPartyId]) =>
        FutureUnlessShutdown.pure(parties)
      }
    snapshot
  }

  private def createResponses(
      checkResult: ExternalCallCheck.Result
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
      authorizationResult = Map.empty,
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
      timeValidationResultE = Right(()),
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
    "reject every view on behalf of all hosted confirming parties on a rejected check" in {
      val responses = createResponses(ExternalCallCheck.Rejected("disagreeing external call"))

      responses should have size 2
      forEvery(responses) { response =>
        inside(response) { case ConfirmationResponse(_, reject: LocalReject, parties) =>
          reject.isMalformed shouldBe false
          reject.reason.message shouldBe
            "LOCAL_VERDICT_EXTERNAL_CALL_RESULT_DISAGREEMENT(8,0): " +
            "Rejected transaction due to inconsistent external call results: " +
            "disagreeing external call"
          parties shouldBe confirmers
        }
      }
    }

    "abstain instead of approving when the check cannot validate" in {
      val responses = createResponses(
        ExternalCallCheck.CannotValidate("extension service is not configured")
      )

      responses should have size 2
      forEvery(responses) { response =>
        inside(response) { case ConfirmationResponse(_, abstain: LocalAbstain, parties) =>
          abstain.reason.message shouldBe
            "CANNOT_PERFORM_ALL_VALIDATIONS(9,0): " +
            "Cannot perform all validations: extension service is not configured"
          parties shouldBe confirmers
        }
      }
    }

    "approve when the check passed" in {
      val responses = createResponses(ExternalCallCheck.Passed)

      responses should have size 2
      forEvery(responses) { response =>
        inside(response) { case ConfirmationResponse(_, LocalApprove(), parties) =>
          parties shouldBe confirmers
        }
      }
    }
  }
}
