// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.validation

import cats.data.EitherT
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.data.ViewPosition
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.participant.protocol.LedgerEffectAbsolutizer.ViewAbsoluteLedgerEffect
import com.digitalasset.canton.participant.protocol.ProtocolProcessor
import com.digitalasset.canton.participant.protocol.conflictdetection.ConflictDetectionHelpers
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
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{BaseTestWordSpec, HasExecutionContext, LfPartyId}

final class TransactionConfirmationResponsesFactoryExternalCallTest
    extends BaseTestWordSpec
    with HasExecutionContext
    with ExternalCallValidationTestUtil {

  protected val factory =
    new ExampleTransactionFactory(versionOverride = Some(ProtocolVersion.dev))()

  private lazy val sut =
    responseFactory(ProtocolVersion.dev)

  private def responseFactory(
      protocolVersion: ProtocolVersion,
      externalCallValidator: ExternalCallValidator = matchingExternalCallValidator,
  ): TransactionConfirmationResponsesFactory =
    new TransactionConfirmationResponsesFactory(
      submittingParticipant,
      factory.psid.copy(protocolVersion = protocolVersion),
      externalCallValidator,
      PositiveInt.tryCreate(8),
      loggerFactory,
    )

  private val defaultTopologySnapshot = {
    val snapshot = mock[TopologySnapshot]
    when(snapshot.canConfirm(any[ParticipantId], any[Set[LfPartyId]])(anyTraceContext))
      .thenAnswer { (_: ParticipantId, parties: Set[LfPartyId]) =>
        FutureUnlessShutdown.pure(parties)
      }
    snapshot
  }

  private def transactionValidationResult(
      viewValidationResults: Map[ViewPosition, ViewValidationResult],
      authorizationResult: Map[ViewPosition, String] = Map.empty,
      modelConformanceResultE: Either[
        ModelConformanceChecker.ErrorWithSubTransaction[ViewAbsoluteLedgerEffect],
        ModelConformanceChecker.Result,
      ] = Right(defaultModelConformanceResult),
  ): TransactionValidationResult =
    TransactionValidationResult(
      updateId = defaultModelConformanceResult.updateId,
      submitterMetadataO = None,
      workflowIdO = None,
      contractConsistencyResultE = Right(()),
      authenticationResult = Map.empty,
      authorizationResult = authorizationResult,
      modelConformanceResultET = EitherT.fromEither[FutureUnlessShutdown](modelConformanceResultE),
      internalConsistencyResultET = EitherT
        .rightT[FutureUnlessShutdown, InternalConsistencyChecker.ErrorWithInternalConsistencyCheck](
          ()
        ),
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

  private def createResponses(
      transactionValidationResult: TransactionValidationResult,
      responseFactory: TransactionConfirmationResponsesFactory = sut,
      topologySnapshot: TopologySnapshot = defaultTopologySnapshot,
      malformedPayloads: Seq[ProtocolProcessor.MalformedPayload] = Seq.empty,
  ): Seq[ConfirmationResponse] =
    responseFactory
      .createConfirmationResponses(
        requestId,
        malformedPayloads,
        transactionValidationResult,
        topologySnapshot,
      )
      .futureValueUS
      .value
      .responses

  private def conflictingExternalCallViews: Map[ViewPosition, ViewValidationResult] = {
    val example = factory.MultipleRoots
    val confirmers = Set(submitter, signatory)
    val left = withExternalCallResults(
      withConfirmers(example.rootViews(4), confirmers),
      Seq(
        externalCallViewResult(
          exerciseIndex = 0,
          result = externalCallResult,
          checkingParties = Set(submitter),
        )
      ),
    )
    val right = withExternalCallResults(
      withConfirmers(example.rootViews(5), confirmers),
      Seq(
        externalCallViewResult(
          exerciseIndex = 1,
          result = otherExternalCallOutput,
          checkingParties = Set(submitter),
        )
      ),
    )

    Map(
      leftViewPosition -> validationResult(left),
      rightViewPosition -> validationResult(right),
    )
  }

  "TransactionConfirmationResponsesFactory" should {
    "split external-call disagreements from the general verdict by party" in {
      val responses =
        assertRecordedDisagreementAlarms() {
          createResponses(transactionValidationResult(conflictingExternalCallViews))
        }
      val leftResponses = responses.filter(_.viewPositionO.contains(leftViewPosition))

      leftResponses should have size 2
      inside(leftResponses.find(_.confirmingParties == Set(submitter)).value) {
        case ConfirmationResponse(_, reject: LocalReject, _) =>
          reject.isMalformed shouldBe false
          reject.reason.message should include("inconsistent external call results")
      }
      inside(leftResponses.find(_.confirmingParties == Set(signatory)).value) {
        case ConfirmationResponse(_, LocalApprove(), _) =>
          succeed
      }
    }

    "alarm on visible recorded external-call disagreements without hosted confirmers" in {
      val example = factory.MultipleRoots
      val confirmers = Set(submitter, signatory)
      val left = withExternalCallResults(
        withConfirmers(example.rootViews(4), confirmers),
        Seq(
          externalCallViewResult(
            exerciseIndex = 0,
            result = externalCallResult,
            checkingParties = Set(signatory),
          )
        ),
      )
      val right = withExternalCallResults(
        withConfirmers(example.rootViews(5), confirmers),
        Seq(
          externalCallViewResult(
            exerciseIndex = 0,
            result = otherExternalCallOutput,
            checkingParties = Set(signatory),
          )
        ),
      )
      val noHostedConfirmersTopologySnapshot = {
        val snapshot = mock[TopologySnapshot]
        when(snapshot.canConfirm(any[ParticipantId], any[Set[LfPartyId]])(anyTraceContext))
          .thenReturn(FutureUnlessShutdown.pure(Set.empty))
        snapshot
      }

      val responsesO = assertRecordedDisagreementAlarms() {
        sut
          .createConfirmationResponses(
            requestId,
            Seq.empty,
            transactionValidationResult(
              Map(
                leftViewPosition -> validationResult(left),
                rightViewPosition -> validationResult(right),
              )
            ),
            noHostedConfirmersTopologySnapshot,
          )
          .futureValueUS
      }

      responsesO shouldBe None
    }

    "alarm on visible recorded external-call disagreements with malformed payloads" in {
      val responses = loggerFactory.assertLogs(
        createResponses(
          transactionValidationResult(conflictingExternalCallViews),
          malformedPayloads = Seq(ProtocolProcessor.IncompleteLightViewTree(ViewPosition.root)),
        ),
        { (logEntry: LogEntry) =>
          logEntry.shouldBeCantonErrorCode(
            ExternalCallValidationError.ExternalCallResultDisagreementAlarm
          )
          logEntry.mdc should contain("requestId" -> requestId.toString)
        },
        { (logEntry: LogEntry) =>
          logEntry.shouldBeCantonErrorCode(LocalRejectError.MalformedRejects.Payloads)
          logEntry.mdc should contain("requestId" -> requestId.toString)
        },
      )

      responses should have size 1
      inside(responses.loneElement) {
        case ConfirmationResponse(None, reject: LocalReject, parties) =>
          reject.isMalformed shouldBe true
          parties shouldBe Set.empty
      }
    }

    "abstain when locally responsible validation cannot obtain comparable output bytes" in {
      val example = factory.MultipleRoots
      val confirmers = Set(submitter, signatory)
      val view = withExternalCallResults(
        withConfirmers(example.rootViews(4), confirmers),
        Seq(
          externalCallViewResult(
            exerciseIndex = 0,
            result = externalCallResult,
            checkingParties = Set(submitter),
          )
        ),
      )
      val key = DAMLe.ExternalCallKey.fromResult(externalCallResult)
      val validator = new RecordingExternalCallValidator(
        Map(key -> ExternalCallValidator.UnableToValidate("extension service is not configured"))
      )

      val responses = createResponses(
        transactionValidationResult(Map(leftViewPosition -> validationResult(view))),
        responseFactory = responseFactory(ProtocolVersion.dev, validator),
      )

      validator.observed shouldBe Seq(key -> externalCallResult.output)
      inside(responses.find(_.confirmingParties == Set(submitter)).value) {
        case ConfirmationResponse(_, abstain: LocalAbstain, _) =>
          abstain.reason.message should include("Cannot perform all validations")
          abstain.reason.message should include("extension service is not configured")
      }
      inside(responses.find(_.confirmingParties == Set(signatory)).value) {
        case ConfirmationResponse(_, LocalApprove(), _) =>
          succeed
      }
    }

    "not run local external-call validation when a malformed verdict already wins" in {
      val example = factory.MultipleRoots
      val confirmers = Set(submitter)
      val view = withExternalCallResults(
        withConfirmers(example.rootViews(4), confirmers),
        Seq(
          externalCallViewResult(
            exerciseIndex = 0,
            result = externalCallResult,
            checkingParties = Set(submitter),
          )
        ),
      )
      val key = DAMLe.ExternalCallKey.fromResult(externalCallResult)
      val validator = new RecordingExternalCallValidator(
        Map(key -> ExternalCallValidator.UnableToValidate("extension service is not configured"))
      )

      val responses = loggerFactory.assertLogs(
        createResponses(
          transactionValidationResult(
            Map(leftViewPosition -> validationResult(view)),
            authorizationResult = Map(leftViewPosition -> "authorization failure"),
          ),
          responseFactory = responseFactory(ProtocolVersion.dev, validator),
        ),
        _.shouldBeCantonErrorCode(LocalRejectError.MalformedRejects.MalformedRequest),
      )

      validator.observed shouldBe Seq.empty
      inside(responses.loneElement) { case ConfirmationResponse(_, reject: LocalReject, parties) =>
        reject.isMalformed shouldBe true
        parties shouldBe Set.empty
      }
    }

    "preserve recorded external-call disagreements over ordinary non-malformed rejects" in {
      val example = factory.MultipleRoots
      val confirmers = Set(submitter, signatory)
      val left = withExternalCallResults(
        withConfirmers(example.rootViews(4), confirmers),
        Seq(
          externalCallViewResult(
            exerciseIndex = 0,
            result = externalCallResult,
            checkingParties = Set(submitter),
          )
        ),
      )
      val right = withExternalCallResults(
        withConfirmers(example.rootViews(5), confirmers),
        Seq(
          externalCallViewResult(
            exerciseIndex = 1,
            result = otherExternalCallOutput,
            checkingParties = Set(submitter),
          )
        ),
      )
      val inactiveInput = left.viewParticipantData.tryUnwrap.coreInputs.keySet.headOption.value
      val key = DAMLe.ExternalCallKey.fromResult(externalCallResult)
      val validator = new RecordingExternalCallValidator(
        Map(key -> ExternalCallValidator.UnableToValidate("extension service is not configured"))
      )

      val responses = assertRecordedDisagreementAlarms() {
        createResponses(
          transactionValidationResult(
            Map(
              leftViewPosition -> validationResult(
                left,
                ViewActivenessResult(
                  inactiveContracts = Set(inactiveInput),
                  alreadyLockedContracts = Set.empty,
                  existingContracts = Set.empty,
                ),
              ),
              rightViewPosition -> validationResult(right),
            )
          ),
          responseFactory = responseFactory(ProtocolVersion.dev, validator),
        )
      }

      validator.observed shouldBe Seq.empty
      val leftResponses = responses.filter(_.viewPositionO.contains(leftViewPosition))
      leftResponses should have size 2
      inside(leftResponses.find(_.confirmingParties == Set(submitter)).value) {
        case ConfirmationResponse(_, reject: LocalReject, _) =>
          reject.isMalformed shouldBe false
          reject.reason.message should include("inconsistent external call results")
      }
      inside(leftResponses.find(_.confirmingParties == Set(signatory)).value) {
        case ConfirmationResponse(_, reject: LocalReject, _) =>
          reject.isMalformed shouldBe false
          reject.reason.message should not include "inconsistent external call results"
      }
      responses.exists {
        case ConfirmationResponse(_, reject: LocalReject, parties) =>
          reject.isMalformed && parties.isEmpty
        case _ => false
      } shouldBe false
    }

    "prefer malformed verdicts over external-call disagreement responses" in {
      val responses = loggerFactory.assertLogs(
        createResponses(
          transactionValidationResult(
            conflictingExternalCallViews,
            authorizationResult = Map(leftViewPosition -> "authorization failure"),
          )
        ),
        _.shouldBeCantonErrorCode(
          ExternalCallValidationError.ExternalCallResultDisagreementAlarm
        ),
        _.shouldBeCantonErrorCode(LocalRejectError.MalformedRejects.MalformedRequest),
      )
      val leftResponses = responses.filter(_.viewPositionO.contains(leftViewPosition))

      inside(leftResponses.loneElement) {
        case ConfirmationResponse(_, reject: LocalReject, confirmingParties) =>
          reject.isMalformed shouldBe true
          confirmingParties shouldBe Set.empty
      }
    }

    "route recorded external-call result disagreements by checking party" in {
      val viewValidationResults = conflictingExternalCallViews
      val leftView = viewValidationResults(leftViewPosition).view.unwrap

      val responses = assertRecordedDisagreementAlarms() {
        createResponses(
          transactionValidationResult(
            viewValidationResults,
            modelConformanceResultE =
              Left(modelConformanceRecordedDisagreement(leftView, externalCallResult)),
          )
        )
      }

      val leftResponses = responses.filter(_.viewPositionO.contains(leftViewPosition))
      leftResponses should have size 2
      inside(leftResponses.find(_.confirmingParties == Set(submitter)).value) {
        case ConfirmationResponse(_, reject: LocalReject, _) =>
          reject.isMalformed shouldBe false
          reject.reason.message should include("inconsistent external call results")
      }
      inside(leftResponses.find(_.confirmingParties == Set(signatory)).value) {
        case ConfirmationResponse(_, LocalApprove(), _) =>
          succeed
      }
      responses.exists {
        case ConfirmationResponse(_, reject: LocalReject, confirmingParties) =>
          reject.isMalformed && confirmingParties.isEmpty
        case _ => false
      } shouldBe false
    }

    "route recorded external-call replay ambiguity for disjoint checking parties" in {
      val example = factory.MultipleRoots
      val confirmers = Set(submitter, signatory)
      val left = withExternalCallResults(
        withConfirmers(example.rootViews(4), confirmers),
        Seq(
          externalCallViewResult(
            exerciseIndex = 0,
            result = externalCallResult,
            checkingParties = Set(submitter),
          )
        ),
      )
      val right = withExternalCallResults(
        withConfirmers(example.rootViews(5), confirmers),
        Seq(
          externalCallViewResult(
            exerciseIndex = 1,
            result = otherExternalCallOutput,
            checkingParties = Set(signatory),
          )
        ),
      )

      val responses = assertRecordedDisagreementAlarms() {
        createResponses(
          transactionValidationResult(
            Map(
              leftViewPosition -> validationResult(left),
              rightViewPosition -> validationResult(right),
            ),
            modelConformanceResultE = Left(
              modelConformanceRecordedDisagreement(left, externalCallResult)
            ),
          )
        )
      }

      val leftResponses = responses.filter(_.viewPositionO.contains(leftViewPosition))
      leftResponses should have size 2
      inside(leftResponses.find(_.confirmingParties == Set(submitter)).value) {
        case ConfirmationResponse(_, reject: LocalReject, _) =>
          reject.isMalformed shouldBe false
          reject.reason.message should include("inconsistent external call results")
      }
      inside(leftResponses.find(_.confirmingParties == Set(signatory)).value) {
        case ConfirmationResponse(_, LocalApprove(), _) =>
          succeed
      }

      val rightResponses = responses.filter(_.viewPositionO.contains(rightViewPosition))
      rightResponses should have size 2
      inside(rightResponses.find(_.confirmingParties == Set(signatory)).value) {
        case ConfirmationResponse(_, reject: LocalReject, _) =>
          reject.isMalformed shouldBe false
          reject.reason.message should include("inconsistent external call results")
      }
      inside(rightResponses.find(_.confirmingParties == Set(submitter)).value) {
        case ConfirmationResponse(_, LocalApprove(), _) =>
          succeed
      }

      responses.exists {
        case ConfirmationResponse(_, reject: LocalReject, confirmingParties) =>
          reject.isMalformed && confirmingParties.isEmpty
        case _ => false
      } shouldBe false
    }

  }
}
