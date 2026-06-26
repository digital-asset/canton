// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.validation

import cats.data.EitherT
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveInt}
import com.digitalasset.canton.data.{
  CantonTimestamp,
  ParticipantTransactionView,
  PathRollbackContextFactory,
  TransactionView,
  ViewConfirmationParameters,
  ViewPosition,
}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.participant.protocol.LedgerEffectAbsolutizer.ViewAbsoluteLedgerEffect
import com.digitalasset.canton.participant.protocol.ProtocolProcessor
import com.digitalasset.canton.participant.protocol.conflictdetection.ConflictDetectionHelpers
import com.digitalasset.canton.participant.protocol.validation.ExternalCallValidationTestUtil.{
  externalCallViewResult,
  withExternalCallResults,
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
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{BaseTestWordSpec, HasExecutionContext, LfPartyId}
import com.digitalasset.daml.lf.data.Bytes
import com.digitalasset.daml.lf.transaction.ExternalCallResult

import scala.jdk.CollectionConverters.*

final class TransactionConfirmationResponsesFactoryExternalCallTest
    extends BaseTestWordSpec
    with HasExecutionContext {

  private val requestId = RequestId(CantonTimestamp.Epoch)
  private val factory =
    new ExampleTransactionFactory(versionOverride = Some(ProtocolVersion.dev))()

  private final class RecordingExternalCallValidator(
      results: Map[DAMLe.ExternalCallKey, ExternalCallValidator.Result]
  ) extends ExternalCallValidator {
    private val observedKeys =
      new java.util.concurrent.ConcurrentLinkedQueue[(DAMLe.ExternalCallKey, Bytes)]

    def observed: Seq[(DAMLe.ExternalCallKey, Bytes)] = observedKeys.asScala.toSeq

    override def validate(
        key: DAMLe.ExternalCallKey,
        recordedOutput: Bytes,
    )(implicit
        traceContext: com.digitalasset.canton.tracing.TraceContext
    ): FutureUnlessShutdown[ExternalCallValidator.Result] = {
      observedKeys.add(key -> recordedOutput)
      FutureUnlessShutdown.pure(
        results.getOrElse(key, ExternalCallValidator.Matched)
      )
    }
  }

  private val matchingExternalCallValidator: ExternalCallValidator = new ExternalCallValidator {
    override def validate(
        key: DAMLe.ExternalCallKey,
        recordedOutput: Bytes,
    )(implicit
        traceContext: com.digitalasset.canton.tracing.TraceContext
    ): FutureUnlessShutdown[ExternalCallValidator.Result] =
      FutureUnlessShutdown.pure(ExternalCallValidator.Matched)
  }

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

  private val leftViewPosition = ViewPosition.root
  private val rightViewPosition =
    ViewPosition(
      List(ViewPosition.MerkleSeqIndex(List(ViewPosition.MerkleSeqIndex.Direction.Right)))
    )
  private val unrelatedViewPosition =
    ViewPosition(
      List(ViewPosition.MerkleSeqIndex(List(ViewPosition.MerkleSeqIndex.Direction.Left)))
    )
  private val secondRightViewPosition =
    ViewPosition(
      List(
        ViewPosition.MerkleSeqIndex(
          List(
            ViewPosition.MerkleSeqIndex.Direction.Left,
            ViewPosition.MerkleSeqIndex.Direction.Right,
          )
        )
      )
    )

  private val externalCallResult = ExternalCallResult(
    extensionId = "extension",
    functionId = "function",
    config = Bytes.fromStringUtf8("config"),
    input = Bytes.fromStringUtf8("input"),
    output = Bytes.fromStringUtf8("output"),
  )

  private val otherExternalCallOutput =
    externalCallResult.copy(output = Bytes.fromStringUtf8("other-output"))

  private val defaultTopologySnapshot = {
    val snapshot = mock[TopologySnapshot]
    when(snapshot.canConfirm(any[ParticipantId], any[Set[LfPartyId]])(anyTraceContext))
      .thenAnswer { (_: ParticipantId, parties: Set[LfPartyId]) =>
        FutureUnlessShutdown.pure(parties)
      }
    snapshot
  }

  private def withConfirmers(view: TransactionView, confirmers: Set[LfPartyId]): TransactionView = {
    val confirmationParameters = ViewConfirmationParameters.create(
      informees = confirmers.map(_ -> NonNegativeInt.one).toMap,
      threshold = NonNegativeInt.tryCreate(confirmers.size),
    )
    TransactionView.Optics.viewCommonDataUnsafe
      .modify(commonData =>
        commonData.tryUnwrap.copy(viewConfirmationParameters = confirmationParameters)
      )(view)
  }

  private def validationResult(
      view: TransactionView,
      activenessResult: ViewActivenessResult = ViewActivenessResult(
        inactiveContracts = Set.empty,
        alreadyLockedContracts = Set.empty,
        existingContracts = Set.empty,
      ),
  ): ViewValidationResult =
    ViewValidationResult(
      ParticipantTransactionView.tryCreate(view),
      activenessResult,
    )

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

  private def defaultModelConformanceResult: ModelConformanceChecker.Result = {
    val example = factory.MultipleRoots
    ModelConformanceChecker.Result(
      example.updateId,
      WellFormedTransaction.checkOrThrow(
        example.versionedSuffixedTransaction,
        example.metadata,
        WellFormedTransaction.WithSuffixesAndMerged,
        PathRollbackContextFactory,
      ),
      unmergedTransactionsWithoutTopLevelRollbackNodes = Seq.empty,
    )
  }

  private def modelConformanceRecordedDisagreement(
      view: TransactionView,
      result: ExternalCallResult,
      conflictingOutput: Bytes = otherExternalCallOutput.output,
  ): ModelConformanceChecker.ErrorWithSubTransaction[ViewAbsoluteLedgerEffect] =
    ModelConformanceChecker.ErrorWithSubTransaction(
      NonEmpty(
        Seq,
        ModelConformanceChecker.DAMLeError(
          DAMLe.ExternalCallRecordedResultDisagreement(
            key = DAMLe.ExternalCallKey.fromResult(result),
            outputs = Set(result.output, conflictingOutput),
          ),
          view.viewHash,
        ),
      ),
      validSubTransactionO = None,
      validSubViewEffects = Seq.empty,
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

  private def assertRecordedDisagreementAlarms[A](count: Int = 1)(within: => A): A =
    loggerFactory.assertLogs(
      within,
      Seq.fill(count) { (logEntry: LogEntry) =>
        logEntry.shouldBeCantonErrorCode(
          ExternalCallValidationError.ExternalCallResultDisagreementAlarm
        )
        logEntry.mdc should contain("requestId" -> requestId.toString)
      }*
    )

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

    "reject locally validated output mismatches for otherwise approving hosted checking parties" in {
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
        Map(
          key -> ExternalCallValidator.Mismatched(
            computedOutput = otherExternalCallOutput.output,
            recordedOutput = externalCallResult.output,
          )
        )
      )

      val responses = createResponses(
        transactionValidationResult(Map(leftViewPosition -> validationResult(view))),
        responseFactory = responseFactory(ProtocolVersion.dev, validator),
      )

      validator.observed shouldBe Seq(key -> externalCallResult.output)
      inside(responses.find(_.confirmingParties == Set(submitter)).value) {
        case ConfirmationResponse(_, reject: LocalReject, _) =>
          reject.isMalformed shouldBe false
          reject.reason.message should include("inconsistent external call results")
      }
      inside(responses.find(_.confirmingParties == Set(signatory)).value) {
        case ConfirmationResponse(_, LocalApprove(), _) =>
          succeed
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

    "scope local external-call validation abstains to the affected view" in {
      val example = factory.MultipleRoots
      val confirmers = Set(submitter)
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
      val right = withConfirmers(example.rootViews(5), confirmers)
      val key = DAMLe.ExternalCallKey.fromResult(externalCallResult)
      val validator = new RecordingExternalCallValidator(
        Map(key -> ExternalCallValidator.UnableToValidate("extension service is not configured"))
      )

      val responses = createResponses(
        transactionValidationResult(
          Map(
            leftViewPosition -> validationResult(left),
            rightViewPosition -> validationResult(right),
          )
        ),
        responseFactory = responseFactory(ProtocolVersion.dev, validator),
      )

      validator.observed shouldBe Seq(key -> externalCallResult.output)
      inside(responses.filter(_.viewPositionO.contains(leftViewPosition)).loneElement) {
        case ConfirmationResponse(_, _: LocalAbstain, parties) =>
          parties shouldBe confirmers
      }
      inside(responses.filter(_.viewPositionO.contains(rightViewPosition)).loneElement) {
        case ConfirmationResponse(_, LocalApprove(), parties) =>
          parties shouldBe confirmers
      }
      responses.exists {
        case ConfirmationResponse(_, reject: LocalReject, parties) =>
          reject.isMalformed && parties.isEmpty
        case _ => false
      } shouldBe false
    }

    "not locally validate external calls when no hosted confirmer is a checking party" in {
      val example = factory.MultipleRoots
      val confirmers = Set(submitter, signatory)
      val view = withExternalCallResults(
        withConfirmers(example.rootViews(4), confirmers),
        Seq(
          externalCallViewResult(
            exerciseIndex = 0,
            result = externalCallResult,
            checkingParties = Set(signatory),
          )
        ),
      )
      val key = DAMLe.ExternalCallKey.fromResult(externalCallResult)
      val validator = new RecordingExternalCallValidator(
        Map(
          key -> ExternalCallValidator.Mismatched(
            computedOutput = otherExternalCallOutput.output,
            recordedOutput = externalCallResult.output,
          )
        )
      )
      val submitterOnlyTopologySnapshot = {
        val snapshot = mock[TopologySnapshot]
        when(snapshot.canConfirm(any[ParticipantId], any[Set[LfPartyId]])(anyTraceContext))
          .thenAnswer { (_: ParticipantId, parties: Set[LfPartyId]) =>
            FutureUnlessShutdown.pure(parties.intersect(Set(submitter)))
          }
        snapshot
      }

      val responses = createResponses(
        transactionValidationResult(Map(leftViewPosition -> validationResult(view))),
        responseFactory = responseFactory(ProtocolVersion.dev, validator),
        topologySnapshot = submitterOnlyTopologySnapshot,
      )

      validator.observed shouldBe Seq.empty
      inside(responses.loneElement) { case ConfirmationResponse(_, LocalApprove(), parties) =>
        parties shouldBe Set(submitter)
      }
      responses.exists {
        case ConfirmationResponse(_, _: LocalReject, _) => true
        case ConfirmationResponse(_, _: LocalAbstain, _) => true
        case _ => false
      } shouldBe false
    }

    "deduplicate local external-call validation by semantic key and route the result to all occurrences" in {
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
            result = externalCallResult,
            checkingParties = Set(submitter),
          )
        ),
      )
      val key = DAMLe.ExternalCallKey.fromResult(externalCallResult)
      val validator = new RecordingExternalCallValidator(
        Map(
          key -> ExternalCallValidator.Mismatched(
            computedOutput = otherExternalCallOutput.output,
            recordedOutput = externalCallResult.output,
          )
        )
      )

      val responses = createResponses(
        transactionValidationResult(
          Map(
            leftViewPosition -> validationResult(left),
            rightViewPosition -> validationResult(right),
          )
        ),
        responseFactory = responseFactory(ProtocolVersion.dev, validator),
      )

      validator.observed shouldBe Seq(key -> externalCallResult.output)
      Set(leftViewPosition, rightViewPosition).foreach { viewPosition =>
        val viewResponses = responses.filter(_.viewPositionO.contains(viewPosition))
        inside(viewResponses.find(_.confirmingParties == Set(submitter)).value) {
          case ConfirmationResponse(_, reject: LocalReject, _) =>
            reject.isMalformed shouldBe false
            reject.reason.message should include("inconsistent external call results")
        }
        inside(viewResponses.find(_.confirmingParties == Set(signatory)).value) {
          case ConfirmationResponse(_, LocalApprove(), _) =>
            succeed
        }
      }
    }

    "prefer recorded external-call disagreements over local external-call validation" in {
      val key = DAMLe.ExternalCallKey.fromResult(externalCallResult)
      val validator = new RecordingExternalCallValidator(
        Map(key -> ExternalCallValidator.UnableToValidate("extension service is not configured"))
      )

      val responses =
        assertRecordedDisagreementAlarms() {
          createResponses(
            transactionValidationResult(conflictingExternalCallViews),
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
        case ConfirmationResponse(_, LocalApprove(), _) =>
          succeed
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

    "not attribute external-call disagreements to unrelated hosted views" in {
      val example = factory.MultipleRoots
      val confirmers = Set(submitter, signatory)
      val unrelatedView = withConfirmers(example.rootViews(3), confirmers)
      val responses =
        assertRecordedDisagreementAlarms() {
          createResponses(
            transactionValidationResult(
              conflictingExternalCallViews +
                (unrelatedViewPosition -> validationResult(unrelatedView))
            )
          )
        }

      val unrelatedResponses = responses.filter(_.viewPositionO.contains(unrelatedViewPosition))
      inside(unrelatedResponses.loneElement) {
        case ConfirmationResponse(_, LocalApprove(), confirmingParties) =>
          confirmingParties shouldBe confirmers
      }
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

    "emit external-call disagreement responses for every affected view" in {
      val example = factory.MultipleRoots
      val confirmers = Set(submitter, signatory)
      val firstCall = externalCallResult.copy(functionId = "function-a")
      val secondCall = externalCallResult.copy(functionId = "function-b")

      def view(
          baseView: TransactionView,
          result: ExternalCallResult,
          exerciseIndex: Int,
      ): ViewValidationResult =
        validationResult(
          withExternalCallResults(
            withConfirmers(baseView, confirmers),
            Seq(
              externalCallViewResult(
                exerciseIndex = exerciseIndex,
                result = result,
                checkingParties = Set(submitter),
              )
            ),
          )
        )

      val responses = assertRecordedDisagreementAlarms(count = 2) {
        createResponses(
          transactionValidationResult(
            Map(
              leftViewPosition -> view(example.rootViews(4), firstCall, 0),
              rightViewPosition -> view(
                example.rootViews(5),
                firstCall.copy(output = Bytes.fromStringUtf8("other-output-a")),
                1,
              ),
              unrelatedViewPosition -> view(example.rootViews(4), secondCall, 2),
              secondRightViewPosition -> view(
                example.rootViews(5),
                secondCall.copy(output = Bytes.fromStringUtf8("other-output-b")),
                3,
              ),
            )
          )
        )
      }

      val affectedPositions =
        Set(
          leftViewPosition,
          rightViewPosition,
          unrelatedViewPosition,
          secondRightViewPosition,
        )
      affectedPositions.foreach { viewPosition =>
        val viewResponses = responses.filter(_.viewPositionO.contains(viewPosition))
        inside(viewResponses.find(_.confirmingParties == Set(submitter)).value) {
          case ConfirmationResponse(_, reject: LocalReject, _) =>
            reject.isMalformed shouldBe false
            reject.reason.message should include("inconsistent external call results")
        }
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
