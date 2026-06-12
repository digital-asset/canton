// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.validation

import cats.data.EitherT
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.data.{
  CantonTimestamp,
  PathRollbackContextFactory,
  ParticipantTransactionView,
  TransactionView,
  ViewConfirmationParameters,
  ViewPosition,
}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.participant.protocol.LedgerEffectAbsolutizer.ViewAbsoluteLedgerEffect
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
import com.digitalasset.canton.protocol.messages.{ConfirmationResponse, LocalApprove, LocalReject}
import com.digitalasset.canton.topology.ParticipantId
import com.digitalasset.canton.topology.client.TopologySnapshot
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{BaseTestWordSpec, HasExecutionContext, LfPartyId}
import com.digitalasset.daml.lf.data.{Bytes, ImmArray}
import com.digitalasset.daml.lf.transaction.ExternalCallResult

final class TransactionConfirmationResponsesFactoryExternalCallTest
    extends BaseTestWordSpec
    with HasExecutionContext {

  private val requestId = RequestId(CantonTimestamp.Epoch)
  private val factory =
    new ExampleTransactionFactory(versionOverride = Some(ProtocolVersion.dev))()
  private val sut =
    responseFactory(ProtocolVersion.dev)

  private def responseFactory(
      protocolVersion: ProtocolVersion
  ): TransactionConfirmationResponsesFactory =
    new TransactionConfirmationResponsesFactory(
      submittingParticipant,
      factory.psid.copy(protocolVersion = protocolVersion),
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

  private val topologySnapshot = {
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

  private def validationResult(view: TransactionView): ViewValidationResult =
    ViewValidationResult(
      ParticipantTransactionView.tryCreate(view),
      ViewActivenessResult(
        inactiveContracts = Set.empty,
        alreadyLockedContracts = Set.empty,
        existingContracts = Set.empty,
      ),
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

  private def modelConformanceMismatch(
      view: TransactionView,
      result: ExternalCallResult,
      computedOutput: Bytes = otherExternalCallOutput.output,
  ): ModelConformanceChecker.ErrorWithSubTransaction[ViewAbsoluteLedgerEffect] =
    ModelConformanceChecker.ErrorWithSubTransaction(
      NonEmpty(
        Seq,
        ModelConformanceChecker.DAMLeError(
          DAMLe.ExternalCallResultMismatch(
            extensionId = result.extensionId,
            functionId = result.functionId,
            config = result.config,
            input = result.input,
            computedOutput = computedOutput,
            recordedOutput = result.output,
          ),
          view.viewHash,
        ),
      ),
      validSubTransactionO = None,
      validSubViewEffects = Seq.empty,
    )

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
  ): Seq[ConfirmationResponse] =
    responseFactory
      .createConfirmationResponses(
        requestId,
        malformedPayloads = Seq.empty,
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
      ImmArray(
        externalCallViewResult(
          exerciseIndex = 0,
          result = externalCallResult,
          checkingParties = Set(submitter),
        )
      ),
    )
    val right = withExternalCallResults(
      withConfirmers(example.rootViews(5), confirmers),
      ImmArray(
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
        createResponses(transactionValidationResult(conflictingExternalCallViews))
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

    "not attribute external-call disagreements to unrelated hosted views" in {
      val example = factory.MultipleRoots
      val confirmers = Set(submitter, signatory)
      val unrelatedView = withConfirmers(example.rootViews(3), confirmers)
      val responses =
        createResponses(
          transactionValidationResult(
            conflictingExternalCallViews +
              (unrelatedViewPosition -> validationResult(unrelatedView))
          )
        )

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
            ImmArray(
              externalCallViewResult(
                exerciseIndex = exerciseIndex,
                result = result,
                checkingParties = Set(submitter),
              )
            ),
          )
        )

      val responses = createResponses(
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

      val responses = createResponses(
        transactionValidationResult(
          viewValidationResults,
          modelConformanceResultE =
            Left(modelConformanceRecordedDisagreement(leftView, externalCallResult)),
        )
      )

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
        ImmArray(
          externalCallViewResult(
            exerciseIndex = 0,
            result = externalCallResult,
            checkingParties = Set(submitter),
          )
        ),
      )
      val right = withExternalCallResults(
        withConfirmers(example.rootViews(5), confirmers),
        ImmArray(
          externalCallViewResult(
            exerciseIndex = 1,
            result = otherExternalCallOutput,
            checkingParties = Set(signatory),
          )
        ),
      )

      val responses = createResponses(
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

    "route local external-call result mismatches by checking party" in {
      val example = factory.MultipleRoots
      val confirmers = Set(submitter, signatory)
      val view = withExternalCallResults(
        withConfirmers(example.rootViews(4), confirmers),
        ImmArray(
          externalCallViewResult(
            exerciseIndex = 0,
            result = externalCallResult,
            checkingParties = Set(submitter),
          )
        ),
      )

      val responses = createResponses(
        transactionValidationResult(
          Map(leftViewPosition -> validationResult(view)),
          modelConformanceResultE = Left(modelConformanceMismatch(view, externalCallResult)),
        )
      )

      val viewResponses = responses.filter(_.viewPositionO.contains(leftViewPosition))
      viewResponses should have size 2
      inside(viewResponses.find(_.confirmingParties == Set(submitter)).value) {
        case ConfirmationResponse(_, reject: LocalReject, _) =>
          reject.isMalformed shouldBe false
          reject.reason.message should include("inconsistent external call results")
      }
      inside(viewResponses.find(_.confirmingParties == Set(signatory)).value) {
        case ConfirmationResponse(_, LocalApprove(), _) =>
          succeed
      }
      responses.exists {
        case ConfirmationResponse(_, reject: LocalReject, confirmingParties) =>
          reject.isMalformed && confirmingParties.isEmpty
        case _ => false
      } shouldBe false
    }

    "keep unmatched local external-call result mismatches as malformed model-conformance rejects" in {
      val example = factory.MultipleRoots
      val confirmers = Set(submitter, signatory)
      val view = withConfirmers(example.rootViews(4), confirmers)

      val responses = createResponses(
        transactionValidationResult(
          Map(leftViewPosition -> validationResult(view)),
          modelConformanceResultE = Left(modelConformanceMismatch(view, externalCallResult)),
        )
      )

      inside(responses.loneElement) {
        case ConfirmationResponse(_, reject: LocalReject, confirmingParties) =>
          reject.isMalformed shouldBe true
          confirmingParties shouldBe Set.empty
      }
    }
  }
}
