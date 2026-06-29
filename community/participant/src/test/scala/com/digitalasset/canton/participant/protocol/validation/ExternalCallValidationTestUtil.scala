// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.validation

import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
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
import com.digitalasset.canton.participant.util.DAMLe
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.{BaseTest, LfPartyId}
import com.digitalasset.daml.lf.data.Bytes
import com.digitalasset.daml.lf.transaction.ExternalCallResult
import com.digitalasset.nonempty.NonEmpty

import scala.jdk.CollectionConverters.*

/** Shared fixtures for the external-call validation tests.
  *
  * Mixed into the test bases so that fixtures depending on `loggerFactory` (and other [[BaseTest]]
  * facilities) can live alongside the pure builders.
  */
private[validation] trait ExternalCallValidationTestUtil { self: BaseTest =>

  protected val requestId = RequestId(CantonTimestamp.Epoch)

  /** Provided by the mixing test, where an implicit `ExecutionContext` is available to build it. */
  protected def factory: ExampleTransactionFactory

  def externalCallViewResult(
      exerciseIndex: Int,
      result: ExternalCallResult,
      checkingParties: Set[LfPartyId],
      callIndex: Int = 0,
  ): ViewParticipantData.ViewExternalCallResult =
    ViewParticipantData.ViewExternalCallResult(
      result = result,
      exerciseIndex = NonNegativeInt.tryCreate(exerciseIndex),
      callIndex = NonNegativeInt.tryCreate(callIndex),
      checkingParties = checkingParties,
    )

  def withExternalCallResults(
      view: TransactionView,
      results: Seq[ViewParticipantData.ViewExternalCallResult],
  ): TransactionView =
    TransactionView.Optics.viewParticipantDataUnsafe
      .modify(vpd => vpd.tryUnwrap.copy(externalCallResults = results))(view)

  protected final class RecordingExternalCallValidator(
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

  protected val matchingExternalCallValidator: ExternalCallValidator = new ExternalCallValidator {
    override def validate(
        key: DAMLe.ExternalCallKey,
        recordedOutput: Bytes,
    )(implicit
        traceContext: com.digitalasset.canton.tracing.TraceContext
    ): FutureUnlessShutdown[ExternalCallValidator.Result] =
      FutureUnlessShutdown.pure(ExternalCallValidator.Matched)
  }

  protected val leftViewPosition = ViewPosition.root
  protected val rightViewPosition =
    ViewPosition(
      List(ViewPosition.MerkleSeqIndex(List(ViewPosition.MerkleSeqIndex.Direction.Right)))
    )
  protected val unrelatedViewPosition =
    ViewPosition(
      List(ViewPosition.MerkleSeqIndex(List(ViewPosition.MerkleSeqIndex.Direction.Left)))
    )
  protected val secondRightViewPosition =
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

  protected val externalCallResult = ExternalCallResult(
    extensionId = "extension",
    functionId = "function",
    config = Bytes.fromStringUtf8("config"),
    input = Bytes.fromStringUtf8("input"),
    output = Bytes.fromStringUtf8("output"),
  )

  protected val otherExternalCallOutput =
    externalCallResult.copy(output = Bytes.fromStringUtf8("other-output"))

  protected def withConfirmers(
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

  protected def validationResult(
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

  protected def defaultModelConformanceResult: ModelConformanceChecker.Result = {
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

  protected def modelConformanceRecordedDisagreement(
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

  protected def assertRecordedDisagreementAlarms[A](count: Int = 1)(within: => A): A =
    loggerFactory.assertLogs(
      within,
      Seq.fill(count) { (logEntry: LogEntry) =>
        logEntry.shouldBeCantonErrorCode(
          ExternalCallValidationError.ExternalCallResultDisagreementAlarm
        )
        logEntry.mdc should contain("requestId" -> requestId.toString)
      }*
    )
}
