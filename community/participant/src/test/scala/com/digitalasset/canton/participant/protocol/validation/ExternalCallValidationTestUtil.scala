// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.validation

import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.data.{
  CantonTimestamp,
  ParticipantTransactionView,
  TransactionView,
  ViewConfirmationParameters,
  ViewParticipantData,
  ViewPosition,
}
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.LogEntry
import com.digitalasset.canton.participant.util.DAMLe
import com.digitalasset.canton.protocol.*
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{BaseTest, LfPartyId}
import com.digitalasset.daml.lf.data.Bytes
import com.digitalasset.daml.lf.transaction.ExternalCallResult

import java.util.concurrent.ConcurrentLinkedQueue
import scala.jdk.CollectionConverters.*

/** Shared fixtures for the external-call validation tests.
  *
  * Mixed into the test bases so that fixtures depending on `loggerFactory` (and other [[BaseTest]]
  * facilities) can live alongside the pure builders.
  */
private[validation] trait ExternalCallValidationTestUtil { self: BaseTest =>

  protected val requestId: RequestId = RequestId(CantonTimestamp.Epoch)

  /** Provided by the mixing test, where an implicit `ExecutionContext` is available to build it. */
  protected def factory: ExampleTransactionFactory

  protected def externalCallViewResult(
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

  protected def withExternalCallResults(
      view: TransactionView,
      results: Seq[ViewParticipantData.ViewExternalCallResult],
  ): TransactionView =
    TransactionView.Optics.viewParticipantDataUnsafe
      .modify(vpd => vpd.tryUnwrap.copy(externalCallResults = results))(view)

  protected final class RecordingExternalCallValidator(
      results: Map[DAMLe.ExternalCallKey, ExternalCallValidator.Result]
  ) extends ExternalCallValidator {
    private val observedKeys: ConcurrentLinkedQueue[(DAMLe.ExternalCallKey, Bytes)] =
      new ConcurrentLinkedQueue[(DAMLe.ExternalCallKey, Bytes)]

    def observed: Seq[(DAMLe.ExternalCallKey, Bytes)] = observedKeys.asScala.toSeq

    override def validate(
        key: DAMLe.ExternalCallKey,
        recordedOutput: Bytes,
    )(implicit
        traceContext: TraceContext
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
        traceContext: TraceContext
    ): FutureUnlessShutdown[ExternalCallValidator.Result] =
      FutureUnlessShutdown.pure(ExternalCallValidator.Matched)
  }

  /** Distinct view positions with values matching their names; ordered `left < unrelated < right <
    * secondRight` under [[ViewPosition.orderViewPosition]].
    */
  protected val leftViewPosition: ViewPosition =
    ViewPosition(
      List(ViewPosition.MerkleSeqIndex(List(ViewPosition.MerkleSeqIndex.Direction.Left)))
    )
  protected val rightViewPosition: ViewPosition =
    ViewPosition(
      List(ViewPosition.MerkleSeqIndex(List(ViewPosition.MerkleSeqIndex.Direction.Right)))
    )
  protected val unrelatedViewPosition: ViewPosition =
    ViewPosition(
      List(
        ViewPosition.MerkleSeqIndex(
          List(
            ViewPosition.MerkleSeqIndex.Direction.Left,
            ViewPosition.MerkleSeqIndex.Direction.Left,
          )
        )
      )
    )
  protected val secondRightViewPosition: ViewPosition =
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

  protected val externalCallResult: ExternalCallResult = ExternalCallResult(
    extensionId = "extension",
    functionId = "function",
    config = Bytes.fromStringUtf8("config"),
    input = Bytes.fromStringUtf8("input"),
    output = Bytes.fromStringUtf8("output"),
  )

  protected val otherExternalCallResult: ExternalCallResult =
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
