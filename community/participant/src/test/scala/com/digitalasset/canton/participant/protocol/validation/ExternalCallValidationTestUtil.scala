// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.validation

import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.data.{
  ParticipantTransactionView,
  TransactionView,
  ViewParticipantData,
}
import com.digitalasset.canton.protocol.*
import com.digitalasset.daml.lf.data.Bytes
import com.digitalasset.daml.lf.transaction.ExternalCallResult

/** Shared fixtures for the external-call validation tests. */
private[validation] trait ExternalCallValidationTestUtil {

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

  protected val externalCallResult: ExternalCallResult = ExternalCallResult(
    extensionId = "extension",
    functionId = "function",
    config = Bytes.fromStringUtf8("config"),
    input = Bytes.fromStringUtf8("input"),
    output = Bytes.fromStringUtf8("output"),
  )

  protected val otherExternalCallResult: ExternalCallResult =
    externalCallResult.copy(output = Bytes.fromStringUtf8("other-output"))

  protected def participantView(view: TransactionView): ParticipantTransactionView =
    ParticipantTransactionView.tryCreate(view)

  protected def validationResult(
      view: TransactionView,
      activenessResult: ViewActivenessResult = ViewActivenessResult(
        inactiveContracts = Set.empty,
        alreadyLockedContracts = Set.empty,
        existingContracts = Set.empty,
      ),
  ): ViewValidationResult =
    ViewValidationResult(
      participantView(view),
      activenessResult,
    )
}
