// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.validation

import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.data.{TransactionView, ViewParticipantData}
import com.digitalasset.daml.lf.transaction.ExternalCallResult

private[validation] object ExternalCallValidationTestUtil {

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
}
