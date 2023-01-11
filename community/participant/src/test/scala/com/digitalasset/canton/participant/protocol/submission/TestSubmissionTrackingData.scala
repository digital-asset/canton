// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.protocol.submission

import com.digitalasset.canton.{BaseTest, DefaultDamlValues}

object TestSubmissionTrackingData {

  lazy val default: SubmissionTrackingData =
    TransactionSubmissionTrackingData(
      DefaultDamlValues.completionInfo(List.empty),
      TransactionSubmissionTrackingData.TimeoutCause,
      BaseTest.testedProtocolVersion,
    )
}
