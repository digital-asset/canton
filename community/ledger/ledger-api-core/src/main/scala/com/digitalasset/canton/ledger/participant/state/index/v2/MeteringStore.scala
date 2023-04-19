// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.participant.state.index.v2

import com.daml.lf.data.Ref
import com.daml.lf.data.Ref.ApplicationId
import com.daml.lf.data.Time.Timestamp
import com.daml.logging.LoggingContext
import com.digitalasset.canton.ledger.offset.Offset
import com.digitalasset.canton.ledger.participant.state.index.v2.MeteringStore.ReportData

import scala.concurrent.Future

trait MeteringStore {

  def getMeteringReportData(
      from: Timestamp,
      to: Option[Timestamp],
      applicationId: Option[Ref.ApplicationId],
  )(implicit loggingContext: LoggingContext): Future[ReportData]

}

object MeteringStore {

  final case class ReportData(applicationData: Map[ApplicationId, Long], isFinal: Boolean)

  final case class TransactionMetering(
      applicationId: Ref.ApplicationId,
      actionCount: Int,
      meteringTimestamp: Timestamp,
      ledgerOffset: Offset,
  )

  final case class ParticipantMetering(
      applicationId: Ref.ApplicationId,
      from: Timestamp,
      to: Timestamp,
      actionCount: Int,
      ledgerOffset: Offset,
  )

}