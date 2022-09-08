// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.data

import cats.syntax.traverse._
import com.daml.ledger.api.v1.admin.metering_report_service.{
  ApplicationMeteringReport => ProtoApplicationMeteringReport,
  GetMeteringReportResponse,
  ParticipantMeteringReport,
}
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult

case class ApplicationMeteringReport(applicationId: String, eventCount: Long)

object ApplicationMeteringReport {
  def fromProtoV0(
      value: ProtoApplicationMeteringReport
  ): ParsingResult[ApplicationMeteringReport] = {
    val ProtoApplicationMeteringReport(applicationId, eventCount) = value
    Right(ApplicationMeteringReport(applicationId, eventCount))
  }
}

case class LedgerMeteringReport(
    from: CantonTimestamp,
    isFinal: Boolean,
    reports: Seq[ApplicationMeteringReport],
)

object LedgerMeteringReport {

  def fromProtoV0(
      value: GetMeteringReportResponse
  ): ParsingResult[LedgerMeteringReport] = {
    val GetMeteringReportResponse(
      requestO,
      participantReportO,
      reportGenerationTimeO,
      _meteringReportJson,
    ) = value

    for {
      request <- ProtoConverter.required("request", requestO)
      from <- ProtoConverter.parseRequired(
        CantonTimestamp.fromProtoPrimitive,
        "requestfrom",
        request.from,
      )
      participantReport <- ProtoConverter.required("participantReport", participantReportO)
      ParticipantMeteringReport(
        _,
        isFinal,
        reportsP,
      ) = participantReport
      reports <- reportsP.traverse(ApplicationMeteringReport.fromProtoV0)
    } yield LedgerMeteringReport(from, isFinal, reports)

  }
}
