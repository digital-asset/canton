// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton

import com.digitalasset.canton.ledger.api.refinements.ApiTypes as lar
import com.digitalasset.daml.lf
import com.digitalasset.nonempty.NonEmpty
import org.apache.pekko.http.scaladsl.model.StatusCode

import scala.concurrent.duration.Duration

package object http {

  type LfValue = lf.value.Value
  type Party = lar.Party
  final val Party = lar.Party
  type PartySet = NonEmpty[Set[Party]]

  type UserId = lar.UserId
  val UserId = lar.UserId

}

package http {

  final case class RetryInfoDetailDuration(unwrap: Duration) extends AnyVal {
    override def toString: String = unwrap.toString
  }

  sealed abstract class SyncResponse[+R] extends Product with Serializable {
    def status: StatusCode
  }

  // Important note: when changing this ADT, adapt the custom associated JsonFormat codec in JsonProtocol
  sealed trait ErrorDetail extends Product with Serializable
  final case class ResourceInfoDetail(name: String, typ: String) extends ErrorDetail
  final case class ErrorInfoDetail(errorCodeId: String, metadata: Map[String, String])
      extends ErrorDetail
  final case class RetryInfoDetail(duration: RetryInfoDetailDuration) extends ErrorDetail
  final case class RequestInfoDetail(correlationId: String) extends ErrorDetail

  object ErrorDetail {

    import com.digitalasset.base.error.utils.ErrorDetails
    def fromErrorUtils(errorDetail: ErrorDetails.ErrorDetail): http.ErrorDetail =
      errorDetail match {
        case ErrorDetails.ResourceInfoDetail(name, typ) => http.ResourceInfoDetail(name, typ)
        case ErrorDetails.ErrorInfoDetail(errorCodeId, metadata) =>
          http.ErrorInfoDetail(errorCodeId, metadata)
        case ErrorDetails.RetryInfoDetail(duration) =>
          http.RetryInfoDetail(RetryInfoDetailDuration(duration))
        case ErrorDetails.RequestInfoDetail(correlationId) =>
          http.RequestInfoDetail(correlationId)
      }
  }

  final case class LedgerApiError(
      code: Int,
      message: String,
      details: Seq[ErrorDetail],
  )

  final case class ErrorResponse(
      errors: List[String],
      status: StatusCode,
      ledgerApiError: Option[LedgerApiError] = None,
  ) extends SyncResponse[Nothing]
}
