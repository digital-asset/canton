// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.util

import cats.syntax.apply.*
import com.digitalasset.canton.RequestCounter
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import slick.jdbc.GetResult

/** The time when a request has made a change of state.
  *
  * @param rc
  *   The request counter on the request that triggered the change
  * @param timestamp
  *   The timestamp when this change takes place.
  */
final case class TimeOfRequest(rc: RequestCounter, timestamp: CantonTimestamp)
    extends PrettyPrinting {

  override protected def pretty: Pretty[TimeOfRequest] = prettyOfClass(
    param("request counter", _.rc),
    param("timestamp", _.timestamp),
  )
}

object TimeOfRequest {
  implicit val orderingTimeOfRequest: Ordering[TimeOfRequest] =
    Ordering.by[TimeOfRequest, (CantonTimestamp, RequestCounter)](toc => (toc.timestamp, toc.rc))

  implicit val getResultTimeOfRequest: GetResult[TimeOfRequest] = GetResult { r =>
    val ts = r.<<[CantonTimestamp]
    val rc = r.<<[RequestCounter]
    TimeOfRequest(rc, ts)
  }

  implicit val getResultOptionTimeOfRequest: GetResult[Option[TimeOfRequest]] = GetResult(r =>
    (
      GetResult[Option[RequestCounter]].apply(r),
      GetResult[Option[CantonTimestamp]].apply(r),
    ).mapN(TimeOfRequest.apply)
  )
}
