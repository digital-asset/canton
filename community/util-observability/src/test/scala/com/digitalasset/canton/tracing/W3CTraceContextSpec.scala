// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.tracing

import org.scalatest.OptionValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks

class W3CTraceContextSpec
    extends AnyFlatSpec
    with Matchers
    with OptionValues
    with TableDrivenPropertyChecks {

  behavior of "W3CTraceContext.fromHeaders"
  it should "extract trace context regardless of HTTP header name casing" in {
    val traceparent = "00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01"
    val tracestate = "vendor1=value1"

    val baseTp = "traceparent"
    val baseTs = "tracestate"
    val base = W3CTraceContext.fromHeaders(
      Map(
        HeaderName(baseTp) -> traceparent,
        HeaderName(baseTs) -> tracestate,
      )
    )

    forEvery(
      Table(
        ("tp", "ts"),
        ("traceparent", "tracestate"),
        ("Traceparent", "TraceState"),
        ("TRACEPARENT", "TRACESTATE"),
      )
    ) { (tpHeader, tsHeader) =>
      val context = W3CTraceContext.fromHeaders(
        Map(
          HeaderName(tpHeader) -> traceparent,
          HeaderName(tsHeader) -> tracestate,
        )
      )

      context.value.parent shouldBe traceparent
      context.value.state shouldBe Some(tracestate)
      context shouldBe base
    }
  }
}
