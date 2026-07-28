// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.utils

import com.daml.tracing.SpanAttribute
import com.digitalasset.canton.data.Offset
import com.digitalasset.canton.platform.store.dao.events.OffsetRange
import io.opentelemetry.api.trace.{Span, Tracer}

object Telemetry {

  object Updates {
    def createSpan(tracer: Tracer, offsetRange: OffsetRange)(
        fullyQualifiedFunctionName: String
    ): Span =
      tracer
        .spanBuilder(fullyQualifiedFunctionName)
        .setNoParent()
        .setAttribute(SpanAttribute.OffsetFrom.key, offsetRange.startInclusive.toDecimalString)
        .setAttribute(SpanAttribute.OffsetTo.key, offsetRange.endInclusive.toDecimalString)
        .startSpan()

    def createSpan(tracer: Tracer, activeAt: Offset)(
        fullyQualifiedFunctionName: String
    ): Span =
      tracer
        .spanBuilder(fullyQualifiedFunctionName)
        .setNoParent()
        .setAttribute(SpanAttribute.Offset.key, activeAt.toDecimalString)
        .startSpan()

  }

}
