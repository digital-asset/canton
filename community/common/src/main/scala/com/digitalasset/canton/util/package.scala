// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton

import com.digitalasset.canton.logging.ErrorLoggingContext
import com.digitalasset.canton.tracing.TraceContext

package object util {
  type TracedLazyVal[T] = LazyValWithContext[T, TraceContext]
  type ErrorLoggingLazyVal[T] = LazyValWithContext[T, ErrorLoggingContext]
}
