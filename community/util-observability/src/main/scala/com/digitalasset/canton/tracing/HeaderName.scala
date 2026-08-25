// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.tracing

import java.util.Locale

/** HTTP header name with case-insensitive equality (RFC 7230 §3.2). The apply factory normalizes to
  * lowercase, so equality and hashing on the underlying String are case-insensitive by
  * construction. Use as the key type for any map of HTTP headers to avoid lookup misses caused by
  * upstream case variation.
  */
final class HeaderName private (val value: String) extends AnyVal {
  override def toString: String = value
}

object HeaderName {
  def apply(name: String): HeaderName =
    new HeaderName(name.toLowerCase(Locale.ROOT))
}
