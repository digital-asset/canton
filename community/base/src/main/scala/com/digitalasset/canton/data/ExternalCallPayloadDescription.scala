// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import com.digitalasset.daml.lf.data.Bytes

/** Size-only descriptions for external-call payload diagnostics.
  *
  * External-call payloads may contain application data, so diagnostics must describe payload sizes
  * without rendering the payload bytes themselves.
  */
private[canton] object ExternalCallPayloadDescription {

  def byteCount(bytes: Bytes): Int = bytes.toByteString.size()

  def byteSize(bytes: Bytes): String =
    s"${byteCount(bytes)} bytes"

  def hexPayloadSize(hex: String): String =
    Bytes
      .fromString(hex)
      .fold(_ => s"${hex.length} input characters", byteSize)
}
