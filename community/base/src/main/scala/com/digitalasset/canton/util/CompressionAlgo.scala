// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import com.digitalasset.canton.version.ProtocolVersion

/** Internal compression choice, selected using protocol\-version rules */
sealed trait CompressionAlgo extends Product with Serializable

object CompressionAlgo {
  case object Gzip extends CompressionAlgo
  case object Zstd extends CompressionAlgo

  def apply(protocolVersion: ProtocolVersion): CompressionAlgo =
    if (protocolVersion < ProtocolVersion.v36) Gzip else Zstd
}
