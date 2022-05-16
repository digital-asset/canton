// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import com.digitalasset.canton.version.ProtocolVersion

/** Helper methods to select the appropriate crypto primitive for a particular protocol version. */
object ProtocolCryptoApi {

  def hkdf(hkdfOps: HkdfOps, protocolVersion: ProtocolVersion)(
      keyMaterial: SecureRandomness,
      outputBytes: Int,
      info: HkdfInfo,
  ): Either[HkdfError, SecureRandomness] =
    protocolVersion match {
      case ProtocolVersion.unstable_development =>
        hkdfOps.computeHkdf(keyMaterial.unwrap, outputBytes, info)
      case _ =>
        hkdfOps.hkdfExpand(keyMaterial, outputBytes, info)
    }

}
