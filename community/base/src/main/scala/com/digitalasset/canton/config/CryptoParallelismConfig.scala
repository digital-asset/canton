// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

import com.digitalasset.canton.config.RequireTypes.{PositiveInt, PositiveNumeric}

/** Configuration for crypto-related parallelism settings.
  *
  * @param signatureVerificationParallelism
  *   Maximum number of signature verification tasks to run in parallel per signed payload (when
  *   multiple signatures are provided).
  */
final case class CryptoParallelismConfig(
    signatureVerificationParallelism: PositiveInt =
      CryptoParallelismConfig.defaultSignatureVerificationParallelism
)

object CryptoParallelismConfig {
  val defaultSignatureVerificationParallelism: PositiveInt = PositiveNumeric.tryCreate(10)
}
