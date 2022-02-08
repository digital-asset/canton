// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import com.daml.lf.data.Bytes
import com.digitalasset.canton.crypto.Hash

/** A hash-based identifier for contracts.
  * Must be paired with a discriminator to obtain a complete contract ID.
  */
case class Unicum(unwrap: Hash) extends AnyVal {
  def toContractIdSuffix: Bytes =
    ContractId.suffixPrefix ++ Bytes.fromByteString(unwrap.getCryptographicEvidence)
}
