// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver

import com.digitalasset.canton.crypto.RandomOps
import com.digitalasset.daml.lf.crypto

object SubmissionSeed {

  /** Generate a seed for a submission */
  def generate(randomOps: RandomOps): crypto.Hash =
    crypto.Hash.assertFromByteArray(
      randomOps.generateRandomByteString(crypto.Hash.underlyingHashLength).toByteArray
    )
}
