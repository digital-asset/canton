// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import cats.syntax.either._

object TestSalt {

  // Generates a deterministic salt for hashing based on the provided index
  // Assumes TestHash uses SHA-256
  def generate(index: Int): Salt =
    Salt
      .create(TestHash.digest(index).unwrap, SaltAlgorithm.Hmac(HmacAlgorithm.HmacSha256))
      .valueOr(err => throw new IllegalStateException(err.toString))

}
