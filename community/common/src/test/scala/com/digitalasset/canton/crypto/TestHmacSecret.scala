// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import cats.syntax.either._
import com.google.protobuf.ByteString

object TestHmacSecret {

  def generate(): HmacSecret = {
    val pseudoSecret = PseudoRandom.randomAlphaNumericString(HmacSecret.defaultLength)
    HmacSecret
      .create(ByteString.copyFromUtf8(pseudoSecret))
      .valueOr(err => throw new IllegalStateException(err.toString))
  }

}
