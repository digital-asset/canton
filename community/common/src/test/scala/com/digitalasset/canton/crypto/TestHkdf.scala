// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

/** A testing hkdf that simply concatenates the given info to the given key material, ignoring the length argument.
  * Suitable for use with symbolic crypto only.
  */
class TestHkdf extends HkdfOps with HmacOps {
  override def hkdfExpand(
      keyMaterial: SecureRandomness,
      outputBytes: Int,
      info: HkdfInfo,
      algorithm: HmacAlgorithm,
  ): Either[HkdfError, SecureRandomness] = {
    Right(SecureRandomness(keyMaterial.unwrap.concat(info.bytes)))
  }
}
