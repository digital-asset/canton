// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.util

import com.digitalasset.canton.serialization.DeserializationError
import com.google.protobuf.ByteString

class ByteArrayUtilTest extends GzipCompressionTests {
  override def compressGzip(str: ByteString): ByteString =
    ByteString.copyFrom(ByteArrayUtil.compressGzip(str))

  override def decompressGzip(str: ByteString): Either[DeserializationError, ByteString] =
    ByteArrayUtil.decompressGzip(str.toByteArray).map(ByteString.copyFrom)
}
