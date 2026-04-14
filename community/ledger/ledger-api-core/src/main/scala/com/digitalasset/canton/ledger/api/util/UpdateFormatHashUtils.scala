// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.api.util

import com.daml.ledger.api.v2.transaction_filter.EventFormat
import com.digitalasset.canton.crypto.{Hash, HashBuilder}
import com.google.protobuf.ByteString

object UpdateFormatHashUtils {
  @SuppressWarnings(Array("com.digitalasset.canton.ProtobufToByteString"))
  def hashEventFormat[T <: HashBuilder](
      builder: T
  )(eventFormat: EventFormat): T =
    // since EventFormat contains a Map, we need to be careful with calculating the hash
    builder
      .addMap(eventFormat.filtersByParty)((hashBuilder, key) => hashBuilder.addString(key))(
        (hashBuilder, value) => hashBuilder.addByteString(value.toByteString)
      )
      .addOptional(eventFormat.filtersForAnyParty.map(_.toByteString), _.addByteString)
      .addBool(eventFormat.verbose)

  def toChecksum(hash: Hash): ByteString = hash.unwrap.substring(0, 4)
}
