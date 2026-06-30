// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.version

import com.google.protobuf.ByteString

/** Trait for classes that can be serialized to a [[com.google.protobuf.ByteString]].
  *
  * Typically, these classes also implement the [[HasProtocolVersionedWrapper]] trait. Such classes
  * embed logic together with a representative protocol version that determines the ProtoBuf
  * serialization and deserialization. Hence, [[HasToByteStringF.toByteString]] does not take any
  * arguments. In contrast, [[HasVersionedToByteStringF]] is tailored towards another ProtoBuf
  * serialization/deserialization tooling.
  *
  * @tparam F
  *   Typically Id (for classes whose serialization always succeeds) or Either[String, ?] if
  *   serialization can fail (e.g., because the instance cannot be serialized to the specified
  *   protocol version).
  *
  * See "README.md" for our guidelines on the (de-)serialization tooling.
  */
trait HasToByteStringF[F[_]] {

  def toByteString: F[ByteString]

}
