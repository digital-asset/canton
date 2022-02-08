// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import cats.implicits._
import com.digitalasset.canton.serialization.ProtoConverter
import com.google.protobuf.ByteString
import scalapb.GeneratedMessageCompanion

object TreeSerialization {

  case class TransactionSerializationError(msg: String)

  /** Deserializes bytes representing a proto serialization of a proto node of ProtoType to a proto node */
  def deserializeProtoNode[ProtoType <: scalapb.GeneratedMessage](
      bytes: ByteString,
      protoBuilder: GeneratedMessageCompanion[ProtoType],
  ): Either[String, ProtoType] =
    ProtoConverter.protoParser(protoBuilder.parseFrom)(bytes).leftMap(_.error.getMessage)
}
