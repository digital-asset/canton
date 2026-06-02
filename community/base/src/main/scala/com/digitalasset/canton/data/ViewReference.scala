// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.data

import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.crypto.Hash
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.ViewHash
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.google.protobuf.ByteString

/** A reference identifying a view.
  *
  * A view can be addressed either by its view hash or by a ciphertext-based identifier (PV36+)
  * (ciphertext hash plus index of the view in the list that is encrypted).
  */
sealed trait ViewReference extends PrettyPrinting with Product with Serializable

/** A transaction view identifier composed of the hash of the transaction view. This reference type
  * is used for PV35-.
  *
  * @param viewHash
  *   Hash of the transaction view.
  */
final case class ByViewHash(private val viewHash: ViewHash) extends ViewReference {
  def toProtoPrimitive: ByteString = viewHash.toProtoPrimitive

  override def pretty: Pretty[ByViewHash] = prettyOfClass(param("viewHash", _.viewHash))
}

object ByViewHash {
  def fromProtoPrimitive(viewHashBytes: ByteString): ParsingResult[ByViewHash] =
    ViewHash.fromProtoPrimitive(viewHashBytes).map(ByViewHash(_))
}

/** A transaction view identifier composed of a ciphertext hash and a subview index relative to the
  * encrypted list. This reference type is used for PV36+.
  *
  * @param ciphertextId
  *   Hash of the ciphertext containing the subview.
  * @param index
  *   Position of the subview within the ciphertext. The index differentiates multiple subviews
  *   sharing the same ciphertext hash.
  */
final case class ByCiphertextId(private val ciphertextId: Hash, index: PositiveInt)
    extends ViewReference {

  override def pretty: Pretty[ByCiphertextId] = prettyOfClass(
    param("ciphertextId", _.ciphertextId),
    param("index", _.index),
  )
}
