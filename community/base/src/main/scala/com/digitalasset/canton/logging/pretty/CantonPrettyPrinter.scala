// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.logging.pretty

import com.digitalasset.canton.logging.pretty.Pretty.{
  DefaultEscapeUnicode,
  DefaultIndent,
  DefaultShowFieldNames,
  DefaultWidth,
}
import com.digitalasset.canton.util.ThrowableUtil
import com.digitalasset.canton.validation.{ProtoUnvalidatedSeq, ProtoUnvalidatedString}
import com.google.protobuf.ByteString
import pprint.{PPrinter, Tree}

/** Adhoc pretty printer to nicely print the full structure of a class that does not have an
  * explicit pretty definition
  */
class CantonPrettyPrinter(maxStringLength: Int, maxMessageLines: Int) {

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  def printAdHoc(message: Any): String =
    message match {
      case null => ""
      case product: Product =>
        try {
          pprinter(product).toString
        } catch {
          case err: IllegalArgumentException => ThrowableUtil.messageWithStacktrace(err)
        }
      case _: Any =>
        import com.digitalasset.canton.logging.pretty.Pretty.*
        message.toString.limit(maxStringLength).toString
    }

  private lazy val pprinter: PPrinter = PPrinter.BlackWhite.copy(
    defaultWidth = DefaultWidth,
    defaultHeight = maxMessageLines,
    defaultIndent = DefaultIndent,
    defaultEscapeUnicode = DefaultEscapeUnicode,
    defaultShowFieldNames = DefaultShowFieldNames,
    additionalHandlers = {
      case _: ByteString => Tree.Literal("ByteString")
      case s: String =>
        import com.digitalasset.canton.logging.pretty.Pretty.*
        s.limit(maxStringLength).toTree
      // The wrapper checks the content, the length limit is ours
      case s: ProtoUnvalidatedString =>
        import com.digitalasset.canton.logging.pretty.Pretty.*
        s.toString.limit(maxStringLength).toTree
      // The wrapper caps the count, elements are rendered here so the limits above reach them
      case seq: ProtoUnvalidatedSeq[?] => ProtoUnvalidatedSeq.prettyTree(seq)(treeify)
      case Some(p) => treeify(p)
      case Seq(single) => treeify(single)
    },
  )

  private def treeify(value: Any): Tree =
    pprinter.treeify(
      value,
      escapeUnicode = DefaultEscapeUnicode,
      showFieldNames = DefaultShowFieldNames,
    )

}
