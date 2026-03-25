// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.store.backend.postgresql

import com.digitalasset.canton.platform.store.backend.common.Field
import com.digitalasset.canton.platform.store.interning.StringInterning

private[postgresql] trait PGStringArrayBase[From, To] extends Field[From, To, String] {
  override def selectFieldExpression(inputFieldName: String): String =
    s"string_to_array($inputFieldName, '|')"

  protected def convertBase: Iterable[String] => String = { in =>
    assert(
      in.forall(!_.contains("|")),
      s"The following input string(s) contain the character '|', which is not expected: ${in.filter(_.contains("|")).mkString(", ")}",
    )
    in.mkString("|")
  }
}

private[postgresql] final case class PGStringArray[From](
    extract: StringInterning => From => Iterable[String]
) extends PGStringArrayBase[From, Iterable[String]] {
  override def convert: Iterable[String] => String = convertBase
}

private[postgresql] final case class PGSmallint[From](
    extract: StringInterning => From => Int
) extends Field[From, Int, java.lang.Integer] {
  override def selectFieldExpression(inputFieldName: String): String =
    s"$inputFieldName::smallint"

  override def convert: Int => Integer = Int.box
}

private[postgresql] final case class PGSmallintOptional[From](
    extract: StringInterning => From => Option[Int]
) extends Field[From, Option[Int], java.lang.Integer] {
  override def selectFieldExpression(inputFieldName: String): String =
    s"$inputFieldName::smallint"

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  override def convert: Option[Int] => Integer = _.map(Int.box).orNull
}
