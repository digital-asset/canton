// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.validation

import scalapb.TypeMapper

import scala.language.implicitConversions

/** The type proto `string` fields map to (via the scalapb `field_transformation`), so the raw value
  * is reachable only through `ProtoValidation` `validate`. `AnyVal`, so no wrapper allocation.
  */
final class ProtoUnvalidatedString(private val str: String)
    extends AnyVal
    with ProtoUnvalidated[String] {

  override private[validation] def unvalidated: String = str
}

object ProtoUnvalidatedString {
  def apply(str: String): ProtoUnvalidatedString = new ProtoUnvalidatedString(str)

  implicit val typeMapper: TypeMapper[String, ProtoUnvalidatedString] =
    TypeMapper(new ProtoUnvalidatedString(_))(_.str)

  /** Writing a trusted string out is safe, so `toProto` builders may pass a plain `String`. */
  implicit def fromString(str: String): ProtoUnvalidatedString = apply(str)
}
