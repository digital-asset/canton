// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.validation

import cats.syntax.either.*
import com.digitalasset.base.validation.StringValidator
import com.digitalasset.canton.logging.pretty.{
  Pretty,
  PrettyPrintingCompanion,
  PrettyPrintingFromCompanion,
}
import com.digitalasset.canton.version.ProtocolVersionValidation
import scalapb.TypeMapper

import scala.language.implicitConversions

/** The type proto `string` fields map to (via the scalapb `field_transformation`), so the raw value
  * is reachable only through `ProtoValidation` `validate`. `AnyVal`, so no wrapper allocation.
  */
final class ProtoUnvalidatedString(private val str: String)
    extends AnyVal
    with ProtoUnvalidated[String]
    with PrettyPrintingFromCompanion {

  override private[validation] def unvalidated: String = str

  override def prettyCompanion: PrettyPrintingCompanion[ProtoUnvalidatedString] =
    ProtoUnvalidatedString
}

object ProtoUnvalidatedString extends PrettyPrintingCompanion[ProtoUnvalidatedString] {
  def apply(str: String): ProtoUnvalidatedString = new ProtoUnvalidatedString(str)

  implicit val typeMapper: TypeMapper[String, ProtoUnvalidatedString] =
    TypeMapper(new ProtoUnvalidatedString(_))(_.str)

  /** Writing a trusted string out is safe, so `toProto` builders may pass a plain `String`. */
  implicit def fromString(str: String): ProtoUnvalidatedString = apply(str)

  /** Prints the content only if it passes the check a `fromProto` runs, so an untrusted value
    * cannot inject control characters into a log line. Names the violation, never the content.
    */
  override protected val pretty: Pretty[ProtoUnvalidatedString] = prettyOfString { inst =>
    ProtoValidation
      .validateNoField(inst, ProtocolVersionValidation.AlwaysValidation)
      // The content check accepts tab, line feed and carriage return; a raw line feed would forge a log line, so escape all three.
      .map(StringValidator.escapeAcceptedControls)
      .valueOr(err => s"<invalid string: ${err.message}>")
  }
}
