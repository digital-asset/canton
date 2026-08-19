// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.validation

import com.digitalasset.canton.version.ProtocolVersionValidation
import io.scalaland.chimney.{PartialTransformer, partial}

/** An unvalidated value read off a protobuf message. Reachable only via `ProtoValidation`
  * `validate`, which runs the matching `ProtoValidator[A]`. `extends Any` (a universal trait) so
  * `AnyVal` value classes such as [[ProtoUnvalidatedString]] can mix it in.
  *
  * `Serializable` for the boxes that generic positions such as a repeated `string` field's `Seq`
  * allocate, so record/replay tests can Java-serialize a message
  * ([[com.digitalasset.canton.util.MessageRecorder]]).
  */
trait ProtoUnvalidated[A] extends Any with Serializable {
  private[validation] def unvalidated: A
}

object ProtoUnvalidated {

  /** `toProto`-side write syntax: `import ProtoUnvalidated.syntax.*`. */
  object syntax {
    implicit class ProtoUnvalidatedStringSyntax(private val str: String) extends AnyVal {

      /** Explicit lift for the write sites the `ProtoUnvalidatedString.fromString` implicit can't
        * reach: collection elements (`xs.map(_.toProtoUnvalidated)`) and scalars that would
        * otherwise infer `Any` (`x.getOrElse("").toProtoUnvalidated`). Plain scalar writes use the
        * implicit.
        */
      def toProtoUnvalidated: ProtoUnvalidatedString = ProtoUnvalidatedString(str)
    }
  }

  /** Chimney support for `transformIntoPartial` between a proto whose `string` fields are
    * [[ProtoUnvalidatedString]] and one whose are plain `String` (another module's admin proto, a
    * ledger-API proto). Import it at those boundaries.
    *
    * Safe on untrusted values: it validates instead of unwrapping, reporting invalid content as a
    * partial failure.
    */
  object chimney {
    // TODO(#34846) Migrate ledger-api-core to the transformation
    // TODO(#34848) Migrate admin-api to the transformation; the transformer goes once both land
    implicit val protoUnvalidatedStringToString
        : PartialTransformer[ProtoUnvalidatedString, String] =
      PartialTransformer[ProtoUnvalidatedString, String] { value =>
        partial.Result.fromEitherString(
          ProtoValidation
            .validate(value, None, ProtocolVersionValidation.AlwaysValidation)
            .left
            .map(_.message)
        )
      }

    implicit class PartialResultToEitherStringOps[A](private val result: partial.Result[A])
        extends AnyVal {

      /** Flatten a partial-transformation result to `Either[String, A]` (joined error messages),
        * for call sites that already thread a `Left(String)`.
        */
      def toEitherString: Either[String, A] =
        result.asEitherErrorPathMessageStrings.left.map(
          _.map { case (path, message) =>
            if (path.isEmpty) message else s"$path: $message"
          }.mkString(", ")
        )
    }
  }
}
