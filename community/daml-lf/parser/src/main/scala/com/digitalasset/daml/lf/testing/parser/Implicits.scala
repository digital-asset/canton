// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.testing
package parser

import com.digitalasset.daml.lf.data.{Numeric, Ref}
import com.digitalasset.daml.lf.language.Ast.{Expr, Kind, Module, Package, Type}

object Implicits {

  implicit class SyntaxHelper(val sc: StringContext) extends AnyVal {

    def k(args: Any*): Kind = interpolate(KindParser.kind)(args)

    def i[P](args: Any*)(implicit parserParameters: ParserParameters[P]): Ref.Identifier =
      interpolate(new TypeParser[P](parserParameters).fullIdentifier)(args)

    def t[P](args: Any*)(implicit parserParameters: ParserParameters[P]): Type =
      interpolate(new TypeParser[P](parserParameters).typ)(args)

    def e[P](args: Any*)(implicit parserParameters: ParserParameters[P]): Expr =
      interpolate(new ExprParser[P](parserParameters).expr)(args)

    def m[P](args: Any*)(implicit parserParameters: ParserParameters[P]): Module =
      interpolate(new ModParser[P](parserParameters).mod)(args)

    def p[P](args: Any*)(implicit parserParameters: ParserParameters[P]): Package =
      interpolate(new ModParser[P](parserParameters).pkg)(args)

    @SuppressWarnings(Array("org.wartremover.warts.Any"))
    def n(args: Any*): Ref.Name =
      Ref.Name.assertFromString(
        StringContext.standardInterpolator(identity, args.map(prettyPrint), sc.parts)
      )

    @SuppressWarnings(Array("org.wartremover.warts.Any"))
    private def interpolate[T](p: Parsers.Parser[T])(args: Seq[Any]): T =
      Parsers.parseAll(
        Parsers.phrase(p),
        StringContext.standardInterpolator(identity, args.map(prettyPrint), sc.parts),
      )
  }

  implicit class BigDecimalOp(val x: BigDecimal) extends AnyVal {
    def fmt: String = Numeric.toUnscaledString(Numeric.assertFromUnscaledBigDecimal(x))
  }

  implicit class FullReferenceOp(val x: Ref.FullReference[?]) extends AnyVal {
    def fmt: String = s"'${x.pkg}':${x.qualifiedName}"
  }

  private def prettyPrint(x: Any): String =
    x match {
      case d: BigDecimal => d.fmt
      case d: Float => BigDecimal.valueOf(d.toDouble).fmt
      case d: Double => BigDecimal.valueOf(d).fmt
      case x: Ref.FullReference[?] => x.fmt
      case other: Any => other.toString
    }
}
