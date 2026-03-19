// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf.testing.parser

import com.digitalasset.daml.lf.language.Ast.*
import com.digitalasset.daml.lf.testing.parser.Parsers.*
import com.digitalasset.daml.lf.testing.parser.Token.*

private[daml] object KindParser {

  lazy val kind0: Parser[Kind] =
    `*` ^^ (_ => KStar) |
      Id("nat") ^^ (_ => KNat) |
      (`(` ~> kind <~ `)`)

  lazy val kind: Parser[Kind] = rep1sep(kind0, `->`) ^^ (_.reduceRight(KArrow.apply))

}
