// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.services

import com.digitalasset.daml.lf.data.Ref.Party

final case class RequestWithReadAsParty[R](request: R, readAsParty: Option[Seq[Party]])

object RequestWithReadAsParty {
  def readAsPartyLens[R]: scalapb.lenses.Lens[RequestWithReadAsParty[R], Set[String]] =
    scalapb.lenses.Lens[RequestWithReadAsParty[R], Set[String]](
      _.readAsParty.map(_.map(_.toString).toSet).getOrElse(Set.empty)
    )((req, parties) =>
      if (parties.isEmpty) req.copy(readAsParty = None)
      else req.copy(readAsParty = Some(parties.map(Party.assertFromString).toSeq))
    )
}
