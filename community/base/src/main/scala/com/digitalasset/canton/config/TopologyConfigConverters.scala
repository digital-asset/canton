// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

import cats.syntax.either.*
import com.digitalasset.canton.topology.PartyId
import pureconfig.error.CannotConvert
import pureconfig.{ConfigReader, ConfigWriter}

/** Pureconfig readers and writers for topology identifiers, kept out of the identifiers' companion
  * objects so that the topology types do not depend on the config format.
  */
trait TopologyConfigConverters {

  implicit val partyIdConfigReader: ConfigReader[PartyId] =
    ConfigReader[String].emap { str =>
      PartyId
        .fromProtoPrimitive(str, "party-id")
        .leftMap(err => CannotConvert(str, "PartyId", err.message))
    }

  implicit val partyIdConfigWriter: ConfigWriter[PartyId] =
    ConfigWriter[String].contramap(_.toProtoPrimitive)
}

object TopologyConfigConverters extends TopologyConfigConverters
