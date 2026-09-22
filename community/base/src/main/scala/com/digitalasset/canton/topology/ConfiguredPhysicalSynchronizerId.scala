// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.topology

import com.digitalasset.canton.logging.pretty.{
  Pretty,
  PrettyPrintingCompanion,
  PrettyPrintingFromCompanion,
}
import slick.jdbc.{GetResult, PositionedParameters, SetParameter}

/*
Before the first handshake with a synchronizer, the physical synchronizer id is unknown.
This trait allows for an explicit representation of `Option[PhysicalSynchronizerId]`.
 */
sealed trait ConfiguredPhysicalSynchronizerId extends PrettyPrintingFromCompanion {
  def toOption: Option[PhysicalSynchronizerId]
  def isDefined: Boolean = toOption.isDefined
}

final case class KnownPhysicalSynchronizerId(psid: PhysicalSynchronizerId)
    extends ConfiguredPhysicalSynchronizerId {
  override def prettyCompanion: PrettyPrintingCompanion[KnownPhysicalSynchronizerId] =
    KnownPhysicalSynchronizerId

  override def toOption: Option[PhysicalSynchronizerId] = Some(psid)
}

object KnownPhysicalSynchronizerId extends PrettyPrintingCompanion[KnownPhysicalSynchronizerId] {
  override protected val pretty: Pretty[KnownPhysicalSynchronizerId] =
    prettyOfString(psid => psid.psid.toLengthLimitedString.unwrap)
}

case object UnknownPhysicalSynchronizerId extends ConfiguredPhysicalSynchronizerId {
  override def prettyCompanion: PrettyPrintingCompanion[UnknownPhysicalSynchronizerId.type] =
    UnknownPhysicalSynchronizerIdPrettyPrintingCompanion

  override def toOption: Option[PhysicalSynchronizerId] = None
}

private object UnknownPhysicalSynchronizerIdPrettyPrintingCompanion
    extends PrettyPrintingCompanion[UnknownPhysicalSynchronizerId.type] {
  override protected val pretty: Pretty[UnknownPhysicalSynchronizerId.type] =
    prettyOfObject[UnknownPhysicalSynchronizerId.type]
}

object ConfiguredPhysicalSynchronizerId {
  def apply(psid: Option[PhysicalSynchronizerId]): ConfiguredPhysicalSynchronizerId =
    psid.fold[ConfiguredPhysicalSynchronizerId](UnknownPhysicalSynchronizerId)(
      KnownPhysicalSynchronizerId(_)
    )

  implicit val getResultConfiguredPhysicalSynchronizerId
      : GetResult[ConfiguredPhysicalSynchronizerId] =
    PhysicalSynchronizerId.getResultSynchronizerIdO.andThen(ConfiguredPhysicalSynchronizerId.apply)

  implicit val setParameterConfiguredPhysicalSynchronizerId
      : SetParameter[ConfiguredPhysicalSynchronizerId] =
    (psid: ConfiguredPhysicalSynchronizerId, pp: PositionedParameters) => pp >> psid.toOption

  implicit val configuredPsidOrdering: Ordering[ConfiguredPhysicalSynchronizerId] =
    Ordering.by(_.toOption)
}
