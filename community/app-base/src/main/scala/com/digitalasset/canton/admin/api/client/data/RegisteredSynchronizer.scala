// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.data

import cats.syntax.either.*
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.ProtoDeserializationError.UnrecognizedEnum
import com.digitalasset.canton.admin.api.client.data.RegisteredSynchronizer.Status
import com.digitalasset.canton.admin.participant.v30.ListRegisteredSynchronizersResponse.Status as StatusV30
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.PhysicalSynchronizerId

/** Information about a registered synchronizer
  * @param config
  *   Configuration to connect to the synchronizer
  * @param status
  *   Status of the connection (e.g., Active, Inactive, LSU)
  * @param psid
  *   If known, the physical synchronizer id of the synchronizer. Always known after the first
  *   connection.
  * @param predecessor
  *   If set, the LSU predecessor.
  * @param isConnected
  *   Whether the node is currently connected to the synchronizer
  */
final case class RegisteredSynchronizer(
    config: SynchronizerConnectionConfig,
    status: Status,
    psid: ConfiguredPhysicalSynchronizerId,
    predecessor: Option[SynchronizerPredecessor],
    isConnected: Boolean,
)

sealed trait ConfiguredPhysicalSynchronizerId extends PrettyPrinting {
  def toOption: Option[PhysicalSynchronizerId]
  def isDefined: Boolean = toOption.isDefined
}

object ConfiguredPhysicalSynchronizerId {
  def apply(psid: Option[PhysicalSynchronizerId]): ConfiguredPhysicalSynchronizerId =
    psid.fold[ConfiguredPhysicalSynchronizerId](UnknownPhysicalSynchronizerId)(
      KnownPhysicalSynchronizerId(_)
    )
}

final case class KnownPhysicalSynchronizerId(psid: PhysicalSynchronizerId)
    extends ConfiguredPhysicalSynchronizerId {
  override protected def pretty: Pretty[KnownPhysicalSynchronizerId] =
    prettyOfString(psid => psid.psid.toLengthLimitedString.unwrap)

  override def toOption: Option[PhysicalSynchronizerId] = Some(psid)
}
case object UnknownPhysicalSynchronizerId extends ConfiguredPhysicalSynchronizerId {
  override protected def pretty: Pretty[UnknownPhysicalSynchronizerId.type] =
    prettyOfString(_ => "UnknownPhysicalSynchronizerId")

  override def toOption: Option[PhysicalSynchronizerId] = None
}

object RegisteredSynchronizer {
  sealed trait Status extends Serializable with Product with PrettyPrinting

  object Status {
    def fromProtoV30(proto: StatusV30): ParsingResult[Status] =
      proto match {
        case StatusV30.STATUS_UNSPECIFIED => ProtoDeserializationError.FieldNotSet("status").asLeft
        case StatusV30.STATUS_ACTIVE => Active.asRight
        case StatusV30.STATUS_HARD_MIGRATING_SOURCE => HardMigratingSource.asRight
        case StatusV30.STATUS_HARD_MIGRATING_TARGET => HardMigratingTarget.asRight
        case StatusV30.STATUS_LSU_SOURCE => LsuSource.asRight
        case StatusV30.STATUS_LSU_TARGET => LsuTarget.asRight
        case StatusV30.STATUS_INACTIVE => Inactive.asRight
        case StatusV30.Unrecognized(unrecognizedValue) =>
          UnrecognizedEnum("status", unrecognizedValue).asLeft
      }

    case object Active extends Status {
      override protected def pretty: Pretty[Active.type] = prettyOfString(_ => "Active")
    }

    case object HardMigratingSource extends Status {
      override protected def pretty: Pretty[HardMigratingSource.type] =
        prettyOfString(_ => "HardMigratingSource")
    }
    case object HardMigratingTarget extends Status {
      override protected def pretty: Pretty[HardMigratingTarget.type] =
        prettyOfString(_ => "HardMigratingTarget")
    }

    case object LsuSource extends Status {
      override protected def pretty: Pretty[LsuSource.type] =
        prettyOfString(_ => "LSU source")
    }

    case object LsuTarget extends Status {
      override protected def pretty: Pretty[LsuTarget.type] =
        prettyOfString(_ => "LSU target")
    }

    case object Inactive extends Status {
      override protected def pretty: Pretty[Inactive.type] = prettyOfString(_ => "Inactive")
    }
  }

}
