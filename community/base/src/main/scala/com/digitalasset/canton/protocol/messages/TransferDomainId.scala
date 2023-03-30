// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import com.digitalasset.canton.data.ViewType
import com.digitalasset.canton.data.ViewType.{TransferInViewType, TransferOutViewType}
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.protocol.messages.TransferDomainId.TransferDomainIdCast
import com.digitalasset.canton.topology.DomainId

sealed trait TransferDomainId extends Product with Serializable with PrettyPrinting {
  protected def domain: DomainId

  def isTransferOut: Boolean
  def isTransferIn: Boolean

  def unwrap: DomainId = domain

  def toViewType: ViewType

  override def pretty: Pretty[TransferDomainId] = prettyOfParam(_.unwrap)
}

object TransferDomainId {

  trait TransferDomainIdCast[Kind <: TransferDomainId] {
    def toKind(domain: TransferDomainId): Option[Kind]
  }

  implicit val transferDomainIdCast: TransferDomainIdCast[TransferDomainId] =
    (domain: TransferDomainId) => Some(domain)

}

final case class TransferOutDomainId(override protected val domain: DomainId)
    extends TransferDomainId {
  override def isTransferOut: Boolean = true
  override def isTransferIn: Boolean = false
  override def toViewType: TransferOutViewType = TransferOutViewType
}

object TransferOutDomainId {
  implicit val transferOutDomainIdCast: TransferDomainIdCast[TransferOutDomainId] = {
    case x: TransferOutDomainId => Some(x)
    case _ => None
  }
}

final case class TransferInDomainId(override protected val domain: DomainId)
    extends TransferDomainId {
  override def isTransferOut: Boolean = false
  override def isTransferIn: Boolean = true
  override def toViewType: TransferInViewType = TransferInViewType
}

object TransferInDomainId {
  implicit val transferInDomainIdCast: TransferDomainIdCast[TransferInDomainId] = {
    case x: TransferInDomainId => Some(x)
    case _ => None
  }
}
