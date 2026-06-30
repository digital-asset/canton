// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.user

import com.digitalasset.daml.lf.data.Ref

sealed trait IdentityProviderId {
  def toRequestString: String

  def toDb: Option[IdentityProviderId.Id]
}

object IdentityProviderId {
  final case object Default extends IdentityProviderId {
    override def toRequestString: String = ""

    override def toDb: Option[Id] = None
  }

  final case class Id(value: Ref.LedgerString) extends IdentityProviderId {
    override def toRequestString: String = value

    override def toDb: Option[Id] = Some(this)
  }

  object Id {
    def fromString(id: String): Either[String, IdentityProviderId.Id] =
      Ref.LedgerString.fromString(id).map(Id.apply)

    def assertFromString(id: String): Id =
      Id(Ref.LedgerString.assertFromString(id))
  }

  def apply(identityProviderId: String): IdentityProviderId =
    Some(identityProviderId).filter(_.nonEmpty) match {
      case Some(id) => Id.assertFromString(id)
      case None => Default
    }

  def fromString(identityProviderId: String): Either[String, IdentityProviderId] =
    Some(identityProviderId).filter(_.nonEmpty) match {
      case Some(id) => Id.fromString(id)
      case None => Right(Default)
    }

  def fromDb(identityProviderId: Option[IdentityProviderId.Id]): IdentityProviderId =
    identityProviderId match {
      case None => IdentityProviderId.Default
      case Some(id) => id
    }

  def fromOptionalLedgerString(
      identityProviderId: Option[Ref.LedgerString]
  ): IdentityProviderId =
    identityProviderId match {
      case None => IdentityProviderId.Default
      case Some(id) => IdentityProviderId.Id(id)
    }
}
