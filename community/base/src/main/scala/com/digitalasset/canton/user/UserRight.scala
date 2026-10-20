// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.user

import com.digitalasset.daml.lf.data.Ref

sealed abstract class UserRight extends Product with Serializable {
  def getParty: Option[Ref.Party] = None
}

sealed abstract class UserRightForParty(party: Ref.Party) extends UserRight {
  override def getParty: Option[Ref.Party] = Some(party)
}

object UserRight {
  final case object ParticipantAdmin extends UserRight

  final case object IdentityProviderAdmin extends UserRight

  final case class CanActAs(party: Ref.Party) extends UserRightForParty(party)

  final case class CanReadAs(party: Ref.Party) extends UserRightForParty(party)

  final case object CanReadAsAnyParty extends UserRight

  final case class CanExecuteAs(party: Ref.Party) extends UserRightForParty(party)

  final case object CanExecuteAsAnyParty extends UserRight

  final case object CanActAsAnyParty extends UserRight
}
