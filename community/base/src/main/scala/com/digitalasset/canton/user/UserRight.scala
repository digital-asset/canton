// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.user

import com.digitalasset.daml.lf.data.Ref

sealed trait UserRight extends Product with Serializable

sealed trait UserRightForParty extends UserRight {
  def party: Ref.Party
}

object UserRight {
  final case object ParticipantAdmin extends UserRight

  final case object IdentityProviderAdmin extends UserRight

  final case class CanActAs(party: Ref.Party) extends UserRightForParty

  final case class CanReadAs(party: Ref.Party) extends UserRightForParty

  final case object CanReadAsAnyParty extends UserRight

  final case class CanExecuteAs(party: Ref.Party) extends UserRightForParty

  final case object CanExecuteAsAnyParty extends UserRight

  final case object CanActAsAnyParty extends UserRight
}
