// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.sequencer.errors

import com.digitalasset.canton.topology.Member

/** Possible error from registering a new member */
sealed trait RegisterMemberError

object RegisterMemberError {

  /** The given member is already registered with the sequencer at this time */
  case class AlreadyRegisteredError(member: Member) extends RegisterMemberError

  case class UnexpectedError(member: Member, message: String) extends RegisterMemberError
}
