// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol.messages

import com.digitalasset.canton.data.ViewType
import com.digitalasset.canton.protocol.messages.SignedProtocolMessageContent.SignedMessageContentCast
import com.digitalasset.canton.serialization.MemoizedEvidence

trait MediatorResult extends MemoizedEvidence with HasDomainId with HasRequestId {
  def verdict: Verdict

  def viewType: ViewType
}

/** The mediator issues a regular mediator result for well-formed mediator requests.
  * Malformed mediator requests lead to a [[MalformedMediatorRequestResult]].
  */
trait RegularMediatorResult extends MediatorResult with SignedProtocolMessageContent

object RegularMediatorResult {
  implicit val regularMediatorResultMessageCast: SignedMessageContentCast[RegularMediatorResult] = {
    case m: RegularMediatorResult => Some(m)
    case _ => None
  }
}
