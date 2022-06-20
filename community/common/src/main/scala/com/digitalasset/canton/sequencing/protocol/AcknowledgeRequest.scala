// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.protocol

import cats.syntax.option._
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.api.v0
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.version.HasProtoV0

case class AcknowledgeRequest(member: Member, timestamp: CantonTimestamp)
    extends HasProtoV0[v0.AcknowledgeRequest] {
  override def toProtoV0: v0.AcknowledgeRequest =
    v0.AcknowledgeRequest(member.toProtoPrimitive, timestamp.toProtoPrimitive.some)

}

object AcknowledgeRequest {
  def fromProtoV0(
      reqP: v0.AcknowledgeRequest
  ): ParsingResult[AcknowledgeRequest] =
    for {
      member <- Member.fromProtoPrimitive(reqP.member, "member")
      timestamp <- ProtoConverter.parseRequired(
        CantonTimestamp.fromProtoPrimitive,
        "timestamp",
        reqP.timestamp,
      )
    } yield AcknowledgeRequest(member, timestamp)
}
