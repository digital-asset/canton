// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.console

import com.digitalasset.canton.admin.api.client.commands.{
  DomainAdminCommands,
  ParticipantAdminCommands,
}
import com.digitalasset.canton.admin.api.client.data.{
  CommunityCantonStatus,
  DomainNodeStatus,
  ParticipantStatus,
}
import com.digitalasset.canton.environment.CommunityEnvironment
import io.circe.Encoder
import io.circe.generic.semiauto.deriveEncoder

import scala.annotation.nowarn

@nowarn("cat=lint-byname-implicit") // https://github.com/scala/bug/issues/12072
class CommunityHealthDumpGenerator(
    override val environment: CommunityEnvironment,
    override val grpcAdminCommandRunner: GrpcAdminCommandRunner,
) extends HealthDumpGenerator[CommunityCantonStatus] {
  override protected implicit val statusEncoder: Encoder[CommunityCantonStatus] = {
    import io.circe.generic.auto.*
    import CantonHealthAdministrationEncoders.*
    deriveEncoder[CommunityCantonStatus]
  }

  override def status(): CommunityCantonStatus =
    CommunityCantonStatus.getStatus(
      statusMap(
        environment.config.domainsByString,
        DomainNodeStatus.fromProtoV0,
        DomainAdminCommands.Health.DomainStatusCommand(),
      ),
      statusMap(
        environment.config.participantsByString,
        ParticipantStatus.fromProtoV0,
        ParticipantAdminCommands.Health.ParticipantStatusCommand(),
      ),
    )
}
