// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.config

import com.daml.jwt.JwtTimestampLeeway
import com.daml.lf.data.Ref
import com.digitalasset.canton.platform.apiserver.{ApiServerConfig, AuthServiceConfig}
import com.digitalasset.canton.platform.indexer.IndexerConfig
import com.digitalasset.canton.platform.store.DbSupport.{ConnectionPoolConfig, DataSourceProperties}

import scala.concurrent.duration.*

final case class ParticipantConfig(
    apiServer: ApiServerConfig = ApiServerConfig(),
    authentication: AuthServiceConfig = AuthServiceConfig.Wildcard,
    jwtTimestampLeeway: Option[JwtTimestampLeeway] = None,
    dataSourceProperties: DataSourceProperties = DataSourceProperties(
      connectionPool = ConnectionPoolConfig(
        connectionPoolSize = 16,
        connectionTimeout = 250.millis,
      )
    ),
    indexService: IndexServiceConfig = IndexServiceConfig(),
    indexer: IndexerConfig = IndexerConfig(),
    participantIdOverride: Option[Ref.ParticipantId] = None,
    servicesThreadPoolSize: Int = ParticipantConfig.DefaultServicesThreadPoolSize,
)

object ParticipantConfig {
  val DefaultParticipantId: Ref.ParticipantId = Ref.ParticipantId.assertFromString("default")
  val DefaultServicesThreadPoolSize: Int = Runtime.getRuntime.availableProcessors()
}
