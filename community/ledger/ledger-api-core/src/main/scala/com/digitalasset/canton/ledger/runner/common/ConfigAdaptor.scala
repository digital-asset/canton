// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.ledger.runner.common

import com.daml.metrics.Metrics
import com.digitalasset.canton.ledger.api.auth.{
  AuthService,
  CachedJwtVerifierLoader,
  JwtVerifierLoader,
}
import com.digitalasset.canton.ledger.configuration.Configuration
import com.digitalasset.canton.platform.apiserver.{ApiServerConfig, TimeServiceBackend}
import com.digitalasset.canton.platform.config.ParticipantConfig
import com.digitalasset.canton.platform.configuration.InitialLedgerConfiguration
import com.digitalasset.canton.platform.services.time.TimeProviderType

import java.time.{Duration, Instant}
import scala.concurrent.ExecutionContext

class ConfigAdaptor {
  def initialLedgerConfig(
      maxDeduplicationDuration: Option[Duration]
  ): InitialLedgerConfiguration = {
    val conf = Configuration.reasonableInitialConfiguration
    InitialLedgerConfiguration(
      maxDeduplicationDuration = maxDeduplicationDuration.getOrElse(
        conf.maxDeduplicationDuration
      ),
      avgTransactionLatency = conf.timeModel.avgTransactionLatency,
      minSkew = conf.timeModel.minSkew,
      maxSkew = conf.timeModel.maxSkew,
      // If a new index database is added to an already existing ledger,
      // a zero delay will likely produce a "configuration rejected" ledger entry,
      // because at startup the indexer hasn't ingested any configuration change yet.
      // Override this setting for distributed ledgers where you want to avoid these superfluous entries.
      delayBeforeSubmitting = Duration.ZERO,
    )
  }

  def timeServiceBackend(config: ApiServerConfig): Option[TimeServiceBackend] =
    config.timeProviderType match {
      case TimeProviderType.Static => Some(TimeServiceBackend.simple(Instant.EPOCH))
      case TimeProviderType.WallClock => None
    }

  def authService(participantConfig: ParticipantConfig): AuthService =
    participantConfig.authentication.create(
      participantConfig.jwtTimestampLeeway
    )

  def jwtVerifierLoader(
      participantConfig: ParticipantConfig,
      metrics: Metrics,
      executionContext: ExecutionContext,
  ): JwtVerifierLoader =
    new CachedJwtVerifierLoader(
      jwtTimestampLeeway = participantConfig.jwtTimestampLeeway,
      metrics = metrics,
    )(executionContext)
}
