// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.environment

import cats.syntax.either.*
import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.{LocalNodeConfig, TestingConfigInternal}
import com.digitalasset.canton.crypto.admin.grpc.GrpcVaultService.GrpcVaultServiceFactory
import com.digitalasset.canton.crypto.store.CryptoPrivateStore.CryptoPrivateStoreFactory
import com.digitalasset.canton.environment.CantonNodeBootstrap.HealthDumpFunction
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.resource.StorageFactory
import com.digitalasset.canton.telemetry.ConfiguredOpenTelemetry
import com.digitalasset.canton.time.Clock

final case class NodeFactoryArguments[
    NodeConfig <: LocalNodeConfig,
    ParameterConfig <: CantonNodeParameters,
    Metrics <: BaseMetrics,
](
    name: String,
    config: NodeConfig,
    parameters: ParameterConfig,
    clock: Clock,
    metrics: Metrics,
    testingConfig: TestingConfigInternal,
    futureSupervisor: FutureSupervisor,
    loggerFactory: NamedLoggerFactory,
    writeHealthDumpToFile: HealthDumpFunction,
    configuredOpenTelemetry: ConfiguredOpenTelemetry,
) {
  def toCantonNodeBootstrapCommonArguments(
      storageFactory: StorageFactory,
      cryptoPrivateStoreFactory: CryptoPrivateStoreFactory,
      grpcVaultServiceFactory: GrpcVaultServiceFactory,
  ): Either[String, CantonNodeBootstrapCommonArguments[NodeConfig, ParameterConfig, Metrics]] =
    InstanceName
      .create(name)
      .map(
        CantonNodeBootstrapCommonArguments(
          _,
          config,
          parameters,
          testingConfig,
          clock,
          metrics,
          storageFactory,
          cryptoPrivateStoreFactory,
          grpcVaultServiceFactory,
          futureSupervisor,
          loggerFactory,
          writeHealthDumpToFile,
          configuredOpenTelemetry,
        )
      )
      .leftMap(_.toString)
}
