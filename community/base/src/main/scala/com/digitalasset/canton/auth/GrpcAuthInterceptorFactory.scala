// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.auth

import com.daml.jwt.JwtTimestampLeeway
import com.daml.tracing.Telemetry
import com.digitalasset.canton.concurrent.DirectExecutionContext
import com.digitalasset.canton.config.{
  AdminTokenConfig,
  ApiLoggingConfig,
  AuthServiceConfig,
  JwksCacheConfig,
}
import com.digitalasset.canton.logging.NamedLoggerFactory
import io.grpc.ServerInterceptor

object GrpcAuthInterceptorFactory {
  def createInterceptor(
      loggerFactory: NamedLoggerFactory,
      apiLoggingConfig: ApiLoggingConfig,
      telemetry: Telemetry,
      adminTokenDispenser: CantonAdminTokenDispenser,
      authServiceConfigs: Seq[AuthServiceConfig],
      jwtTimestampLeeway: Option[JwtTimestampLeeway],
      adminTokenConfig: AdminTokenConfig,
      jwksCacheConfig: JwksCacheConfig,
  ): ServerInterceptor = {
    val authServices =
      if (authServiceConfigs.isEmpty)
        Seq(AuthServiceWildcard)
      else
        Seq(new CantonAdminTokenAuthService(adminTokenDispenser, None, adminTokenConfig)) ++
          authServiceConfigs.map(
            _.create(
              jwksCacheConfig,
              jwtTimestampLeeway,
              loggerFactory,
            )
          )
    val genericInterceptor = new AuthInterceptor(
      authServices,
      loggerFactory,
      DirectExecutionContext(
        loggerFactory.getLogger(AuthInterceptor.getClass)
      ),
      RequiringAdminClaimResolver,
    )
    new GrpcAuthInterceptor(
      genericInterceptor,
      telemetry,
      loggerFactory,
      apiLoggingConfig,
      DirectExecutionContext(
        loggerFactory.getLogger(AuthInterceptor.getClass)
      ),
    )
  }

}
