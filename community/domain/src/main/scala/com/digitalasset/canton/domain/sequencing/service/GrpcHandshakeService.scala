// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.service

import com.digitalasset.canton.domain.service.HandshakeValidator
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.Future

trait GrpcHandshakeService {
  protected def loggerFactory: NamedLoggerFactory
  protected def serverVersion: ProtocolVersion

  private lazy val handshakeValidator = new HandshakeValidator(serverVersion, loggerFactory)

  /** The handshake will check whether the client's version is compatible with the one of this domain.
    * This should be called before attempting to connect to the domain to make sure they can operate together.
    */
  def handshake(
      request: com.digitalasset.canton.protocol.v0.Handshake.Request
  ): Future[com.digitalasset.canton.protocol.v0.Handshake.Response] = {
    import com.digitalasset.canton.protocol.v0

    val response = handshakeValidation(request)
      .fold[v0.Handshake.Response.Value](
        failure => v0.Handshake.Response.Value.Failure(v0.Handshake.Failure(failure)),
        _ => v0.Handshake.Response.Value.Success(v0.Handshake.Success()),
      )
    Future.successful(v0.Handshake.Response(serverVersion = serverVersion.fullVersion, response))
  }

  private def handshakeValidation(
      request: com.digitalasset.canton.protocol.v0.Handshake.Request
  ): Either[String, Unit] =
    handshakeValidator.clientIsCompatible(
      request.clientProtocolVersions,
      request.minimumProtocolVersion,
    )
}
