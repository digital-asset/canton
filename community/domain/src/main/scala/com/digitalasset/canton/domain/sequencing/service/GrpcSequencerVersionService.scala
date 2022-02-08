// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.sequencing.service

import com.digitalasset.canton.domain.api.v0.SequencerVersionServiceGrpc.SequencerVersionService
import com.digitalasset.canton.protocol.v0
import com.digitalasset.canton.protocol.v0.Handshake
import com.digitalasset.canton.sequencing.protocol.{HandshakeRequest, HandshakeResponse}
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.version.ProtocolVersion.canClientConnectToServer
import io.grpc.{Status, StatusException}
import cats.syntax.either._

import scala.concurrent.Future

/**
  */
class GrpcSequencerVersionService(serverVersion: ProtocolVersion) extends SequencerVersionService {

  /** Perform a handshake with the initialization service to ensure that the client and server are using compatible versions
    */
  override def handshake(requestP: Handshake.Request): Future[Handshake.Response] =
    handleHandshakeRequest(serverVersion)(requestP)
      .fold(Future.failed, Future.successful)

  /** Handles a request from a client checking that is it compatible with the protocol version of the domain.
    */
  private def handleHandshakeRequest(
      serverVersion: ProtocolVersion
  )(requestP: v0.Handshake.Request): Either[StatusException, v0.Handshake.Response] =
    for {
      request <- HandshakeRequest
        .fromProtoV0(requestP)
        .leftMap(err =>
          Status.INVALID_ARGUMENT.withDescription(s"Failed to deserialize request: $err")
        )
        .leftMap(_.asException())
      response = canClientConnectToServer(request.clientProtocolVersions, serverVersion, None)
        .fold[HandshakeResponse](
          err => HandshakeResponse.Failure(serverVersion, err.description),
          _ => HandshakeResponse.Success(serverVersion),
        )
    } yield response.toProtoV0
}
