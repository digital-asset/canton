// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.commands

import cats.syntax.either._
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.health.admin.data
import com.digitalasset.canton.health.admin.v0
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import io.grpc.ManagedChannel
import com.google.protobuf.empty.Empty

import scala.concurrent.Future

object StatusAdminCommands {
  abstract class GetStatusBase[Result] extends GrpcAdminCommand[Empty, v0.NodeStatus, Result] {
    override type Svc = v0.StatusServiceGrpc.StatusServiceStub
    override def createService(channel: ManagedChannel): v0.StatusServiceGrpc.StatusServiceStub =
      v0.StatusServiceGrpc.stub(channel)
    override def createRequest(): Either[String, Empty] = Right(Empty())
    override def submitRequest(
        service: v0.StatusServiceGrpc.StatusServiceStub,
        request: Empty,
    ): Future[v0.NodeStatus] =
      service.status(request)
  }

  class GetStatus[S <: data.NodeStatus.Status](
      deserialize: v0.NodeStatus.Status => ParsingResult[S]
  ) extends GetStatusBase[data.NodeStatus[S]] {
    override def handleResponse(response: v0.NodeStatus): Either[String, data.NodeStatus[S]] =
      ((response.response match {
        case v0.NodeStatus.Response.NotInitialized(notInitialized) =>
          Right(data.NodeStatus.NotInitialized(notInitialized.active))
        case v0.NodeStatus.Response.Success(status) =>
          deserialize(status).map(data.NodeStatus.Success(_))
        case v0.NodeStatus.Response.Empty => Left(ProtoDeserializationError.FieldNotSet("response"))
      }): ParsingResult[data.NodeStatus[S]]).leftMap(_.toString)
  }

  object IsRunning
      extends StatusAdminCommands.FromStatus({
        case v0.NodeStatus.Response.Empty => false
        case _ => true
      })

  object IsInitialized
      extends StatusAdminCommands.FromStatus({
        case v0.NodeStatus.Response.Success(_) => true
        case _ => false
      })

  class FromStatus(predicate: v0.NodeStatus.Response => Boolean) extends GetStatusBase[Boolean] {
    override def handleResponse(response: v0.NodeStatus): Either[String, Boolean] =
      (response.response match {
        case v0.NodeStatus.Response.Empty => Left(ProtoDeserializationError.FieldNotSet("response"))
        case other => Right(predicate(other))
      }).leftMap(_.toString)
  }
}
