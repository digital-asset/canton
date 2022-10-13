// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.commands

import cats.syntax.either.*
import cats.syntax.traverse.*
import com.digitalasset.canton.admin.api.client.data.{
  StaticDomainParameters as StaticDomainParametersConfig
}
import com.digitalasset.canton.domain.admin.{v0 as adminproto}
import com.digitalasset.canton.domain.config.store.DomainNodeSequencerConfig
import com.digitalasset.canton.domain.service.ServiceAgreementAcceptance
import com.digitalasset.canton.protocol.{StaticDomainParameters as StaticDomainParametersInternal}
import com.digitalasset.canton.sequencing.SequencerConnection
import com.google.protobuf.empty.Empty
import io.grpc.{ManagedChannel, Status}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

object DomainAdminCommands {

  final case class Initialize(sequencerConnection: SequencerConnection)
      extends GrpcAdminCommand[adminproto.DomainInitRequest, Empty, Unit] {
    override type Svc = adminproto.DomainInitializationServiceGrpc.DomainInitializationServiceStub
    override def createService(
        channel: ManagedChannel
    ): adminproto.DomainInitializationServiceGrpc.DomainInitializationServiceStub =
      adminproto.DomainInitializationServiceGrpc.stub(channel)

    override def createRequest(): Either[String, adminproto.DomainInitRequest] =
      Right(
        adminproto.DomainInitRequest(Some(DomainNodeSequencerConfig(sequencerConnection).toProtoV0))
      )

    override def submitRequest(
        service: adminproto.DomainInitializationServiceGrpc.DomainInitializationServiceStub,
        request: adminproto.DomainInitRequest,
    ): Future[Empty] =
      service.init(request)

    override def handleResponse(response: Empty): Either[String, Unit] = Right(())
  }

  abstract class BaseDomainServiceCommand[Req, Rep, Res] extends GrpcAdminCommand[Req, Rep, Res] {
    override type Svc = adminproto.DomainServiceGrpc.DomainServiceStub
    override def createService(
        channel: ManagedChannel
    ): adminproto.DomainServiceGrpc.DomainServiceStub =
      adminproto.DomainServiceGrpc.stub(channel)
  }

  final case object ListAcceptedServiceAgreements
      extends BaseDomainServiceCommand[Empty, adminproto.ServiceAgreementAcceptances, Seq[
        ServiceAgreementAcceptance
      ]] {
    override def createRequest(): Either[String, Empty] = Right(Empty())

    override def submitRequest(
        service: adminproto.DomainServiceGrpc.DomainServiceStub,
        request: Empty,
    ): Future[adminproto.ServiceAgreementAcceptances] =
      service.listServiceAgreementAcceptances(request)

    override def handleResponse(
        response: adminproto.ServiceAgreementAcceptances
    ): Either[String, Seq[ServiceAgreementAcceptance]] =
      response.acceptances
        .traverse(ServiceAgreementAcceptance.fromProtoV0)
        .bimap(_.toString, _.toSeq)
  }

  final case class GetDomainParameters()(implicit ec: ExecutionContext)
      extends BaseDomainServiceCommand[
        adminproto.GetDomainParameters.Request,
        adminproto.GetDomainParameters.Response,
        StaticDomainParametersConfig,
      ] {
    override def createRequest(): Either[String, adminproto.GetDomainParameters.Request] = Right(
      adminproto.GetDomainParameters.Request()
    )
    override def submitRequest(
        service: adminproto.DomainServiceGrpc.DomainServiceStub,
        request: adminproto.GetDomainParameters.Request,
    ): Future[adminproto.GetDomainParameters.Response] = {
      service
        .getDomainParametersVersioned(adminproto.GetDomainParameters.Request())
        .transformWith {
          case Failure(exception: io.grpc.StatusRuntimeException)
              if exception.getStatus.getCode == Status.Code.UNIMPLEMENTED =>
            /*
              The retry here is for backward compatibility reason.
              The initial GetDomainParameters endpoints was not returning properly versioned
              responses. If the new endpoint does not respond, we try the old one.
             */
            service
              .getDomainParameters(Empty())
              .map(adminproto.GetDomainParameters.Response.Parameters.ParametersV0(_))
              .map(adminproto.GetDomainParameters.Response(_))

          case Failure(exception) => Future.failed(exception)

          case Success(value) => Future.successful(value)
        }
    }

    override def handleResponse(
        response: adminproto.GetDomainParameters.Response
    ): Either[String, StaticDomainParametersConfig] = {
      import adminproto.GetDomainParameters.Response.Parameters

      response.parameters match {
        case Parameters.Empty => Left("Field parameters was not found in the response")
        case Parameters.ParametersV0(parametersV0) =>
          (for {
            staticDomainParametersInternal <- StaticDomainParametersInternal.fromProtoV0(
              parametersV0
            )
            sraticDomainParametersConfig <- StaticDomainParametersConfig(
              staticDomainParametersInternal
            )
          } yield sraticDomainParametersConfig).leftMap(_.toString)

        case Parameters.ParametersV1(parametersV1) =>
          (for {
            staticDomainParametersInternal <- StaticDomainParametersInternal.fromProtoV1(
              parametersV1
            )
            staticDomainParametersConfig <- StaticDomainParametersConfig(
              staticDomainParametersInternal
            )
          } yield staticDomainParametersConfig).leftMap(_.toString)
      }
    }
  }
}
