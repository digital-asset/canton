// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.commands

import cats.syntax.either._
import cats.syntax.traverse._
import com.digitalasset.canton.domain.admin.{v0 => adminproto}
import com.digitalasset.canton.domain.config.store.DomainNodeSequencerConfig
import com.digitalasset.canton.domain.service.ServiceAgreementAcceptance
import com.digitalasset.canton.protocol.{StaticDomainParameters, v0}
import com.digitalasset.canton.sequencing.SequencerConnection
import com.google.protobuf.empty.Empty
import io.grpc.ManagedChannel

import scala.concurrent.Future

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

  final case object GetDomainParameters
      extends BaseDomainServiceCommand[Empty, v0.StaticDomainParameters, StaticDomainParameters] {
    override def createRequest(): Either[String, Empty] = Right(Empty())
    override def submitRequest(
        service: adminproto.DomainServiceGrpc.DomainServiceStub,
        request: Empty,
    ): Future[v0.StaticDomainParameters] =
      service.getDomainParameters(request)
    override def handleResponse(
        response: v0.StaticDomainParameters
    ): Either[String, StaticDomainParameters] =
      StaticDomainParameters.fromProtoV0(response).leftMap(_.toString)
  }
}
