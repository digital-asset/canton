// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.admin.api.client.commands

import cats.data.EitherT
import cats.syntax.either._
import cats.syntax.option._
import com.digitalasset.canton.DomainId
import com.digitalasset.canton.admin.api.client.commands.GrpcAdminCommand.{
  DefaultUnboundedTimeout,
  TimeoutType,
}
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.admin.v0
import com.digitalasset.canton.domain.sequencing.admin.client.HttpSequencerAdminClient
import com.digitalasset.canton.domain.sequencing.admin.protocol.{InitRequest, InitResponse}
import com.digitalasset.canton.domain.sequencing.sequencer.{LedgerIdentity, SequencerSnapshot}
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.networking.http.HttpClient
import com.digitalasset.canton.protocol.StaticDomainParameters
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.topology.store.StoredTopologyTransactions
import com.digitalasset.canton.topology.transaction.TopologyChangeOp
import com.digitalasset.canton.tracing.TraceContext
import com.google.protobuf.empty.Empty
import io.grpc.ManagedChannel

import java.net.URL
import scala.concurrent.{ExecutionContext, Future}

object EnterpriseSequencerAdminCommands {
  abstract class BaseSequencerInitializationCommand[Req, Rep, Res]
      extends GrpcAdminCommand[Req, Rep, Res] {
    override type Svc = v0.SequencerInitializationServiceGrpc.SequencerInitializationServiceStub
    override def createService(
        channel: ManagedChannel
    ): v0.SequencerInitializationServiceGrpc.SequencerInitializationServiceStub =
      v0.SequencerInitializationServiceGrpc.stub(channel)
  }

  abstract class BaseSequencerAdministrationCommand[Req, Rep, Res]
      extends GrpcAdminCommand[Req, Rep, Res] {
    override type Svc =
      v0.EnterpriseSequencerAdministrationServiceGrpc.EnterpriseSequencerAdministrationServiceStub
    override def createService(
        channel: ManagedChannel
    ): v0.EnterpriseSequencerAdministrationServiceGrpc.EnterpriseSequencerAdministrationServiceStub =
      v0.EnterpriseSequencerAdministrationServiceGrpc.stub(channel)
  }

  abstract class BaseSequencerTopologyBootstrapCommand[Req, Rep, Res]
      extends GrpcAdminCommand[Req, Rep, Res] {
    override type Svc = v0.TopologyBootstrapServiceGrpc.TopologyBootstrapServiceStub
    override def createService(
        channel: ManagedChannel
    ): v0.TopologyBootstrapServiceGrpc.TopologyBootstrapServiceStub =
      v0.TopologyBootstrapServiceGrpc.stub(channel)
  }

  case class Initialize(
      domainId: DomainId,
      topologySnapshot: StoredTopologyTransactions[TopologyChangeOp.Positive],
      domainParameters: StaticDomainParameters,
      snapshotO: Option[SequencerSnapshot] = None,
  ) extends BaseSequencerInitializationCommand[v0.InitRequest, v0.InitResponse, InitResponse] {
    override def createRequest(): Either[String, v0.InitRequest] = {
      val request = InitRequest(domainId, topologySnapshot, domainParameters, snapshotO)
      Right(request.toProtoV0)
    }
    override def submitRequest(
        service: v0.SequencerInitializationServiceGrpc.SequencerInitializationServiceStub,
        request: v0.InitRequest,
    ): Future[v0.InitResponse] =
      service.init(request)
    override def handleResponse(response: v0.InitResponse): Either[String, InitResponse] =
      InitResponse
        .fromProtoV0(response)
        .leftMap(err => s"Failed to deserialize response: $err")

    override def timeoutType: TimeoutType = DefaultUnboundedTimeout
  }

  case class HttpInitialize(
      domainId: DomainId,
      topologySnapshot: StoredTopologyTransactions[TopologyChangeOp.Add],
      domainParameters: StaticDomainParameters,
  ) extends HttpAdminCommand[InitRequest, InitResponse, InitResponse] {
    override type Svc = HttpSequencerAdminClient

    override def createService(
        baseUrl: URL,
        httpClient: HttpClient,
        timeouts: ProcessingTimeout,
        loggerFactory: NamedLoggerFactory,
    )(implicit ec: ExecutionContext): HttpSequencerAdminClient =
      new HttpSequencerAdminClient(baseUrl, httpClient, timeouts, loggerFactory)

    override def submitRequest(service: HttpSequencerAdminClient, request: InitRequest)(implicit
        traceContext: TraceContext
    ): EitherT[Future, String, InitResponse] =
      service.initialize(InitRequest(domainId, topologySnapshot, domainParameters))

    override def createRequest(): Either[String, InitRequest] = Right(
      InitRequest(domainId, topologySnapshot, domainParameters)
    )

    override def handleResponse(response: InitResponse): Either[String, InitResponse] = Right(
      response
    )
  }

  case class Snapshot(timestamp: CantonTimestamp)
      extends BaseSequencerAdministrationCommand[
        v0.Snapshot.Request,
        v0.Snapshot.Response,
        SequencerSnapshot,
      ] {
    override def createRequest(): Either[String, v0.Snapshot.Request] = {
      Right(v0.Snapshot.Request(Some(timestamp.toProtoPrimitive)))
    }

    override def submitRequest(
        service: v0.EnterpriseSequencerAdministrationServiceGrpc.EnterpriseSequencerAdministrationServiceStub,
        request: v0.Snapshot.Request,
    ): Future[v0.Snapshot.Response] = service.snapshot(request)

    override def handleResponse(response: v0.Snapshot.Response): Either[String, SequencerSnapshot] =
      response.value match {
        case v0.Snapshot.Response.Value.Failure(v0.Snapshot.Failure(reason)) => Left(reason)
        case v0.Snapshot.Response.Value.Success(v0.Snapshot.Success(Some(result))) =>
          SequencerSnapshot.fromProtoV0(result).leftMap(_.toString)
        case _ => Left("response is empty")
      }

    //  command will potentially take a long time
    override def timeoutType: TimeoutType = DefaultUnboundedTimeout

  }

  final case class Prune(timestamp: CantonTimestamp)
      extends BaseSequencerAdministrationCommand[v0.Pruning.Request, v0.Pruning.Response, String] {
    override def createRequest(): Either[String, v0.Pruning.Request] =
      Right(v0.Pruning.Request(timestamp.toProtoPrimitive.some))

    override def submitRequest(
        service: v0.EnterpriseSequencerAdministrationServiceGrpc.EnterpriseSequencerAdministrationServiceStub,
        request: v0.Pruning.Request,
    ): Future[v0.Pruning.Response] =
      service.prune(request)
    override def handleResponse(response: v0.Pruning.Response): Either[String, String] =
      Either.cond(
        response.details.nonEmpty,
        response.details,
        "Pruning response did not contain details",
      )

    //  command will potentially take a long time
    override def timeoutType: TimeoutType = DefaultUnboundedTimeout

  }

  final case class DisableMember(member: Member)
      extends BaseSequencerAdministrationCommand[v0.DisableMemberRequest, Empty, Unit] {
    override def createRequest(): Either[String, v0.DisableMemberRequest] =
      Right(v0.DisableMemberRequest(member.toProtoPrimitive))
    override def submitRequest(
        service: v0.EnterpriseSequencerAdministrationServiceGrpc.EnterpriseSequencerAdministrationServiceStub,
        request: v0.DisableMemberRequest,
    ): Future[Empty] = service.disableMember(request)
    override def handleResponse(response: Empty): Either[String, Unit] = Right(())
  }

  final case class AuthorizeLedgerIdentity(ledgerIdentity: LedgerIdentity)
      extends BaseSequencerAdministrationCommand[
        v0.LedgerIdentity.AuthorizeRequest,
        v0.LedgerIdentity.AuthorizeResponse,
        Unit,
      ] {
    override def createRequest(): Either[String, v0.LedgerIdentity.AuthorizeRequest] =
      Right(v0.LedgerIdentity.AuthorizeRequest(Some(ledgerIdentity.toProtoV0)))
    override def submitRequest(
        service: v0.EnterpriseSequencerAdministrationServiceGrpc.EnterpriseSequencerAdministrationServiceStub,
        request: v0.LedgerIdentity.AuthorizeRequest,
    ): Future[v0.LedgerIdentity.AuthorizeResponse] = service.authorizeLedgerIdentity(request)
    override def handleResponse(
        response: v0.LedgerIdentity.AuthorizeResponse
    ): Either[String, Unit] = Right(())
  }

  final case class BootstrapTopology(
      topologySnapshot: StoredTopologyTransactions[TopologyChangeOp.Positive]
  ) extends BaseSequencerTopologyBootstrapCommand[v0.TopologyBootstrapRequest, Empty, Unit] {
    override def createRequest(): Either[String, v0.TopologyBootstrapRequest] =
      Right(v0.TopologyBootstrapRequest(Some(topologySnapshot.toProtoV0)))

    override def submitRequest(
        service: v0.TopologyBootstrapServiceGrpc.TopologyBootstrapServiceStub,
        request: v0.TopologyBootstrapRequest,
    ): Future[Empty] =
      service.bootstrap(request)

    override def handleResponse(response: Empty): Either[String, Unit] = Right(())

    //  command will potentially take a long time
    override def timeoutType: TimeoutType = DefaultUnboundedTimeout

  }
}
