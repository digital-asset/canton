// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.admin

import cats.data.EitherT
import cats.implicits._
import com.digitalasset.canton.common.domain.ServiceAgreementId
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.error.CantonErrorGroups.ParticipantErrorGroup.DomainConnectivityErrorGroup
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown.syntax._
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.domain.AgreementService.AgreementServiceError
import com.digitalasset.canton.participant.domain._
import com.digitalasset.canton.participant.sync.CantonSyncService
import com.digitalasset.canton.participant.sync.SyncServiceError.SyncServiceInternalError.DomainIsMissingInternally
import com.digitalasset.canton.participant.sync.SyncServiceError.SyncServiceUnknownDomain
import com.digitalasset.canton.sequencing.{GrpcSequencerConnection, SequencerConnection}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.EitherTUtil
import com.digitalasset.canton.util.Thereafter.syntax.ThereafterOps
import com.digitalasset.canton.{DomainAlias, DomainId}
import io.grpc.StatusRuntimeException

import scala.concurrent.{ExecutionContext, Future}

class DomainConnectivityService(
    sync: CantonSyncService,
    aliasManager: DomainAliasManager,
    agreementService: AgreementService,
    sequencerConnectClientBuilder: DomainConnectionConfig => EitherT[
      Future,
      DomainRegistryError,
      SequencerConnectClient,
    ],
    timeouts: ProcessingTimeout,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends NamedLogging {

  import com.digitalasset.canton.networking.grpc.CantonGrpcUtil._

  private def waitUntilActiveIfSuccess(success: Boolean, domain: DomainAlias)(implicit
      traceContext: TraceContext
  ): EitherT[Future, StatusRuntimeException, Unit] =
    if (success) waitUntilActive(domain) else EitherT.rightT(())

  private def waitUntilActive(
      domain: DomainAlias
  )(implicit traceContext: TraceContext): EitherT[Future, StatusRuntimeException, Unit] = {
    val clientE = for {
      domainId <- aliasManager
        .domainIdForAlias(domain)
        .toRight(DomainIsMissingInternally(domain, "aliasManager"))
      client <- sync.syncCrypto.ips
        .forDomain(domainId)
        .toRight(DomainIsMissingInternally(domain, "ips"))
    } yield client
    for {
      client <- mapErrNew(clientE)
      active <- EitherT
        .right(client.await(_.isParticipantActive(sync.participantId), timeouts.network.unwrap))
        .onShutdown(Right(true)) // don't emit ugly warnings on shutdown
      _ <- mapErrNew(
        Either
          .cond(
            active,
            (),
            DomainRegistryError.ConnectionErrors.ParticipantIsNotActive.Error(
              s"While domain $domain promised, participant ${sync.participantId} never became active within a reasonable timeframe."
            ),
          )
      )
    } yield ()

  }

  def connectDomain(domainAlias: String, keepRetrying: Boolean)(implicit
      traceContext: TraceContext
  ): Future[v0.ConnectDomainResponse] = {
    val resp = for {
      alias <- mapErr(DomainAlias.create(domainAlias))
      success <- mapErrNew(sync.connectDomain(alias, keepRetrying))
      _ <- waitUntilActiveIfSuccess(success, alias)
    } yield v0.ConnectDomainResponse(connectedSuccessfully = success)
    EitherTUtil.toFuture(resp)
  }

  def disconnectDomain(
      domainAlias: String
  )(implicit traceContext: TraceContext): Future[v0.DisconnectDomainResponse] = {
    val res = for {
      alias <- mapErr(DomainAlias.create(domainAlias))
      disconnect <- mapErrNew(sync.disconnectDomain(alias))
    } yield disconnect
    EitherTUtil
      .toFuture(res)
      .map(_ => v0.DisconnectDomainResponse())
  }

  def listConnectedDomains(): Future[v0.ListConnectedDomainsResponse] =
    Future.successful(v0.ListConnectedDomainsResponse(sync.readyDomains.map {
      case (alias, (domainId, healthy)) =>
        new v0.ListConnectedDomainsResponse.Result(
          domainAlias = alias.unwrap,
          domainId = domainId.toProtoPrimitive,
          healthy = healthy,
        )
    }.toSeq))

  def listConfiguredDomains(): Future[v0.ListConfiguredDomainsResponse] = {
    val connected = sync.readyDomains
    val configuredDomains = sync.configuredDomains
    Future.successful(
      v0.ListConfiguredDomainsResponse(
        results = configuredDomains
          .map(cnf =>
            new v0.ListConfiguredDomainsResponse.Result(
              config = Some(cnf.toProtoV0),
              connected = connected.contains(cnf.domain),
            )
          )
      )
    )
  }

  def registerDomain(
      request: v0.DomainConnectionConfig
  )(implicit traceContext: TraceContext): Future[v0.RegisterDomainResponse] = {
    logger.info(show"Registering ${request.domainAlias}")
    val resp = for {
      conf <- mapErr(DomainConnectionConfig.fromProtoV0(request))
      _ <- mapErrNew(sync.addDomain(conf))
      _ <-
        if (!conf.manualConnect) for {
          success <- mapErrNew(sync.connectDomain(conf.domain, keepRetrying = false))
          _ <- waitUntilActiveIfSuccess(success, conf.domain)
        } yield ()
        else EitherT.rightT[Future, StatusRuntimeException](())
    } yield v0.RegisterDomainResponse()
    EitherTUtil.toFuture(resp)
  }

  def modifyDomain(
      request: v0.DomainConnectionConfig
  )(implicit traceContext: TraceContext): Future[v0.ModifyDomainResponse] = {
    val resp = for {
      conf <- mapErr(DomainConnectionConfig.fromProtoV0(request))
      _ <- mapErrNew(sync.modifyDomain(conf))
    } yield v0.ModifyDomainResponse()
    EitherTUtil.toFuture(resp)
  }

  /** Get the service agreement from the domain's domain service */
  def getAgreement(
      domainAlias: String
  )(implicit traceContext: TraceContext): Future[v0.GetAgreementResponse] = {
    val res = for {
      settingsAndId <- getDomainIdAndSettings(domainAlias)
      (sequencerConnection, domainId) = settingsAndId
      optAgreement <- mapErr(sequencerConnection match {
        case grpc: GrpcSequencerConnection => agreementService.getAgreement(domainId, grpc)
        case _ => EitherT.rightT[Future, AgreementServiceError](None)
      })
      accepted <- optAgreement.fold(EitherT.rightT[Future, StatusRuntimeException](false))(ag =>
        mapErrNew(EitherT.right(agreementService.hasAcceptedAgreement(domainId, ag.id)))
      )
      agreement = optAgreement.map(ag =>
        v0.Agreement(ag.id.toProtoPrimitive, ag.text.toProtoPrimitive)
      )
    } yield v0.GetAgreementResponse(domainId = domainId.toProtoPrimitive, agreement, accepted)
    EitherTUtil.toFuture(res)
  }

  private def getDomainIdAndSettings(domainAlias: String)(implicit
      traceContext: TraceContext
  ): EitherT[Future, StatusRuntimeException, (SequencerConnection, DomainId)] = {
    for {
      alias <- mapErr(DomainAlias.create(domainAlias))
      connection <- mapErrNew(
        sync
          .domainConnectionConfigByAlias(alias)
          .leftMap(_ => SyncServiceUnknownDomain.Error(alias))
      )
      sequencerConnectClient <- mapErrNew(sequencerConnectClientBuilder(connection))
      domainId <-
        mapErrNew(
          sequencerConnectClient
            .getDomainId(alias)
            .leftMap(DomainRegistryHelpers.toDomainRegistryError(alias))
        ).thereafter(_ => sequencerConnectClient.close())
    } yield (connection.sequencerConnection, domainId)
  }

  /** Accept the agreement for the domain */
  def acceptAgreement(domainAlias: String, agreementId: String)(implicit
      traceContext: TraceContext
  ): Future[v0.AcceptAgreementResponse] = {
    val res = for {
      settingsAndId <- getDomainIdAndSettings(domainAlias)
      (_, domainId) = settingsAndId
      agreementId <- mapErr(ServiceAgreementId.create(agreementId))
      _ <- mapErr(agreementService.acceptAgreement(domainId, agreementId))
    } yield v0.AcceptAgreementResponse()
    EitherTUtil.toFuture(res)
  }

  def reconnectDomains(ignoreFailures: Boolean): Future[Unit] = TraceContext.fromGrpcContext {
    implicit traceContext =>
      val ret = for {
        aliases <- mapErrNew(sync.reconnectDomains(ignoreFailures = ignoreFailures))
        _ <- aliases.traverse(waitUntilActive)
      } yield ()
      EitherTUtil.toFuture(ret)
  }

  def getDomainId(domainAlias: String)(implicit traceContext: TraceContext): Future[DomainId] =
    EitherTUtil.toFuture(getDomainIdAndSettings(domainAlias).map(_._2))

}

object DomainConnectivityService extends DomainConnectivityErrorGroup {}
