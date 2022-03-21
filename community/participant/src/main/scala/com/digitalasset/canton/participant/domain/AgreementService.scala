// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.domain

import cats.data.EitherT
import cats.syntax.foldable._
import com.digitalasset.canton.common.domain.{ServiceAgreement, ServiceAgreementId}
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.lifecycle.{FlagCloseable, Lifecycle}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.domain.AgreementService.AgreementServiceError
import com.digitalasset.canton.participant.store.ServiceAgreementStore
import com.digitalasset.canton.sequencing.GrpcSequencerConnection
import com.digitalasset.canton.topology.DomainId
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.{ExecutionContextExecutor, Future}

class AgreementService(
    acceptedAgreements: ServiceAgreementStore,
    domainServiceClient: DomainServiceClient,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContextExecutor)
    extends NamedLogging
    with FlagCloseable {

  def isRequiredAgreementAccepted(sequencerConnection: GrpcSequencerConnection, domainId: DomainId)(
      implicit traceContext: TraceContext
  ): EitherT[Future, AgreementServiceError, Option[ServiceAgreement]] =
    for {
      requiredAgreement <- getAgreement(domainId, sequencerConnection)
      acceptedAgreement <- requiredAgreement match {
        case Some(agreement) =>
          for {
            haveAccepted <- EitherT.right[AgreementServiceError](
              acceptedAgreements.containsAcceptedAgreement(domainId, agreement.id)
            )
            agreement <- EitherT.cond[Future](
              haveAccepted,
              Some(agreement),
              AgreementServiceError("Service agreement has not been accepted yet"),
            )
          } yield agreement
        case None =>
          // there's not required agreement to check acceptance
          EitherT.pure[Future, AgreementServiceError](None)
      }
    } yield acceptedAgreement

  def getAcceptedAgreementId(domainId: DomainId)(implicit
      traceContext: TraceContext
  ): Future[Option[ServiceAgreementId]] =
    acceptedAgreements.listAcceptedAgreements(domainId).map(_.headOption)

  def getAgreement(domainId: DomainId, sequencerConnection: GrpcSequencerConnection)(implicit
      traceContext: TraceContext
  ): EitherT[Future, AgreementServiceError, Option[ServiceAgreement]] =
    for {
      optAgreement <- domainServiceClient
        .getAgreement(domainId, sequencerConnection)
        .leftMap(err => AgreementServiceError(err.message))
      _ <- optAgreement.traverse_(ag =>
        acceptedAgreements
          .storeAgreement(domainId, ag.id, ag.text)
          .leftMap(err => AgreementServiceError(err.description))
      )
    } yield optAgreement

  def acceptAgreement(domainId: DomainId, agreementId: ServiceAgreementId)(implicit
      traceContext: TraceContext
  ): EitherT[Future, AgreementServiceError, Unit] =
    acceptedAgreements
      .insertAcceptedAgreement(domainId, agreementId)
      .leftMap(err => AgreementServiceError(err.description))

  def hasAcceptedAgreement(domainId: DomainId, agreementId: ServiceAgreementId)(implicit
      traceContext: TraceContext
  ): Future[Boolean] =
    acceptedAgreements.containsAcceptedAgreement(domainId, agreementId)

  override def onClosed(): Unit = Lifecycle.close(acceptedAgreements)(logger)
}

object AgreementService {
  case class AgreementServiceError(reason: String)
}
