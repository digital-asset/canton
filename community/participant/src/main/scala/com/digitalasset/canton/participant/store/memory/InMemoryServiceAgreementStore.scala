// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.memory

import java.util.concurrent.ConcurrentHashMap

import cats.data.EitherT
import com.digitalasset.canton.DomainId
import com.digitalasset.canton.common.domain.{ServiceAgreement, ServiceAgreementId}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.participant.store.ServiceAgreementStore
import com.digitalasset.canton.tracing.TraceContext

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}

class InMemoryServiceAgreementStore(protected val loggerFactory: NamedLoggerFactory)(implicit
    ec: ExecutionContext
) extends ServiceAgreementStore
    with NamedLogging {

  import ServiceAgreementStore._

  private val agreements = TrieMap.empty[(DomainId, ServiceAgreementId), String]
  private val acceptedAgreements = new ConcurrentHashMap[DomainId, Set[ServiceAgreementId]]()

  override def storeAgreement(
      domainId: DomainId,
      agreementId: ServiceAgreementId,
      agreementText: String,
  )(implicit traceContext: TraceContext): EitherT[Future, ServiceAgreementStoreError, Unit] = {
    val _ = agreements.putIfAbsent((domainId, agreementId), agreementText)
    EitherT.rightT(())
  }

  override def listAgreements(implicit
      traceContext: TraceContext
  ): Future[Seq[(DomainId, ServiceAgreement)]] =
    Future.successful(agreements.toSeq.map { case ((domainId, agreementId), text) =>
      (domainId, ServiceAgreement(agreementId, text))
    })

  override def getAgreement(domainId: DomainId, agreementId: ServiceAgreementId)(implicit
      traceContext: TraceContext
  ): ServiceAgreementStoreT[String] =
    EitherT.fromEither(
      agreements
        .get((domainId, agreementId))
        .toRight(UnknownServiceAgreement(domainId, agreementId))
    )

  override def containsAgreement(domainId: DomainId, agreementId: ServiceAgreementId)(implicit
      traceContext: TraceContext
  ): Future[Boolean] =
    Future.successful(agreements.contains((domainId, agreementId)))

  override def insertAcceptedAgreement(domainId: DomainId, agreementId: ServiceAgreementId)(implicit
      traceContext: TraceContext
  ): EitherT[Future, ServiceAgreementStoreError, Unit] = {
    EitherT.cond(
      agreements.contains((domainId, agreementId)), {
        acceptedAgreements.computeIfPresent(domainId, (_, agreements) => agreements + agreementId)
        val _ = acceptedAgreements.putIfAbsent(domainId, Set(agreementId))
      },
      UnknownServiceAgreement(domainId, agreementId),
    )
  }

  def listAcceptedAgreements(domainId: DomainId)(implicit
      traceContext: TraceContext
  ): Future[Seq[ServiceAgreementId]] =
    Future.successful(acceptedAgreements.getOrDefault(domainId, Set()).toSeq)

  override def containsAcceptedAgreement(domainId: DomainId, agreementId: ServiceAgreementId)(
      implicit traceContext: TraceContext
  ): Future[Boolean] =
    Future.successful(acceptedAgreements.getOrDefault(domainId, Set()).contains(agreementId))
}
