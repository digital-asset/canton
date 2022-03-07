// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store

import cats.data.EitherT
import com.digitalasset.canton.DomainId
import com.digitalasset.canton.common.domain.{ServiceAgreement, ServiceAgreementId}
import com.digitalasset.canton.config.RequireTypes.String256M
import com.digitalasset.canton.tracing.TraceContext

import scala.concurrent.Future

trait ServiceAgreementStore extends AutoCloseable {

  import ServiceAgreementStore.ServiceAgreementStoreError

  type ServiceAgreementStoreT[A] = EitherT[Future, ServiceAgreementStoreError, A]

  /** Stores the agreement for the domain with the agreement text.
    *
    * Fails if the agreement has been stored already with a different text.
    */
  def storeAgreement(
      domainId: DomainId,
      agreementId: ServiceAgreementId,
      agreementText: String256M,
  )(implicit
      traceContext: TraceContext
  ): ServiceAgreementStoreT[Unit]

  /** List all stored agreements. */
  def listAgreements(implicit traceContext: TraceContext): Future[Seq[(DomainId, ServiceAgreement)]]

  /** Get the agreement text of a stored agreement. */
  def getAgreement(domainId: DomainId, agreementId: ServiceAgreementId)(implicit
      traceContext: TraceContext
  ): ServiceAgreementStoreT[String256M]

  /** Check if the agreement has been stored already. */
  def containsAgreement(domainId: DomainId, agreementId: ServiceAgreementId)(implicit
      traceContext: TraceContext
  ): Future[Boolean]

  /** Store the acceptance of a previously stored agreement. */
  def insertAcceptedAgreement(domainId: DomainId, agreementId: ServiceAgreementId)(implicit
      traceContext: TraceContext
  ): ServiceAgreementStoreT[Unit]

  /** List all accepted agreements for the domain. */
  def listAcceptedAgreements(domainId: DomainId)(implicit
      traceContext: TraceContext
  ): Future[Seq[ServiceAgreementId]]

  /** Check if the given agreement has been accepted for the domain. */
  def containsAcceptedAgreement(domainId: DomainId, agreementId: ServiceAgreementId)(implicit
      traceContext: TraceContext
  ): Future[Boolean]
}

object ServiceAgreementStore {

  sealed trait ServiceAgreementStoreError extends Product with Serializable {
    def description = toString
  }

  case class UnknownServiceAgreement(domainId: DomainId, agreementId: ServiceAgreementId)
      extends ServiceAgreementStoreError {
    override def toString = s"The agreement '$agreementId' is not known at domain '$domainId'."
  }

  case class ServiceAgreementAlreadyExists(domainId: DomainId, existingAgreement: ServiceAgreement)
      extends ServiceAgreementStoreError

  case class FailedToStoreAgreement(
      domainId: DomainId,
      agreementId: ServiceAgreementId,
      override val description: String,
  ) extends ServiceAgreementStoreError

  case class FailedToAcceptAgreement(
      domainId: DomainId,
      agreementId: ServiceAgreementId,
      override val description: String,
  ) extends ServiceAgreementStoreError

}
