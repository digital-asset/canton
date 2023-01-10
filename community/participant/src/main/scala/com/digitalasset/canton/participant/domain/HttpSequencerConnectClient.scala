// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.domain

import cats.data.EitherT
import com.digitalasset.canton.DomainAlias
import com.digitalasset.canton.common.domain.ServiceAgreement
import com.digitalasset.canton.lifecycle.Lifecycle
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.StaticDomainParameters
import com.digitalasset.canton.sequencing.client.http.{
  HttpSequencerClient,
  HttpSequencerClientError,
}
import com.digitalasset.canton.sequencing.protocol.{
  HandshakeRequest,
  HandshakeResponse,
  VerifyActiveRequest,
  VerifyActiveResponse,
}
import com.digitalasset.canton.topology.{DomainId, ParticipantId}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.HandshakeErrors.DeprecatedProtocolVersion

import scala.concurrent.{ExecutionContextExecutor, Future}

class HttpSequencerConnectClient(
    httpSequencerClient: HttpSequencerClient,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContextExecutor)
    extends SequencerConnectClient
    with NamedLogging {
  def getDomainId(domainAlias: DomainAlias)(implicit
      traceContext: TraceContext
  ): EitherT[Future, SequencerConnectClient.Error, DomainId] =
    httpSequencerClient.getDomainId().leftMap(toSequencerConnectError)

  def getDomainParameters(domainAlias: DomainAlias)(implicit
      traceContext: TraceContext
  ): EitherT[Future, SequencerConnectClient.Error, StaticDomainParameters] =
    httpSequencerClient.getDomainParameters().leftMap(toSequencerConnectError)

  def isActive(participantId: ParticipantId, waitForActive: Boolean)(implicit
      traceContext: TraceContext
  ): EitherT[Future, SequencerConnectClient.Error, Boolean] =
    httpSequencerClient
      .verifyActive(VerifyActiveRequest())
      .leftMap(toSequencerConnectError)
      .flatMap {
        case VerifyActiveResponse.Success(isActive) =>
          EitherT.pure[Future, SequencerConnectClient.Error](isActive)
        case VerifyActiveResponse.Failure(reason) =>
          EitherT.fromEither[Future](Left(SequencerConnectClient.Error.Transport(reason)))
      }

  def handshake(
      domainAlias: DomainAlias,
      request: HandshakeRequest,
      dontWarnOnDeprecatedPV: Boolean,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, SequencerConnectClient.Error, HandshakeResponse] = for {
    res <- httpSequencerClient
      .handshakeUnauthenticated(request)
      .leftMap(toSequencerConnectError)
    _ = if (res.serverProtocolVersion.isDeprecated && !dontWarnOnDeprecatedPV)
      DeprecatedProtocolVersion.WarnSequencerClient(domainAlias, res.serverProtocolVersion)
  } yield res

  override def getAgreement(domainId: DomainId)(implicit
      traceContext: TraceContext
  ): EitherT[Future, SequencerConnectClient.Error, Option[ServiceAgreement]] = {
    logger.info("GetAgreement is not implemented for HTTP sequencers; returning none")
    EitherT.pure(None)
  }

  private def toSequencerConnectError(
      error: HttpSequencerClientError
  ): SequencerConnectClient.Error = error match {
    case HttpSequencerClientError.ClientError(error) =>
      SequencerConnectClient.Error.Transport(error.toString)

    case HttpSequencerClientError.DeserializationError(error) =>
      SequencerConnectClient.Error.DeserializationFailure(error.toString)

    case timeout: HttpSequencerClientError.TransactionTimeout =>
      SequencerConnectClient.Error.Transport(timeout.toString)

    case invalidTransaction: HttpSequencerClientError.InvalidTransaction =>
      SequencerConnectClient.Error.InvalidState(invalidTransaction.toString)

    case unknownTransaction: HttpSequencerClientError.UnknownTransaction =>
      SequencerConnectClient.Error.InvalidState(unknownTransaction.toString)

    case transactionCheckFailed: HttpSequencerClientError.TransactionCheckFailed =>
      SequencerConnectClient.Error.Transport(transactionCheckFailed.toString)
  }

  override def close(): Unit = Lifecycle.close(httpSequencerClient)(logger)
}
