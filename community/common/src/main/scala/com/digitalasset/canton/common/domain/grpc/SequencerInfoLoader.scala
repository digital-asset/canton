// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.common.domain.grpc

import akka.stream.Materializer
import cats.data.EitherT
import cats.instances.future.*
import cats.syntax.either.*
import com.daml.nonempty.{NonEmpty, NonEmptyUtil}
import com.digitalasset.canton.*
import com.digitalasset.canton.common.domain.SequencerConnectClient
import com.digitalasset.canton.common.domain.SequencerConnectClient.DomainClientBootstrapInfo
import com.digitalasset.canton.common.domain.grpc.SequencerInfoLoader.{
  SequencerAggregatedInfo,
  SequencerInfoLoaderError,
}
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.protocol.StaticDomainParameters
import com.digitalasset.canton.sequencing.protocol.{HandshakeRequest, HandshakeResponse}
import com.digitalasset.canton.sequencing.{
  GrpcSequencerConnection,
  SequencerConnection,
  SequencerConnections,
}
import com.digitalasset.canton.topology.*
import com.digitalasset.canton.tracing.{TraceContext, TracingConfig}
import com.digitalasset.canton.util.MonadUtil
import com.digitalasset.canton.util.Thereafter.syntax.ThereafterOps
import com.digitalasset.canton.version.ProtocolVersion
import io.opentelemetry.api.trace.Tracer

import scala.concurrent.{ExecutionContextExecutor, Future}

class SequencerInfoLoader(
    timeouts: ProcessingTimeout,
    traceContextPropagation: TracingConfig.Propagation,
    clientProtocolVersions: NonEmpty[Seq[ProtocolVersion]],
    minimumProtocolVersion: Option[ProtocolVersion],
    dontWarnOnDeprecatedPV: Boolean,
    protected val loggerFactory: NamedLoggerFactory,
    sequencerInfoLoadParallelism: NonNegativeInt = NonNegativeInt.tryCreate(4),
)(implicit
    val ec: ExecutionContextExecutor,
    val materializer: Materializer,
    val tracer: Tracer,
) extends NamedLogging {

  private def sequencerConnectClientBuilder: SequencerConnectClient.Builder = {
    (config: SequencerConnection) =>
      SequencerConnectClient(
        config,
        timeouts,
        traceContextPropagation,
        loggerFactory,
      )
  }

  private def extractSingleError(
      errors: Seq[SequencerInfoLoaderError]
  ): SequencerInfoLoaderError = {
    require(errors.nonEmpty, "Non-empty list of errors is expected")
    val nonEmptyResult = NonEmptyUtil.fromUnsafe(errors)
    if (nonEmptyResult.size == 1) nonEmptyResult.head1
    else {
      val message = nonEmptyResult.map(_.cause).mkString(",")
      SequencerInfoLoaderError.FailedToConnectToSequencers(message)
    }
  }

  private def aggregateBootstrapInfo(sequencerConnections: SequencerConnections)(
      result: Seq[(SequencerAlias, (DomainClientBootstrapInfo, StaticDomainParameters))]
  ): Either[SequencerInfoLoaderError, SequencerAggregatedInfo] = {
    require(result.nonEmpty, "Non-empty list of sequencerId-to-endpoint pair is expected")
    val nonEmptyResult = NonEmptyUtil.fromUnsafe(result)
    val infoResult = nonEmptyResult.map(_._2)
    val domainIds = infoResult.map(_._1).map(_.domainId).toSet
    val staticDomainParameters = infoResult.map(_._2).toSet
    val expectedSequencers = NonEmptyUtil.fromUnsafe(
      nonEmptyResult.groupBy(_._1).view.mapValues(_.map(_._2._1.sequencerId).head1).toMap
    )
    if (domainIds.sizeIs > 1) {
      SequencerInfoLoaderError
        .SequencersFromDifferentDomainsAreConfigured(
          s"Non-unique domain ids received by connecting to sequencers: [${domainIds.mkString(",")}]"
        )
        .asLeft
    } else if (staticDomainParameters.sizeIs > 1) {
      SequencerInfoLoaderError
        .MisconfiguredStaticDomainParameters(
          s"Non-unique static domain parameters received by connecting to sequencers"
        )
        .asLeft
    } else
      SequencerAggregatedInfo(
        domainId = domainIds.head1,
        staticDomainParameters = staticDomainParameters.head1,
        expectedSequencers = expectedSequencers,
        sequencerConnections = sequencerConnections,
      ).asRight
  }

  private def getBootstrapInfoDomainParameters(
      domainAlias: DomainAlias,
      sequencerAlias: SequencerAlias,
      client: SequencerConnectClient,
  )(implicit
      traceContext: TraceContext
  ): EitherT[
    Future,
    SequencerInfoLoaderError,
    (DomainClientBootstrapInfo, StaticDomainParameters),
  ] = {
    for {
      bootstrapInfo <- client
        .getDomainClientBootstrapInfo(domainAlias)
        .leftMap(SequencerInfoLoader.fromSequencerConnectClientError(domainAlias))

      _ <- performHandshake(client, domainAlias, sequencerAlias, bootstrapInfo.domainId)

      domainParameters <- client
        .getDomainParameters(domainAlias)
        .leftMap(SequencerInfoLoader.fromSequencerConnectClientError(domainAlias))
    } yield (bootstrapInfo, domainParameters)
  }

  private def getBootstrapInfoDomainParameters(
      domainAlias: DomainAlias
  )(connection: SequencerConnection)(implicit
      traceContext: TraceContext
  ): EitherT[Future, Seq[
    SequencerInfoLoaderError
  ], (SequencerAlias, (DomainClientBootstrapInfo, StaticDomainParameters))] =
    connection match {
      case grpc: GrpcSequencerConnection =>
        (for {
          client <- sequencerConnectClientBuilder(grpc).leftMap(
            SequencerInfoLoader.fromSequencerConnectClientError(domainAlias)
          )
          bootstrapInfoDomainParameters <- getBootstrapInfoDomainParameters(
            domainAlias,
            grpc.sequencerAlias,
            client,
          )
            .thereafter(_ => client.close())
        } yield connection.sequencerAlias -> bootstrapInfoDomainParameters).leftMap(x => Seq(x))
    }

  private def performHandshake(
      sequencerConnectClient: SequencerConnectClient,
      alias: DomainAlias,
      sequencerAlias: SequencerAlias,
      domainId: DomainId,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, SequencerInfoLoaderError, Unit] =
    for {
      success <- sequencerConnectClient
        .handshake(
          alias,
          HandshakeRequest(
            clientProtocolVersions,
            minimumProtocolVersion,
          ),
          dontWarnOnDeprecatedPV,
        )
        .leftMap(SequencerInfoLoader.fromSequencerConnectClientError(alias))
        .subflatMap {
          case success: HandshakeResponse.Success => success.asRight
          case HandshakeResponse.Failure(_, reason) =>
            SequencerInfoLoaderError.HandshakeFailedError(reason).asLeft
        }
    } yield {
      logger.info(
        s"Version handshake with sequencer ${sequencerAlias} and domain using protocol version ${success.serverProtocolVersion} succeeded."
      )
      ()
    }

  def loadSequencerEndpoints(
      domainAlias: DomainAlias,
      sequencerConnections: SequencerConnections,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, SequencerInfoLoaderError, SequencerAggregatedInfo] =
    MonadUtil
      .parTraverseWithLimit(
        parallelism = sequencerInfoLoadParallelism.unwrap
      )(sequencerConnections.connections)(getBootstrapInfoDomainParameters(domainAlias))
      .leftMap(extractSingleError)
      .subflatMap(aggregateBootstrapInfo(sequencerConnections))

}

object SequencerInfoLoader {
  final case class SequencerAggregatedInfo(
      domainId: DomainId,
      staticDomainParameters: StaticDomainParameters,
      expectedSequencers: NonEmpty[Map[SequencerAlias, SequencerId]],
      sequencerConnections: SequencerConnections,
  )

  sealed trait SequencerInfoLoaderError extends Product with Serializable {
    def cause: String
  }
  object SequencerInfoLoaderError {
    final case class DeserializationFailure(cause: String) extends SequencerInfoLoaderError
    final case class InvalidResponse(cause: String) extends SequencerInfoLoaderError
    final case class InvalidState(cause: String) extends SequencerInfoLoaderError
    final case class HandshakeFailedError(cause: String) extends SequencerInfoLoaderError
    final case class DomainAliasDuplication(
        domainId: DomainId,
        alias: DomainAlias,
        previousDomainId: DomainId,
    ) extends SequencerInfoLoaderError {
      def cause =
        "The domain with the given alias reports a different domain id than the participant is expecting"
    }
    final case class SequencersFromDifferentDomainsAreConfigured(cause: String)
        extends SequencerInfoLoaderError

    final case class MisconfiguredStaticDomainParameters(cause: String)
        extends SequencerInfoLoaderError
    final case class FailedToConnectToSequencers(cause: String) extends SequencerInfoLoaderError
    final case class DomainIsNotAvailableError(alias: DomainAlias, cause: String)
        extends SequencerInfoLoaderError
  }

  def fromSequencerConnectClientError(alias: DomainAlias)(
      error: SequencerConnectClient.Error
  ): SequencerInfoLoaderError = error match {
    case SequencerConnectClient.Error.DeserializationFailure(e) =>
      SequencerInfoLoaderError.DeserializationFailure(e)
    case SequencerConnectClient.Error.InvalidResponse(cause) =>
      SequencerInfoLoaderError.InvalidResponse(cause)
    case SequencerConnectClient.Error.InvalidState(cause) =>
      SequencerInfoLoaderError.InvalidState(cause)
    case SequencerConnectClient.Error.Transport(message) =>
      SequencerInfoLoaderError.DomainIsNotAvailableError(alias, message)
  }
}
