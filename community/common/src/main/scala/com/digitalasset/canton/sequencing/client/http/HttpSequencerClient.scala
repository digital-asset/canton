// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.sequencing.client.http

import cats.data.EitherT
import cats.syntax.either.*
import cats.syntax.traverse.*
import com.digitalasset.canton.config.{Password, ProcessingTimeout}
import com.digitalasset.canton.crypto.Crypto
import com.digitalasset.canton.crypto.store.ProtectedKeyStore
import com.digitalasset.canton.domain.api.v0.SequencerConnect.GetDomainParameters.Response.Parameters
import com.digitalasset.canton.domain.api.v0 as domainProto
import com.digitalasset.canton.lifecycle.{FlagCloseable, Lifecycle}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.networking.http.HttpClientError.{
  HttpRequestProtocolError,
  HttpResponseDeserializationError,
}
import com.digitalasset.canton.networking.http.{HttpClient, HttpClientError}
import com.digitalasset.canton.protocol.StaticDomainParameters
import com.digitalasset.canton.sequencing.client.SendAsyncClientError
import com.digitalasset.canton.sequencing.client.http.HttpSequencerEndpoints.endpointVersion
import com.digitalasset.canton.sequencing.protocol.*
import com.digitalasset.canton.sequencing.{HttpSequencerConnection, OrdinarySerializedEvent}
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.store.SequencedEventStore.OrdinarySequencedEvent
import com.digitalasset.canton.topology.{DomainId, Member}
import com.digitalasset.canton.tracing.{
  SerializableTraceContext,
  TraceContext,
  Traced,
  TracingConfig,
}
import com.digitalasset.canton.util.retry
import com.digitalasset.canton.util.retry.RetryUtil.AllExnRetryable
import com.digitalasset.canton.version.ProtocolVersion
import com.digitalasset.canton.{ProtoDeserializationError, SequencerCounter}
import io.circe.Codec
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.semiauto.deriveEnumerationCodec
import io.circe.generic.semiauto.deriveCodec
import sttp.client3.*
import sttp.model.{Header, Uri}

import java.net.URL
import scala.concurrent.duration.*
import scala.concurrent.{ExecutionContext, Future}

/** Low-level HTTP Sequencer API Client.
  *
  * Provides the send and a subscribe/polling methods for the sequencer client, otherwise it is a thin layer on top of the generic HTTP client.
  * Specific to a CCF sequencer, this client takes care of waiting for the global commit of write operations.
  *
  * @param endpoints Write and read URLs to the HTTP sequencer API.
  * @param httpClient A generic HTTP client that provides client certificate authentication.
  */
class HttpSequencerClient(
    endpoints: HttpSequencerEndpoints,
    httpClient: HttpClient,
    traceContextPropagation: TracingConfig.Propagation,
    override protected val timeouts: ProcessingTimeout,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends FlagCloseable
    with NamedLogging {

  import HttpSequencerClientError.*

  /** CCF returns the body and in the headers the transaction sequencer number and transaction view, which we use to
    * validate that a transaction has been globally committed.
    */
  private case class TxMeta(txView: Int, txSeqNo: Int)

  private def extractTxMeta(headers: Seq[Header]): Either[String, TxMeta] = {
    val ccfTxViewHeader = "x-ccf-tx-view"
    val ccfTxSeqNoHeader = "x-ccf-tx-seqno"

    def extractHeader(headers: Seq[Header], name: String): Option[String] = {
      headers.find(_.name.equalsIgnoreCase(name)).map(_.value)
    }

    for {
      txSeqNo <- extractHeader(headers, ccfTxSeqNoHeader)
        .toRight(s"Missing header $ccfTxSeqNoHeader in response: $headers")
        .flatMap(str =>
          Either
            .catchOnly[NumberFormatException](str.toInt)
            .leftMap(err => s"Failed to convert CCF transaction sequencer number: $err")
        )
      txView <- extractHeader(headers, ccfTxViewHeader)
        .toRight(s"Missing header $ccfTxViewHeader in response: $headers")
        .flatMap(str =>
          Either
            .catchOnly[NumberFormatException](str.toInt)
            .leftMap(err => s"Failed to convert CCF transaction view number: $err")
        )
    } yield TxMeta(txView, txSeqNo)
  }

  private def uriWrite(packageName: String, serviceName: String, endpointName: String): Uri =
    uri(endpoints.write, packageName, serviceName, endpointName)

  private def uriRead(packageName: String, serviceName: String, endpointName: String): Uri =
    uri(endpoints.write, packageName, serviceName, endpointName)

  private def uri(base: URL, packageName: String, serviceName: String, endpointName: String): Uri =
    uri"$base/app/com.digitalasset.canton.$packageName.$endpointVersion.$serviceName/$endpointName"

  /** Wait until the given transaction is globally committed.
    * See https://microsoft.github.io/CCF/main/use_apps/verify_tx.html#checking-for-commit
    */
  private def waitForCommitted(txView: Int, txSeqNo: Int)(implicit
      traceContext: TraceContext
  ): EitherT[Future, HttpSequencerClientError, Unit] = {

    // Retry until the transaction is not pending anymore
    implicit val success: retry.Success[Either[HttpSequencerClientError, TransactionStatus]] =
      retry.Success(_ != Right(TransactionStatus.Pending))

    EitherT {
      retry
        .Pause(logger, this, maxRetries = 100, delay = 30.milliseconds, "wait for committed")
        .apply(
          {
            httpClient
              .getAsJson[TransactionStatusResponse](
                uri"${endpoints.write}/app/tx?view=$txView&seqno=$txSeqNo"
              )
              .map(_.body.status)
              .leftMap[HttpSequencerClientError](err =>
                TransactionCheckFailed(txView, txSeqNo, err.toString)
              )
              .value
          },
          AllExnRetryable,
        )
    }.flatMap {
      case TransactionStatus.Committed => EitherT.rightT(())
      case TransactionStatus.Invalid => EitherT.leftT(InvalidTransaction(txView, txSeqNo))
      case TransactionStatus.Unknown => EitherT.leftT(UnknownTransaction(txView, txSeqNo))
      case TransactionStatus.Pending => EitherT.leftT(TransactionTimeout(txView, txSeqNo))
    }
  }

  def sendAsync(
      submission: SubmissionRequest,
      requiresAuthentication: Boolean,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, SendAsyncClientError, Unit] =
    for {
      _ <- httpClient
        .postAsBytes(
          uriWrite(
            "domain.api",
            "SequencerService",
            if (requiresAuthentication) "SendAsync" else "SendAsyncUnauthenticated",
          ),
          submission.toByteArray,
          generateHeaders,
        )
        .leftMap[SendAsyncClientError](err => SendAsyncClientError.RequestFailed(err.toString))
    } yield ()

  private def generateHeaders(implicit traceContext: TraceContext): Map[String, String] = {
    traceContextPropagation match {
      // if tracing is enabled and we have a trace to present
      // populate headers conforming to w3c tracecontext spec: https://www.w3.org/TR/trace-context
      case TracingConfig.Propagation.Enabled =>
        traceContext.asW3CTraceContext.map(_.asHeaders).getOrElse(Map.empty)
      case _ => Map.empty[String, String]
    }
  }

  def handshakeUnauthenticated(
      request: HandshakeRequest
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, HttpSequencerClientError, HandshakeResponse] =
    for {
      response <- httpClient
        // Unauthorized version
        .postAsBytes(
          uriWrite("domain.api", "SequencerConnectService", "Handshake"),
          request.toByteArrayV0,
        )
        .leftMap(ClientError(_))
      handshakeP <- ProtoConverter
        .protoParserArray(com.digitalasset.canton.protocol.v0.Handshake.Response.parseFrom)(
          response.body
        )
        .leftMap[HttpSequencerClientError](DeserializationError(_))
        .toEitherT[Future]
      handshake <- HandshakeResponse
        .fromProtoV0(handshakeP)
        .leftMap[HttpSequencerClientError](DeserializationError(_))
        .toEitherT[Future]
    } yield handshake

  def getDomainId()(implicit
      traceContext: TraceContext
  ): EitherT[Future, HttpSequencerClientError, DomainId] =
    for {
      response <- httpClient
        .postAsBytes(
          uriWrite("domain.api", "SequencerConnectService", "GetDomainId"),
          domainProto.SequencerConnect.GetDomainId.Request().toByteString.toByteArray,
        )
        .leftMap(ClientError(_))
      responseP <- ProtoConverter
        .protoParserArray(domainProto.SequencerConnect.GetDomainId.Response.parseFrom)(
          response.body
        )
        .leftMap[HttpSequencerClientError](DeserializationError(_))
        .toEitherT[Future]
      response <- DomainId
        .fromProtoPrimitive(responseP.domainId, "domainId")
        .leftMap[HttpSequencerClientError](DeserializationError(_))
        .toEitherT[Future]
    } yield response

  def getDomainParameters()(implicit
      traceContext: TraceContext
  ): EitherT[Future, HttpSequencerClientError, StaticDomainParameters] =
    for {
      response <- httpClient
        .postAsBytes(
          uriWrite("domain.api", "SequencerConnectService", "GetDomainParameters"),
          domainProto.SequencerConnect.GetDomainParameters.Request().toByteString.toByteArray,
        )
        .leftMap(ClientError(_))
      responseP <- ProtoConverter
        .protoParserArray(domainProto.SequencerConnect.GetDomainParameters.Response.parseFrom)(
          response.body
        )
        .leftMap[HttpSequencerClientError](DeserializationError(_))
        .toEitherT[Future]

      domainParametersE = responseP.parameters match {
        case Parameters.Empty =>
          Left(ProtoDeserializationError.FieldNotSet("GetDomainParameters.parameters"))
        case Parameters.ParametersV0(parametersV0) =>
          StaticDomainParameters.fromProtoV0(parametersV0)
        case Parameters.ParametersV1(parametersV1) =>
          StaticDomainParameters.fromProtoV1(parametersV1)
      }
      domainParameters <- EitherT
        .fromEither[Future](domainParametersE)
        .leftMap[HttpSequencerClientError](DeserializationError(_))

    } yield domainParameters

  def verifyActive(
      request: VerifyActiveRequest
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, HttpSequencerClientError, VerifyActiveResponse] =
    httpClient
      .postAsBytes(
        uriWrite("domain.api", "SequencerConnectService", "VerifyActive"),
        request.toByteArrayV0,
      )
      .biflatMap(
        {
          case HttpRequestProtocolError(statusCode, reason)
              if statusCode.code == 403 && reason == "Failed to submit request: error=Could not find matching user certificate" =>
            EitherT.pure[Future, HttpSequencerClientError](
              VerifyActiveResponse.Success(isActive = false)
            )

          case other => EitherT.fromEither[Future](Left(ClientError(other)))
        },
        { response =>
          for {
            responseP <- ProtoConverter
              .protoParserArray(domainProto.SequencerConnect.VerifyActive.Response.parseFrom)(
                response.body
              )
              .leftMap[HttpSequencerClientError](DeserializationError(_))
              .toEitherT[Future]
            response <- EitherT
              .fromEither[Future](VerifyActiveResponse.fromProtoV0(responseP))
              .leftMap[HttpSequencerClientError](DeserializationError(_))
          } yield response
        },
      )

  def readNextEvent(
      member: Member,
      protocolVersion: ProtocolVersion,
      requiresAuthentication: Boolean,
  )(
      tracedCounter: Traced[SequencerCounter]
  )(implicit
      executionContext: ExecutionContext
  ): EitherT[Future, HttpSequencerClientError, Option[OrdinarySerializedEvent]] =
    tracedCounter.withTraceContext { implicit traceContext => counter =>
      for {
        response <- httpClient
          .postAsBytes(
            uriRead(
              "domain.api",
              "SequencerService",
              if (requiresAuthentication) "Recv" else "RecvUnauthenticated",
            ),
            // if we ever introduce a V1 SubscriptionRequest, then we will need to bump the endpoint
            SubscriptionRequest(member, counter, protocolVersion).toByteArray,
          )
          .leftMap(ClientError)
        txMeta <- EitherT
          .fromEither[Future](extractTxMeta(response.headers))
          .leftMap[HttpSequencerClientError](err =>
            ClientError(HttpResponseDeserializationError(err))
          )
        response <- ProtoConverter
          // if we ever want to be able to parse a V1 SubscriptionResponse, then those should be send to an endpoint with
          // a bumped version number
          .protoParserArray(domainProto.SubscriptionResponse.parseFrom)(response.body)
          .leftMap(DeserializationError)
          .toEitherT[Future]
        traceContext = response.traceContext
          .flatMap(tc => SerializableTraceContext.fromProtoV0(tc).toOption)
          .fold(TraceContext.empty)(_.unwrap)
        mbEvent = response.signedSequencedEvent
        mbSigned <- mbEvent
          .traverse(
            SignedContent.fromProtoV0(SequencedEvent.fromByteString(ClosedEnvelope.fromProtoV0), _)
          )
          .leftMap[HttpSequencerClientError](DeserializationError)
          .toEitherT[Future]
        // If we have an event, wait for global commit in CCF before returning from read
        // If there's no event returned there's no need to wait that the empty response was committed
        // There's no trace context here as we're polling for the next item which itself may be traced
        _ <- mbSigned.fold(EitherT.pure[Future, HttpSequencerClientError](())) { _ =>
          waitForCommitted(txMeta.txView, txMeta.txSeqNo)(TraceContext.empty)
        }

      } yield mbSigned.map(OrdinarySequencedEvent(_)(traceContext))
    }

  override protected def onClosed(): Unit =
    Lifecycle.close(httpClient)(logger)

}

object HttpSequencerClient {
  def apply(
      crypto: Crypto,
      connection: HttpSequencerConnection,
      processingTimeout: ProcessingTimeout,
      traceContextPropagation: TracingConfig.Propagation,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      executionContext: ExecutionContext,
      traceContext: TraceContext,
  ): EitherT[Future, String, HttpSequencerClient] =
    for {
      keystore <- crypto.javaKeyConverter
        .toJava(crypto.cryptoPrivateStore, crypto.cryptoPublicStore, Password.empty)
        .leftMap(err => s"Failed to create Java keystore: $err")
      httpClient <- HttpClient.Insecure
        .create(connection.certificate, ProtectedKeyStore(keystore, Password.empty), None)(
          processingTimeout,
          loggerFactory,
        )
        .leftMap(err => s"Failed to create http transport for sequencer client: $err")
        .toEitherT[Future]
    } yield new HttpSequencerClient(
      connection.urls,
      httpClient,
      traceContextPropagation,
      processingTimeout,
      loggerFactory,
    )
}

/** CCF returns a transaction status to check for global commit. */
case class TransactionStatusResponse(status: TransactionStatus)
object TransactionStatusResponse {
  @annotation.nowarn("cat=lint-byname-implicit")
  implicit val codec: Codec[TransactionStatusResponse] = deriveCodec[TransactionStatusResponse]
}

/** Possible states of a transaction: https://microsoft.github.io/CCF/ccf-0.11.7/users/issue_commands.html#checking-for-commit */
sealed trait TransactionStatus
object TransactionStatus {

  case object Unknown extends TransactionStatus
  case object Pending extends TransactionStatus
  case object Committed extends TransactionStatus
  case object Invalid extends TransactionStatus

  private implicit val config: Configuration =
    Configuration.default.copy(transformConstructorNames = _.toUpperCase)

  @annotation.nowarn("cat=lint-byname-implicit")
  implicit val codec: Codec[TransactionStatus] = deriveEnumerationCodec[TransactionStatus]
}

sealed trait HttpSequencerClientError extends Product with Serializable
object HttpSequencerClientError {

  case class ClientError(error: HttpClientError) extends HttpSequencerClientError
  case class DeserializationError(error: ProtoDeserializationError) extends HttpSequencerClientError

  case class TransactionTimeout(txView: Int, txSeqNo: Int) extends HttpSequencerClientError
  case class InvalidTransaction(txView: Int, txSeqNo: Int) extends HttpSequencerClientError
  case class UnknownTransaction(txView: Int, txSeqNo: Int) extends HttpSequencerClientError

  case class TransactionCheckFailed(txView: Int, txSeqNo: Int, reason: String)
      extends HttpSequencerClientError
}
