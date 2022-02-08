// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.networking.http

import java.io.IOException
import java.security._
import java.util.Base64
import better.files._
import cats.Show
import cats.data.EitherT
import cats.instances.future._
import cats.syntax.either._
import com.digitalasset.canton.concurrent.ExecutorServiceExtensions
import com.digitalasset.canton.config.{KeyStoreConfig, Password, ProcessingTimeout}
import com.digitalasset.canton.crypto.X509CertificatePem
import com.digitalasset.canton.crypto.store.{ProtectedKeyStore, TrustStore}
import com.digitalasset.canton.lifecycle.{AsyncCloseable, FlagCloseable, Lifecycle}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging, TracedLogger}
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.EitherTUtil
import com.google.protobuf.ByteString
import io.circe.syntax._
import io.circe.{Decoder, Encoder}

import javax.net.ssl._
import okhttp3.OkHttpClient
import sttp.capabilities.WebSockets
import sttp.client3
import sttp.client3._
import sttp.client3.circe.asJson
import sttp.client3.okhttp.OkHttpFutureBackend
import sttp.model.{Header, StatusCode, Uri}

import scala.concurrent.{ExecutionContext, Future}

/** A generic HTTP client with client certificate authentication and optional request signing.
  *
  * Provides methods to POST with JSON as well as protobuf/bytearray requests and responses.
  * Provides GET method with JSON response.
  */
class HttpClient private (
    sslContext: SSLContext,
    trustManager: X509TrustManager,
    protectedKeyStore: ProtectedKeyStore,
    keyName: Option[String],
)(
    protected override val timeouts: ProcessingTimeout,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends NamedLogging
    with FlagCloseable {

  import HttpClientError._

  // CCF doesn't use hostname verification but only relies a trusted root CA that signs all nodes' certs.
  private val hostnameVerifier = new HostnameVerifier {
    override def verify(s: String, sslSession: SSLSession): Boolean = true
  }

  private val okHttpClient =
    new OkHttpClient.Builder()
      .sslSocketFactory(sslContext.getSocketFactory, trustManager)
      .hostnameVerifier(hostnameVerifier)
      .build()

  private implicit val sttpBackend: SttpBackend[Future, WebSockets] =
    OkHttpFutureBackend.usingClient(okHttpClient)

  private def sha256(bytes: ByteString): ByteString = {
    val md = MessageDigest.getInstance("SHA-256")
    ByteString.copyFrom(md.digest(bytes.toByteArray))
  }

  private def base64(bytes: ByteString): String =
    Base64.getEncoder.encodeToString(bytes.toByteArray)

  // Sign payload with private key from keystore
  private def sign(bytes: ByteString): Either[String, ByteString] =
    for {
      key <- Either
        .catchNonFatal(
          protectedKeyStore.store
            .getKey(keyName.getOrElse("ccf-member"), protectedKeyStore.password.toCharArray)
        )
        .leftMap(err => s"Failed to get key from keystore: $err")
      privateKey <- key match {
        case pkey: PrivateKey => Right(pkey)
        case _ => Left(s"Key from keystore is not a private key")
      }
      // Signs the request with EC-DSA using P-384, but hash with sha256
      signature = Signature.getInstance("SHA256withECDSA")
      _ = signature.initSign(privateKey)
      _ = signature.update(bytes.toByteArray)
      signatureBytes <- Either
        .catchOnly[SignatureException](signature.sign())
        .leftMap(err => s"Failed to sign: $err")
    } yield ByteString.copyFrom(signatureBytes)

  /** Authorization headers using signatures: https://tools.ietf.org/html/draft-cavage-http-signatures-12 */
  private def signatureHeaders(
      uri: Uri,
      body: Array[Byte],
  ): Either[HttpClientError, Map[String, String]] = {

    val requestHash = base64(sha256(ByteString.copyFrom(body)))
    val contentLength = body.length

    val stringToSign = s"""(request-target): post /${uri.path.mkString("/")}
                          |digest: SHA-256=$requestHash
                          |content-length: $contentLength""".stripMargin

    for {
      signature <- sign(ByteString.copyFromUtf8(stringToSign))
        .map(base64)
        .leftMap(FailedToSignRequest)

      headers = Map(
        "digest" -> s"SHA-256=$requestHash",
        "authorization" -> s"""Signature keyId="tls",algorithm="ecdsa-sha256",headers="(request-target) digest content-length",signature="$signature"""",
      )
    } yield headers
  }

  private def sendRequest[Res, Err](request: RequestT[client3.Identity, Either[Err, Res], Any])(
      errHandler: (StatusCode, Err) => HttpClientError
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, HttpClientError, HttpClient.Response[Res]] = {

    def send() = for {
      response <- EitherTUtil.fromFuture(
        request.send[Future, WebSockets](sttpBackend),
        {
          case err: SttpClientException => HttpRequestNetworkError(s"Sending $request failed: $err")
          case err => throw err
        },
      )
      body <- EitherT.fromEither(response.body.leftMap(err => errHandler(response.code, err)))
    } yield HttpClient.Response(body, response.headers)

    performUnlessClosing(send()).onShutdown {
      logger.info("Shutdown in progress, not sending request")
      EitherT.leftT(HttpClientError.ShutdownInProgressError)
    }
  }

  private def jsonErrorHandler(
      code: StatusCode,
      error: ResponseException[String, Exception],
  ): HttpClientError =
    error match {
      case HttpError(body, statusCode) =>
        HttpRequestProtocolError(
          code,
          s"Failed to submit request: $body and status code: $statusCode",
        )
      case DeserializationException(body, error) =>
        HttpResponseDeserializationError(
          s"Failed to deserialize response: error=${error.getMessage}, errorBody=$body"
        )

    }

  def postAsBytes(
      uri: Uri,
      requestBody: Array[Byte],
      headers: Map[String, String] = Map.empty,
  )(implicit
      traceContext: TraceContext
  ): EitherT[Future, HttpClientError, HttpClient.Response[Array[Byte]]] =
    sendRequest(basicRequest.post(uri).headers(headers).body(requestBody).response(asByteArray)) {
      (code, err) =>
        HttpRequestProtocolError(code, s"Failed to submit request: error=$err")
    }

  def postAsJson[Req, Res](uri: Uri, requestBody: Req, signRequest: Boolean = true)(implicit
      encoder: Encoder[Req],
      decoder: Decoder[Res],
      traceContext: TraceContext,
  ): EitherT[Future, HttpClientError, HttpClient.Response[Res]] = {

    // CCF doesn't like the charset utf8 in content-type, therefore we pass in raw bytes
    val requestBytes = requestBody.asJson.noSpaces.getBytes

    val baseRequest = basicRequest
      .post(uri)
      .contentType("application/json")
      .body(requestBytes)
      .response(asJson[Res])

    for {
      // Sign the request if needed before sending it off
      finalRequest <-
        if (signRequest) {
          signatureHeaders(uri, requestBytes).toEitherT.map(baseRequest.headers(_))
        } else EitherT.rightT(baseRequest)
      response <- sendRequest(finalRequest)(jsonErrorHandler)
    } yield response

  }

  def getAsJson[Res](uri: Uri)(implicit
      decoder: Decoder[Res],
      traceContext: TraceContext,
  ): EitherT[Future, HttpClientError, HttpClient.Response[Res]] =
    sendRequest(basicRequest.get(uri).response(asJson[Res]))(jsonErrorHandler)

  override protected def onClosed(): Unit = {
    import TraceContext.Implicits.Empty._
    Lifecycle.close(
      AsyncCloseable(
        "http-client-sttp-backend",
        sttpBackend.close(),
        timeouts.shutdownNetwork.unwrap,
      ),
      // by default sttp wont shut down okhttp on our behalf so we'll do it ourselves
      ExecutorServiceExtensions(okHttpClient.dispatcher().executorService())(logger, timeouts),
    )(logger)
  }
}

object HttpClient {

  import HttpClientError._

  case class Response[Res](body: Res, headers: Seq[Header])

  def create(trustStore: TrustStore, protectedKeyStore: ProtectedKeyStore, keyName: Option[String])(
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit ec: ExecutionContext, tc: TraceContext): Either[HttpClientError, HttpClient] = {
    val logger = loggerFactory.getTracedLogger(HttpClient.getClass)

    for {
      customSslContext <- buildCustomSslContext(trustStore, protectedKeyStore, logger)
        .leftMap[HttpClientError](FailedToBuildSslContext)
      (sslContext, trustManager) = customSslContext
      client = new HttpClient(sslContext, trustManager, protectedKeyStore, keyName)(
        timeouts,
        loggerFactory,
      )
    } yield client
  }

  def create(
      certificate: X509CertificatePem,
      protectedKeyStore: ProtectedKeyStore,
      keyName: Option[String],
  )(timeouts: ProcessingTimeout, loggerFactory: NamedLoggerFactory)(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): Either[HttpClientError, HttpClient] =
    for {
      trustStore <- TrustStore.create(certificate).leftMap(FailedToLoadTrustStore)
      client <- create(trustStore, protectedKeyStore, keyName)(timeouts, loggerFactory)
    } yield client

  def create(
      certificate: X509CertificatePem,
      keyStoreConfig: KeyStoreConfig,
      keyName: Option[String],
  )(timeouts: ProcessingTimeout, loggerFactory: NamedLoggerFactory)(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): Either[HttpClientError, HttpClient] = {
    val logger = loggerFactory.getTracedLogger(HttpClient.getClass)

    for {
      trustStore <- TrustStore.create(certificate).leftMap(FailedToLoadTrustStore)
      protectedKeyStore <- loadKeyStore(keyStoreConfig, logger)
      client <- create(trustStore, protectedKeyStore, keyName)(timeouts, loggerFactory)
    } yield client
  }

  @SuppressWarnings(Array("org.wartremover.warts.Null")) // legacy java APIs are quite fond of nulls
  def buildCustomSslContext(
      trustStore: TrustStore,
      protectedKeyStore: ProtectedKeyStore,
      logger: TracedLogger,
  )(implicit tc: TraceContext): Either[String, (SSLContext, X509TrustManager)] =
    for {
      sslContext <- Either
        .catchOnly[NoSuchAlgorithmException](SSLContext.getInstance("TLS"))
        .leftMap(err => s"Failed to get SSL context instance: $err")

      trustManagerFactory <- Either
        .catchOnly[NoSuchAlgorithmException](
          TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm)
        )
        .leftMap(err => s"Failed to create trust manager factory: $err")

      _ <- Either
        .catchOnly[KeyStoreException](trustManagerFactory.init(trustStore.unwrap))
        .leftMap(err => s"Failed to initialize trust manager factory: $err")

      keyManagerFactory <- Either
        .catchOnly[NoSuchAlgorithmException](
          KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm)
        )
        .leftMap(err => s"Failed to create key manager factory: $err")

      _ <- Either
        .catchNonFatal(
          keyManagerFactory.init(protectedKeyStore.store, protectedKeyStore.password.toCharArray)
        )
        .leftMap(err => s"Failed to initialize key manager factory: $err")

      trustManagers = trustManagerFactory.getTrustManagers
      keyManagers = keyManagerFactory.getKeyManagers

      _ <- Either
        .catchOnly[KeyManagementException](
          sslContext.init(
            keyManagers,
            trustManagers,
            null, // we don't need to customize the random provider
          )
        )
        .leftMap(err => s"Failed to initialize SSL context: $err")

      _ = sslContext.createSSLEngine()

      trustManager <- trustManagers match {
        // OkHttp only accept a single trust manager
        case Array(tm: X509TrustManager) => Right(tm)
        case Array(tm: X509TrustManager, _*) =>
          logger.info(s"More than one trust manager, ignoring the others.")
          Right(tm)
        case _ => Left(s"Invalid trust managers: $trustManagers")
      }
    } yield (sslContext, trustManager)

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  def loadKeyStore[A](
      keyStoreConfig: KeyStoreConfig,
      logger: TracedLogger,
  )(implicit tc: TraceContext): Either[HttpClientError, ProtectedKeyStore] = {
    val password = keyStoreConfig.password
    loadP12Store(keyStoreConfig.path.toScala, password, logger)
      .map(store => ProtectedKeyStore(store, password))
      .leftMap(FailedToLoadKeyStore)
  }

  @SuppressWarnings(Array("org.wartremover.warts.IsInstanceOf"))
  private def loadP12Store(
      path: File,
      password: Password,
      logger: TracedLogger,
  )(implicit tc: TraceContext): Either[String, KeyStore] =
    for {
      store <- Either
        .catchOnly[KeyStoreException](KeyStore.getInstance("PKCS12"))
        .leftMap(err => s"Failed to create key store instance: $err")

      _ <- Either
        .catchNonFatal(path.inputStream.foreach(store.load(_, password.toCharArray)))
        .leftMap {
          case ioException: IOException
              if ioException.getCause.isInstanceOf[UnrecoverableKeyException] =>
            logger.warn(
              "encountered IOException caused by a UnrecoverableKeyException while loading a keystore likely indicating an incorrect password was used",
              ioException,
            )
            s"Password for the store at [$path] is likely incorrect"
          case ex => ex.toString
        }

      // make sure the store is not empty
      _ <- Either.cond(store.size() > 0, (), s"store at [$path] is empty")
    } yield store

}

sealed trait HttpClientError extends Product with Serializable

object HttpClientError {

  case class FailedToLoadTrustStore(reason: String) extends HttpClientError
  case class FailedToLoadKeyStore(reason: String) extends HttpClientError
  case class FailedToBuildSslContext(reason: String) extends HttpClientError
  case class FailedToSignRequest(reason: String) extends HttpClientError
  case class HttpRequestNetworkError(reason: String) extends HttpClientError
  case object ShutdownInProgressError extends HttpClientError
  case class HttpRequestProtocolError(code: StatusCode, reason: String) extends HttpClientError
  case class HttpResponseDeserializationError(reason: String) extends HttpClientError

  implicit val show: Show[HttpClientError] = Show.fromToString
}
