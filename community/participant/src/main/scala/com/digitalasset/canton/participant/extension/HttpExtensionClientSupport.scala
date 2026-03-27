// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.extension

import com.digitalasset.canton.concurrent.Threading

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.time.Duration
import java.util.UUID
import scala.jdk.CollectionConverters.*
import scala.util.Random

private[extension] trait HttpExtensionClientRuntime {
  def nowMillis(): Long
  def sleepMillis(ms: Long): Unit
  def newRequestId(): String
  def nextRetryJitterDouble(): Double
}

private[extension] object HttpExtensionClientRuntime {
  val system: HttpExtensionClientRuntime = new HttpExtensionClientRuntime {
    override def nowMillis(): Long = System.currentTimeMillis()
    override def sleepMillis(ms: Long): Unit = Threading.sleep(ms)
    override def newRequestId(): String = UUID.randomUUID().toString
    override def nextRetryJitterDouble(): Double = Random.nextDouble()
  }
}

private[extension] final case class HttpExtensionClientRequest(
    uri: URI,
    timeout: Duration,
    headers: Seq[(String, String)],
    body: String,
)

private[extension] final case class HttpExtensionClientResponse(
    statusCode: Int,
    body: String,
    headers: Map[String, Seq[String]],
)

private[extension] trait HttpExtensionClientTransport {
  def send(request: HttpExtensionClientRequest): HttpExtensionClientResponse
}

private[extension] final class JdkHttpExtensionClientTransport(httpClient: HttpClient)
    extends HttpExtensionClientTransport {

  override def send(request: HttpExtensionClientRequest): HttpExtensionClientResponse = {
    val requestBuilder = HttpRequest
      .newBuilder()
      .uri(request.uri)
      .timeout(request.timeout)

    request.headers.foreach { case (header, value) =>
      val _ = requestBuilder.header(header, value)
    }

    val httpRequest = requestBuilder
      .POST(HttpRequest.BodyPublishers.ofString(request.body))
      .build()

    val response = httpClient.send(httpRequest, HttpResponse.BodyHandlers.ofString())

    HttpExtensionClientResponse(
      statusCode = response.statusCode(),
      body = response.body(),
      headers = response.headers().map().asScala.iterator.map { case (header, values) =>
        header -> values.asScala.toSeq
      }.toMap,
    )
  }
}
