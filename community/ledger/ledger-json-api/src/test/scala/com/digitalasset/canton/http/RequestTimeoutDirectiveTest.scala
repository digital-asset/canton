// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.http

import com.digitalasset.canton.http.RequestTimeoutDirective.Validated
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.model.headers.RawHeader
import org.apache.pekko.http.scaladsl.model.{HttpRequest, HttpResponse, StatusCodes}
import org.apache.pekko.http.scaladsl.server.Directives.*
import org.apache.pekko.http.scaladsl.server.Route
import org.apache.pekko.http.scaladsl.settings.ServerSettings
import org.apache.pekko.http.scaladsl.testkit.ScalatestRouteTest
import org.apache.pekko.http.scaladsl.unmarshalling.Unmarshal
import org.apache.pekko.pattern.after as pekkoAfter
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration.*
import scala.concurrent.{Await, Future}

class RequestTimeoutDirectiveTest
    extends AnyWordSpec
    with Matchers
    with ScalatestRouteTest
    with BeforeAndAfterAll {

  private val requestTimeoutHeader = RequestTimeoutDirective.RequestTimeoutHeaderName

  private val responseDelay = 1500.millis
  private val slowRoute: Route = RequestTimeoutDirective {
    get {
      onSuccess(pekkoAfter(responseDelay, system.scheduler)(Future.successful("slow done"))) {
        body =>
          complete(body)
      }
    }
  }
  private val serverSettings = ServerSettings(system).mapTimeouts(_.withRequestTimeout(30.seconds))

  private lazy val binding = Await.result(
    Http()
      .newServerAt("localhost", 0)
      .withSettings(serverSettings)
      .bind(slowRoute),
    30.seconds,
  )
  private lazy val baseUri = s"http://localhost:${binding.localAddress.getPort}/"

  private def call(headerValue: Option[String]): (HttpResponse, String) = {
    val request = HttpRequest(uri = baseUri).withHeaders(
      headerValue.map(RawHeader(requestTimeoutHeader, _)).toList
    )
    val response = Await.result(Http().singleRequest(request), 30.seconds)
    val body = Await.result(
      Unmarshal(response.entity).to[String],
      30.seconds,
    )
    (response, body)
  }

  override def afterAll(): Unit = {
    Await.result(binding.unbind(), 30.seconds)
    super.afterAll()
  }

  private def invalidMessage(rawValue: String): String =
    s"Invalid $requestTimeoutHeader header value '$rawValue': " +
      "expected a positive integer number of milliseconds."

  private def belowLowerBoundMessage(
      rawValue: String,
      configured: FiniteDuration = JsonApiConfig.defaultRequestTimeoutLowerBound,
  ): String =
    s"$requestTimeoutHeader '$rawValue' ms is below the minimum of " +
      s"${configured.toMillis} ms."

  private def aboveUpperBoundMessage(
      rawValue: String,
      configured: FiniteDuration = JsonApiConfig.defaultRequestTimeoutUpperBound,
  ): String =
    s"$requestTimeoutHeader '$rawValue' ms is above the maximum of " +
      s"${configured.toMillis} ms."

  "ClientRequestTimeoutConfig" should {
    "accept positive, ordered bounds with millisecond precision" in {
      ClientRequestTimeoutConfig(1500.millis, 2.seconds) shouldBe
        ClientRequestTimeoutConfig(1500.millis, 2.seconds)
    }

    "reject a lower bound below one millisecond" in {
      an[IllegalArgumentException] should be thrownBy ClientRequestTimeoutConfig(
        Duration.Zero,
        1.second,
      )
      an[IllegalArgumentException] should be thrownBy ClientRequestTimeoutConfig(
        500.micros,
        1.second,
      )
    }

    "reject a lower bound above the upper bound" in {
      an[IllegalArgumentException] should be thrownBy ClientRequestTimeoutConfig(
        2.seconds,
        1.second,
      )
    }
  }

  "RequestTimeoutDirective.validate" should {
    "reject a non-numeric value as invalid" in {
      RequestTimeoutDirective.validate("abc") shouldBe Validated.Invalid
      RequestTimeoutDirective.validate("") shouldBe Validated.Invalid
      RequestTimeoutDirective.validate("   ") shouldBe Validated.Invalid
      RequestTimeoutDirective.validate("1.5") shouldBe Validated.Invalid
    }

    "reject a zero or negative value as below the lower bound" in {
      RequestTimeoutDirective.validate("0") shouldBe Validated.BelowLowerBound
      RequestTimeoutDirective.validate("-5") shouldBe Validated.BelowLowerBound
    }

    "reject a value below the lower bound" in {
      val justBelow = (JsonApiConfig.defaultRequestTimeoutLowerBound.toMillis - 1).toString
      RequestTimeoutDirective.validate(justBelow) shouldBe Validated.BelowLowerBound
    }

    "accept the lower bound itself (inclusive)" in {
      RequestTimeoutDirective.validate(
        JsonApiConfig.defaultRequestTimeoutLowerBound.toMillis.toString
      ) shouldBe
        Validated.Enforce(JsonApiConfig.defaultRequestTimeoutLowerBound)
    }

    "accept a value within the window as-is" in {
      RequestTimeoutDirective.validate("5000") shouldBe Validated.Enforce(5000.millis)
    }

    "accept the upper bound itself (inclusive)" in {
      RequestTimeoutDirective.validate(
        JsonApiConfig.defaultRequestTimeoutUpperBound.toMillis.toString
      ) shouldBe
        Validated.Enforce(JsonApiConfig.defaultRequestTimeoutUpperBound)
    }

    "reject a value above the upper bound" in {
      val aboveUpper = (JsonApiConfig.defaultRequestTimeoutUpperBound.toMillis + 1).toString
      RequestTimeoutDirective.validate(aboveUpper) shouldBe
        Validated.AboveUpperBound
      RequestTimeoutDirective.validate("999999999") shouldBe
        Validated.AboveUpperBound
    }

    "trim surrounding whitespace" in {
      RequestTimeoutDirective.validate("  5000  ") shouldBe Validated.Enforce(5000.millis)
    }

    "honour explicitly-supplied lower and upper bounds instead of the defaults" in {
      val customLower = 2.seconds
      val customUpper = 10.seconds
      RequestTimeoutDirective.validate("1000", customLower, customUpper) shouldBe
        Validated.BelowLowerBound
      RequestTimeoutDirective.validate("5000", customLower, customUpper) shouldBe
        Validated.Enforce(5000.millis)
      RequestTimeoutDirective.validate("20000", customLower, customUpper) shouldBe
        Validated.AboveUpperBound
    }
  }

  "RequestTimeoutDirective (routing)" should {
    val invocations = new AtomicInteger(0)
    val innerRoute: Route = get {
      invocations.incrementAndGet()
      complete("ok")
    }
    val route: Route = Route.seal(RequestTimeoutDirective(innerRoute))

    "process the request normally when no header is present" in {
      invocations.set(0)
      Get() ~> route ~> check {
        status shouldBe StatusCodes.OK
        invocations.get() shouldBe 1
      }
    }

    "reject an invalid header value with 400 and not process the request" in {
      invocations.set(0)
      Get().withHeaders(RawHeader(requestTimeoutHeader, "not-a-number")) ~> route ~> check {
        status shouldBe StatusCodes.BadRequest
        responseAs[String] shouldBe invalidMessage("not-a-number")
        invocations.get() shouldBe 0
      }
    }

    "reject a below-lower-bound header value with 400 and not process the request" in {
      invocations.set(0)
      val belowLower = (JsonApiConfig.defaultRequestTimeoutLowerBound.toMillis - 1).toString
      Get().withHeaders(RawHeader(requestTimeoutHeader, belowLower)) ~> route ~> check {
        status shouldBe StatusCodes.BadRequest
        responseAs[String] shouldBe belowLowerBoundMessage(belowLower)
        invocations.get() shouldBe 0
      }
    }

    "reject an above-upper-bound header value with 400 and not process the request" in {
      invocations.set(0)
      val aboveUpper = (JsonApiConfig.defaultRequestTimeoutUpperBound.toMillis + 1).toString
      Get().withHeaders(RawHeader(requestTimeoutHeader, aboveUpper)) ~> route ~> check {
        status shouldBe StatusCodes.BadRequest
        responseAs[String] shouldBe aboveUpperBoundMessage(aboveUpper)
        invocations.get() shouldBe 0
      }
    }

    "process the request for an in-window header value" in {
      invocations.set(0)
      Get().withHeaders(RawHeader(requestTimeoutHeader, "5000")) ~> route ~> check {
        status shouldBe StatusCodes.OK
        invocations.get() shouldBe 1
      }
    }

  }

  "RequestTimeoutDirective (routing, with configured bounds)" should {
    val customLower = 2.seconds
    val customUpper = 10.seconds
    val invocations = new AtomicInteger(0)
    val innerRoute: Route = get {
      invocations.incrementAndGet()
      complete("ok")
    }
    val route: Route =
      Route.seal(RequestTimeoutDirective(innerRoute, customLower, customUpper))

    "reject a header value below the configured lower bound with 400" in {
      invocations.set(0)
      val belowLower = (customLower.toMillis - 1).toString
      Get().withHeaders(RawHeader(requestTimeoutHeader, belowLower)) ~> route ~> check {
        status shouldBe StatusCodes.BadRequest
        responseAs[String] shouldBe belowLowerBoundMessage(belowLower, customLower)
        invocations.get() shouldBe 0
      }
    }

    "reject a header value above the configured upper bound with 400" in {
      invocations.set(0)
      val aboveUpper = (customUpper.toMillis + 1).toString
      Get().withHeaders(RawHeader(requestTimeoutHeader, aboveUpper)) ~> route ~> check {
        status shouldBe StatusCodes.BadRequest
        responseAs[String] shouldBe aboveUpperBoundMessage(aboveUpper, customUpper)
        invocations.get() shouldBe 0
      }
    }

    "process a header value that is within the configured window" in {
      invocations.set(0)
      Get().withHeaders(RawHeader(requestTimeoutHeader, "5000")) ~> route ~> check {
        status shouldBe StatusCodes.OK
        invocations.get() shouldBe 1
      }
    }
  }

  "RequestTimeoutDirective (against a real server)" should {
    "use the server default timeout (no 408) when no header is present" in {
      call(headerValue = None)._1.status shouldBe StatusCodes.OK
    }

    "return 400 without waiting when the header is below the lower bound" in {
      val belowLower = (JsonApiConfig.defaultRequestTimeoutLowerBound.toMillis - 1).toString
      val (response, body) = call(headerValue = Some(belowLower))
      response.status shouldBe StatusCodes.BadRequest
      body shouldBe belowLowerBoundMessage(belowLower)
    }

    "return 400 without waiting when the header is above the upper bound" in {
      val aboveUpper = (JsonApiConfig.defaultRequestTimeoutUpperBound.toMillis + 1).toString
      val (response, body) = call(headerValue = Some(aboveUpper))
      response.status shouldBe StatusCodes.BadRequest
      body shouldBe aboveUpperBoundMessage(aboveUpper)
    }

    "return 400 without waiting when the header is invalid" in {
      val (response, body) = call(headerValue = Some("not-a-number"))
      response.status shouldBe StatusCodes.BadRequest
      body shouldBe invalidMessage("not-a-number")
    }

    "return 408 when the request exceeds an in-window client timeout" in {
      val inWindow = JsonApiConfig.defaultRequestTimeoutLowerBound.toMillis.toString
      // The 408 Request Timeout response carries an empty entity, so there is no body to assert.
      call(headerValue = Some(inWindow))._1.status shouldBe StatusCodes.RequestTimeout
    }

  }
}
