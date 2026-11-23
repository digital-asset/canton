// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.networking.grpc.ratelimiting

import com.digitalasset.canton.config.RateLimitersConfig
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.metrics.RateLimitMetrics
import com.digitalasset.canton.networking.grpc.ratelimiting.IpAwareRateLimitInterceptor.{
  AdmissionResult,
  RejectionReason,
}
import com.digitalasset.canton.networking.grpc.ratelimiting.LimitResult.FullMethodName
import com.digitalasset.canton.networking.grpc.{CantonGrpcUtil, GrpcAddressHelper}
import com.digitalasset.canton.tracing.{TraceContext, TraceContextGrpc}
import com.google.common.net.InetAddresses
import io.grpc.*

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}

/** Rate limits gRPC calls by full method name and, if configured, by client IP (with per-CIDR
  * overrides), using [[TokenBucketRateLimiter]] and [[PerAttributeRateLimiter]].
  *
  * Intended to run *before* [[ActiveRequestCounterInterceptor]] in the interceptor chain (see
  * `CantonCommunityServerInterceptors`).
  */
class IpAwareRateLimitInterceptor(
    config: RateLimitersConfig,
    maxLoggingRatePerSecond: NonNegativeInt,
    metrics: RateLimitMetrics,
    val loggerFactory: NamedLoggerFactory,
) extends ServerInterceptor
    with NamedLogging {

  private val globalRateLimiter: (TokenBucketRateLimiter, PerAttributeRateLimiter) =
    (
      new TokenBucketRateLimiter(
        IpAwareRateLimitInterceptor.GlobalLimiter,
        config.global,
        metrics,
      ),
      new PerAttributeRateLimiter(
        IpAwareRateLimitInterceptor.GlobalLimiter,
        IpAwareRateLimitInterceptor.ClientIpAttribute,
        config.global.perClientIp,
        metrics,
        logger,
        IpCidrRateLimits.matchClientIp,
      ),
    )

  // Metadata keys are resolved once rather than per request. Only the headers the operator
  // explicitly configured are trusted, see RateLimitersConfig.clientIpHeaders. Blank entries are
  // dropped as splice does, because Metadata.Key.of rejects a name that is empty or padded with
  // spaces and would fail node startup over a stray entry in the config list. A list that names
  // no usable header at all is rejected when the config is validated, so the node refuses to start
  // instead of rate limiting every client of a proxy as one (as it works in splice).
  private val clientIpHeaderKeys: Seq[Metadata.Key[String]] =
    config.clientIpHeaders
      .map(_.trim)
      .filter(_.nonEmpty)
      .map(Metadata.Key.of(_, Metadata.ASCII_STRING_MARSHALLER))

  // need to cache it as interceptCall is invoked for every request
  private val perMethodRateLimiters =
    new ConcurrentHashMap[FullMethodName, (TokenBucketRateLimiter, PerAttributeRateLimiter)]()

  private def perMethodRateLimiter(
      fullMethodName: FullMethodName
  ): (TokenBucketRateLimiter, PerAttributeRateLimiter) =
    perMethodRateLimiters.computeIfAbsent(
      fullMethodName,
      { method =>
        val methodConfig = config.forRateLimiter(method)
        (
          new TokenBucketRateLimiter(method, methodConfig, metrics),
          new PerAttributeRateLimiter(
            method,
            IpAwareRateLimitInterceptor.ClientIpAttribute,
            methodConfig.perClientIp,
            metrics,
            logger,
            IpCidrRateLimits.matchClientIp,
          ),
        )
      },
    )

  // Cache to hold the static, pre-calculated (Status, Metadata) pair for each (method, limiter)
  // pair, avoiding the cost of filling in a stack trace on every single rejection.
  private val cachedRejectionResponses =
    new ConcurrentHashMap[(FullMethodName, RejectionReason), (io.grpc.Status, io.grpc.Metadata)]()

  // System.nanoTime() uses a fixed but arbitrary origin time, possibly in the future, so values may be negative.
  // So, it is critical to initialize properly so that the difference reflects the time elapsed.
  private val logFrequency = new AtomicReference[(Double, Long)]((0.0, System.nanoTime()))
  private val logSuspended = new AtomicBoolean(false)

  /** Logs the rejection, which the cached response below cannot do: a rejected call is closed in
    * this interceptor and never reaches `GrpcRequestLoggingInterceptor`, and `TooManyRequests` sets
    * `logOnCreation = false` precisely so that the cached response does not log. The error built
    * here is a second, uncached one that carries the real client IP, and it is logged at most at
    * `maxLoggingRatePerSecond` so that a flood of requests does not become a flood of logs. Same
    * approach as [[ActiveRequestCounterInterceptor]], whose leaky bucket for log suppression is
    * reproduced here.
    */
  private def logRejection(
      fullMethodName: FullMethodName,
      clientIp: String,
      reason: RejectionReason,
  )(implicit traceContext: TraceContext): Unit = {
    val error =
      CantonGrpcUtil.GrpcErrors.Overloaded.TooManyRequests(fullMethodName, clientIp, reason.label)
    val (count, _) = logFrequency.updateAndGet { case (count, lastLogged) =>
      val newLastLogged = System.nanoTime()

      // Set a max cap for the count to ensure that logs are not silenced for more than 5 minutes
      // (the minimum recovery time after a log count of C_T is given by
      // (1 + C_T - maxLoggingRatePerSecond)/maxLoggingRatePerSecond).
      // For maxLoggingRatePerSecond = 10, countCap= 3009
      val countCap =
        (5.0 * 60.0 * maxLoggingRatePerSecond.value.toDouble) + maxLoggingRatePerSecond.value.toDouble - 1.0

      // number of "leaked" or "drained" logs in the elapsed time
      val drainAmount = (maxLoggingRatePerSecond.value / 1e9) * (newLastLogged - lastLogged)

      // remaining level (capped at 0)
      val levelAfterDrain = Math.max(0.0, count - drainAmount)

      // add 1 to the count corresponding to the present request
      // and cap the count value at countCap
      val newCount = Math.min(1.0 + levelAfterDrain, countCap)

      (newCount, newLastLogged)
    }
    val suspendLogs = count > maxLoggingRatePerSecond.value
    if (suspendLogs && !logSuspended.getAndSet(true)) {
      error.log()
      logger.warn(
        s"Suspending detailed overload logging for excessive rejections until rate drops below $maxLoggingRatePerSecond"
      )
    } else if (count <= maxLoggingRatePerSecond.value) {
      logSuspended.set(false)
      error.log()
    }
  }

  /** The admission control decision, kept apart from the gRPC specifics of `interceptCall` so it
    * can be tested without a real `ServerCall`.
    *
    * The limiters are checked narrowest first, and the check short-circuits, so that a rejected
    * request does not consume budget from a wider limiter it shares with other clients.
    *
    * @return
    *   [[AdmissionResult.Admitted]], or [[AdmissionResult.Rejected]] naming the limiter that
    *   rejected the request
    */
  private[ratelimiting] def admit(
      fullMethodName: FullMethodName,
      clientIp: Option[String],
  ): AdmissionResult = {
    val (globalLimiter, globalClientIpLimiter) = globalRateLimiter
    val (methodLimiter, methodClientIpLimiter) = perMethodRateLimiter(fullMethodName)

    if (!methodClientIpLimiter.markRun(clientIp))
      AdmissionResult.Rejected(RejectionReason.PerClientIpMethod(fullMethodName))
    else if (!globalClientIpLimiter.markRun(clientIp))
      AdmissionResult.Rejected(RejectionReason.PerClientIpGlobal)
    else if (!methodLimiter.markRun())
      AdmissionResult.Rejected(RejectionReason.PerMethod(fullMethodName))
    else if (!globalLimiter.markRun()) AdmissionResult.Rejected(RejectionReason.Global)
    else AdmissionResult.Admitted
  }

  /** Extracts the IP from the header mentioned in the `client-ip-headers` config.
    *
    * A client IP header is set by the client and can hence be spoofed. It can be trusted only when
    * a proxy in front of the node overwrites it with the true source IP of the client, so the
    * operator decides in `client-ip-headers` which headers to trust, see
    * [[com.digitalasset.canton.networking.grpc.GrpcAddressHelper.extractHeaderIpLiteral]].
    */
  private def resolveClientIp[ReqT, RespT](
      call: ServerCall[ReqT, RespT],
      headers: Metadata,
  ): Option[String] =
    GrpcAddressHelper
      .extractHeaderIpLiteral(headers, clientIpHeaderKeys)
      .orElse(GrpcAddressHelper.extractSocketIpLiteral(call))

  override def interceptCall[ReqT, RespT](
      call: ServerCall[ReqT, RespT],
      headers: Metadata,
      next: ServerCallHandler[ReqT, RespT],
  ): ServerCall.Listener[ReqT] = {
    val fullMethodName = call.getMethodDescriptor.getFullMethodName
    val resolvedIp = resolveClientIp(call, headers)
    val clientIp = resolvedIp.map(IpAwareRateLimitInterceptor.rateLimitKey)

    admit(fullMethodName, clientIp) match {
      case AdmissionResult.Admitted => next.startCall(call, headers)
      case AdmissionResult.Rejected(reason) =>
        logRejection(fullMethodName, resolvedIp.getOrElse("unknown"), reason)(
          TraceContextGrpc.fromGrpcContextOrNew("IpAwareRateLimitInterceptor")
        )

        // empty to prevent info leakage
        implicit val traceContext: TraceContext = TraceContext.empty

        val (status, trailers) = cachedRejectionResponses.computeIfAbsent(
          fullMethodName -> reason,
          { _ =>
            // The response is built once per (method, limiter) and then reused for every
            // rejection, so it must not contain the IP of the client that happened to trip the
            // limiter first. Every later client would receive that stranger's IP in its error
            // trailers, so a "redacted-ip" placeholder is used instead.
            // Reusing the response also means the cost of building it is paid once rather than on
            // every rejected request, including the stack trace that asGrpcError fills in.
            val sanitizedError = CantonGrpcUtil.GrpcErrors.Overloaded
              .TooManyRequests(fullMethodName, "redacted-ip", reason.label)
              .toCantonRpcError
            val ex = sanitizedError.asGrpcError
            (ex.getStatus, ex.getTrailers)
          },
        )
        call.close(status, trailers)
        new ServerCall.Listener[ReqT]() {}
    }
  }
}

object IpAwareRateLimitInterceptor {

  private val ClientIpAttribute = "client_ip"
  private[ratelimiting] val GlobalLimiter = "global"

  /** The limiter that rejected a request, named as it appears in the error and in the logs. */
  sealed trait RejectionReason {
    def label: String
  }

  object RejectionReason {
    final case class PerClientIpMethod(methodName: FullMethodName) extends RejectionReason {
      override val label: String = s"per-client-ip $methodName"
    }

    case object PerClientIpGlobal extends RejectionReason {
      override val label: String = s"per-client-ip $GlobalLimiter"
    }

    final case class PerMethod(methodName: FullMethodName) extends RejectionReason {
      override val label: String = methodName
    }

    case object Global extends RejectionReason {
      override val label: String = GlobalLimiter
    }
  }

  sealed trait AdmissionResult

  object AdmissionResult {
    case object Admitted extends AdmissionResult
    final case class Rejected(reason: RejectionReason) extends AdmissionResult
  }

  /** Collapses an IPv6 address to its /64 prefix before it is used as the limiter key. IPv4
    * addresses are used as they are.
    *
    * A single client is typically assigned a whole IPv6 /64 network or larger, and can send from
    * any address in it. Keying on the full IPv6 address would therefore give such a client a fresh
    * rate limit for every request, which bypasses the per client IP limit altogether. All addresses
    * of one /64 share a single limiter instead:
    *
    * {{{
    * 2001:db8:1:2::1    =>  2001:db8:1:2::/64   one limiter for all three
    * 2001:db8:1:2::2    =>  2001:db8:1:2::/64
    * 2001:db8:1:2::dead =>  2001:db8:1:2::/64
    * 203.0.113.5        =>  203.0.113.5         IPv4 is one address per client already
    * ::ffff:203.0.113.5 =>  203.0.113.5         same client, same limiter
    * }}}
    *
    * Every parsed address is rendered in its canonical form, so that the spellings of one address
    * cannot each claim a limiter of their own. A client reaching a dual stack socket is reported as
    * an IPv4-mapped IPv6 address, which is the same client as the plain IPv4 address, and an IPv6
    * address can be written with or without zero compression.
    *
    * Ported from splice's `HttpRateLimiter.rateLimitKey`, adapted to operate on the plain IP that
    * [[resolveClientIp]] already resolved, rather than on a pekko `InetAddress`.
    */
  private[ratelimiting] def rateLimitKey(address: String): String =
    IpCidr.parse(address) match {
      case Some(ip) if ip.addressBits == 128 => ip.copy(low = 0L, prefixLength = 64).toString
      // IpCidr parses an IPv4-mapped IPv6 address as the IPv4 address it stands for, so rendering
      // the parsed address gives both spellings the same key
      case Some(_) => InetAddresses.toAddrString(InetAddresses.forString(address.trim))
      case None => address
    }
}
