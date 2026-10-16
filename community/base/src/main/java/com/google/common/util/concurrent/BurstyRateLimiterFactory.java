// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.google.common.util.concurrent;

/**
 * Shim that constructs a Guava {@link RateLimiter} backed by {@code SmoothBursty} with a custom
 * maximum burst duration. It lives in this package because the relevant {@code
 * SmoothRateLimiter.SmoothBursty} constructor and its permit bookkeeping fields are
 * package-private.
 *
 * <p>In contrast to {@code RateLimiter.create(double)}, the limiters created here already hold
 * {@code permitsPerSecond} permits (capped by the maximum burst budget) at creation time, instead
 * of starting with an empty bucket that only fills up over time. That matters for limiters that are
 * created lazily (e.g. one per client IP), which would otherwise reject an initial burst that an
 * already running limiter would have accepted.
 *
 * <p>Ported from splice's {@code
 * apps/common/src/main/java/com/google/common/util/concurrent/BurstyRateLimiterFactory.java}.
 */
public final class BurstyRateLimiterFactory {

  /** Guava's default burst window for {@code RateLimiter.create(double)}. */
  private static final double DEFAULT_MAX_BURST_SECONDS = 1.0;

  private BurstyRateLimiterFactory() {}

  /**
   * Creates a {@link RateLimiter} allowing {@code permitsPerSecond} permits per second, starting
   * with {@code permitsPerSecond} permits already available.
   */
  public static RateLimiter create(double permitsPerSecond) {
    return create(permitsPerSecond, DEFAULT_MAX_BURST_SECONDS);
  }

  /**
   * Creates a bursty {@link RateLimiter} that sustains {@code permitsPerSecond} on average while
   * allowing bursts of up to {@code permitsPerSecond * maxBurstSeconds} permits after idle periods.
   * The limiter starts with one second worth of permits, i.e. {@code permitsPerSecond} permits
   * (capped by the maximum burst budget), already available.
   */
  public static RateLimiter create(double permitsPerSecond, double maxBurstSeconds) {
    SmoothRateLimiter.SmoothBursty rateLimiter =
        new SmoothRateLimiter.SmoothBursty(
            RateLimiter.SleepingStopwatch.createFromSystemTimer(), maxBurstSeconds);
    rateLimiter.setRate(permitsPerSecond);
    synchronized (rateLimiter) {
      rateLimiter.storedPermits = Math.min(permitsPerSecond, rateLimiter.maxPermits);
    }
    return rateLimiter;
  }
}
