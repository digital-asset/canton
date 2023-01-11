// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.mediator

import cats.data.NonEmptySeq
import com.digitalasset.canton.BaseTest
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.domain.mediator.Mediator.{Safe, SafeUntil}
import com.digitalasset.canton.protocol.{
  DomainParameters,
  DynamicDomainParameters,
  TestDomainParameters,
}
import com.digitalasset.canton.time.NonNegativeFiniteDuration
import org.scalatest.Assertion
import org.scalatest.wordspec.AnyWordSpec

class MediatorTest extends AnyWordSpec with BaseTest {
  def parametersWith(participantResponseTimeout: NonNegativeFiniteDuration) =
    TestDomainParameters.defaultDynamic.tryUpdate(participantResponseTimeout =
      participantResponseTimeout
    )

  val defaultTimeout = NonNegativeFiniteDuration.ofSeconds(10)
  val defaultParameters = parametersWith(defaultTimeout)

  val origin = CantonTimestamp.now()
  def relTime(offset: Long): CantonTimestamp = origin.plusSeconds(offset)

  "Mediator.checkPruningStatus" should {
    "deal with current domain parameters" in {
      val parameters = DomainParameters.WithValidity[DynamicDomainParameters](
        CantonTimestamp.Epoch,
        None,
        defaultParameters,
      )

      val cleanTimestamp = CantonTimestamp.now()
      val earliestPruningTimestamp = cleanTimestamp - defaultTimeout

      Mediator.checkPruningStatus(parameters, cleanTimestamp) shouldBe SafeUntil(
        earliestPruningTimestamp
      )
    }

    "cap the time using DomainParameters.WithValidity[DynamicDomainParameters].validFrom" in {
      val validFrom = origin

      def test(validUntil: Option[CantonTimestamp]): Assertion = {
        val parameters = DomainParameters.WithValidity(validFrom, validUntil, defaultParameters)

        // Capping happen
        Mediator.checkPruningStatus(parameters, validFrom.plusSeconds(1)) shouldBe SafeUntil(
          validFrom
        )

        Mediator.checkPruningStatus(
          parameters,
          validFrom + defaultTimeout + NonNegativeFiniteDuration.ofSeconds(1),
        ) shouldBe SafeUntil(validFrom.plusSeconds(1))
      }

      test(validUntil = None)
      test(validUntil = Some(validFrom.plus(defaultTimeout.unwrap.multipliedBy(2))))
    }

    "deal with future domain parameters" in {
      val parameters = DomainParameters.WithValidity(origin, None, defaultParameters)

      Mediator.checkPruningStatus(
        parameters,
        origin - NonNegativeFiniteDuration.ofSeconds(10),
      ) shouldBe Safe
    }

    "deal with past domain parameters" in {
      val dpChangeTs = relTime(60)

      val parameters = DomainParameters.WithValidity[DynamicDomainParameters](
        origin,
        Some(dpChangeTs),
        defaultParameters,
      )

      {
        val cleanTimestamp = dpChangeTs + NonNegativeFiniteDuration.ofSeconds(1)
        Mediator.checkPruningStatus(
          parameters,
          cleanTimestamp,
        ) shouldBe SafeUntil(cleanTimestamp - defaultTimeout)
      }

      {
        val cleanTimestamp = dpChangeTs + defaultTimeout
        Mediator.checkPruningStatus(
          parameters,
          cleanTimestamp,
        ) shouldBe Safe
      }
    }
  }

  "Mediator.latestSafePruningTsBefore" should {
    /*
      We consider the following setup:

                  O          20=dpChangeTs1    40=dpChangeTs2
      time        |-----------------|-----------------|---------------->
      timeout              10s            10 days            10s
     */

    val hugeTimeout = NonNegativeFiniteDuration.ofDays(10)

    val dpChangeTs1 = relTime(20)
    val dpChangeTs2 = relTime(40)

    val parameters = NonEmptySeq.of(
      DomainParameters.WithValidity(origin, Some(dpChangeTs1), defaultParameters),
      // This one prevents pruning for some time
      DomainParameters.WithValidity(dpChangeTs1, Some(dpChangeTs2), parametersWith(hugeTimeout)),
      DomainParameters.WithValidity(dpChangeTs2, None, defaultParameters),
    )

    "query in the first slice" in {
      // Tests in the first slice (timeout = defaultTimeout)
      Mediator.latestSafePruningTsBefore(
        parameters,
        origin + defaultTimeout - NonNegativeFiniteDuration.ofSeconds(1),
      ) shouldBe Some(origin) // capping happens

      Mediator.latestSafePruningTsBefore(
        parameters,
        dpChangeTs1,
      ) shouldBe Some(dpChangeTs1 - defaultTimeout)
    }

    "query in the second slice" in {
      {
        val cleanTs = dpChangeTs1 + NonNegativeFiniteDuration.ofSeconds(5)
        Mediator.latestSafePruningTsBefore(
          parameters,
          cleanTs,
        ) shouldBe Some(cleanTs - defaultTimeout) // effect of the first domain parameters
      }

      {
        val cleanTs = dpChangeTs1 + defaultTimeout + NonNegativeFiniteDuration.ofSeconds(10)
        Mediator.latestSafePruningTsBefore(
          parameters,
          cleanTs,
        ) shouldBe Some(dpChangeTs1)
      }
    }

    "query in the third slice" in {
      // We cannot allow any request in second slice to be issued -> dpChangeTs1
      Mediator.latestSafePruningTsBefore(
        parameters,
        relTime(40),
      ) shouldBe Some(dpChangeTs1)

      // We cannot allow any request in second slice to be issued -> dpChangeTs1
      Mediator.latestSafePruningTsBefore(
        parameters,
        relTime(60),
      ) shouldBe Some(dpChangeTs1)

      // If enough time elapsed since huge timeout was revoked, we are fine again
      val endOfHugeTimeoutEffect = dpChangeTs2 + hugeTimeout
      Mediator.latestSafePruningTsBefore(
        parameters,
        endOfHugeTimeoutEffect,
      ) shouldBe Some(endOfHugeTimeoutEffect - defaultTimeout)
    }
  }
}
