// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.scalatest

import org.mockito.ArgumentMatchersSugar
import org.scalatest.concurrent.{PatienceConfiguration, ScalaFutures}
import org.scalatest.time.{Millis, Seconds, Span}

trait DefaultCantonUnitTestPatience extends PatienceConfiguration {

  /** Increased default Scalatest patience, used for FutureValues and Scalatest `eventually`.
    *
    * Generally, this trait should be used in every test suite, as the default timeout is too low
    * even for some of our unit tests, especially in CI with noisy neighbors.
    *
    * Note: This implicit shadows the default `patienceConfig` implicit in
    * [[org.scalatest.concurrent.PatienceConfiguration]]
    */
  implicit val defaultPatience: PatienceConfig =
    PatienceConfig(timeout = scaled(Span(5, Seconds)), interval = scaled(Span(20, Millis)))
}

trait ScalaFuturesWithPatience extends ScalaFutures with DefaultCantonUnitTestPatience

trait ScalatestEssentials
    extends ScalaFuturesWithPatience // There are many MockitoSugar implementations, but only this one is not deprecated and
    // supports when, verify, ...
    with org.mockito.MockitoSugar
    with ArgumentMatchersSugar
