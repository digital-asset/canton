// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.error

import java.util.concurrent.atomic.AtomicReference

/** This trait should be implemented by classes that can experience a degradation.
  * It has methods that enable the tracking and logging-upon-resolution of stateful degradation.
  *  Example scenario:
  *    - A participant is anomalously disconnect from domain -> we log an error and track that the error/degradation has occurred
  *    - The participant is reconnected to the domain -> we log a resolution message referring to the previous error and
  *    log that the degradation has been fixed
  * For an example use of this trait see CantonSyncService
  * @tparam E error type we are tracking
  */
trait HasDegradationState[E <: CantonError] {

  protected val degradationState = new AtomicReference[Option[E]](None)

  def isDegraded: Boolean = degradationState.get().nonEmpty

  def getDegradationState: Option[E] = degradationState.get()

  def resolveDegradationIfExists(message: E => String): Unit = {
    val errorOpt = degradationState.getAndSet(None)
    errorOpt.foreach { e =>
      e.loggingContext.logger
        .info(
          s"Degradation ${e.code.codeStr(e.loggingContext.traceContext.traceId)} was resolved: " + message(
            e
          )
        )(e.loggingContext.traceContext)
    }
  }

  /** Track that the given error has occurred for the specified key
    */
  def degradationOccurred(error: E): Unit = {
    degradationState.set(Some(error))
  }

}
