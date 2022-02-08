// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.mediator

import cats.syntax.foldable._
import com.digitalasset.canton.crypto.DomainSyncCryptoClient
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.topology.MediatorId
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import java.util.concurrent.atomic.AtomicReference

import com.digitalasset.canton.util.EitherTUtil

import scala.concurrent.{ExecutionContext, Future}

/** When a Mediator starts they will not be able to sign any events until their key has been assigned to the mediator
  * identifier. For the first mediator on the domain this will happen immediately when the domain is initialized so
  * has historically hasn't been a problem. However for additional mediators their key may not be added for many events after
  * genesis and will be unable to send anything until this happens.
  * [[MediatorReadyCheck]] efficiently checks that the local mediator
  * is able to sign events on the local domain. We then cache the result of this check so it can be regularly queried
  * without performing identity operations. Callers should ensure to call [[reset]] after every topology transaction
  * where the state could change.
  */
class MediatorReadyCheck(
    val check: Traced[CantonTimestamp] => Future[Boolean],
    protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends NamedLogging {
  private val cachedReady = new AtomicReference[Option[Boolean]](None)

  /** Query whether the Mediator is ready to interact on the domain */
  def isReady(ts: CantonTimestamp)(implicit traceContext: TraceContext): Future[Boolean] =
    // if a value isn't cached then update and return
    cachedReady.get().fold(checkAndUpdateCache(ts))(Future.successful)

  /** Update the ready status (should be called after processing topology transactions) */
  def reset(): Unit = cachedReady.set(None)

  private def logSignificantChanges(priorValue: Option[Boolean], newValue: Boolean)(implicit
      traceContext: TraceContext
  ): Unit = {
    if (priorValue.forall(_ == false) && newValue) {
      logger.info(s"Mediator is now ready to interact with the domain")
    } else if (priorValue.contains(true) && !newValue) {
      // I expect this practically shouldn't happen unless the key is manually removed with admin commands
      logger.warn(
        s"Mediator is no longer able to interact with the domain as they do not have an active signing key"
      )
    }
  }

  private def checkAndUpdateCache(
      ts: CantonTimestamp
  )(implicit traceContext: TraceContext): Future[Boolean] = {
    val initialValue = cachedReady.get()

    for {
      ready <- check(Traced(ts))
      _ = logSignificantChanges(initialValue, ready)
      _ = cachedReady.set(Some(ready))
    } yield ready
  }
}

object MediatorReadyCheck {

  def apply(
      mediatorId: MediatorId,
      syncCrypto: DomainSyncCryptoClient,
      loggerFactory: NamedLoggerFactory,
  )(implicit executionContext: ExecutionContext): MediatorReadyCheck = {
    def checkKeys(ts: CantonTimestamp)(implicit traceContext: TraceContext): Future[Boolean] = {
      for {
        snapshot <- syncCrypto.ips.awaitSnapshot(ts)
        // get all possible mediator keys on the domain
        mediatorKeys <- snapshot.signingKeys(mediatorId)
        // see if we have one of these locally that we can use
        hasSigningKey <- EitherTUtil.toFuture(
          mediatorKeys
            .map(_.fingerprint)
            .toList
            .existsM(fingerprint =>
              syncCrypto.crypto.cryptoPrivateStore.existsSigningKey(fingerprint)
            )
            .leftMap(storeError =>
              new RuntimeException(
                s"Store error while checking signing key's existence: $storeError"
              )
            )
        )
      } yield hasSigningKey
    }

    new MediatorReadyCheck(Traced.lift(checkKeys(_)(_)), loggerFactory)
  }

  def alwaysReady(loggerFactory: NamedLoggerFactory)(implicit
      executionContext: ExecutionContext
  ): MediatorReadyCheck =
    new MediatorReadyCheck(_ => Future.successful(true), loggerFactory)
}
