// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.topology.client

import cats.instances.future._
import cats.instances.list._
import cats.syntax.traverse._
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.topology.client.{
  DomainTopologyClient,
  StoreBasedDomainTopologyClient,
  StoreBasedTopologySnapshot,
}
import com.digitalasset.canton.topology.store.TopologyStore
import com.digitalasset.canton.topology.{DomainId, DomainTopologyManagerId, SequencerId}
import com.digitalasset.canton.tracing.NoTracing

import scala.concurrent.{ExecutionContext, Future}

/** checks and notifies once the domain is initialized */
class DomainInitializationObserver(
    domainId: DomainId,
    client: DomainTopologyClient,
    sequencedStore: TopologyStore,
    processingTimeouts: ProcessingTimeout,
    val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends NamedLogging
    with NoTracing {

  private val needKeys =
    List(
      SequencerId(domainId.unwrap),
      DomainTopologyManagerId(domainId.unwrap),
    )

  private def initialisedAt(timestamp: CantonTimestamp): Future[Boolean] = {
    val dbSnapshot = new StoreBasedTopologySnapshot(
      timestamp,
      sequencedStore,
      initKeys =
        Map(), // we need to do this because of this map here, as the target client will mix-in the map into the response
      useStateTxs = true,
      packageDependencies = StoreBasedDomainTopologyClient.NoPackageDependencies,
      loggerFactory,
    )
    for {
      hasParams <- dbSnapshot.findDynamicDomainParameters.map(_.nonEmpty)
      hasKeys <- needKeys
        .traverse(dbSnapshot.signingKey(_).map(_.nonEmpty))
        .map(_.forall(identity))
    } yield hasParams && hasKeys
  }

  /** returns true if the initialisation data exists (but might not yet be effective) */
  def initialisedAtHead: Future[Boolean] =
    sequencedStore.timestamp().flatMap {
      case Some(ts) => initialisedAt(ts.immediateSuccessor)
      case None => Future.successful(false)
    }

  /** future that will complete once the domain has the correct initialisation data AND the data is effective */
  val waitUntilInitialisedAndEffective: FutureUnlessShutdown[Boolean] = client
    .await(
      snapshot => {
        // this is a bit stinky, but we are using the "check on a change" mechanism of the
        // normal client to just get notified whenever there was an update to the topology state
        initialisedAt(snapshot.timestamp).map { res =>
          logger.debug(s"Domain ready at=${snapshot.timestamp} is ${res}.")
          res
        }
      },
      processingTimeouts.unbounded.duration,
    )
    .map { init =>
      if (init)
        logger.debug("Domain is now initialised and effective.")
      else
        logger.error("Domain initialisation failed!")
      init
    }

}

object DomainInitializationObserver {
  def apply(
      domainId: DomainId,
      client: DomainTopologyClient,
      sequencedStore: TopologyStore,
      processingTimeout: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      executionContext: ExecutionContext
  ): Future[DomainInitializationObserver] = {
    val obs =
      new DomainInitializationObserver(
        domainId,
        client,
        sequencedStore,
        processingTimeout,
        loggerFactory,
      )
    Future.successful(obs)
  }
}
