// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.protocol

import com.digitalasset.canton.concurrent.FutureSupervisor
import com.digitalasset.canton.config.RequireTypes.NonNegativeInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.time.PositiveSeconds
import com.digitalasset.canton.topology.client.DomainTopologyClient
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.version.ProtocolVersion

import scala.concurrent.{ExecutionContext, Future}

/** This class allows to query domain parameters, regardless of whether they are
  * static or dynamic. The returned value can be a single parameter (see reconciliation
  * interval example below) or a set of values.
  *
  * Type parameter `P` is the type of the returned value.
  */
sealed trait DomainParametersLookup[+P] {

  /** Return one value, valid at the specified timestamp
    * @param warnOnUsingDefaults Log a warning if dynamic domain parameters are not set
    *                            and default value is used.
    */
  def get(validAt: CantonTimestamp, warnOnUsingDefaults: Boolean = true)(implicit
      traceContext: TraceContext
  ): Future[P]

  /** If the parameter is static, return its value.
    *       If the parameter is dynamic, return the value of the topology snapshot approximation.
    */
  def getApproximate(implicit traceContext: TraceContext): Future[P]

  /** Return a list of parameters, together with their validity interval,
    * @param warnOnUsingDefaults Log a warning if dynamic domain parameters are not set
    *                            and default value is used.
    */
  def getAll(validAt: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[Seq[DomainParameters.WithValidity[P]]]

  /** Return the approximate latest validity/freshness.
    * For parameters that are static, the returned value is `Canton.MaxTimestamp`.
    * For parameters that are dynamic, the returned value is the approximate timestamp
    * of the `TopologyClient`.
    */
  def approximateTimestamp: CantonTimestamp
}

class StaticDomainParametersLookup[P](parameters: P) extends DomainParametersLookup[P] {

  def get(validAt: CantonTimestamp, warnOnUsingDefaults: Boolean)(implicit
      traceContext: TraceContext
  ): Future[P] = Future.successful(parameters)

  def getApproximate(implicit traceContext: TraceContext): Future[P] =
    Future.successful(parameters)

  def getAll(validAt: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[Seq[DomainParameters.WithValidity[P]]] = Future.successful(
    Seq(DomainParameters.WithValidity(CantonTimestamp.MinValue, None, parameters))
  )

  def approximateTimestamp: CantonTimestamp = CantonTimestamp.MaxValue
}

class DynamicDomainParametersLookup[P](
    projector: DynamicDomainParameters => P,
    topologyClient: DomainTopologyClient,
    futureSupervisor: FutureSupervisor,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends DomainParametersLookup[P]
    with NamedLogging {
  def get(validAt: CantonTimestamp, warnOnUsingDefaults: Boolean = true)(implicit
      traceContext: TraceContext
  ): Future[P] = futureSupervisor
    .supervised(s"Querying for domain parameters valid at $validAt") {
      topologyClient.awaitSnapshot(validAt)
    }
    .flatMap(_.findDynamicDomainParametersOrDefault(warnOnUsingDefaults))
    .map(projector)

  def getApproximate(implicit
      traceContext: TraceContext
  ): Future[P] = {
    topologyClient.currentSnapshotApproximation
      .findDynamicDomainParametersOrDefault()
      .map(projector)
  }

  def getAll(validAt: CantonTimestamp)(implicit
      traceContext: TraceContext
  ): Future[Seq[DomainParameters.WithValidity[P]]] =
    futureSupervisor
      .supervised(s"Querying for list of domain parameters changes valid at $validAt") {
        topologyClient.awaitSnapshot(validAt)
      }
      .flatMap(_.listDynamicDomainParametersChanges())
      .map { domainParametersChanges =>
        domainParametersChanges.map(_.map(projector))
      }

  def approximateTimestamp: CantonTimestamp = topologyClient.approximateTimestamp
}

object DomainParametersLookup {
  def forReconciliationInterval(
      staticDomainParameters: StaticDomainParameters,
      topologyClient: DomainTopologyClient,
      futureSupervisor: FutureSupervisor,
      loggerFactory: NamedLoggerFactory,
  )(implicit ec: ExecutionContext): DomainParametersLookup[PositiveSeconds] = {
    // TODO(#9800) migrate to stable version
    if (staticDomainParameters.protocolVersion < ProtocolVersion.dev)
      new StaticDomainParametersLookup(staticDomainParameters.reconciliationInterval)
    else
      new DynamicDomainParametersLookup(
        _.reconciliationInterval,
        topologyClient,
        futureSupervisor,
        loggerFactory,
      )
  }

  def forMaxRatePerParticipant(
      staticDomainParameters: StaticDomainParameters,
      topologyClient: DomainTopologyClient,
      futureSupervisor: FutureSupervisor,
      loggerFactory: NamedLoggerFactory,
  )(implicit ec: ExecutionContext): DomainParametersLookup[NonNegativeInt] = {
    // TODO(#9800) migrate to stable version
    if (staticDomainParameters.protocolVersion < ProtocolVersion.dev)
      new StaticDomainParametersLookup(staticDomainParameters.maxRatePerParticipant)
    else {
      new DynamicDomainParametersLookup(
        _.maxRatePerParticipant,
        topologyClient,
        futureSupervisor,
        loggerFactory,
      )
    }
  }
}