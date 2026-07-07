// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.tea.ingestion

import com.daml.grpc.adapter.{ExecutionSequencerFactory, PekkoExecutionSequencerPool}
import com.digitalasset.canton.config.CantonRequireTypes.InstanceName
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.ledger.client.LedgerClient
import com.digitalasset.canton.ledger.client.configuration.{
  CommandClientConfiguration,
  LedgerClientConfiguration,
}
import com.digitalasset.canton.lifecycle.LifeCycle.CloseableChannel
import com.digitalasset.canton.lifecycle.{FlagCloseable, LifeCycle}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.user.store.UserManagementStore
import org.apache.pekko.actor.ActorSystem

import scala.concurrent.ExecutionContext

/** Resources shared by every ingestion service: a single
  * [[com.digitalasset.canton.ledger.client.LedgerClient]] (and therefore a single gRPC channel) and
  * an [[com.daml.grpc.adapter.ExecutionSequencerFactory]] used by the Pekko gRPC client adapter.
  */
final class TeaLedgerResources private (
    val client: LedgerClient,
    val executionSequencerFactory: ExecutionSequencerFactory,
    override val loggerFactory: NamedLoggerFactory,
    override val timeouts: ProcessingTimeout,
) extends FlagCloseable
    with NamedLogging {
  override def onClosed(): Unit =
    LifeCycle.close(
      client,
      executionSequencerFactory,
    )(logger)
}

object TeaLedgerResources {

  /** Build the shared resources around a gRPC channel provided by an embedded host (for example an
    * [[io.grpc.inprocess.InProcessChannelBuilder]] channel that targets the participant's in-JVM
    * Ledger API services, removing the network hop entirely).
    *
    * The channel will be closed when the TeaLedgerResources is closed.
    */
  def fromChannel(
      channel: CloseableChannel,
      token: () => Option[String],
      forNode: InstanceName,
      loggerFactory: NamedLoggerFactory,
      timeouts: ProcessingTimeout,
  )(implicit ec: ExecutionContext, system: ActorSystem): TeaLedgerResources = {
    implicit val esf: ExecutionSequencerFactory = new PekkoExecutionSequencerPool(
      poolName = s"tea-ledger-client-esf-${forNode.unwrap}"
    )
    // The channel is closed by this client
    val client = LedgerClient.withoutToken(
      channel.channel,
      clientConfiguration(token),
      loggerFactory,
    )
    new TeaLedgerResources(client, esf, loggerFactory, timeouts)
  }

  private def clientConfiguration(
      token: () => Option[String]
  ): LedgerClientConfiguration =
    LedgerClientConfiguration(
      userId = UserManagementStore.DefaultParticipantAdminUserId,
      commandClient = CommandClientConfiguration.default,
      token = token,
    )
}
