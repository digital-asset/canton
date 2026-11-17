// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.performance.elements

import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.metrics.api.MetricHandle.LabeledMetricsFactory
import com.daml.metrics.api.MetricName
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.performance.acs.ContractStore
import com.digitalasset.canton.performance.elements.CmdStatus.CmdSubmitted
import com.digitalasset.canton.performance.model.java as M
import com.digitalasset.canton.performance.model.java.orchestration.Role
import com.digitalasset.canton.performance.model.java.orchestration.runtype.TriParty
import com.digitalasset.canton.performance.model.java.triparty.bootstrap.{Bootstrap, PreBootstrap}
import com.digitalasset.canton.performance.{Connectivity, PartyRole}
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.util.FutureUtil
import org.apache.pekko.actor.ActorSystem

import java.util.Optional
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.jdk.CollectionConverters.*

class MasterDriverTriParty(
    connectivity: Connectivity,
    masterPartyLf: LfPartyId,
    config: PartyRole.Master,
    prefix: MetricName,
    metricsFactory: LabeledMetricsFactory,
    loggerFactory: NamedLoggerFactory,
    control: DriverControl,
    baseSynchronizerId: SynchronizerId,
)(implicit
    ec: ExecutionContextExecutor,
    actorSystem: ActorSystem,
    executionSequencerFactory: ExecutionSequencerFactory,
) extends MasterDriver(
      connectivity,
      masterPartyLf,
      config,
      prefix,
      metricsFactory,
      loggerFactory,
      control,
      baseSynchronizerId,
    ) {

  // track if we already created the bootstrap contract
  private val createdBootstrap = new AtomicReference[CmdStatus](CmdStatus.CmdNotSubmitted)
  private val batchSize = new AtomicInteger(config.batchSize)
  private def reduceBatchSizeOnFailure(curBatchSize: Int, fut: Future[Boolean]): Unit =
    fut.foreach { success =>
      if (!success) {
        val newBatchSize = Math.max(1, Math.min(curBatchSize - 1, (curBatchSize * 0.97).toInt))
        if (batchSize.compareAndExchange(curBatchSize, newBatchSize) == curBatchSize)
          logger.info(s"Reducing batch size to $newBatchSize after failure")
      }
    }

  private val preBootstrap =
    new ContractStore[
      M.triparty.bootstrap.PreBootstrap.Contract,
      M.triparty.bootstrap.PreBootstrap.ContractId,
      M.triparty.bootstrap.PreBootstrap,
      Unit,
    ](
      "issuer-bootstrap",
      M.triparty.bootstrap.PreBootstrap.COMPANION,
      index = _ => (),
      filter = _ => true,
      loggerFactory,
    ) {
      override protected def contractCreated(
          create: PreBootstrap.Contract,
          index: Unit,
          synchronizerId: String,
      ): Unit = createdBootstrap.set(CmdStatus.CmdSucceeded)
    }

  private val bootstrap =
    new ContractStore[
      M.triparty.bootstrap.Bootstrap.Contract,
      M.triparty.bootstrap.Bootstrap.ContractId,
      M.triparty.bootstrap.Bootstrap,
      Unit,
    ](
      "bootstrap",
      M.triparty.bootstrap.Bootstrap.COMPANION,
      index = _ => (),
      filter = _ => true,
      loggerFactory,
    ) {
      override protected def contractCreated(
          create: Bootstrap.Contract,
          index: Unit,
          synchronizerId: String,
      ): Unit =
        createdBootstrap.set(CmdStatus.CmdSucceeded)
    }

  private val bootstrapUser =
    new ContractStore[
      M.triparty.bootstrap.BootstrapUser.Contract,
      M.triparty.bootstrap.BootstrapUser.ContractId,
      M.triparty.bootstrap.BootstrapUser,
      Unit,
    ](
      "bootstrap-custodian",
      M.triparty.bootstrap.BootstrapUser.COMPANION,
      index = _ => (),
      filter = _ => true,
      loggerFactory,
    )

  listeners.addAll(Seq(preBootstrap, bootstrap, bootstrapUser))

  override protected def prepareBootstrap(): Boolean = config.runConfig.runType match {
    case cfg: TriParty
        if createdBootstrap.compareAndSet(CmdStatus.CmdNotSubmitted, CmdStatus.CmdSubmitted) =>
      FutureUtil.doNotAwait(
        submitCommand(
          baseDescription = "setup-bootstrap",
          new M.triparty.bootstrap.PreBootstrap(
            masterPartyLf,
            Optional.empty(),
            List().asJava,
            List().asJava,
            List().asJava,
            List().asJava,
            cfg.numAgents,
            cfg.numAssetsPerIssuer,
            cfg.numAccountsPerIssuer,
            cfg.capNumCounterparties,
          ).create.commands.asScala.toSeq,
          reference = "Create prebootstrap",
          synchronizerId = Some(baseSynchronizerId),
          fixedCommandId = Some("setup-bootstrap"),
        ).map { success =>
          if (success) {
            createdBootstrap.set(CmdStatus.CmdSucceeded)
          } else if (!isClosing) {
            logger.warn("Bootstrap creation failed. Retrying")
            createdBootstrap.set(CmdStatus.CmdNotSubmitted)
          }
        },
        "creating preboostrap failed",
      )
      false
    case _: TriParty if createdBootstrap.get() == CmdSubmitted => false
    case _ => true
  }

  override protected def flushParticipantRequests(
      master: M.orchestration.TestRun.Contract
  ): Unit = {
    if (!requests.entirelyEmpty || !bootstrapUser.entirelyEmpty) {
      if (numPending.get() < config.maxPending) {
        // first, onboard new nodes
        (preBootstrap.one(()), bootstrap.one(())) match {
          case (Some((_, pre)), None) =>
            logger.info(
              s"In prepare issuer mode ${master.data.mode}, have ${master.data.providers.size()} passive roles:\n"
                + s"  agents=${pre.data.agents.size()}\n"
                + s"  subregisters=${pre.data.subregisters.size()}\n"
                + s"  takers=${pre.data.takers.size()}\n"
                + s"  required=${pre.data.numAgents}\n"
                + s"  cash-issuer=${pre.data.cashIssuer}\n"
                + s"  issuers=${pre.data.issuers.size()}\n"
            )
            if (master.data.providers.size() == config.quorumProviders) {
              logger.info("We reached the quorum, moving to custodian bootstrapping")
              val cmd = pre.id.exerciseReadyForBootstrap().commands().asScala.toSeq
              val submissionF = submitCommand(
                "move-to-custodian-bootstrap",
                cmd,
                s"moving to custodian bootstrap",
                synchronizerId = Some(baseSynchronizerId),
              )
              setPending(preBootstrap, pre.id, submissionF)
            } else if (!requests.entirelyEmpty) {
              val takeItems = batchSize.get()
              val open =
                requests.allAvailable.filter(_.data.role == Role.PROVIDER).take(takeItems)
              if (open.nonEmpty) {
                val allNames = open.map(request => request.data.party).toSeq.sorted.mkString(", ")
                logger.info(
                  s"Accepting issuer participation request of $allNames"
                )
                val acceptRegistration = master.id
                  .exerciseBulkAddTriPartyProvider(
                    open.map(_.id).toSeq.asJava,
                    pre.id,
                  )
                  .commands
                  .asScala
                  .toSeq
                val submissionF = submitCommand(
                  "accept-issuer-participation",
                  acceptRegistration,
                  s"registering participants",
                  synchronizerId = Some(baseSynchronizerId),
                )
                reduceBatchSizeOnFailure(takeItems, submissionF)
                setPending(masterContract, master.id, submissionF)
                open.foreach { request =>
                  setPending(requests, request.id, submissionF)
                }
                setPending(preBootstrap, pre.id, submissionF)
              }
            }
          case (None, Some((_, bootstrapC))) =>
            val takeItems = batchSize.get()
            val open = requests.allAvailable.filter(_.data.role == Role.USER).take(takeItems)
            logger.info(
              s"In custodian onboarding mode, grabbing ${open.size} requests to process out of ${requests.totalNum} requests and have further ${bootstrapUser.totalNum} custodian bootstraps"
            )
            if (open.nonEmpty) {
              val allNames = open.map(request => request.data.party).toSeq.sorted.mkString(", ")
              logger
                .info(
                  s"Accepting trader participation request of $allNames"
                )
              val acceptRegistration = master.id
                .exerciseBulkAddTriPartyUsers(
                  open.map(_.id).toSeq.asJava,
                  bootstrapC.id,
                )
                .commands
                .asScala
                .toSeq
              val submissionF = submitCommand(
                "accept-trader-participation",
                acceptRegistration,
                s"registering participants",
                synchronizerId = Some(baseSynchronizerId),
              )
              reduceBatchSizeOnFailure(takeItems, submissionF)
              setPending(masterContract, master.id, submissionF)
              open.foreach { request =>
                setPending(requests, request.id, submissionF)
              }
              setPending(bootstrap, bootstrapC.id, submissionF)
            }
          case (None, None) => // fine
          case (Some(_), Some(_)) =>
            logger.error(
              "Both pre-bootstrap and bootstrap contracts exist simultaneously. This should not happen."
            )
        }
        // then process pending custodian batches
        val available = config.maxPending - numPending.get()
        if (available > 0) {
          bootstrapUser.take((), available).foreach { case (_, request) =>
            val takeItems = batchSize.get()
            val cmd = request.id.exerciseProcessBatch(takeItems).commands().asScala.toSeq
            val submissionF = submitCommand(
              "process-custodian-batch",
              cmd,
              s"creating asset requests for custodian",
              synchronizerId = Some(baseSynchronizerId),
            )
            reduceBatchSizeOnFailure(takeItems, submissionF)
            setPending(bootstrapUser, request.id, submissionF)
          }
        }
      } else {
        logger.debug(
          s"At capacity with ${numPending.get()} pending requests, waiting for completion before processing more"
        )
      }
    }
  }

}
