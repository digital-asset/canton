// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.performance.elements.transfer

import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.ledger.api.v2.commands.Command
import com.daml.ledger.javaapi.data.Party
import com.daml.metrics.api.MetricHandle.LabeledMetricsFactory
import com.daml.metrics.api.{MetricInfo, MetricName, MetricQualification, MetricsContext}
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.performance.Connectivity
import com.digitalasset.canton.performance.PartyRole.Transfer
import com.digitalasset.canton.performance.RateSettings.SubmissionRateSettings
import com.digitalasset.canton.performance.acs.ContractStore
import com.digitalasset.canton.performance.elements.DriverStatus.{
  StepStatus,
  TraderStatus,
  TransferStatus,
}
import com.digitalasset.canton.performance.elements.dvp.{AssetUserDriver, TraderStats}
import com.digitalasset.canton.performance.elements.{
  DriverControl,
  ParticipantDriver,
  StatsUpdater,
  SubCommand,
}
import com.digitalasset.canton.performance.model.java as M
import com.digitalasset.canton.performance.model.java.dvp.asset.{Asset, TransferPreapproval}
import com.digitalasset.canton.performance.model.java.orchestration.Mode
import com.digitalasset.canton.performance.model.java.orchestration.runtype.TransferRun
import com.digitalasset.canton.topology.{PartyId, SynchronizerId}
import org.apache.pekko.actor.ActorSystem

import java.time.Instant
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}
import scala.Ordered.orderingToOrdered
import scala.concurrent.ExecutionContextExecutor
import scala.jdk.CollectionConverters.*
import scala.util.Success

class TransferDriver(
    connectivity: Connectivity,
    partyLf: LfPartyId,
    masterLf: LfPartyId,
    role: Transfer,
    prefix: MetricName,
    metricsFactory: LabeledMetricsFactory,
    loggerFactory: NamedLoggerFactory,
    control: DriverControl,
    protected val baseSynchronizerId: SynchronizerId,
    protected val otherSynchronizers: Seq[SynchronizerId],
)(implicit
    val ec: ExecutionContextExecutor,
    actorSystem: ActorSystem,
    executionSequencerFactory: ExecutionSequencerFactory,
) extends ParticipantDriver(
      connectivity,
      partyLf,
      masterLf,
      role,
      registerGenerator = true,
      prefix,
      metricsFactory,
      loggerFactory,
      control,
      baseSynchronizerId,
    )
    with AssetUserDriver
    with StatsUpdater {

  private val transferCounter = new AtomicLong(0)
  private val allAssetsInPlace = new AtomicBoolean(false)

  private val transferMetric =
    metricsFactory.gauge[Long](
      MetricInfo(
        prefix :+ "transfers",
        "How many open transfer we have outstanding",
        MetricQualification.Debug,
      ),
      0,
    )(
      MetricsContext.Empty
    ) // sum accepts and proposals together

  protected val preapprovals = new ContractStore[
    M.dvp.asset.TransferPreapproval.Contract,
    M.dvp.asset.TransferPreapproval.ContractId,
    M.dvp.asset.TransferPreapproval,
    (String, String, String),
  ](
    "preapprovals",
    M.dvp.asset.TransferPreapproval.COMPANION,
    index = x => (x.data.owner, x.data.counterparty, x.data.location),
    filter = _ => true,
    loggerFactory,
  ) {

    override protected def contractCreated(
        create: TransferPreapproval.Contract,
        index: (String, String, String),
    ): Unit = ()

    override protected def contractArchived(
        archive: TransferPreapproval.Contract,
        index: (String, String, String),
    ): Unit = ()

  }

  override def assetArchived(): Unit = transferStats.incrementObserved()

  private val transferStats = new TraderStats(transferMetric)
  listeners.appendAll(Seq(assets, assetRequests, preapprovals))

  private def createTransferPreapprovals(traders: Seq[String]): Unit =
    generator.one(()).foreach { case (_, gen) =>
      val missing = traders.filter { trader =>
        preapprovals.one((partyLf, trader, locations.head1)).isEmpty && trader != party.getValue
      }
      if (missing.nonEmpty) {
        logger.info(s"Creating preapprovals for $missing")
        val cmd = gen.id
          .exerciseGeneratePreapproval(missing.asJava, locations.forgetNE.asJava)
          .commands
          .asScala
          .toSeq
        val submissionF =
          submitCommand(
            "create-preapprovals",
            cmd,
            s"creating pre-approvals for $missing",
          )
        setPending(generator, gen.id, submissionF)
        submissionF.onComplete {
          case Success(true) =>
          case x =>
            logger.warn(s"Creating preapprovals for $missing failed, retrying $x")
        }
      }
    }

  private def determineAvailable() = {
    val submit1 = settings.submissionRateSettings match {
      case tl: SubmissionRateSettings.TargetLatency =>
        Math.min(
          rate.available,
          Math.max(
            0,
            Math.max(1, ((tl.targetLatencyMs * rate.maxRate) / 1000.0)).toInt - rate.pending,
          ),
        )
      case _: SubmissionRateSettings.FixedRate =>
        rate.available
    }
    val submit2 = Math.min(
      submit1,
      Math.max(submit1 * settings.factorOfMaxSubmissionsPerIteration, 1).toInt,
    )
    (submit1, submit2)
  }

  private def determineTargetSynchronizer() = {
    val index = transferCounter.incrementAndGet()
    val synchronizer = synchronizers((index % synchronizers.length).toInt)
    val location = locations((index % locations.length).toInt)
    (synchronizer, location)
  }

  /** Preferably picks assets not yet in place such that we only have reassignments in the beginning
    */
  private def pickAssets(issuer: Party, num: Int, location: String) = {
    def pick(filter: (String, Asset.Contract) => Boolean) = assets
      .take(
        issuer,
        num,
        // only pick assets of a matching synchronizer
        filter = { case (synchronizer, asset) =>
          filter(synchronizer, asset)
        },
      )
      .map(_._2)
    def defaultPick = pick { case (_, asset) => asset.data.location == location }
    if (!allAssetsInPlace.get()) {
      val notInPlace = pick { case (synchronizer, asset) =>
        asset.data.location == location && !synchronizer.startsWith(location + "::")
      }
      if (notInPlace.isEmpty) {
        // try to update all assets in place
        val haveOne = assets.all.exists { case (synchronizer, _, asset) =>
          !synchronizer.startsWith(asset.data.location + "::")
        }
        if (!haveOne) {
          logger.info("All assets are now in place")
          allAssetsInPlace.set(true)
        }
        defaultPick
      } else {
        notInPlace
      }
    } else defaultPick
  }

  private def signalReadyIfTransferDriverIs(
      processedAllIssuers: Boolean,
      numAssetsPerIssuer: Int,
  ): Boolean =
    testResult
      .one(())
      .exists { case (_, res) =>
        if (res.data.flag == M.orchestration.ParticipantFlag.READY)
          true
        else if (res.data.flag == M.orchestration.ParticipantFlag.INITIALISING) {
          if (assets.all.sizeIs >= numAssetsPerIssuer && processedAllIssuers) {
            // signal that we are ready if we have enough assets
            updateFlag(res, M.orchestration.ParticipantFlag.READY)
          } else {
            val numAssets = assets.all.size
            logger.info(
              s"Driver is not yet read. Num assets=$numAssets, processed-all=$processedAllIssuers"
            )
          }
          false
        } else false
      }

  override def flush(): Boolean = if (running.get()) {
    val reflush = masterContract
      .one(())
      .flatMap { case (_, master) =>
        master.data.runType match {
          case xfer: TransferRun => Some((master, xfer))
          case va =>
            logger.error(s"This is a TransferRun driver while the test run uses $va ")
            None
        }
      }
      .map { case (master, params) =>
        updateTransferStats(
          master.data.mode,
          transferStats,
          assets.totalNum,
          issuerBalancer.state(),
          traderBalancer.state(),
        )
        master.data.mode match {
          case Mode.PREPAREISSUER => false
          case Mode.PREPARETRADER =>
            initExisting()
            createTransferPreapprovals(master.data.traders.asScala.toSeq)
            val ready = acquireAssets(master, params.payloadSize, params.numAssetsPerIssuer)
            signalReadyIfTransferDriverIs(ready, params.numAssetsPerIssuer.toInt).discard
            false
          case Mode.THROUGHPUT =>
            initExisting()
            createTransferPreapprovals(master.data.traders.asScala.toSeq)
            val ready = acquireAssets(master, params.payloadSize, params.numAssetsPerIssuer)
            if (!signalReadyIfTransferDriverIs(ready, params.numAssetsPerIssuer.toInt)) {
              false
            } else {
              rate.updateRate(CantonTimestamp.now())
              val (submit1, submit2) = determineAvailable()
              val cmds = (0 until submit2).flatMap { submissionIdx =>
                val next = traderBalancer.next()
                val (synchronizer, location) = determineTargetSynchronizer()
                preapprovals.one((next.getValue, party.getValue, location)).toList.flatMap {
                  case (preapprovalSynchronizer, value) =>
                    val preapprovalReassignment =
                      !preapprovalSynchronizer.startsWith(value.data.location + "::")
                    val issuer = issuerBalancer.next()
                    val toTransfer = pickAssets(issuer, params.transferBatchSize.toInt, location)
                    val transferSize = toTransfer.size
                    if (toTransfer.nonEmpty) {
                      val cmd =
                        value.id
                          .exerciseSend(toTransfer.map(_.id).toList.asJava, issuer.getValue)
                          .commands
                          .asScala
                          .headOption
                          .getOrElse(sys.error("must exist"))
                      val cnt = transferStats.updateSubmittedCount(transferSize)
                      val issuerParty = PartyId.tryFromProtoPrimitive(issuer.getValue)
                      val nextParty = PartyId.tryFromProtoPrimitive(next.getValue)
                      logger.info(
                        s"Sending ${toTransfer.size} of issuer ${issuerParty.partyId.toString} via sync $location, movesync=$preapprovalReassignment, it=${submissionIdx + 1}/$submit2, available=$submit1"
                      )
                      List(
                        SubCommand(
                          s"xfer-$cnt",
                          s"Sending $transferSize} contracts of issuer $issuerParty to $nextParty",
                          Command.fromJavaProto(cmd.toProtoCommand),
                          synchronizer,
                          submissionF => {
                            toTransfer.foreach { asset =>
                              setPending(assets, asset.id, submissionF)
                            }
                            // if the preapproval will be reassigned, we mark it as pending
                            // so we don't get reassignment conflicts
                            if (preapprovalReassignment) {
                              logger.debug(s"Preapproval needs reassignment ${value.id}")
                              submissionF.onComplete {
                                case Success(true) =>
                                  logger.debug(s"Marking ${value.id} as no longer pending")
                                  preapprovals.setPending(value.id, isPending = false).discard
                                case _ => ()
                              }.discard
                              setPending(preapprovals, value.id, submissionF).discard
                            }
                          },
                          () => {
                            // on command failure, update accounting
                            val res = transferStats.updateSubmittedCount(-transferSize)
                            logger.debug(s"Decrementing submitted to=$res after batch failure")
                          },
                        )
                      )
                    } else {
                      List.empty
                    }
                }
              }
              // submit batches
              submitBatched(
                cmds,
                settings.batchSize,
                rate,
                settings.duplicateSubmissionDelay,
              )
              updateStats(master)
              signalFinishedIfWeAre(master)
              if (submit2 > 0 && cmds.isEmpty) {
                val (total, numPending) = preapprovals.all.foldLeft((0, 0)) {
                  case ((total, numpending), (_, pending, _)) =>
                    (total + 1, if (pending) numpending + 1 else numpending)
                }
                logger.info(
                  s"Inventory ${assets.totalNum}, rate $rate, pending: ${rate.pending}, submit: $submit2, preapprovals=$total/$numPending"
                )
              }
              submit2 > 0
            }
          case Mode.DONE =>
            finished()
            false
        }
      }
    reflush.getOrElse(false)
  } else {
    false
  }

  private def signalFinishedIfWeAre(master: M.orchestration.TestRun.Contract): Unit =
    testResult.one(()).foreach { case (_, res) =>
      // we are done if we submitted enough proposals and all proposals we are involved in vanished
      logger.debug(
        s"Current flag=${res.data.flag}, transferred=${transferStats.observed}"
      )
      if (
        res.data.flag == M.orchestration.ParticipantFlag.READY && transferStats.observed >= master.data.totalCycles
      ) {
        updateFlag(res, M.orchestration.ParticipantFlag.FINISHED)
      }
    }

  private def updateStats(master: M.orchestration.TestRun.Contract): Unit =
    // update statistics if necessary
    testResult.one(()).foreach { case (_, res) =>
      sendStatsUpdates(res, master, None, Some(transferStats))
    }

  private def updateTransferStats(
      mode: M.orchestration.Mode,
      transferStats: TraderStats,
      numAssets: Int,
      issuerStats: Seq[(Party, Int, Int)],
      counterPartyStats: Seq[(Party, Int, Int)],
  ): Unit = {

    val lastStatsUpdate = currentStatus.get() match {
      case Some(ts: TraderStatus) => ts.timestamp
      case _ => Instant.MIN
    }
    if (lastStatsUpdate < Instant.now.minusMillis(500)) {
      currentStatus.set(
        Some(
          TransferStatus(
            name = name,
            timestamp = Instant.now,
            mode = mode.toString,
            currentRate = rate.currentRate,
            maxRate = rate.maxRate,
            latencyMs = rate.latencyMs,
            pending = rate.pending,
            failed = rate.failed,
            assets = numAssets,
            transfers = StepStatus(transferStats.submitted, transferStats.observed, open = 0),
            counterParties = counterPartyStats,
            issuers = issuerStats,
          )
        )
      )
    }
  }

}
