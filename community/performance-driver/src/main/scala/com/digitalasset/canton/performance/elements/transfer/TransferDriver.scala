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
import com.digitalasset.canton.performance.model.java.orchestration.runtype.TransferRun
import com.digitalasset.canton.performance.model.java.orchestration.{Mode, TestRun}
import com.digitalasset.canton.topology.{PartyId, SynchronizerId}
import org.apache.pekko.actor.ActorSystem

import java.time.Instant
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicReference}
import scala.Ordered.orderingToOrdered
import scala.annotation.tailrec
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
        synchronizer: String,
    ): Unit = ()

    override protected def contractArchived(
        archive: TransferPreapproval.Contract,
        index: (String, String, String),
    ): Unit = ()

  }

  override def assetArchived(): Unit = transferStats.incrementObserved()

  private val transferStats = new TraderStats(transferMetric)
  listeners.appendAll(Seq(assets, assetRequests, preapprovals))

  private def createTransferPreapprovals(traders: Seq[String]): Boolean =
    generator.one(()).exists { case (_, gen) =>
      val missing = traders.filter { trader =>
        preapprovals
          .one((partyLf, trader, synchronizers.head1.uid.identifier.str))
          .isEmpty && trader != party.getValue
      }
      if (missing.nonEmpty && rate.available > 0) {
        logger.info(s"Creating preapprovals for $missing")
        val cmd = gen.id
          .exerciseGeneratePreapproval(
            missing.asJava,
            synchronizers.map(_.uid.identifier.str).forgetNE.asJava,
          )
          .commands
          .asScala
          .toSeq
        val submissionF =
          submitCommand(
            "create-preapprovals",
            cmd,
            s"creating pre-approvals for $missing",
          )
        rate.newSubmission(submissionF)
        setPending(generator, gen.id, submissionF)
        submissionF.onComplete {
          case Success(true) =>
          case x =>
            logger.warn(s"Creating preapprovals for $missing failed, retrying $x")
        }
        false
      } else if (missing.nonEmpty) false
      else true
    }

  private def determineAvailable() = {
    val submit1 = settings.submissionRateSettings match {
      case _: SubmissionRateSettings.TargetLatencyNew =>
        rate.available
      case tl: SubmissionRateSettings.TargetLatency =>
        val balance =
          // if we have too many assets, submit more
          if (assets.totalNum > referenceAssetQuantity.get() / role.balancingThreshold) {
            logger.debug(
              s"Boost because I have too many ${assets.totalNum} vs $referenceAssetQuantity"
            )
            1
            // if we have too few assets, throttle more
          } else if (assets.totalNum < referenceAssetQuantity.get() * role.balancingThreshold) {
            logger.debug(
              s"Throttle because I have too few ${assets.totalNum} vs $referenceAssetQuantity"
            )
            -1
          } else 0
        Math.min(
          rate.availableBalanced(balance),
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
    submit2
  }

  private val ready = new AtomicBoolean(false)
  private val masterChanged = new AtomicBoolean(false)
  private val referenceAssetQuantity = new AtomicInteger(1)
  private val reassignIssuers = new AtomicReference[Set[Party]](Set.empty)
  private val traderBalancer = new AtomicReference[IndexedSeq[Party]](IndexedSeq.empty)

  override def masterCreated(master: TestRun.Contract): Unit = {
    super.masterCreated(master)
    traderBalancer.set(
      master.data.traders.asScala.map(new Party(_)).filter(_ != party).toIndexedSeq
    )
    masterChanged.set(true)
  }

  override protected def assetCreated(create: Asset.Contract, synchronizerId: String): Unit =
    // keep track of issuers with assets not on the right synchronizer
    if (!synchronizerId.startsWith(create.data.location + ":")) {
      reassignIssuers.updateAndGet(_ + (new Party(create.data.issuer))).discard
    }

  private def updateParticipantFlagAndReturnReady(
      master: M.orchestration.TestRun.Contract,
      processedAllIssuers: Boolean,
  ): Boolean =
    testResult
      .one(())
      .exists { case (_, res) =>
        // we are done if we submitted enough transfers
        if (
          res.data.flag == M.orchestration.ParticipantFlag.READY && transferStats.observed >= master.data.totalCycles
        ) {
          updateFlag(res, M.orchestration.ParticipantFlag.FINISHED)
          false
        } else if (res.data.flag == M.orchestration.ParticipantFlag.READY) {
          true
        } else if (res.data.flag == M.orchestration.ParticipantFlag.INITIALISING) {
          if (assets.totalNum > 0 && processedAllIssuers) {
            // signal that we are ready if we have enough assets
            updateFlag(res, M.orchestration.ParticipantFlag.READY)
          } else {
            val numAssets = assets.totalNum
            logger.info(
              s"Driver is not yet ready. Num assets=$numAssets, processed-all=$processedAllIssuers"
            )
          }
          false
        } else false // we are finished
      }

  private def buildAssetTransfer(
      toTransfer: Seq[Asset.Contract],
      issuer: Party,
      newOwner: Party,
      synchronizerId: SynchronizerId,
  ) = preapprovals
    .one((newOwner.getValue, party.getValue, synchronizerId.uid.identifier.str))
    .map { case (preapprovalSync, preapproval) =>
      val cmd =
        preapproval.id
          .exerciseSend(toTransfer.map(_.id).toList.asJava, issuer.getValue)
          .commands
          .asScala
          .headOption
          .getOrElse(sys.error("must exist"))
      val cnt = transferStats.updateSubmittedCount(toTransfer.size)
      val issuerParty = PartyId.tryFromProtoPrimitive(issuer.getValue)
      val nextParty = PartyId.tryFromProtoPrimitive(newOwner.getValue)
      SubCommand(
        s"xfer-$cnt",
        s"Sending ${toTransfer.size} contracts of issuer $issuerParty to $nextParty",
        Command.fromJavaProto(cmd.toProtoCommand),
        synchronizerId,
        submissionF => {
          // mark assets as pending
          toTransfer.foreach { asset =>
            setPending(assets, asset.id, submissionF)
          }
          // mark preapproval as pending if we need to reassign it
          if (!preapprovalSync.startsWith(preapproval.data.location + ":")) {
            setPending(preapprovals, preapproval.id, submissionF)
            // preapproval is non-consuming which means we have to mark it as non-pending on success
            submissionF.onComplete {
              case Success(true) =>
                logger.debug(s"Marking ${preapproval.id} as no longer pending")
                preapprovals.setPending(preapproval.id, isPending = false).discard
              case _ =>
            }
          }
        },
        () => {
          // on command failure, update accounting
          val res = transferStats.updateSubmittedCount(-toTransfer.size)
          logger.debug(s"Decrementing submitted to=$res after batch failure")
        },
      )
    }

  private def prepareAndDetermineIfWeAreReady(
      master: TestRun.Contract,
      params: TransferRun,
  ): Boolean = if (ready.get() && !masterChanged.get())
    updateParticipantFlagAndReturnReady(master, processedAllIssuers = true)
  else {
    initExisting()
    val havePreapprovals = createTransferPreapprovals(master.data.traders.asScala.toSeq)
    val haveAssets =
      acquireAssets(master, params.payloadSize, params.numAssetsPerActor, params.transferBatchSize)
    val res = updateParticipantFlagAndReturnReady(master, havePreapprovals && haveAssets)
    ready.set(res)
    masterChanged.set(false)
    res
  }

  protected def acquireAssets(
      master: M.orchestration.TestRun.Contract,
      payloadSize: Long,
      numAssetsPerActor: Long,
      transferBatchSize: Long,
  ): Boolean =
    generator
      .one(())
      .exists {
        case (_, res) if rate.available > 0 =>
          val baseFilter = role.issuers.map(_ + "::")
          // if there is a new issuer that we haven't seen before
          val issuers =
            master.data.issuers.asScala.toSeq
              .filterNot(res.data.processedIssuers.contains)
              .filter(issuerUid => baseFilter.isEmpty || baseFilter.exists(issuerUid.startsWith))
          if (issuers.isEmpty)
            true // all processed, so we are done here
          else {
            val numRequest =
              if (baseFilter.isEmpty) numAssetsPerActor
              else {
                (Math.max(
                  1,
                  Math
                    .ceil(
                      (numAssetsPerActor.toDouble / (baseFilter.size.toDouble * synchronizers.size.toDouble * transferBatchSize.toDouble))
                    )
                    .toLong,
                ) * transferBatchSize)
              }
            sendAssetRequest(res, numRequest, issuers, payloadSize)
            false
          }
        case _ => false
      }

  private def pickRandom[T](seq: Seq[T]): T =
    seq(ThreadLocalRandom.current().nextInt(seq.length))

  @tailrec
  private def computeCommands(
      available: Int,
      numAssets: Int,
      cmds: List[SubCommand],
      usedIssuer: Set[Party],
      possibleReceiver: IndexedSeq[Party],
  ): List[SubCommand] = if (available == 0 || possibleReceiver.isEmpty) cmds
  else {
    val pickedIssuer =
      // get issuer that needs reassignment
      reassignIssuers.get().find(c => !usedIssuer.contains(c)).map((_, true)).orElse {
        // otherwise get issuer we haven't processed yet (assets are only marked pending afterwards)
        val availableIssuers = assets.keys().filterNot(usedIssuer.contains).toList
        if (availableIssuers.nonEmpty) {
          Some(
            (pickRandom(availableIssuers), false)
          )
        } else None
      }
    // determine target sync
    def pickSynchronizer(issuer: Party, reassign: Boolean) = if (reassign) {
      // if we need to reassign
      val misplaced = assets
        .take(
          issuer,
          1,
          { case (syncId, asset) =>
            !syncId.startsWith(asset.data.location + "::")
          },
        )
        .headOption
      misplaced
        .flatMap { case (_, asset) =>
          synchronizers.find(_.uid.identifier.str == asset.data.location)
        }
        .getOrElse {
          logger.info(s"All assets of $issuer have been reassigned to the right synchronizer")
          reassignIssuers.updateAndGet(_ - issuer).discard
          pickRandom(synchronizers)
        }
    } else pickRandom(synchronizers)

    def pickAssets(issuer: Party, synchronizerId: SynchronizerId, reassign: Boolean) =
      assets.take(
        issuer,
        numAssets,
        {
          // pick assets for this target sync
          case (syncId, asset) if synchronizerId.uid.identifier.str == asset.data.location =>
            // if we are in reassignment mode, pick assets not yet in place
            if (reassign) syncId != synchronizerId.uid.toProtoPrimitive
            else true
          case _ => false
        },
      )

    pickedIssuer match {
      case None => cmds // no more issuer, we are done
      case Some((issuer, reassign)) =>
        val synchronizer = pickSynchronizer(issuer, reassign)
        val toTransfer = pickAssets(issuer, synchronizer, reassign).map(_._2)
        if (toTransfer.nonEmpty) {
          val next = possibleReceiver(ThreadLocalRandom.current().nextInt(possibleReceiver.length))
          val cmd = buildAssetTransfer(
            toTransfer = toTransfer,
            issuer = issuer,
            newOwner = next,
            synchronizerId = synchronizer,
          )

          val (newAvailable, newUsedIssuers, newCommands) = cmd
            .map { cmd =>
              (available - 1, usedIssuer + issuer, cmd +: cmds)
            }
            .getOrElse((available, usedIssuer, cmds))

          computeCommands(
            newAvailable,
            numAssets,
            newCommands,
            newUsedIssuers,
            possibleReceiver.filterNot(_ == next),
          )

        } else {
          computeCommands(available, numAssets, cmds, usedIssuer + issuer, possibleReceiver)
        }
    }
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
        )
        master.data.mode match {
          case Mode.PREPAREISSUER => false
          case Mode.PREPARETRADER =>
            prepareAndDetermineIfWeAreReady(master, params).discard
            false
          case Mode.THROUGHPUT =>
            val ready = prepareAndDetermineIfWeAreReady(master, params)
            updateStats(master)
            if (!ready) {
              false
            } else {
              rate.updateRate(CantonTimestamp.now())
              val available = determineAvailable()
              if (available > 0) {
                val cmds = computeCommands(
                  available,
                  params.transferBatchSize.toInt,
                  List.empty,
                  Set.empty,
                  this.traderBalancer.get(),
                )
                // submit batches
                submitBatched(
                  cmds,
                  settings.batchSize,
                  rate,
                  settings.duplicateSubmissionDelay,
                )
              }

              available > 0
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

  private def updateStats(master: M.orchestration.TestRun.Contract): Unit =
    // update statistics if necessary
    testResult.one(()).foreach { case (_, res) =>
      sendStatsUpdates(res, master, None, Some(transferStats))
    }

  private def updateTransferStats(
      mode: M.orchestration.Mode,
      transferStats: TraderStats,
      numAssets: Int,
  ): Unit = {

    val lastStatsUpdate = currentStatus.get() match {
      case Some(ts: TraderStatus) => ts.timestamp
      case _ => Instant.MIN
    }
    if (lastStatsUpdate < Instant.now.minusMillis(500)) {
      logger.info(
        s"Inventory ${assets.totalNum} (r=${assets.totalNum / Math
            .max(1.0, referenceAssetQuantity.get().toDouble)}), rate $rate, stats $transferStats"
      )
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
          )
        )
      )
    }
  }

}
