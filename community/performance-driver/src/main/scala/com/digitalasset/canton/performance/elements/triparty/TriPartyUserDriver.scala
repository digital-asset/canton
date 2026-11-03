// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.performance.elements.triparty

import com.daml.grpc.adapter.ExecutionSequencerFactory
import com.daml.ledger.javaapi
import com.daml.metrics.api.MetricHandle.LabeledMetricsFactory
import com.daml.metrics.api.{MetricInfo, MetricName, MetricQualification, MetricsContext}
import com.digitalasset.canton.LfPartyId
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.performance.Connectivity
import com.digitalasset.canton.performance.PartyRole.TriPartyUser
import com.digitalasset.canton.performance.acs.ContractStore
import com.digitalasset.canton.performance.elements.DriverStatus.{
  StepStatus,
  TraderStatus,
  TransferStatus,
}
import com.digitalasset.canton.performance.elements.dvp.TraderStats
import com.digitalasset.canton.performance.elements.{DriverControl, ParticipantDriver, StatsUpdater}
import com.digitalasset.canton.performance.model.java as M
import com.digitalasset.canton.performance.model.java.orchestration.{Mode, ParticipantFlag}
import com.digitalasset.canton.topology.SynchronizerId
import org.apache.pekko.actor.ActorSystem

import java.time.Instant
import java.util.concurrent.atomic.AtomicBoolean
import scala.annotation.tailrec
import scala.collection.mutable
import scala.concurrent.ExecutionContextExecutor
import scala.jdk.CollectionConverters.*
import scala.math.Ordered.orderingToOrdered

class TriPartyUserDriver(
    connectivity: Connectivity,
    partyLf: LfPartyId,
    masterLf: LfPartyId,
    role: TriPartyUser,
    prefix: MetricName,
    metricsFactory: LabeledMetricsFactory,
    loggerFactory: NamedLoggerFactory,
    control: DriverControl,
    protected val baseSynchronizerId: SynchronizerId,
    protected val otherSynchronizers: Seq[SynchronizerId],
)(implicit
    ec: ExecutionContextExecutor,
    actorSystem: ActorSystem,
    executionSequencerFactory: ExecutionSequencerFactory,
) extends ParticipantDriver(
      connectivity,
      partyLf,
      masterLf,
      role,
      registerGenerator = false,
      prefix,
      metricsFactory,
      loggerFactory,
      control,
      baseSynchronizerId,
    )
    with StatsUpdater {

  private val transferMetric =
    metricsFactory.gauge[Long](
      MetricInfo(
        prefix :+ "transfers",
        "How many substitution cycles have been performed",
        MetricQualification.Debug,
      ),
      0,
    )(
      MetricsContext.Empty
    )
  private val transferStats = new TraderStats(transferMetric)
  private val allAreOpen = new AtomicBoolean(false)
  private val accountToSync = new mutable.ArrayBuffer[(String, SynchronizerId)]()

  private val assets = new ContractStore[
    M.triparty.asset.Asset.Contract,
    M.triparty.asset.Asset.ContractId,
    M.triparty.asset.Asset,
    String,
  ](
    "assets",
    M.triparty.asset.Asset.COMPANION,
    index = x => x.data.account,
    filter = _ => true,
    loggerFactory,
  ) {}

  private val agreements = new ContractStore[
    M.triparty.tripartyagreement.TriPartyAgreement.Contract,
    M.triparty.tripartyagreement.TriPartyAgreement.ContractId,
    M.triparty.tripartyagreement.TriPartyAgreement,
    String,
  ](
    "agreements",
    M.triparty.tripartyagreement.TriPartyAgreement.COMPANION,
    index = x => x.data.account,
    filter = _ => true,
    loggerFactory,
  )

  private val settlementApp = new ContractStore[
    M.triparty.settle.SettlementApp.Contract,
    M.triparty.settle.SettlementApp.ContractId,
    M.triparty.settle.SettlementApp,
    String,
  ](
    "settlementApp",
    M.triparty.settle.SettlementApp.COMPANION,
    index = x => x.data.registry,
    filter = _ => true,
    loggerFactory,
  ) {}

  private val assetRequests = new ContractStore[
    M.triparty.bootstrap.BootstrapAsset.Contract,
    M.triparty.bootstrap.BootstrapAsset.ContractId,
    M.triparty.bootstrap.BootstrapAsset,
    Unit,
  ](
    "assetRequests",
    M.triparty.bootstrap.BootstrapAsset.COMPANION,
    index = _ => (),
    filter = _ => true,
    loggerFactory,
  ) {}

  listeners.appendAll(Seq(assets, agreements, settlementApp, assetRequests))

  protected def subscribeToTemplates: Seq[javaapi.data.Identifier] = Seq(
    M.triparty.asset.Asset.TEMPLATE_ID,
    M.triparty.tripartyagreement.TriPartyAgreement.TEMPLATE_ID,
    M.triparty.settle.SettlementApp.TEMPLATE_ID,
    M.triparty.bootstrap.BootstrapAsset.TEMPLATE_ID,
  )

  override def flush(): Boolean =
    masterContract.one(()).exists { case (_, master) =>
      if (master.data.mode == Mode.DONE) {
        finished()
        false
      } else if (
        updateParticipantFlagAndReturnReady(
          master
        ) && !settlementApp.entirelyEmpty && assetRequests.entirelyEmpty
      ) {
        rate.updateRate(CantonTimestamp.now())
        updateStats(master)
        if (master.data.mode == Mode.PREPARETRADER || master.data.mode == Mode.THROUGHPUT) {
          updateTransferStats(master.data.mode)
          if (rate.available > 0) {
            // open agreements first
            if (!allAreOpen.get()) {
              initAccountToSync(master.data)
              openAgreements()
            } else {
              // run substitution transfers
              substituteNext(attempts = accountToSync.size)
            }
          } else false
        } else false
      } else false
    }

  private def updateStats(master: M.orchestration.TestRun.Contract): Unit =
    // update statistics if necessary
    testResult.one(()).foreach { case (_, res) =>
      sendStatsUpdates(res, master, None, Some(transferStats))
    }

  private def updateTransferStats(
      mode: M.orchestration.Mode
  ): Unit = {

    val lastStatsUpdate = currentStatus.get() match {
      case Some(ts: TraderStatus) => ts.timestamp
      case _ => Instant.MIN
    }
    if (lastStatsUpdate < Instant.now.minusMillis(500)) {
      logger.info(
        s"mode $mode rate $rate, stats $transferStats"
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
            assets = assets.totalNum,
            transfers = StepStatus(transferStats.submitted, transferStats.observed, open = 0),
          )
        )
      )
    }
  }

  @tailrec
  private def substituteNext(attempts: Int): Boolean = if (attempts == 0) false
  else {
    // pick a random account
    val (account, sync) = accountToSync(scala.util.Random.nextInt(accountToSync.size))
    agreements.one(account) match {
      case Some((_, agreement)) =>
        val (eligible, assetsInAccount) = eligibleAssets(agreement.data)
        val substitutions = eligible
          .grouped(2)
          .flatMap {
            case List((one, oneQty), (two, twoQty)) =>
              val ret = if (twoQty > 0) {
                new M.triparty.tripartyagreement.Substitution(
                  one,
                  twoQty.bigDecimal, // in the test, all assets have the same qty
                  two,
                  twoQty.bigDecimal,
                )
              } else {
                new M.triparty.tripartyagreement.Substitution(
                  two,
                  oneQty.bigDecimal,
                  one,
                  oneQty.bigDecimal,
                )
              }
              Some(ret)
            case _ => None
          }
          .toList
          .asJava
        assetsInAccount.headOption.map(_.data.registry).flatMap(settlementApp.one) match {
          case Some(settleCid) =>
            val cmd = agreement.id
              .exerciseSubstitute(
                substitutions,
                assetsInAccount.map(_.id).asJava,
                settleCid._2.id,
              )
              .commands()
              .asScala
              .toSeq
            logger.debug(
              s"Submitting to $sync substitution for agreement " + agreement.data.account + "\n  " + substitutions.asScala
                .map { s =>
                  s"sub ${s.oldAssetId} / ${s.oldQty} => ${s.newAssetId} / ${s.newQty}"
                }
                .mkString("\n  ") + "\n  assets\n  :"
                + assetsInAccount
                  .map { a =>
                    s"own=${a.data.owner} id=${a.data.id} acc=${a.data.account} qt=${a.data.qty}, cid=${a.id}\n"
                  }
                  .mkString("\n  ")
            )
            val submissionF =
              submitCommand(
                "substitute",
                cmd,
                s"Substituting assets for agreement ${agreement.data.account}",
                synchronizerId = Some(sync),
              )
            val num = substitutions.size()
            transferStats.updateSubmittedCount(num).discard
            submissionF.foreach { success =>
              if (success) {
                transferStats.updateObservedCount(num).discard
              } else {
                transferStats.updateSubmittedCount(-num).discard
              }
            }

            // won't set the assets to pending as they are moved in sync with the rest
            setPending(agreements, agreement.id, submissionF)
            rate.newSubmission(submissionF)
            true
          case None =>
            logger.error("Where is my settlement app????")
            false
        }

      case None => substituteNext(attempts - 1)
    }

  }

  private def eligibleAssets(
      agreement: M.triparty.tripartyagreement.TriPartyAgreement
  ) = {
    val assetsInAccount = assets.find(agreement.account).map(_._2)
    val assetMap = assetsInAccount.filter(_.data.owner == partyLf).groupBy { asset =>
      asset.data.id
    }
    val eligible = agreement.eligibility.asScala.iterator.zipWithIndex.map { case (jtuple, _) =>
      val id = jtuple._1
      val qty =
        assetMap
          .getOrElse(id, Seq.empty)
          .map(_.data.qty)
          .foldLeft(BigDecimal.valueOf(0)) { case (a, b) =>
            a + b
          }
      (id, qty)
    }.toList
    (eligible, assetsInAccount)
  }

  private def initAccountToSync(master: M.orchestration.TestRun): Unit =
    // as bootstrap creates all agreements at once, we can populate the accountToSync once
    if (accountToSync.isEmpty && !agreements.entirelyEmpty && assetRequests.entirelyEmpty) {
      val myIdx = master.traders.indexOf(partyLf)
      if (myIdx < 0) {
        sys.error("The party is not listed in the master contracts traders list.")
      }
      val allSyncs = baseSynchronizerId +: otherSynchronizers

      val allSubregisters = settlementApp.all
        .map { case (_, _, settle) =>
          settle
        }
        .toSeq
        .sortBy(_.data.registry)
      val accountsWithSubregister = agreements.all
        .map { case (_, _, agreement) =>
          agreement
        }
        .toSeq
        .sortBy(_.data.account)
        .flatMap { agreement =>
          // figure out the subregistry by looking at one asset
          val account = agreement.data.account
          assets
            .one(account)
            .map { case (_, asset) =>
              (account, asset.data.registry)
            }
        }
      accountsWithSubregister.foreach { case (account, subregister) =>
        val subIdx = allSubregisters.indexWhere(_.data.registry == subregister)
        if (subIdx < 0) {
          logger.error(
            s"Could not find a settlement app for subregister $subregister for account $account"
          )
        } else {
          // pick a fixed synchronizer per settlement app, but rotate it based on the
          // index of the trader in order to spread the load across synchronizers
          val sync = allSyncs((subIdx + myIdx) % allSyncs.length)
          accountToSync.append((account, sync))
        }
      }
      logger.info(
        "Account to sync:\n  " + accountToSync
          .map { case (account, sync) =>
            s"$account => $sync"
          }
          .mkString("\n  ")
      )
    }

  private def openAgreements(): Boolean = {
    // we need to "open" the agreements
    val notOpen = agreements.allAvailable
      .filterNot(_.data.amount.compareTo(BigDecimal.valueOf(0)) > 0)
    if (notOpen.nonEmpty) {
      notOpen.headOption
        .exists { agreement =>
          // find assets in the account
          val (eligible, assetsInAccount) = eligibleAssets(agreement.data)
          // pledge all the even numbered assets fully
          val open = eligible.zipWithIndex
            .filter { case (_, idx) => idx % 2 == 0 }
            .map { case ((id, qty), _) =>
              new com.digitalasset.canton.performance.model.java.da.types.Tuple2(
                id,
                qty.bigDecimal,
              )
            }
            .toList
            .asJava
          // submit open choice
          assetsInAccount.headOption.map(_.data.registry).flatMap(settlementApp.one) match {
            case Some((_, settle)) =>
              val cmd = agreement.id
                .exerciseOpen(
                  open,
                  assetsInAccount.map(_.id).asJava,
                  settle.id,
                )
                .commands
                .asScala
                .toSeq
              val syncId = accountToSync
                .find { case (account, _) => account == agreement.data.account }
                .map(_._2)
              logger.info(
                s"Opening agreement ${agreement.data.account} on sync $syncId with sub ${settle.data.registry}"
              )
              val submissionF =
                submitCommand(
                  "open-agreement",
                  cmd,
                  s"Opening agreement ${agreement.data.account}",
                  synchronizerId = syncId,
                )
              setPending(agreements, agreement.id, submissionF)
              setPending(settlementApp, settle.id, submissionF, willArchive = false)
              rate.newSubmission(submissionF)
            case None =>
              logger.debug(
                s"No settlement app for ${agreement.data.account}, cannot open yet (likely concurrent submission that might perform reassignment)"
              )
          }
          true
        }
    } else if (!agreements.hasPending) {
      logger.info("All agreements are open!")
      allAreOpen.set(true)
      true
    } else false
  }

  private def updateParticipantFlagAndReturnReady(
      master: M.orchestration.TestRun.Contract
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
          // waiting for settlement app to show up
          if (settlementApp.entirelyEmpty || !assetRequests.entirelyEmpty || !allAreOpen.get()) {
            ensureFlag(ParticipantFlag.INITIALISING)
            val (open, notopen) = agreements.all.partition { case (_, _, agreement) =>
              agreement.data.amount.compareTo(BigDecimal.valueOf(0)) > 0
            }
            logger.info(
              s"Driver is not yet ready. SettlementApps=${settlementApp.totalNum}, asset-requests=${assetRequests.totalNum}, agreements-open=${open.size}, agreements-not-open=${notopen.size}, all-are-open=${allAreOpen.get()}"
            )
          } else {
            updateFlag(res, M.orchestration.ParticipantFlag.READY)
          }
          true
        } else false // we are finished
      }

}
