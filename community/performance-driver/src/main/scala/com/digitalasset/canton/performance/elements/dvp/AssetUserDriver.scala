// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.performance.elements.dvp

import com.daml.ledger.javaapi.data.{Identifier, Party}
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.performance.acs.ContractStore
import com.digitalasset.canton.performance.control.Balancer
import com.digitalasset.canton.performance.elements.ParticipantDriver
import com.digitalasset.canton.performance.model.java as M
import com.digitalasset.canton.performance.model.java.orchestration.TestRun
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.util.ErrorUtil

import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.ExecutionContextExecutor
import scala.jdk.CollectionConverters.*
import scala.util.{Random, Success}

trait AssetUserDriver {
  self: ParticipantDriver =>

  protected implicit def ec: ExecutionContextExecutor

  protected val issuerBalancer = new Balancer()
  protected val traderBalancer = new Balancer()

  protected val assets = new ContractStore[
    M.dvp.asset.Asset.Contract,
    M.dvp.asset.Asset.ContractId,
    M.dvp.asset.Asset,
    Party,
  ](
    "assets i own",
    M.dvp.asset.Asset.COMPANION,
    index = x => new Party(x.data.issuer),
    filter = x => x.data.owner == party.getValue,
    loggerFactory,
  ) {
    override def update(before: M.dvp.asset.Asset.Contract): M.dvp.asset.Asset.Contract =
      new M.dvp.asset.Asset.Contract(
        before.id,
        new M.dvp.asset.Asset(
          before.data.registrar,
          before.data.issuer,
          before.data.id,
          // replace payload with payload string length to not run into OOM if we use large payloads
          before.data.payload.length.toString,
          before.data.owner,
          before.data.bystander,
          before.data.location,
        ),
        before.signatories,
        before.observers,
      )

    override protected def contractArchived(
        archive: M.dvp.asset.Asset.Contract,
        owner: Party,
    ): Unit = assetArchived()

  }

  protected def assetArchived(): Unit = ()

  protected val assetRequests = new ContractStore[
    M.dvp.asset.AssetRequest.Contract,
    M.dvp.asset.AssetRequest.ContractId,
    M.dvp.asset.AssetRequest,
    Unit,
  ](
    "assets i requested",
    M.dvp.asset.AssetRequest.COMPANION,
    index = _ => (),
    filter = x => x.data.requester == party.getValue,
    loggerFactory,
  )

  protected def baseSynchronizerId: SynchronizerId
  protected def otherSynchronizers: Seq[SynchronizerId]

  protected val synchronizers: NonEmpty[Seq[SynchronizerId]] =
    NonEmpty.mk(Seq, baseSynchronizerId, otherSynchronizers*)

  protected val locations: NonEmpty[Seq[String]] = {
    val prefixes = synchronizers.map(_.uid.identifier.str)
    ErrorUtil.requireState(
      prefixes.sizeIs == prefixes.distinct.length,
      "I NEED UNIQUE SYNCHRONIZER PREFIX NAMES BUT HAVE " + synchronizers,
    )
    prefixes
  }

  protected val shouldBeConsistent = new AtomicBoolean(false)

  override protected def subscribeToTemplates: Seq[Identifier] =
    Seq(
      M.dvp.trade.Propose.TEMPLATE_ID,
      M.dvp.asset.Asset.TEMPLATE_ID,
      M.dvp.asset.AssetTransfer.TEMPLATE_ID,
      M.dvp.asset.AssetRequest.TEMPLATE_ID,
      M.dvp.asset.TransferPreapproval.TEMPLATE_ID,
    )

  override protected def masterCreated(master: TestRun.Contract): Unit = {
    issuerBalancer.updateMembers(master.data.issuers.asScala.toSeq.map(new Party(_)))
    traderBalancer.updateMembers(
      master.data.traders.asScala.toSeq.filter(_ != party.getValue).map(new Party(_))
    )
  }

  protected def acquireAssets(
      master: M.orchestration.TestRun.Contract,
      payloadSize: Long,
      numAssetsPerIssuer: Long,
  ): Boolean =
    generator.one(()).exists { case (_, res) =>
      // if there is a new issuer that we haven't seen before
      val issuers = master.data.issuers.asScala.toSeq.filterNot(res.data.processedIssuers.contains)
      if (issuers.nonEmpty) {
        logger.info(s"Acquiring assets from ${issuers.length} issuers $issuers")
        val ids = issuers.map(_ => UUID.randomUUID().toString)
        val payload = Random.alphanumeric.take(payloadSize.toInt).mkString
        // the exercise will add the issuer to the generator, ensuring we don't request twice
        val cmd =
          res.id
            .exerciseRequestAssets(
              numAssetsPerIssuer,
              issuers.asJava,
              ids.asJava,
              payload,
              locations.forgetNE.asJava,
            )
            .commands
            .asScala
            .toSeq
        val submissionF =
          submitCommand(
            "request-assets",
            cmd,
            s"acquiring $numAssetsPerIssuer assets from $issuers",
          )
        setPending(generator, res.id, submissionF)
        shouldBeConsistent.set(false)
        // unmark if command fails (we will retry)
        submissionF.onComplete {
          case Success(true) =>
          case x =>
            logger.warn(s"Requesting assets failed with $x, retrying")
        }
        false
      } else true
    }

  protected def signalReadyIfWeAre(
      master: M.orchestration.TestRun.Contract,
      numAssetsPerIssuer: Long,
      numInTransfer: Int,
  ): Boolean =
    testResult
      .one(())
      .exists { case (_, res) =>
        if (res.data.flag == M.orchestration.ParticipantFlag.READY)
          true
        else if (res.data.flag == M.orchestration.ParticipantFlag.INITIALISING) {
          val expected = locations.size * numAssetsPerIssuer * master.data.issuers.size
          val actual = assets.allAvailable.size + numInTransfer
          if (actual == expected) {
            // signal that we are ready if we have enough assets
            updateFlag(res, M.orchestration.ParticipantFlag.READY)
          } else {
            logger.info(
              s"Driver does not yet have the required number of assets (expected: $expected, actual: $actual)."
            )
          }
          false
        } else false
      }

}
