// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.performance.elements.dvp

import com.daml.ledger.javaapi.data.{Identifier, Party}
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.performance.acs.ContractStore
import com.digitalasset.canton.performance.elements.ParticipantDriver
import com.digitalasset.canton.performance.model.java as M
import com.digitalasset.canton.performance.model.java.dvp.asset.Asset
import com.digitalasset.canton.performance.model.java.generator.Generator
import com.digitalasset.canton.topology.SynchronizerId
import com.digitalasset.canton.util.ErrorUtil

import java.util.UUID
import scala.concurrent.ExecutionContextExecutor
import scala.jdk.CollectionConverters.*
import scala.util.{Random, Success}

trait AssetUserDriver {
  self: ParticipantDriver =>

  protected implicit def ec: ExecutionContextExecutor

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

    override protected def contractCreated(
        create: Asset.Contract,
        index: Party,
        synchronizerId: String,
    ): Unit =
      assetCreated(create, synchronizerId)

    override protected def contractArchived(
        archive: M.dvp.asset.Asset.Contract,
        owner: Party,
    ): Unit = assetArchived()

  }
  protected def assetCreated(create: Asset.Contract, synchronizerId: String): Unit = ()
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

  protected val synchronizers: NonEmpty[Seq[SynchronizerId]] = {
    val tmp = NonEmpty.mk(Seq, baseSynchronizerId, otherSynchronizers*)
    ErrorUtil.requireState(
      tmp.sizeIs == tmp.distinct.length,
      "I NEED UNIQUE SYNCHRONIZER PREFIX NAMES BUT HAVE " + tmp,
    )
    tmp
  }

  override protected def subscribeToTemplates: Seq[Identifier] =
    Seq(
      M.dvp.trade.Propose.TEMPLATE_ID,
      M.dvp.asset.Asset.TEMPLATE_ID,
      M.dvp.asset.AssetTransfer.TEMPLATE_ID,
      M.dvp.asset.AssetRequest.TEMPLATE_ID,
      M.dvp.asset.TransferPreapproval.TEMPLATE_ID,
    )

  protected def sendAssetRequest(
      res: Generator.Contract,
      numAssetsToRequest: Long,
      issuers: Seq[String],
      payloadSize: Long,
  ): Unit = {
    logger.info(s"Acquiring $numAssetsToRequest assets from ${issuers.length} issuers $issuers")
    val ids = issuers.map(_ => UUID.randomUUID().toString)
    val payload = Random.alphanumeric.take(payloadSize.toInt).mkString
    // the exercise will add the issuer to the generator, ensuring we don't request twice
    val cmd =
      res.id
        .exerciseRequestAssets(
          numAssetsToRequest,
          issuers.asJava,
          ids.asJava,
          payload,
          synchronizers.map(_.uid.identifier.str).forgetNE.asJava,
        )
        .commands
        .asScala
        .toSeq
    val submissionF =
      submitCommand(
        "request-assets",
        cmd,
        s"acquiring $numAssetsToRequest assets from $issuers",
      )
    setPending(generator, res.id, submissionF)
    rate.newSubmission(submissionF)
    // unmark if command fails (we will retry)
    submissionF.onComplete {
      case Success(true) =>
      case x =>
        logger.warn(s"Requesting assets failed with $x, retrying")
    }
  }

}
