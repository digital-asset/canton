// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.data.db

import cats.syntax.either.*
import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.resource.DbStorage.DbAction
import com.digitalasset.canton.resource.DbStorage.Implicits.setParameterByteString
import com.digitalasset.canton.resource.DbStorage.Profile.{H2, Postgres}
import com.digitalasset.canton.resource.{DbStorage, DbStore}
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.store.db.DbDeserializationException
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.bindings.pekko.PekkoModuleSystem.{
  PekkoEnv,
  PekkoFutureUnlessShutdown,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.integration.canton.topology.TopologyActivationTime
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.IssConsensusModule
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.data.EpochStore.{
  Block,
  Epoch,
  EpochInProgress,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.modules.consensus.iss.data.{
  EpochStore,
  EpochStoreReader,
  Genesis,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.BftOrderingIdentifiers.{
  BftNodeId,
  BlockNumber,
  EpochLength,
  EpochNumber,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.SignedMessage
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.iss.EpochInfo
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.data.ordering.{
  CommitCertificate,
  OrderedBlock,
  OrderedBlockForOutput,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.ConsensusSegment.ConsensusMessage
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.framework.modules.ConsensusSegment.ConsensusMessage.{
  Commit,
  PbftNetworkMessage,
  PbftViewChangeMessage,
  PrePrepare,
}
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.v30
import com.digitalasset.canton.synchronizer.sequencing.sequencer.bftordering.v30.ConsensusMessage as ProtoConsensusMessage
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.{ProtoDeserializationError, RichGeneratedMessage}
import com.google.protobuf.ByteString
import slick.dbio.DBIOAction
import slick.jdbc.{GetResult, PositionedResult, SetParameter}

import scala.concurrent.ExecutionContext
import scala.util.Try

import DbEpochStore.*

class DbEpochStore(
    override protected val storage: DbStorage,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends EpochStore[PekkoEnv]
    with EpochStoreReader[PekkoEnv]
    with DbStore {

  import storage.api.*

  private val profile = storage.profile

  private implicit val readEpoch: GetResult[EpochInfo] = GetResult { r =>
    EpochInfo(
      EpochNumber(r.nextLong()),
      BlockNumber(r.nextLong()),
      EpochLength(r.nextLong()),
      TopologyActivationTime(CantonTimestamp.assertFromLong(r.nextLong())),
    )
  }

  private implicit val readPbftMessage: GetResult[SignedMessage[PbftNetworkMessage]] =
    GetResult(parseSignedMessage(from => IssConsensusModule.parseConsensusNetworkMessage(from, _)))

  private implicit val tryReadPrePrepareMessageAndEpochInfo: GetResult[(PrePrepare, EpochInfo)] =
    GetResult { r =>
      val prePrepare = parseSignedMessage(_ => PrePrepare.fromProtoConsensusMessage)(r)
      prePrepare.message -> readEpoch(r)
    }

  private implicit val readCommitMessage: GetResult[SignedMessage[Commit]] = GetResult {
    parseSignedMessage(_ => Commit.fromProtoConsensusMessage)
  }

  private def createFuture[X](
      actionName: String,
      orderingStage: String,
  )(future: => FutureUnlessShutdown[X])(implicit
      traceContext: TraceContext
  ): PekkoFutureUnlessShutdown[X] =
    PekkoFutureUnlessShutdown(
      actionName,
      () => storage.synchronizeWithClosing(actionName)(future),
      Some(orderingStage),
    )

  private def parseSignedMessage[A <: PbftNetworkMessage](
      parse: BftNodeId => ProtoConsensusMessage => ByteString => ParsingResult[A]
  )(
      r: PositionedResult
  ): SignedMessage[A] = {
    val messageBytes = r.nextBytes()

    val messageOrError = for {
      signedMessageProto <- Try(v30.SignedMessage.parseFrom(messageBytes)).toEither
        .leftMap(x => ProtoDeserializationError.OtherError(x.toString))
      message <- SignedMessage.fromProtoWithNodeId(v30.ConsensusMessage)(parse)(
        signedMessageProto
      )
    } yield message

    messageOrError.fold(
      error => throw new DbDeserializationException(s"Could not deserialize pbft message: $error"),
      identity,
    )
  }

  private implicit val setPbftMessagesParameter: SetParameter[SignedMessage[PbftNetworkMessage]] =
    (msg, pp) => pp >> msg.toProtoV1.checkedToByteString

  override def startEpoch(
      epoch: EpochInfo
  )(implicit traceContext: TraceContext): PekkoFutureUnlessShutdown[Unit] =
    createFuture(startEpochActionName(epoch), orderingStage = functionFullName) {
      storage.update_(
        profile match {
          case _: Postgres =>
            sqlu"""insert into ord_epochs(epoch_number, start_block_number, epoch_length, topology_ts, in_progress)
                   values (${epoch.number}, ${epoch.startBlockNumber}, ${epoch.length}, ${epoch.topologyActivationTime.value}, true)
                   on conflict (epoch_number) do nothing
                """
          case _: H2 =>
            sqlu"""merge into ord_epochs
                   using dual on (
                     ord_epochs.epoch_number = ${epoch.number}
                   )
                   when not matched then
                     insert (epoch_number, start_block_number, epoch_length, topology_ts, in_progress)
                     values (${epoch.number}, ${epoch.startBlockNumber}, ${epoch.length}, ${epoch.topologyActivationTime.value},  true)
                """
        },
        functionFullName,
      )
    }

  override def completeEpoch(
      epochNumber: EpochNumber
  )(implicit traceContext: TraceContext): PekkoFutureUnlessShutdown[Unit] =
    createFuture(completeEpochActionName(epochNumber), orderingStage = functionFullName) {
      storage.update_(
        for {
          // delete all in-progress messages after an epoch ends and before we start adding new messages in the new epoch
          _ <- sqlu"delete from ord_pbft_messages_in_progress"
          _ <- sqlu"""update ord_epochs set in_progress = false
                      where epoch_number = $epochNumber
                      """
        } yield (),
        functionFullName,
      )
    }

  override def latestEpoch(includeInProgress: Boolean)(implicit
      traceContext: TraceContext
  ): PekkoFutureUnlessShutdown[Epoch] =
    createFuture(latestEpochActionName, orderingStage = functionFullName) {
      storage
        .query(
          for {
            epochInfo <-
              if (!includeInProgress)
                sql"""select epoch_number, start_block_number, epoch_length, topology_ts
                    from ord_epochs
                    where in_progress = false
                    order by epoch_number desc
                    limit 1
                 """.as[EpochInfo]
              else
                sql"""select epoch_number, start_block_number, epoch_length, topology_ts
                    from ord_epochs
                    order by epoch_number desc
                    limit 1
                 """.as[EpochInfo]
            epoch = epochInfo.lastOption.getOrElse(Genesis.GenesisEpochInfo)
            lastBlockCommitMessages <-
              sql"""select message
                  from ord_pbft_messages_completed pbft_message
                  where pbft_message.block_number = ${epoch.lastBlockNumber} and pbft_message.discriminator = $CommitMessageDiscriminator
                  order by pbft_message.from_sequencer_id
               """.as[SignedMessage[Commit]]
          } yield Epoch(epoch, lastBlockCommitMessages),
          functionFullName,
        )
    }

  override def addPrePrepare(prePrepare: SignedMessage[ConsensusMessage.PrePrepare])(implicit
      traceContext: TraceContext
  ): PekkoFutureUnlessShutdown[Unit] =
    createFuture(addPrePrepareActionName(prePrepare), orderingStage = functionFullName) {
      storage.update_(
        DBIOAction.sequence(insertInProgressPbftMessages(Seq(prePrepare))),
        functionFullName,
      )
    }

  override def addPrepares(
      prepares: Seq[SignedMessage[ConsensusMessage.Prepare]]
  )(implicit traceContext: TraceContext): PekkoFutureUnlessShutdown[Unit] =
    createFuture(addPreparesActionName, orderingStage = functionFullName) {
      storage.update_(
        DBIOAction.sequence(insertInProgressPbftMessages(prepares)).transactionally,
        functionFullName,
      )
    }

  override def addViewChangeMessage[M <: PbftViewChangeMessage](
      viewChangeMessage: SignedMessage[M]
  )(implicit
      traceContext: TraceContext
  ): PekkoFutureUnlessShutdown[Unit] =
    createFuture(
      addViewChangeMessageActionName(viewChangeMessage),
      orderingStage = functionFullName,
    ) {
      storage.update_(
        DBIOAction.sequence(insertInProgressPbftMessages(Seq(viewChangeMessage))).transactionally,
        functionFullName,
      )
    }

  override def addOrderedBlock(
      prePrepare: SignedMessage[PrePrepare],
      commitMessages: Seq[SignedMessage[Commit]],
  )(implicit
      traceContext: TraceContext
  ): PekkoFutureUnlessShutdown[Unit] = {
    val epochNumber = prePrepare.message.blockMetadata.epochNumber
    val blockNumber = prePrepare.message.blockMetadata.blockNumber
    createFuture(
      addOrderedBlockActionName(epochNumber, blockNumber),
      orderingStage = functionFullName,
    ) {
      storage.update_(
        DBIOAction
          .sequence(
            cleanUpInProgressMessages(blockNumber) +: (insertFinalPbftMessages(
              commitMessages
            ) ++ insertFinalPbftMessages(Seq(prePrepare)))
          )
          .transactionally,
        functionFullName,
      )
    }
  }

  private def cleanUpInProgressMessages(blockNumber: Long) =
    // delete pre-prepares and prepares, but not view change messages
    sqlu"delete from ord_pbft_messages_in_progress where block_number=$blockNumber and discriminator <= $PrepareMessageDiscriminator"

  private def insertInProgressPbftMessages[M <: PbftNetworkMessage](
      messages: Seq[SignedMessage[M]]
  ): Seq[DbAction.WriteOnly[Int]] =
    messages.headOption.fold(Seq.empty[DbAction.WriteOnly[Int]]) { head =>
      val discriminator = getDiscriminator(head.message)
      messages.map { msg =>
        val from = msg.from
        val blockNumber = msg.message.blockMetadata.blockNumber
        val epochNumber = msg.message.blockMetadata.epochNumber
        val viewNumber = msg.message.viewNumber

        profile match {
          case _: Postgres =>
            sqlu"""insert into ord_pbft_messages_in_progress(block_number, epoch_number, view_number, message, discriminator, from_sequencer_id)
                   values ($blockNumber, $epochNumber, $viewNumber, $msg, $discriminator, $from)
                   on conflict (block_number, view_number, discriminator, from_sequencer_id) do nothing
                """
          case _: H2 =>
            sqlu"""merge into ord_pbft_messages_in_progress
                   using dual on (ord_pbft_messages_in_progress.block_number = $blockNumber
                     and ord_pbft_messages_in_progress.epoch_number = $epochNumber
                     and ord_pbft_messages_in_progress.view_number = $viewNumber
                     and ord_pbft_messages_in_progress.discriminator = $discriminator
                     and ord_pbft_messages_in_progress.from_sequencer_id = $from
                   )
                   when not matched then
                     insert (block_number, epoch_number, view_number, message, discriminator, from_sequencer_id)
                     values ($blockNumber, $epochNumber, $viewNumber, $msg, $discriminator, $from)
                """
        }
      }
    }

  private def insertFinalPbftMessages[M <: PbftNetworkMessage](
      messages: Seq[SignedMessage[M]]
  ): Seq[DbAction.WriteOnly[Int]] =
    messages.headOption.fold(Seq.empty[DbAction.WriteOnly[Int]]) { head =>
      val discriminator = getDiscriminator(head.message)
      messages.map { msg =>
        val sequencerId = msg.from
        val blockNumber = msg.message.blockMetadata.blockNumber
        val epochNumber = msg.message.blockMetadata.epochNumber

        profile match {
          case _: Postgres =>
            sqlu"""insert into ord_pbft_messages_completed(block_number, epoch_number, message, discriminator, from_sequencer_id)
                   values ($blockNumber, $epochNumber, $msg, $discriminator, $sequencerId)
                   on conflict (block_number, epoch_number, discriminator, from_sequencer_id) do nothing
                """
          case _: H2 =>
            sqlu"""merge into ord_pbft_messages_completed
                   using dual on (ord_pbft_messages_completed.block_number = $blockNumber
                     and ord_pbft_messages_completed.epoch_number = $epochNumber
                     and ord_pbft_messages_completed.discriminator = $discriminator
                     and ord_pbft_messages_completed.from_sequencer_id = $sequencerId
                   )
                   when not matched then
                     insert (block_number, epoch_number, message, discriminator, from_sequencer_id)
                     values ($blockNumber, $epochNumber, $msg, $discriminator, $sequencerId)
                """
        }
      }
    }

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  override def loadEpochProgress(activeEpochInfo: EpochInfo)(implicit
      traceContext: TraceContext
  ): PekkoFutureUnlessShutdown[EpochInProgress] =
    createFuture(loadEpochProgressActionName(activeEpochInfo), orderingStage = functionFullName) {
      storage
        .query(
          for {
            completeBlockMessages <-
              sql"""select message
                    from ord_pbft_messages_completed
                    where epoch_number = ${activeEpochInfo.number}
                    order by from_sequencer_id, block_number
                 """.as[SignedMessage[PbftNetworkMessage]](readPbftMessage)
            pbftMessagesForIncompleteBlocks <-
              sql"""select message
                    from ord_pbft_messages_in_progress
                    where epoch_number = ${activeEpochInfo.number}
                    order by from_sequencer_id, block_number, discriminator, view_number
                 """
                // had to set the GetResult explicitly because for some reason it was picking the one for commit messages
                .as[SignedMessage[PbftNetworkMessage]](readPbftMessage)
          } yield {
            val commits = getCommits(completeBlockMessages)
            val sortedBlocks = completeBlockMessages
              .collect { case s @ SignedMessage(pp: PrePrepare, _) =>
                val blockNumber = pp.blockMetadata.blockNumber
                Block(
                  activeEpochInfo.number,
                  blockNumber,
                  CommitCertificate(
                    s.asInstanceOf[SignedMessage[PrePrepare]],
                    commits.getOrElse(blockNumber, Seq.empty),
                  ),
                )
              }
              .sortBy(_.blockNumber)
            EpochInProgress(sortedBlocks, pbftMessagesForIncompleteBlocks)
          },
          functionFullName,
        )
    }

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  override def loadCompleteBlocks(
      startEpochNumberInclusive: EpochNumber,
      endEpochNumberInclusive: EpochNumber,
  )(implicit
      traceContext: TraceContext
  ): PekkoFutureUnlessShutdown[Seq[Block]] =
    createFuture(
      loadPrePreparesActionName(startEpochNumberInclusive, endEpochNumberInclusive),
      orderingStage = functionFullName,
    ) {
      storage
        .query(
          for {
            completeBlockMessages <-
              sql"""select message
                    from ord_pbft_messages_completed
                    where epoch_number >= $startEpochNumberInclusive
                      and epoch_number <= $endEpochNumberInclusive
                    order by from_sequencer_id, block_number
                 """.as[SignedMessage[PbftNetworkMessage]](readPbftMessage)
          } yield {
            val commits = getCommits(completeBlockMessages)
            completeBlockMessages
              .collect { case s @ SignedMessage(pp: PrePrepare, _) =>
                val blockNumber = pp.blockMetadata.blockNumber
                Block(
                  pp.blockMetadata.epochNumber,
                  blockNumber,
                  CommitCertificate(
                    s.asInstanceOf[SignedMessage[PrePrepare]],
                    commits.getOrElse(blockNumber, Seq.empty),
                  ),
                )
              }
              .sortBy(_.blockNumber)
          },
          functionFullName,
        )
    }

  override def loadEpochInfo(
      epochNumber: EpochNumber
  )(implicit traceContext: TraceContext): PekkoFutureUnlessShutdown[Option[EpochInfo]] =
    createFuture(loadEpochInfoActionName(epochNumber), orderingStage = functionFullName) {
      storage
        .query(
          sql"""select epoch_number, start_block_number, epoch_length, topology_ts
                from ord_epochs
                where epoch_number = $epochNumber
                limit 1
           """.as[EpochInfo],
          functionFullName,
        )
    }.map(_.headOption)

  override def loadOrderedBlocks(
      initialBlockNumber: BlockNumber
  )(implicit traceContext: TraceContext): PekkoFutureUnlessShutdown[Seq[OrderedBlockForOutput]] =
    createFuture(
      loadOrderedBlocksActionName(initialBlockNumber),
      orderingStage = functionFullName,
    ) {
      storage
        .query(
          sql"""select
                  completed_message.message,
                  epoch.epoch_number, epoch.start_block_number, epoch.epoch_length, epoch.topology_ts
                from
                  ord_pbft_messages_completed completed_message
                  inner join ord_epochs epoch
                    on epoch.epoch_number = completed_message.epoch_number
                where
                  completed_message.discriminator = $PrePrepareMessageDiscriminator and
                  completed_message.block_number >= $initialBlockNumber
                order by
                  completed_message.block_number
              """.as[(PrePrepare, EpochInfo)](tryReadPrePrepareMessageAndEpochInfo),
          functionFullName,
        )
        .map {
          _.map { case (prePrepare, epochInfo) =>
            OrderedBlockForOutput(
              OrderedBlock(
                prePrepare.blockMetadata,
                prePrepare.block.proofs,
                prePrepare.canonicalCommitSet,
              ),
              prePrepare.viewNumber,
              prePrepare.from,
              epochInfo.lastBlockNumber == prePrepare.blockMetadata.blockNumber,
              OrderedBlockForOutput.Mode.FromConsensus,
            )
          }
        }
    }

  override def loadNumberOfRecords(implicit
      traceContext: TraceContext
  ): PekkoFutureUnlessShutdown[EpochStore.NumberOfRecords] =
    createFuture(loadNumberOfRecordsName, orderingStage = functionFullName) {
      storage.query(
        for {
          numberOfEpochs <- sql"""select count(*) from ord_epochs""".as[Long].head
          numberOfMsgsCompleted <- sql"""select count(*) from ord_pbft_messages_completed"""
            .as[Long]
            .head
          numberOfMsgsInProgress <- sql"""select count(*) from ord_pbft_messages_in_progress"""
            .as[Int]
            .head
        } yield EpochStore
          .NumberOfRecords(numberOfEpochs, numberOfMsgsCompleted, numberOfMsgsInProgress),
        functionFullName,
      )
    }

  override def prune(epochNumberExclusive: EpochNumber)(implicit
      traceContext: TraceContext
  ): PekkoFutureUnlessShutdown[EpochStore.NumberOfRecords] =
    createFuture(pruneName(epochNumberExclusive), orderingStage = functionFullName) {
      for {
        epochsDeleted <- storage.update(
          sqlu""" delete from ord_epochs where epoch_number < $epochNumberExclusive """,
          functionFullName,
        )
        pbftMessagesCompletedDeleted <- storage.update(
          sqlu""" delete from ord_pbft_messages_completed where epoch_number < $epochNumberExclusive """,
          functionFullName,
        )
        pbftMessagesInProgressDeleted <- storage.update(
          sqlu""" delete from ord_pbft_messages_in_progress where epoch_number < $epochNumberExclusive """,
          functionFullName,
        )
      } yield EpochStore.NumberOfRecords(
        epochsDeleted.toLong,
        pbftMessagesCompletedDeleted.toLong,
        pbftMessagesInProgressDeleted,
      )
    }

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  private def getCommits(
      completeBlockMessages: Vector[SignedMessage[PbftNetworkMessage]]
  ): Map[BlockNumber, Vector[SignedMessage[Commit]]] =
    completeBlockMessages
      .collect { case s @ SignedMessage(_: Commit, _) =>
        s.asInstanceOf[SignedMessage[Commit]]
      }
      .groupBy(_.message.blockMetadata.blockNumber)
}

object DbEpochStore {
  private val PrePrepareMessageDiscriminator = 0
  private val PrepareMessageDiscriminator = 1
  private val CommitMessageDiscriminator = 2
  private val ViewChangeDiscriminator = 3
  private val NewViewDiscriminator = 4

  private def getDiscriminator[M <: PbftNetworkMessage](message: M): Int =
    message match {
      case _: PrePrepare => PrePrepareMessageDiscriminator
      case _: ConsensusMessage.Prepare => PrepareMessageDiscriminator
      case _: Commit => CommitMessageDiscriminator
      case _: ConsensusMessage.ViewChange => ViewChangeDiscriminator
      case _: ConsensusMessage.NewView => NewViewDiscriminator
    }
}
