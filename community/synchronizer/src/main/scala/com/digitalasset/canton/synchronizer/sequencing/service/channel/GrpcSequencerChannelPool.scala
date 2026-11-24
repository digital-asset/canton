// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencing.service.channel

import com.daml.nameof.NameOf.functionFullName
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.config.RequireTypes.PositiveInt
import com.digitalasset.canton.data.CantonTimestamp
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.lifecycle.{FlagCloseable, HasCloseContext, UnlessShutdown}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.sequencer.api.v30
import com.digitalasset.canton.sequencing.protocol.channel.{
  SequencerChannelId,
  SequencerChannelMetadata,
}
import com.digitalasset.canton.synchronizer.sequencing.service.channel.SequencerChannelInternalError.{
  InvalidRequest,
  PendingConnectionClosed,
  PoolCapacityExhausted,
  PoolClosed,
}
import com.digitalasset.canton.time.Clock.ClockHandle
import com.digitalasset.canton.time.{Clock, PositiveFiniteDuration}
import com.digitalasset.canton.topology.Member
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.tracing.TraceContext.withNewTraceContext
import com.digitalasset.canton.util.Mutex
import com.digitalasset.canton.version.ProtocolVersion
import io.grpc.stub.{ServerCallStreamObserver, StreamObserver}

import java.util.concurrent.atomic.{AtomicLong, AtomicReference}
import scala.annotation.tailrec
import scala.collection.mutable
import scala.concurrent.ExecutionContext
import scala.util.chaining.scalaUtilChainingOps
import scala.util.control.NonFatal

/** GrpcSequencerChannelPool tracks active sequencer channels.
  *
  * The dynamically changing channels are kept up to date in response to
  *   1. sequencer-server initiated reasons such as sequencer shutdown or when a member's topology
  *      permissions are revoked and
  *   1. reasons arising in the sequencer channel such as when the sequencer channel conversation
  *      completes.
  *
  * As channels are initialized in a delayed fashion, the pool tracks initialized and uninitialized
  * channels separately.
  *
  * The pool has a configurable size limit, and each sequencer channel has a TTL measured from when
  * the first member connects.
  */
@SuppressWarnings(Array("com.digitalasset.canton.RequireBlocking"))
private[channel] final class GrpcSequencerChannelPool(
    clock: Clock,
    protocolVersion: ProtocolVersion, // technically the channel protocol is separate from the canton protocol, but use for now at least
    override protected val timeouts: ProcessingTimeout,
    maxPoolSize: PositiveInt,
    channelTtl: PositiveFiniteDuration,
    maxMetadataWaitingTime: PositiveFiniteDuration,
    protected val loggerFactory: NamedLoggerFactory,
)(implicit executionContext: ExecutionContext)
    extends FlagCloseable
    with HasCloseContext
    with NamedLogging {

  // As sequencer channels are mutable, any access or modifications to channels are expected to be synchronized.
  // Initialized channels merely mean that the pool has identified the members that are intending to connect to the channel,
  // but not necessarily that both members have already connected. To be more precise, at least one member has connected
  // to an initialized channel.
  private val initializedChannels = mutable.HashMap[SequencerChannelId, GrpcSequencerChannel]()
  private val uninitializedChannels = mutable.Buffer[UninitializedGrpcSequencerChannel]()
  private val lock = new Mutex()

  // Free connection slots (2 per channel), limiting concurrent connections to 2 * maxPoolSize.
  // Acquired per incoming connection; moved to the channel on successful initialization; released when a
  // pending connection completes or a channel is removed (1 if waiting, 2 if fully connected).
  // The number of channels is capped separately at maxPoolSize.
  private val availableSlots: AtomicLong = new AtomicLong(maxPoolSize.value.toLong * 2)

  private def withAcquiredSlot[T](
      f: => Either[SequencerChannelInternalError, T]
  )(implicit traceContext: TraceContext): Either[SequencerChannelInternalError, T] = {
    @tailrec
    def tryAcquireSlot(): Boolean = {
      val curr = availableSlots.get()
      if (curr > 0) {
        if (availableSlots.compareAndSet(curr, curr - 1)) true
        else tryAcquireSlot()
      } else false
    }

    if (tryAcquireSlot()) {
      try f
      catch {
        case NonFatal(t) =>
          // Release the slot if f throws before handing it over.
          availableSlots.incrementAndGet().discard
          throw t
      }
    } else {
      logger.info(
        s"Rejecting new connection: ${PoolCapacityExhausted.message}"
      )
      Left(PoolCapacityExhausted)
    }
  }

  // Exposed for tests (and future metrics, TODO(#21339)).
  private[channel] def size: Int = lock.exclusive(initializedChannels.size)
  private[channel] def numOfActiveChannels: Int =
    lock.exclusive(initializedChannels.count(_._2.isFullyConnected))
  private[channel] def numOfWaitingChannels: Int =
    lock.exclusive(initializedChannels.count(!_._2.isFullyConnected))
  private[channel] def numOfPendingConnections: Int = lock.exclusive(uninitializedChannels.size)
  private[channel] def numOfAvailableSlots: Long = availableSlots.get()

  /** @param responseObserver
    *   GRPC response observer for messages to the client
    * @param authTokenExpiresAtO
    *   The member's sequencer token expiration time
    * @param authenticationCheck
    *   Callback to use upon channel initialization
    * @return
    *   GRPC request observer needed by GrpcSequencerChannelService
    */
  def createUninitializedChannel(
      responseObserver: ServerCallStreamObserver[v30.ConnectToSequencerChannelResponse],
      authTokenExpiresAtO: Option[CantonTimestamp],
      authenticationCheck: Member => Either[String, Unit],
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit
      traceContext: TraceContext
  ): Either[SequencerChannelInternalError, StreamObserver[v30.ConnectToSequencerChannelRequest]] =
    synchronizeWithClosingSync(functionFullName) {
      // Acquired outside the lock, so that rejections are fast and don't contend.
      withAcquiredSlot {
        lock.exclusive {
          val (pendingConnectionExpiresAt, channelExpiresAt) = {
            val now = clock.now
            val metadataExpiresAt = now.add(maxMetadataWaitingTime.duration)
            val pendingExpiresAt =
              authTokenExpiresAtO.fold(metadataExpiresAt)(metadataExpiresAt.min)

            (pendingExpiresAt, now.add(channelTtl.duration))
          }
          val pendingTtlHandle = new AtomicReference[ClockHandle[Unit]](ClockHandle.Dummy)
          val channel = new UninitializedGrpcSequencerChannel(
            responseObserver,
            authTokenExpiresAtO,
            authenticationCheck,
            channel => {
              // Cancel the pending connection's TTL job.
              pendingTtlHandle.get.cancel(UnlessShutdown.unit)
              onUninitializedChannelCompleted(channel)
            },
            protocolVersion,
            timeouts,
            loggerFactory,
          ) {

            /** Transitions an uninitialized channel to a newly initialized channel or an existing
              * channel already created by the "opposite" side/member, guarding the maximum pool
              * size cap. Returns Left(PoolCapacityExhausted) if the pool already holds maxPoolSize
              * channels.
              *
              * Also schedules closing of the channel upon member token or channel TTL expiration.
              *
              * @param metadata
              *   Sequencer channel metadata that uniquely identifies the channel and includes the
              *   members.
              * @param uninitializedChannel
              *   The uninitialized channel to initialize.
              * @param tokenExpiresAt
              *   Token expiration time
              * @param createChannel
              *   Helper to build the initialized channel from the metadata
              * @return
              *   the newly created "first" or "second" member message handler (determined by the
              *   order in which the members connect)
              */
            override protected def ensureChannelAndConnect(
                metadata: SequencerChannelMetadata,
                uninitializedChannel: UninitializedGrpcSequencerChannel,
                tokenExpiresAt: Option[CantonTimestamp],
                createChannel: SequencerChannelMetadata => GrpcSequencerChannel,
                tc: TraceContext,
            ): Either[SequencerChannelInternalError, GrpcSequencerChannelMemberMessageHandler] = {
              implicit val traceContext: TraceContext = tc
              synchronizeWithClosingSync(functionFullName) {
                lock.exclusive {
                  if (!uninitializedChannels.contains(uninitializedChannel))
                    Left(PendingConnectionClosed)
                  else
                    for {
                      channelInfo <- initializedChannels.get(metadata.channelId) match {
                        case None if initializedChannels.sizeIs < maxPoolSize.value =>
                          logger.debug(
                            s"Creating new channel in response to connect request by first member ${metadata.initiatingMember}"
                          )
                          val newChannel = createChannel(metadata)
                          initializedChannels.put(metadata.channelId, newChannel).discard
                          newChannel.onClosed(() => removeChannel(newChannel))
                          val handle = scheduleOwnedJob(
                            "closing channel at TTL expiry",
                            channelExpiresAt,
                          ) { _ =>
                            logger.info(
                              s"Closing channel ${metadata.channelId} because the channel TTL expired at $channelExpiresAt"
                            )
                            closeChannel(newChannel)
                          }
                          newChannel.onClosed(() => handle.cancel(UnlessShutdown.unit))
                          Right((newChannel, newChannel.firstMemberHandler))
                        case None =>
                          logger.debug(
                            s"Cannot create channel ${metadata.channelId} initiated by ${metadata.initiatingMember}: ${PoolCapacityExhausted.message}"
                          )
                          Left(PoolCapacityExhausted)
                        case Some(existingChannel) =>
                          for {
                            _ <- Either.cond(
                              !existingChannel.isFullyConnected,
                              (),
                              InvalidRequest(
                                s"Sequencer channel ${metadata.channelId} already exists: $metadata"
                              ),
                            )
                            _ = logger.debug(
                              s"Attaching to existing channel in response to connect request by second member ${metadata.initiatingMember}"
                            )
                            // If channel already exists, we expect the opposite member positions to reflect that
                            // channel.firstMemberToConnect has already connected and channel.secondMemberToConnect
                            // is now connecting and that each request to the channel expects the other member.
                            _ <- Either.cond(
                              existingChannel.secondMember == metadata.initiatingMember,
                              (),
                              InvalidRequest(
                                s"Sequencer channel ${metadata.channelId} initiating member mismatch: Expected ${existingChannel.secondMember}, but request contains ${metadata.initiatingMember}."
                              ),
                            )
                            _ <- Either.cond(
                              existingChannel.firstMember == metadata.receivingMember,
                              (),
                              InvalidRequest(
                                s"Sequencer channel ${metadata.channelId} connect-to member mismatch: Expected ${existingChannel.firstMember}, but request contains ${metadata.receivingMember}."
                              ),
                            )
                            handler <- existingChannel.addSecondMemberToConnectHandler(
                              uninitializedChannel.responseObserver
                            )
                          } yield (existingChannel, handler)
                      }
                      // On success the pending connection's slot is handed over to the initialized channel.
                      // On failure the caller completes the uninitialized channel, which releases its slot
                      // via onUninitializedChannelCompleted.
                      _ = uninitializedChannels -= uninitializedChannel
                      // Cancel the pending connection's TTL job.
                      _ = pendingTtlHandle.get.cancel(UnlessShutdown.unit)
                      (channel, handler) = channelInfo
                      // schedule expiring the channel once token expires
                      // this could potentially happen immediately if the expiration has already passed.
                      _ = tokenExpiresAt.foreach { ts =>
                        val handle = scheduleOwnedJob(
                          "closing channel at member token expiry",
                          ts,
                        ) { _ =>
                          logger.info(
                            s"Closing channel ${metadata.channelId} because the token of ${metadata.initiatingMember} expired at $ts"
                          )
                          closeChannel(channel)
                        }
                        channel.onClosed(() => handle.cancel(UnlessShutdown.unit))
                      }
                    } yield handler
                }
              } onShutdown Left(PoolClosed)
            }
          }
          uninitializedChannels += channel

          val handle = scheduleOwnedJob(
            "closing pending channel connection at TTL expiry",
            pendingConnectionExpiresAt,
          ) { _ =>
            closeIfStillPending(channel, "TTL expired")
          }
          pendingTtlHandle.set(handle)

          Right(channel.requestObserverAdapter)
        }
      }
    } onShutdown Left(PoolClosed)

  /** Schedules `action` at `ts`. The job is cancelled when the pool closes and through the returned
    * handle (normal path). The action does nothing if the pool is already closing.
    */
  private def scheduleOwnedJob(taskName: String, ts: CantonTimestamp)(action: TraceContext => Unit)(
      implicit traceContext: TraceContext
  ): ClockHandle[Unit] =
    clock
      .scheduleAtCancellableCancelledOnShutdown[Unit](
        _ =>
          synchronizeWithClosingSync(taskName)(action(traceContext))
            .onShutdown(logger.debug(s"Skipping '$taskName': pool is closing")),
        s"${getClass.getName}: $taskName",
        ts,
      )
      ._2

  /** Removes `channel` from the pending connections and releases its slot. Returns false if someone
    * else already removed it (initialized, completed or closed). Acquires pool `lock`.
    */
  private def detachPendingConnection(channel: UninitializedGrpcSequencerChannel): Boolean =
    lock.exclusive {
      val idx = uninitializedChannels.indexOf(channel)
      (idx >= 0).tap { found =>
        if (found) {
          uninitializedChannels.remove(idx).discard
          availableSlots.incrementAndGet().discard
        }
      }
    }

  /** Removes `channel` from the pool, if it is still the channel registered under its id, and
    * releases its slots. Does not close it.
    *
    * @param channel
    *   The channel to be removed
    */
  private def removeChannel(channel: GrpcSequencerChannel)(implicit
      traceContext: TraceContext
  ): Unit =
    lock.exclusive {
      initializedChannels.get(channel.channelId) match {
        case Some(ch) if ch eq channel =>
          initializedChannels.remove(channel.channelId).discard
          if (channel.isFullyConnected) availableSlots.addAndGet(2).discard
          else availableSlots.incrementAndGet().discard
          logger.debug(s"Removed channel ${channel.channelId}")
        case _ =>
          // No-op - channel already removed or replaced by a newer channel with the same id
          ()
      }
    }

  private def closeChannel(
      channel: GrpcSequencerChannel,
      waitForClosed: Boolean = false,
  )(implicit traceContext: TraceContext): Unit = {
    removeChannel(channel)
    try {
      logger.debug(s"Closing channel ${channel.channelId}")
      channel.close()
      if (waitForClosed) {
        timeouts.network.await(s"closing channel ${channel.channelId}")(channel.closedF)
      }
    } catch {
      // We don't want to throw if closing fails because it will stop the chain of closing subsequent channels
      case NonFatal(e) => logger.warn(s"Failed to close channel ${channel.channelId}", e)
    }
  }

  def closeChannels(
      memberO: Option[Member] = None, // None means close all channels i.e. of all members
      waitForClosed: Boolean = false,
  )(implicit
      traceContext: TraceContext
  ): Unit = {
    val (channels, pending) = lock.exclusive {
      val channels = initializedChannels.toList.filter { case (_, ch) =>
        memberO.forall(m => m == ch.firstMember || m == ch.secondMember)
      }
      channels.foreach { case (_, ch) => removeChannel(ch) } // remove + release slots
      val pending =
        if (memberO.isEmpty) {
          val all = uninitializedChannels.toList
          uninitializedChannels.clear()
          availableSlots.addAndGet(all.size.toLong).discard
          all
        } else Nil
      (channels, pending)
    }
    pending.foreach { ch =>
      // A failure to close one pending connection must not stop closing the others.
      try ch.close()
      catch {
        case NonFatal(e) => logger.warn("Failed to close pending connection", e)
      }
    }
    channels.foreach { case (_, ch) => closeChannel(ch, waitForClosed) }
  }

  private def onUninitializedChannelCompleted(
      channel: UninitializedGrpcSequencerChannel
  )(implicit traceContext: TraceContext): Unit =
    if (!detachPendingConnection(channel)) logger.debug("Pending connection already detached")

  private def closeIfStillPending(channel: UninitializedGrpcSequencerChannel, reason: String)(
      implicit traceContext: TraceContext
  ): Unit =
    if (detachPendingConnection(channel)) {
      logger.info(s"Closing pending sequencer channel connection: $reason")
      channel.close()
    }

  override def onClosed(): Unit =
    withNewTraceContext("close_grpc_channel") { implicit traceContext =>
      logger.debug("Closing all channels in pool")
      // Wait for the channels to actually close in case they are already in the process of closing
      // in which case FlagClosable doesn't wait.
      closeChannels(waitForClosed = true)
    }
}
