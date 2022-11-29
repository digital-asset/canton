// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.domain.block

import akka.stream.KillSwitch
import akka.stream.scaladsl.Source
import com.digitalasset.canton.logging.NamedLoggerFactory
import com.digitalasset.canton.time.TimeProvider
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.google.protobuf.ByteString
import io.grpc.BindableService
import pureconfig.{ConfigReader, ConfigWriter}

import scala.concurrent.{ExecutionContext, Future}

/** Factory for creating a [[SequencerDriver]] for a block-based sequencer,
  * including methods for dealing with configuration of the ledger driver.
  */
trait SequencerDriverFactory {

  /** The name of the ledger driver
    * Used in Canton configurations to specify the ledger driver as in `type = name`.
    * {{{
    *    sequencer {
    *      type = "foobar"
    *      config = { config specific to driver foobar }
    *    }
    * }}}
    */
  def name: String

  /** The Scala type holding the driver-specific configuration */
  type ConfigType

  /** Parser for the driver-specific configuration. */
  def configParser: ConfigReader[ConfigType]

  /** Serializer for the driver-specific configuration. */
  def configWriter: ConfigWriter[ConfigType]

  /** Creates a new ledger driver instance
    *
    * @param config The driver-specific configuration
    * @param timeProvider Time provider to obtain time readings from.
    *                     If [[usesTimeProvider]] returns true, must be used instead of system time
    *                     so that we can modify time in tests
    * @param loggerFactory A logger factory through which all logging should be done.
    *                      Useful in tests as we can capture log entries and check them.
    */
  def create(config: ConfigType, timeProvider: TimeProvider, loggerFactory: NamedLoggerFactory)(
      implicit executionContext: ExecutionContext
  ): SequencerDriver

  /** Returns whether the [[SequencerDriver]] produced by [[create]] will use the [[com.digitalasset.canton.time.TimeProvider]]
    * for generating timestamps on [[com.digitalasset.canton.domain.block.RawLedgerBlock.RawBlockEvent.Send]] events.
    *
    * This information is used to prevent using the [[SequencerDriver]] in an environment
    * that needs to control time, e.g., for testing.
    */
  def usesTimeProvider: Boolean
}

/** Defines methods for synchronizing data in blocks among all sequencer nodes of a domain.
  *
  * The write operations sequence and distribute different kinds of requests.
  * They can all be implemented by the same mechanism of sequencing a bytestring,
  * but are kept separately for legacy reasons: The Fabric, Ethereum and Vmbc drivers have separate
  * entry points or messages for the different request kinds.
  *
  * Sequenced requests are delivered in a stream of [[RawLedgerBlock]]s ordered by their block height.
  * The driver must make sure that all sequencer nodes of a domain receive the same stream of [[RawLedgerBlock]]s eventually.
  * That is, if one sequencer node receives a block `b` at block height `h`, then every other sequencer node has already
  * or will eventually receive `b` at height `h` unless the node fails permanently.
  * Each [[RawLedgerBlock]] contains [[com.digitalasset.canton.domain.block.RawLedgerBlock.RawBlockEvent]]s
  * that correspond to the sequenced requests. The [[com.digitalasset.canton.tracing.TraceContext]]
  * passed to the write operations should be propagated into the corresponding
  * [[com.digitalasset.canton.domain.block.RawLedgerBlock.RawBlockEvent]].
  *
  * All write operations are asynchronous: the [[scala.concurrent.Future]] may complete
  * before the request is actually sequenced. Under normal circumstances, the request should then also eventually
  * be delivered in a [[com.digitalasset.canton.domain.block.RawLedgerBlock.RawBlockEvent]],
  * but there is no guarantee. A write operation may fail with an exception; in that case,
  * the request must not be sequenced. Every write operation may result in at most one corresponding
  * [[com.digitalasset.canton.domain.block.RawLedgerBlock.RawBlockEvent]].
  *
  * The [[SequencerDriver]] is responsible for assigning timestamps to
  * [[com.digitalasset.canton.domain.block.RawLedgerBlock.RawBlockEvent.Send]] events.
  * The assigned timestamps must be close to real-world time given the trust assumptions of the [[SequencerDriver]].
  * For example, assume that the clocks among all honest sequencer nodes are synchronized up to a given `skew`.
  * Let `ts0` be the local sequencer's time when an honest sequencer node calls [[SequencerDriver.send]].
  * Let `ts1` be the local sequencer's time when it receives the corresponding
  * [[com.digitalasset.canton.domain.block.RawLedgerBlock.RawBlockEvent.Send]].
  * Then the assigned timestamp `ts` must satisfy `ts0 - skew <= ts <= ts1 + skew`.
  *
  * Several [[com.digitalasset.canton.domain.block.RawLedgerBlock.RawBlockEvent.Send]] events may
  * have the same timestamp or go backwards, as long as they remain close to real-world time.
  */
trait SequencerDriver extends AutoCloseable {

  // Admin end points

  /** Services for administering the ledger driver.
    * These services will be exposed on the sequencer node's admin API endpoint.
    */
  def adminServices: Seq[BindableService]

  // Write operations

  /** Register the given member.
    * Results in a [[com.digitalasset.canton.domain.block.RawLedgerBlock.RawBlockEvent.AddMember]].
    */
  def registerMember(member: String)(implicit traceContext: TraceContext): Future[Unit]

  /** Distribute an acknowledgement request.
    * Results in a [[com.digitalasset.canton.domain.block.RawLedgerBlock.RawBlockEvent.Acknowledgment]].
    */
  def acknowledge(acknowledgement: ByteString)(implicit traceContext: TraceContext): Future[Unit]

  /** Send a submission request.
    * Results in a [[com.digitalasset.canton.domain.block.RawLedgerBlock.RawBlockEvent.Send]].
    */
  def send(request: ByteString)(implicit traceContext: TraceContext): Future[Unit]

  // Read operations

  /** Delivers a stream of blocks starting with `firstBlockHeight` (if specified) or the first block serveable.
    * Block heights must be consecutive.
    *
    * Fails if `firstBlockHeight` refers to a block whose sequencing the sequencer node has not yet observed
    * or that has already been pruned.
    *
    * Must succeed if an earlier call to `subscribe` delivered a block with height `firstBlockHeight`
    * unless the block has been pruned in between.
    */
  def subscribe(firstBlockHeight: Option[Long])(implicit
      traceContext: TraceContext
  ): Source[RawLedgerBlock, KillSwitch]

  // Operability

  def health(implicit traceContext: TraceContext): Future[SequencerDriverHealthStatus]

}

/** A block that a [[SequencerDriver]] delivers to the sequencer node.
  *
  * @param blockHeight The height of the block. Block heights must be consecutive.
  * @param events The events in the given block.
  */
case class RawLedgerBlock(
    blockHeight: Long,
    events: Seq[Traced[RawLedgerBlock.RawBlockEvent]],
)

object RawLedgerBlock {
  sealed trait RawBlockEvent extends Product with Serializable

  object RawBlockEvent {

    case class Send(
        request: ByteString,
        microsecondsSinceEpoch: Long,
    ) extends RawBlockEvent

    case class AddMember(member: String) extends RawBlockEvent

    case class Acknowledgment(acknowledgement: ByteString) extends RawBlockEvent
  }
}

case class SequencerDriverHealthStatus(
    isActive: Boolean,
    description: Option[String],
)
