// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.sequencer

import cats.syntax.option.*
import com.digitalasset.canton.ProtoDeserializationError
import com.digitalasset.canton.config.RequireTypes.{NonNegativeInt, PositiveDouble, PositiveInt}
import com.digitalasset.canton.config.{NonNegativeFiniteDuration, PositiveDurationSeconds}
import com.digitalasset.canton.sequencer.admin.v30 as adminProto
import com.digitalasset.canton.sequencing.protocol.SubmissionRequestType
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.serialization.ProtoConverter.ParsingResult
import com.digitalasset.canton.synchronizer.sequencer.BlockSequencerConfig.ThroughputCapByMessageTypeConfig.{
  DefaultGlobalKbpsCap,
  DefaultGlobalTpsCap,
  DefaultPerClientKbpsCap,
  DefaultPerClientKbpsCommitmentsCap,
  DefaultPerClientTpsCap,
  DefaultPerClientTpsCommitmentCap,
}
import com.digitalasset.canton.synchronizer.sequencer.BlockSequencerConfig.{
  CircuitBreakerConfig,
  ThroughputCapConfig,
}
import com.digitalasset.canton.synchronizer.sequencer.BlockSequencerStreamInstrumentationConfig.DefaultBufferSize
import com.digitalasset.canton.synchronizer.sequencer.DatabaseSequencerConfig.{
  SequencerPruningConfig,
  TestingInterceptor,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftBlockOrdererConfig
import com.digitalasset.canton.time.Clock
import com.digitalasset.nonempty.NonEmpty
import pureconfig.error.CannotConvert
import pureconfig.generic.semiauto.{deriveReader, deriveWriter}
import pureconfig.{ConfigReader, ConfigWriter}

import scala.concurrent.ExecutionContext

sealed trait SequencerConfig

object SequencerConfig {

  final case class BftSequencer(
      // To avoid having to include an empty "block" node if defaults are fine
      block: BlockSequencerConfig = BlockSequencerConfig(),
      // To avoid having to include an empty "config" node if defaults are fine
      config: BftBlockOrdererConfig = BftBlockOrdererConfig(),
  ) extends SequencerConfig

  def default: SequencerConfig = BftSequencer()

  final case class DeprecatedSequencerConfig(sequencerType: String) extends SequencerConfig

}

/** Unsealed trait so the database sequencer config can be reused between community and enterprise
  */
trait DatabaseSequencerConfig {

  val writer: SequencerWriterConfig
  val reader: SequencerReaderConfig
  val testingInterceptor: Option[DatabaseSequencerConfig.TestingInterceptor]
  val pruning: SequencerPruningConfig
}

object DatabaseSequencerConfig {

  /** The Postgres sequencer supports adding an interceptor within the sequencer itself for
    * manipulating sequence behavior during tests. This is used for delaying and/or dropping
    * messages to verify the behavior of transaction processing in abnormal scenarios in a
    * deterministic way. It is not expected to be used at runtime in any capacity and is not
    * possible to set through pureconfig.
    */
  type TestingInterceptor =
    Clock => Sequencer => ExecutionContext => Sequencer

  /** Configuration for database sequencer pruning
    *
    * @param maxPruningBatchSize
    *   Maximum number of events to prune from a sequencer at a time, used to break up batches
    *   internally
    * @param pruningMetricUpdateInterval
    *   How frequently to update the `max-event-age` pruning progress metric in the background. A
    *   setting of None disables background metric updating.
    * @param trafficPurchasedRetention
    *   Retention duration on how long to retain traffic purchased entry updates for each member
    */
  final case class SequencerPruningConfig(
      maxPruningBatchSize: PositiveInt =
        PositiveInt.tryCreate(50000), // Large default for database-range-delete based pruning
      pruningMetricUpdateInterval: Option[PositiveDurationSeconds] =
        PositiveDurationSeconds.ofHours(1L).some,
      trafficPurchasedRetention: NonNegativeFiniteDuration = NonNegativeFiniteDuration.ofHours(1),
  )

}

final case class BlockSequencerStreamInstrumentationConfig(
    isEnabled: Boolean = false,
    bufferSize: PositiveInt = DefaultBufferSize,
)

object BlockSequencerStreamInstrumentationConfig {
  val DefaultBufferSize = PositiveInt.tryCreate(128)
}

final case class BlockSequencerConfig(
    writer: SequencerWriterConfig = SequencerWriterConfig.HighThroughput(),
    reader: SequencerReaderConfig = SequencerReaderConfig(),
    testingInterceptor: Option[DatabaseSequencerConfig.TestingInterceptor] = None,
    circuitBreaker: CircuitBreakerConfig = CircuitBreakerConfig(),
    throughputCap: ThroughputCapConfig = ThroughputCapConfig(),
    streamInstrumentation: BlockSequencerStreamInstrumentationConfig =
      BlockSequencerStreamInstrumentationConfig(),
) { self =>
  def toDatabaseSequencerConfig: DatabaseSequencerConfig = new DatabaseSequencerConfig {
    override val writer: SequencerWriterConfig = self.writer
    override val reader: SequencerReaderConfig = self.reader
    override val testingInterceptor: Option[TestingInterceptor] = self.testingInterceptor
    // TODO(#15987): Take pruning config from BlockSequencerConfig once block sequencer supports pruning.
    override val pruning: SequencerPruningConfig = SequencerPruningConfig()
  }
}

object BlockSequencerConfig {

  /** Control throughput caps
    *
    * Strict mode means that if we reach the cap, we'll allocate the same bandwidth to everyone. If
    * false, then we sort by usage descending and compute the usage cap for the highest spenders at
    * which their spend will fall below the threshold
    *
    * @param strict
    *   how should the per member rate cap be computed
    * @param updateEveryMs
    *   how often should the new caps be computed (only used in non-strict mode)
    */
  final case class ThroughputCapConfig(
      enabled: Boolean = false,
      clockTickInterval: NonNegativeFiniteDuration = NonNegativeFiniteDuration.ofMillis(100),
      messages: Map[String, ThroughputCapByMessageTypeConfig] = Map(
        "confirmation-request" -> ThroughputCapByMessageTypeConfig(
          requestType = SubmissionRequestType.ConfirmationRequest
        ),
        "topology" -> ThroughputCapByMessageTypeConfig(
          requestType = SubmissionRequestType.TopologyTransaction,
          globalTpsCap = PositiveDouble.tryCreate(2.0),
          perClientTpsCap = PositiveDouble.tryCreate(1.0),
        ),
        "confirmation-response" -> ThroughputCapByMessageTypeConfig(
          requestType = SubmissionRequestType.ConfirmationResponse,
          globalTpsCap = DefaultGlobalTpsCap * PositiveDouble.tryCreate(20),
          perClientTpsCap = DefaultGlobalTpsCap * PositiveDouble.tryCreate(1.3),
        ),
        "commitments" -> ThroughputCapByMessageTypeConfig(
          requestType = SubmissionRequestType.Commitment,
          globalTpsCap = PositiveDouble.tryCreate(100),
          perClientTpsCap = DefaultPerClientTpsCommitmentCap,
          perClientKbpsCap = DefaultPerClientKbpsCommitmentsCap,
        ),
        "time-proofs" -> ThroughputCapByMessageTypeConfig(
          requestType = SubmissionRequestType.TimeProof,
          perClientTpsCap = PositiveDouble.tryCreate(0.1),
          observationPeriodSeconds = PositiveInt.tryCreate(60),
        ),
      ),
      strict: Boolean = true,
      thresholds: NonEmpty[Seq[PositiveDouble]] = NonEmpty.mk(Seq, PositiveDouble.tryCreate(0.9)),
      updateEveryMs: NonNegativeInt = NonNegativeInt.tryCreate(100),
  )

  /** Input compatibility for the fixed message-type caps used before named settings. */
  final case class LegacyThroughputCapConfig(
      enabled: Boolean = false,
      observationPeriodSeconds: PositiveInt = PositiveInt.tryCreate(60),
      clockTickInterval: NonNegativeFiniteDuration = NonNegativeFiniteDuration.ofMillis(100),
      messages: LegacyThroughputCapByMessageTypeConfig = LegacyThroughputCapByMessageTypeConfig(),
      strict: Boolean = true,
      delayedActivation: Boolean = true,
      thresholds: NonEmpty[Seq[PositiveDouble]] = NonEmpty.mk(Seq, PositiveDouble.tryCreate(0.9)),
      updateEveryMs: NonNegativeInt = NonNegativeInt.tryCreate(100),
  ) {
    def toThroughputCapConfig: ThroughputCapConfig = {
      val defaults = ThroughputCapConfig()
      val legacyMessages = Seq[
        (String, SubmissionRequestType, LegacyIndividualThroughputCapConfig)
      ](
        (
          "confirmation-request",
          SubmissionRequestType.ConfirmationRequest,
          messages.confirmationRequest,
        ),
        ("topology", SubmissionRequestType.TopologyTransaction, messages.topology),
        (
          "confirmation-response",
          SubmissionRequestType.ConfirmationResponse,
          messages.confirmationResponse,
        ),
      ).map { case (name, requestType, setting) =>
        name -> setting.toThroughputCapByMessageTypeConfig(requestType, observationPeriodSeconds)
      }.toMap
      defaults.copy(
        enabled = enabled,
        clockTickInterval = clockTickInterval,
        messages = defaults.messages ++ legacyMessages,
        strict = strict,
        thresholds = thresholds,
        updateEveryMs = updateEveryMs,
      )
    }
  }

  final case class LegacyThroughputCapByMessageTypeConfig(
      confirmationRequest: LegacyIndividualThroughputCapConfig =
        LegacyIndividualThroughputCapConfig(),
      topology: LegacyIndividualThroughputCapConfig = LegacyIndividualThroughputCapConfig(
        globalTpsCap = PositiveDouble.tryCreate(2.0),
        perClientTpsCap = PositiveDouble.tryCreate(1.0),
      ),
      confirmationResponse: LegacyIndividualThroughputCapConfig =
        LegacyIndividualThroughputCapConfig(
          globalTpsCap = DefaultGlobalTpsCap * PositiveDouble.tryCreate(20),
          perClientTpsCap = DefaultGlobalTpsCap * PositiveDouble.tryCreate(1.3),
        ),
  )

  final case class LegacyIndividualThroughputCapConfig(
      globalTpsCap: PositiveDouble = DefaultGlobalTpsCap,
      globalKbpsCap: PositiveDouble = DefaultGlobalKbpsCap,
      perClientTpsCap: PositiveDouble = DefaultPerClientTpsCap,
      perClientKbpsCap: PositiveDouble = DefaultPerClientKbpsCap,
  ) {
    def toThroughputCapByMessageTypeConfig(
        requestType: SubmissionRequestType,
        observationPeriodSeconds: PositiveInt,
    ): ThroughputCapByMessageTypeConfig =
      ThroughputCapByMessageTypeConfig(
        requestType = requestType,
        globalTpsCap = globalTpsCap,
        globalKbpsCap = globalKbpsCap,
        perClientTpsCap = perClientTpsCap,
        perClientKbpsCap = perClientKbpsCap,
        observationPeriodSeconds = observationPeriodSeconds,
      )
  }

  /** Each entry in `messages` is named by its map key and has an independent observation window.
    *
    * @param observationPeriodSeconds
    *   duration of the observation window for this message type. Uses ~ 100 bytes per event (stores
    *   the member-id).
    */
  final case class ThroughputCapByMessageTypeConfig(
      requestType: SubmissionRequestType,
      globalTpsCap: PositiveDouble = DefaultGlobalTpsCap,
      globalKbpsCap: PositiveDouble = DefaultGlobalKbpsCap,
      perClientTpsCap: PositiveDouble = DefaultPerClientTpsCap,
      perClientKbpsCap: PositiveDouble = DefaultPerClientKbpsCap,
      observationPeriodSeconds: PositiveInt = PositiveInt.tryCreate(60),
  ) {

    def toAdminProto: adminProto.IndividualThroughputCapConfig =
      adminProto.IndividualThroughputCapConfig(
        globalTpsCap = globalTpsCap.value,
        globalKbpsCap = globalKbpsCap.value,
        perClientTpsCap = perClientTpsCap.value,
        perClientKbpsCap = perClientKbpsCap.value,
        observationPeriodSeconds = observationPeriodSeconds.value,
        requestType = requestType.name,
      )

  }

  object ThroughputCapByMessageTypeConfig {

    private[sequencer] val DefaultGlobalTpsCap: PositiveDouble = PositiveDouble.tryCreate(10.0)
    private[sequencer] val DefaultGlobalKbpsCap: PositiveDouble = PositiveDouble.tryCreate(2000.0)
    private[sequencer] val DefaultPerClientTpsCap: PositiveDouble = PositiveDouble.tryCreate(4.0)
    private[sequencer] val DefaultPerClientKbpsCap: PositiveDouble =
      PositiveDouble.tryCreate(1000.0)
    private[sequencer] val DefaultPerClientTpsCommitmentCap = PositiveDouble.tryCreate(1.0)
    private[sequencer] val DefaultPerClientKbpsCommitmentsCap: PositiveDouble =
      PositiveDouble.tryCreate(100.0)

    object ConfigImplicits {
      private implicit val requestTypeReader: ConfigReader[SubmissionRequestType] =
        ConfigReader.fromString(str =>
          SubmissionRequestType
            .fromStringForCap(str)
            .toRight(
              CannotConvert(str, "SubmissionRequestType", "Unsupported throughput cap message type")
            )
        )
      private implicit val requestTypeWriter: ConfigWriter[SubmissionRequestType] =
        ConfigWriter.toString(_.name)
      final implicit val throughputCapByMessageTypeConfigReader
          : ConfigReader[ThroughputCapByMessageTypeConfig] =
        deriveReader[ThroughputCapByMessageTypeConfig]
      final implicit val throughputCapByMessageTypeConfigWriter
          : ConfigWriter[ThroughputCapByMessageTypeConfig] =
        deriveWriter[ThroughputCapByMessageTypeConfig]
    }

    def fromAdminProto(
        value: adminProto.IndividualThroughputCapConfig
    ): ParsingResult[ThroughputCapByMessageTypeConfig] = {
      val adminProto.IndividualThroughputCapConfig(
        globalTpsCapP,
        globalKbpsCapP,
        perClientTpsCapP,
        perClientKbpsCapP,
        observationPeriodSecondsP,
        requestTypeP,
      ) = value
      for {
        globalTpsCap <- ProtoConverter.parsePositiveDouble("global_tps_cap", globalTpsCapP)
        globalKbpsCap <- ProtoConverter.parsePositiveDouble("global_kbps_cap", globalKbpsCapP)
        perClientTpsCap <- ProtoConverter.parsePositiveDouble(
          "per_client_tps_cap",
          perClientTpsCapP,
        )
        perClientKbpsCap <- ProtoConverter.parsePositiveDouble(
          "per_client_kbps_cap",
          perClientKbpsCapP,
        )
        observationPeriodSeconds <- ProtoConverter.parsePositiveInt(
          "observation_period_seconds",
          observationPeriodSecondsP,
        )
        requestType <- SubmissionRequestType
          .fromStringForCap(requestTypeP)
          .toRight(
            ProtoDeserializationError.ValueConversionError(
              "request_type",
              s"Unknown submission type $requestTypeP",
            )
          )
      } yield ThroughputCapByMessageTypeConfig(
        globalTpsCap = globalTpsCap,
        globalKbpsCap = globalKbpsCap,
        perClientTpsCap = perClientTpsCap,
        perClientKbpsCap = perClientKbpsCap,
        observationPeriodSeconds = observationPeriodSeconds,
        requestType = requestType,
      )
    }

  }

  final case class CircuitBreakerConfig(
      enabled: Boolean = true,
      messages: CircuitBreakerByMessageTypeConfig = CircuitBreakerByMessageTypeConfig(),
  )

  private val default1 = IndividualCircuitBreakerConfig(maxFailures = 10)
  private val default2 = IndividualCircuitBreakerConfig(maxFailures = 30)
  private val default3 = IndividualCircuitBreakerConfig(maxFailures = 60)

  final case class CircuitBreakerByMessageTypeConfig(
      confirmationRequest: IndividualCircuitBreakerConfig = default1,
      topology: IndividualCircuitBreakerConfig = default1,
      timeProof: IndividualCircuitBreakerConfig = default2,
      commitment: IndividualCircuitBreakerConfig = default2,
      topUp: IndividualCircuitBreakerConfig = default2,
      confirmationResponse: IndividualCircuitBreakerConfig = default3,
      verdict: IndividualCircuitBreakerConfig = default3,
      acknowledgement: IndividualCircuitBreakerConfig = default1,
      lsuSequencingTest: IndividualCircuitBreakerConfig = default1,
      unexpected: IndividualCircuitBreakerConfig = default1,
  )

  final case class IndividualCircuitBreakerConfig(
      allowedBlockDelay: NonNegativeFiniteDuration = NonNegativeFiniteDuration.ofSeconds(90),
      maxFailures: Int = 10,
      resetTimeout: NonNegativeFiniteDuration = NonNegativeFiniteDuration.ofSeconds(30),
      exponentialBackoffFactor: Double = 1.0,
      maxResetTimeout: NonNegativeFiniteDuration = NonNegativeFiniteDuration.ofDays(36500),
  )
}

/** Health check related sequencer config
  * @param backendCheckPeriod
  *   interval with which the sequencer will poll the health of its backend connection or state.
  */
final case class SequencerHealthConfig(
    backendCheckPeriod: NonNegativeFiniteDuration = NonNegativeFiniteDuration.ofSeconds(5)
)
