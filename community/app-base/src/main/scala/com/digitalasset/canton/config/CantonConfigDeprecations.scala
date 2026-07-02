// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

import com.digitalasset.canton.config.DeprecatedConfigUtils.*
import com.digitalasset.canton.participant.config.{
  LedgerApiServerConfig,
  LsuConfig,
  ParticipantNodeParameterConfig,
  *,
}
import com.digitalasset.canton.sequencing.client.SequencerClientConfig
import com.digitalasset.canton.synchronizer.mediator.MediatorConfig
import com.digitalasset.canton.synchronizer.sequencer.config.{
  AsyncWriterConfig,
  SequencerNodeParameterConfig,
}
import com.digitalasset.canton.version.ParticipantProtocolVersion
import pureconfig.ConfigReader

import scala.concurrent.duration.FiniteDuration

trait CantonSharedDeprecations {
  implicit val cryptoDeprecations: DeprecatedFieldsFor[CryptoConfig] =
    new DeprecatedFieldsFor[CryptoConfig] {
      override def movedFields: List[DeprecatedConfigUtils.MovedConfigPath] = List(
        DeprecatedConfigUtils.MovedConfigPath(
          "kms.session-signing-keys",
          since = "3.5.0",
          to = Seq("session-signing-keys"),
        )
      )
    }

  implicit def cacheDeprecations(implicit
      cacheConfigReader: ConfigReader[CacheConfig]
  ): DeprecatedFieldsFor[CachingConfigs] =
    new DeprecatedFieldsFor[CachingConfigs] {
      override def deprecatePath: List[DeprecatedConfigPath[?]] = List(
        DeprecatedConfigPath[CacheConfig]("package-dependency-cache", "3.5.0")
      )
    }

  implicit val fullClientConfigReaderDeprecations: DeprecatedFieldsFor[FullClientConfig] =
    new DeprecatedFieldsFor[FullClientConfig] {
      override def movedFields: List[DeprecatedConfigUtils.MovedConfigPath] = List(
        DeprecatedConfigUtils.MovedConfigPath(
          "keep-alive-client",
          since = "3.5.0",
          to = Seq("channel.keep-alive-client"),
        )
      )
    }

  implicit val sequencerApiclientConfigReaderDeprecations
      : DeprecatedFieldsFor[SequencerApiClientConfig] =
    new DeprecatedFieldsFor[SequencerApiClientConfig] {
      override def movedFields: List[DeprecatedConfigUtils.MovedConfigPath] = List(
        DeprecatedConfigUtils.MovedConfigPath(
          "keep-alive-client",
          since = "3.5.0",
          to = Seq("channel.keep-alive-client"),
        )
      )
    }

  implicit val topologyConfigReaderDeprecations: DeprecatedFieldsFor[TopologyConfig] =
    new DeprecatedFieldsFor[TopologyConfig] {

      override def deprecatePath: List[DeprecatedConfigPath[?]] =
        List(
          DeprecatedConfigPath(
            "use-new-processor",
            since = "3.5.0",
            valueFilter = Some(false),
          ),
          DeprecatedConfigPath(
            "use-new-client",
            since = "3.5.0",
            valueFilter = Some(false),
          ),
        )
    }
  implicit val sequencerClientConfigReaderDeprecations: DeprecatedFieldsFor[SequencerClientConfig] =
    new DeprecatedFieldsFor[SequencerClientConfig] {

      override def deprecatePath: List[DeprecatedConfigPath[?]] =
        List(
          DeprecatedConfigPath[Boolean](
            "use-new-connection-pool",
            since = "3.5.0",
          )
        )
    }
}

trait CantonSynchronizerNodeDeprecations {
  implicit val asyncWriterConfigReaderDeprecations: DeprecatedFieldsFor[AsyncWriterConfig] =
    new DeprecatedFieldsFor[AsyncWriterConfig] {

      override def deprecatePath: List[DeprecatedConfigPath[?]] =
        List(
          DeprecatedConfigPath(
            "enabled",
            since = "3.5.0",
            valueFilter = Some(false),
          )
        )
    }

  implicit val sequencerNodeParametersConfigReaderDeprecations
      : DeprecatedFieldsFor[SequencerNodeParameterConfig] =
    new DeprecatedFieldsFor[SequencerNodeParameterConfig] {
      override def movedFields: List[DeprecatedConfigUtils.MovedConfigPath] = List(
        DeprecatedConfigUtils.MovedConfigPath(
          "unsafe-enable-online-party-replication",
          since = "3.5.0",
          to = Seq("unsafe-sequencer-channel-support"),
        )
      )
    }

  implicit val mediatorConfigReaderDeprecations: DeprecatedFieldsFor[MediatorConfig] =
    new DeprecatedFieldsFor[MediatorConfig] {

      override def deprecatePath: List[DeprecatedConfigPath[?]] =
        List(
          DeprecatedConfigPath[Boolean](
            "asynchronous-processing",
            since = "3.5.1",
          )
        )
    }

}

trait CantonParticipantNodeDeprecations {

  implicit val ledgerApiServerConfigReaderDeprecatedFields
      : DeprecatedFieldsFor[LedgerApiServerConfig] =
    new DeprecatedFieldsFor[LedgerApiServerConfig] {

      override def deprecatePath: List[DeprecatedConfigPath[?]] =
        List(
          DeprecatedConfigPath(
            "index-service.prepare-package-metadata-time-out-warning",
            since = "3.5.0",
            valueFilter = None: Option[NonNegativeFiniteDuration],
          )
        )
    }

  implicit def deprecatedParticipantNodeConfig[X <: ParticipantNodeConfig]: DeprecatedFieldsFor[X] =
    new DeprecatedFieldsFor[ParticipantNodeConfig] {
      override def movedFields: List[DeprecatedConfigUtils.MovedConfigPath] = List(
        DeprecatedConfigUtils.MovedConfigPath(
          "http-ledger-api.server",
          since = "3.4.0",
          to = Seq("http-ledger-api"),
        ),
        DeprecatedConfigUtils.MovedConfigPath(
          "features.profileDir",
          since = "3.5.0",
          to = Seq("parameters.engine"),
        ),
        DeprecatedConfigUtils.MovedConfigPath(
          "features.snapshotDir",
          since = "3.5.0",
          to = Seq("parameters.engine"),
        ),
      )
    }

  implicit val participantNodeParameterConfigReaderDeprecations
      : DeprecatedFieldsFor[ParticipantNodeParameterConfig] =
    new DeprecatedFieldsFor[ParticipantNodeParameterConfig] {
      override def movedFields: List[DeprecatedConfigUtils.MovedConfigPath] = List(
        DeprecatedConfigUtils.MovedConfigPath(
          "automatically-perform-logical-synchronizer-upgrade",
          since = "3.5.0",
          to = Seq("automatically-perform-lsu"),
        ),
        DeprecatedConfigUtils.MovedConfigPath(
          "automatically-perform-lsu",
          since = "3.6.0",
          to = Seq("lsu.automatically-perform-lsu"),
        ),
        DeprecatedConfigUtils.MovedConfigPath(
          "automatically-perform-logical-synchronizer-upgrade",
          since = "3.6.0",
          to = Seq("lsu.automatically-perform-lsu"),
        ),
        DeprecatedConfigUtils.MovedConfigPath(
          "unsafe-online-party-replication",
          since = "3.5.0",
          to = Seq("alpha-online-party-replication-support"),
        ),
        DeprecatedConfigUtils.MovedConfigPath(
          "alpha-multi-synchronizer-support",
          since = "3.5.4",
          to = Seq("enable-all-ledger-api-reassignments"),
        ),
      )

      override def deprecatePath: List[DeprecatedConfigPath[?]] = List(
        DeprecatedConfigUtils.DeprecatedConfigPath(
          path = "initial-protocol-version",
          since = "3.5.0",
          valueFilter = None: Option[ParticipantProtocolVersion],
        )
      )
    }

  implicit val packageMetadataViewConfigReaderDeprecations
      : DeprecatedFieldsFor[PackageMetadataViewConfig] =
    new DeprecatedFieldsFor[PackageMetadataViewConfig] {

      override def deprecatePath: List[DeprecatedConfigPath[?]] =
        List(
          DeprecatedConfigPath(
            "init-takes-too-long-interval",
            since = "3.5.0",
            valueFilter = None: Option[FiniteDuration],
          )
        )
    }

  implicit val reassignmentsFieldsDeprecations: DeprecatedFieldsFor[ReassignmentsConfig] =
    new DeprecatedFieldsFor[ReassignmentsConfig] {
      override def deprecatePath: List[DeprecatedConfigPath[?]] =
        List(
          DeprecatedConfigPath[NonNegativeFiniteDuration](
            "target-timestamp-forward-tolerance",
            since = "3.6.0",
          )
        )
    }

  implicit val lsuConfigDeprecations: DeprecatedFieldsFor[LsuConfig] =
    new DeprecatedFieldsFor[LsuConfig] {
      override def movedFields: List[DeprecatedConfigUtils.MovedConfigPath] = List(
        DeprecatedConfigUtils.MovedConfigPath(
          "handshake-retry",
          since = "3.5.1",
          to = Seq("handshake.retry"),
        )
      )
    }
}

object CantonConfigDeprecations
    extends CantonSharedDeprecations
    with CantonSynchronizerNodeDeprecations
    with CantonParticipantNodeDeprecations {}
