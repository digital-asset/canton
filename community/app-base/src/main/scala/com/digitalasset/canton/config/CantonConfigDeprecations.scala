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
import com.digitalasset.canton.synchronizer.mediator.{
  MediatorBatchingConfig,
  MediatorCachingConfigs,
  MediatorConfig,
}
import com.digitalasset.canton.synchronizer.sequencer.block.bftordering.core.BftBlockOrdererConfig
import com.digitalasset.canton.synchronizer.sequencer.config.{
  AsyncWriterConfig,
  SequencerBatchingConfig,
  SequencerCachingConfigs,
  SequencerNodeParameterConfig,
}

trait CantonSharedDeprecations {
  private def deprecatedSince38(paths: String*): List[DeprecatedConfigPath] =
    paths.toList.map(DeprecatedConfigPath(_, since = "3.8.0"))

  private val packageDependencyCache =
    DeprecatedConfigPath("package-dependency-cache", since = "3.5.0")

  implicit val participantBatchingDeprecations: DeprecatedFieldsFor[ParticipantBatchingConfig] =
    new DeprecatedFieldsFor[ParticipantBatchingConfig] {
      override def movedFields: List[MovedConfigPath] = List(
        MovedConfigPath(
          "contract-store-aggregator",
          since = "3.9.0",
          to = Seq("contract-store-read-aggregator"),
        )
      )

      override def deprecatePath: List[DeprecatedConfigPath] = deprecatedSince38(
        "in-flight-aggregations-query-interval",
        "mediator-fetch-finalized-responses-aggregator",
        "mediator-store-finalized-responses-aggregator",
        "max-pruning-time-interval",
      )
    }

  implicit val mediatorBatchingDeprecations: DeprecatedFieldsFor[MediatorBatchingConfig] =
    new DeprecatedFieldsFor[MediatorBatchingConfig] {
      override def deprecatePath: List[DeprecatedConfigPath] = deprecatedSince38(
        "ledger-api-pruning-batch-size",
        "max-acs-import-batch-size",
        "in-flight-aggregations-query-interval",
        "contract-store-aggregator",
        "contract-journal-aggregator",
        "max-pruning-batch-size",
        "max-pruning-time-interval",
        "max-stakeholder-groups-batch-size",
      )
    }

  implicit val sequencerBatchingDeprecations: DeprecatedFieldsFor[SequencerBatchingConfig] =
    new DeprecatedFieldsFor[SequencerBatchingConfig] {
      override def deprecatePath: List[DeprecatedConfigPath] = deprecatedSince38(
        "ledger-api-pruning-batch-size",
        "max-acs-import-batch-size",
        "contract-store-aggregator",
        "contract-journal-aggregator",
        "mediator-fetch-finalized-responses-aggregator",
        "mediator-store-finalized-responses-aggregator",
        "max-pruning-batch-size",
        "max-stakeholder-groups-batch-size",
      )
    }

  implicit val participantCachingDeprecations: DeprecatedFieldsFor[ParticipantCachingConfigs] =
    new DeprecatedFieldsFor[ParticipantCachingConfigs] {
      override def deprecatePath: List[DeprecatedConfigPath] =
        packageDependencyCache :: deprecatedSince38(
          "finalized-mediator-confirmation-requests",
          "sequencer-payload-cache",
          "sequencer-catchup-payload-cache",
          "bft-ordering-batch-cache",
        )
    }

  implicit val mediatorCachingDeprecations: DeprecatedFieldsFor[MediatorCachingConfigs] =
    new DeprecatedFieldsFor[MediatorCachingConfigs] {
      override def deprecatePath: List[DeprecatedConfigPath] =
        packageDependencyCache :: deprecatedSince38(
          "contract-store",
          "package-upgrade-cache",
          "sequencer-payload-cache",
          "sequencer-catchup-payload-cache",
          "bft-ordering-batch-cache",
        )
    }

  implicit val sequencerCachingDeprecations: DeprecatedFieldsFor[SequencerCachingConfigs] =
    new DeprecatedFieldsFor[SequencerCachingConfigs] {
      override def deprecatePath: List[DeprecatedConfigPath] =
        packageDependencyCache :: deprecatedSince38(
          "contract-store",
          "package-upgrade-cache",
          "finalized-mediator-confirmation-requests",
        )
    }

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

  implicit val sequencerClientConfigReaderDeprecations: DeprecatedFieldsFor[SequencerClientConfig] =
    new DeprecatedFieldsFor[SequencerClientConfig] {
      override def deprecatePath: List[DeprecatedConfigPath] =
        List(
          DeprecatedConfigPath("use-new-connection-pool", since = "3.5.0")
        )
    }

  implicit val bftBlockOrdererConfigDeprecations: DeprecatedFieldsFor[BftBlockOrdererConfig] =
    new DeprecatedFieldsFor[BftBlockOrdererConfig] {
      override def deprecatePath: List[DeprecatedConfigPath] =
        List(
          DeprecatedConfigPath("segment-length-for-pv-34", since = "3.8.0"),
          DeprecatedConfigPath("leader-selection-policy-config-for-pv-34", since = "3.8.0"),
        )
    }
}

trait CantonSynchronizerNodeDeprecations {
  implicit val asyncWriterConfigReaderDeprecations: DeprecatedFieldsFor[AsyncWriterConfig] =
    new DeprecatedFieldsFor[AsyncWriterConfig] {

      override def deprecatePath: List[DeprecatedConfigPath] =
        List(
          DeprecatedConfigPath(
            "enabled",
            since = "3.5.0",
            value = false,
          )
        )
    }

  implicit val sequencerNodeParametersConfigReaderDeprecations
      : DeprecatedFieldsFor[SequencerNodeParameterConfig] =
    new DeprecatedFieldsFor[SequencerNodeParameterConfig] {
      override def deprecatePath: List[DeprecatedConfigPath] = List(
        // removed as the feature is now always enabled and the flag is no longer needed (https://github.com/DACH-NY/canton/issues/30769)
        DeprecatedConfigPath("produce-post-ordering-topology-ticks", since = "3.8.0"),
        DeprecatedConfigPath("enable-reject-delivered-aggregations-on-pv-35", since = "3.8.0"),
        DeprecatedConfigPath("unsafe-enable-online-party-replication", since = "3.5.0"),
        DeprecatedConfigPath("unsafe-sequencer-channel-support", since = "3.7.1"),
      )
    }

  implicit val mediatorConfigReaderDeprecations: DeprecatedFieldsFor[MediatorConfig] =
    new DeprecatedFieldsFor[MediatorConfig] {

      override def deprecatePath: List[DeprecatedConfigPath] =
        List(
          DeprecatedConfigPath("asynchronous-processing", since = "3.5.1")
        )
    }
}

trait CantonParticipantNodeDeprecations {

  implicit val ledgerApiServerConfigReaderDeprecatedFields
      : DeprecatedFieldsFor[LedgerApiServerConfig] =
    new DeprecatedFieldsFor[LedgerApiServerConfig] {

      override def deprecatePath: List[DeprecatedConfigPath] =
        List(
          DeprecatedConfigPath(
            "index-service.prepare-package-metadata-time-out-warning",
            since = "3.5.0",
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
        DeprecatedConfigUtils.MovedConfigPath(
          "traffic-enforcement",
          since = "3.6.0",
          to = Seq("traffic-accounting"),
        ),
      )

      override def deprecatePath: List[DeprecatedConfigPath] = List(
        DeprecatedConfigPath("features.deprecated.enable-deprecated-endpoints-34", since = "3.7.0"),
        DeprecatedConfigPath(
          "features.deprecated.enable-deprecated-parameters-34",
          since = "3.7.0",
        ),
        DeprecatedConfigPath(
          "features.deprecated.enable-deprecated-endpoints-35",
          since = "3.7.0",
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

      override def deprecatePath: List[DeprecatedConfigPath] = List(
        DeprecatedConfigUtils
          .DeprecatedConfigPath(path = "initial-protocol-version", since = "3.5.0"),
        DeprecatedConfigUtils.DeprecatedConfigPath(
          path = "acs-commitments.enable-new-acs-commitment-processor",
          since = "3.8.0",
        ),
      )
    }

  implicit val packageMetadataViewConfigReaderDeprecations
      : DeprecatedFieldsFor[PackageMetadataViewConfig] =
    new DeprecatedFieldsFor[PackageMetadataViewConfig] {

      override def deprecatePath: List[DeprecatedConfigPath] =
        List(
          DeprecatedConfigPath("init-takes-too-long-interval", since = "3.5.0")
        )
    }

  implicit val reassignmentsFieldsDeprecations: DeprecatedFieldsFor[ReassignmentsConfig] =
    new DeprecatedFieldsFor[ReassignmentsConfig] {
      override def deprecatePath: List[DeprecatedConfigPath] =
        List(
          DeprecatedConfigPath("target-timestamp-forward-tolerance", since = "3.6.0")
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
