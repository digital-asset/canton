// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.synchronizer.config

import com.digitalasset.canton.synchronizer.sequencer.BlockSequencerConfig.IndividualThroughputCapConfig

// TODO(#30999) deduplicate throughput cap config from block sequencer and store it in the node config store
final case class DeclarativeSequencerConfig(
    throughputCap: Map[String, IndividualThroughputCapConfig] = Map.empty,
    removeCaps: Boolean = false,
    checkSelfConsistency: Boolean = false,
)

object DeclarativeSequencerConfig {

  object Readers {

    import pureconfig.ConfigReader
    import pureconfig.generic.semiauto.*

    // import canton config to include the implicit that prevents unknown keys
    implicit val declarativeSequencerConfigReader: ConfigReader[DeclarativeSequencerConfig] = {
      import IndividualThroughputCapConfig.ConfigImplicits.individualCapConfigReader
      deriveReader[DeclarativeSequencerConfig]
    }
  }

  object Writers {

    import pureconfig.ConfigWriter
    import pureconfig.generic.semiauto.*

    implicit final val declarativeSequencerConfigWriter
        : ConfigWriter[DeclarativeSequencerConfig] = {
      import IndividualThroughputCapConfig.ConfigImplicits.individualCapConfigWriter
      deriveWriter[DeclarativeSequencerConfig]
    }

  }

}
