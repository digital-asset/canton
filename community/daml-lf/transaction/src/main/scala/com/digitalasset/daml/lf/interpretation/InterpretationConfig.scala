// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.daml.lf
package interpretation

import com.digitalasset.daml.lf.language.LanguageVersion
import com.google.common.annotations.VisibleForTesting

case class InterpretationConfig(
    allowedLanguageVersions: List[LanguageVersion],
    errorOnContractKeyHashMismatch: Boolean,
)

object InterpretationConfig {

  /* One constant per protocol version. Canton maps protocol versions to these constants in
   * `com.digitalasset.canton.version.InterpretationConfig.forProtocolVersion`.
   *
   * Once the corresponding protocol version is stable, a constant MUST NOT change.
   *
   * Adding or changing a constant, or the range of a `featurePV<N>` feature, requires a review from
   * the Daml language team.
   */

  val PV36: InterpretationConfig = InterpretationConfig(
    allowedLanguageVersions =
      LanguageVersion.allLfVersions.filter(LanguageVersion.featurePV36.enabledIn),
    errorOnContractKeyHashMismatch = false,
  )

  val PV37: InterpretationConfig = InterpretationConfig(
    allowedLanguageVersions =
      LanguageVersion.allLfVersions.filter(LanguageVersion.featurePV37.enabledIn),
    errorOnContractKeyHashMismatch = true,
  )

  // PV38 is still alpha: this constant may change until PV37 becomes stable.
  val PV38: InterpretationConfig = InterpretationConfig(
    allowedLanguageVersions =
      LanguageVersion.allLfVersions.filter(LanguageVersion.featurePV38.enabledIn),
    errorOnContractKeyHashMismatch = true,
  )

  // The config of dev protocol versions. Not frozen: may change at any time.
  val Dev: InterpretationConfig = InterpretationConfig(
    allowedLanguageVersions = LanguageVersion.allLfVersions,
    errorOnContractKeyHashMismatch = true,
  )

  /** Not tied to any protocol version: changes over time, as the language evolves. Production code
    * must use one of the per-protocol-version constants above (or [[Dev]]) instead.
    */
  @VisibleForTesting
  val DefaultForTesting: InterpretationConfig = InterpretationConfig(
    allowedLanguageVersions = LanguageVersion.stableLfVersions,
    errorOnContractKeyHashMismatch = true,
  )
}
