// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

/** Used to set parameters for testing when these don't need to be exposed in a config file.
  *
  * @param testSequencerClientFor: members that should use a
  * [[com.digitalasset.canton.sequencing.client.DelayedSequencerClient]] for testing
  */
case class TestingConfigInternal(
    testSequencerClientFor: Set[TestSequencerClientFor] = Set.empty,
    useCausalityTracking: Boolean = false,
)

/** @param environmentId ID used to disambiguate tests running in parallel
  * @param memberName The name of the member that should use a delayed sequencer client
  * @param domainName The name of the domain for which the member should use a delayed sequencer client
  */
case class TestSequencerClientFor(environmentId: String, memberName: String, domainName: String)
