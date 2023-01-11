// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.config

/** Configuration for the domain that uses a CCF-based sequencer.
  *
  * @param memberKeyStore Key store with the member's private key and certificate to use for governance operations.
  * @param memberKeyName  Name of the member's key name in the member key store to sign governance requests with.
  */
case class DomainCcfConfig(memberKeyStore: KeyStoreConfig, memberKeyName: String)
