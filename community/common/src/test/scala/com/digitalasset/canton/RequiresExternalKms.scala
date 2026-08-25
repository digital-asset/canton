// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton

import org.scalatest.Tag

/** Tag used to mark test suites that require access to real AWS or GCP KMS credentials.
  *
  * See [[annotations.RequiresExternalKms]] for more information.
  */
object RequiresExternalKms extends Tag("RequiresExternalKms")
