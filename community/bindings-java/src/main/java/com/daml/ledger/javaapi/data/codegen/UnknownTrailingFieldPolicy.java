// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data.codegen;

/**
 * Policy for handling unknown trailing fields when decoding a Daml record into a Java Class by
 * codegen
 */
public enum UnknownTrailingFieldPolicy {
  /** Ignore unknown trailing fields */
  IGNORE,
  /** Fail with an exception if unknown, non-empty trailing fields are encountered */
  STRICT,
}
