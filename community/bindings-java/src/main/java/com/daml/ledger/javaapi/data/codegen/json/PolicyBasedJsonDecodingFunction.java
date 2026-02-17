// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data.codegen.json;

import com.daml.ledger.javaapi.data.codegen.UnknownTrailingFieldPolicy;

/**
 * <strong>INTERNAL API</strong>: this is meant for use by <a
 * href="https://docs.daml.com/app-dev/bindings-java/codegen.html">the Java code generator</a>, and
 * <em>should not be referenced directly</em>. Applications should use a code-generated {@code
 * valueDecoder} method instead.
 *
 * @hidden
 */
@FunctionalInterface
public interface PolicyBasedJsonDecodingFunction<T> {
  T apply(JsonLfReader r, UnknownTrailingFieldPolicy policy) throws JsonLfDecoder.Error;
}
