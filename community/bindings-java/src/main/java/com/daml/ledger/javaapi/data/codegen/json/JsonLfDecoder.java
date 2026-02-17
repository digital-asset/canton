// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data.codegen.json;

import com.daml.ledger.javaapi.data.codegen.UnknownTrailingFieldPolicy;

import java.io.IOException;

@FunctionalInterface
public interface JsonLfDecoder<T> {
  public T decode(JsonLfReader r) throws Error;

  default T decode(JsonLfReader r, UnknownTrailingFieldPolicy policy) throws Error {
    if (policy == UnknownTrailingFieldPolicy.STRICT) {
      return decode(r);
    }
    throw new UnsupportedOperationException(
        String.format("This decoder does not support UnknownTrailingFieldPolicy.%s. ", policy)
            + "Please regenerate your code using codegen 3.5.0 or greater.");
  }

  /**
   * <strong>INTERNAL API</strong>: this is meant for use by <a
   * href="https://docs.daml.com/app-dev/bindings-java/codegen.html">the Java code generator</a>,
   * and <em>should not be referenced directly</em>. Applications should use a code-generated {@code
   * valueDecoder} method instead.
   *
   * @hidden
   */
  static <T> JsonLfDecoder<T> create(PolicyBasedJsonDecodingFunction<T> function) {
    return new JsonLfDecoder<>() {

      @Override
      public T decode(JsonLfReader r) throws Error {
        return decode(r, UnknownTrailingFieldPolicy.STRICT);
      }

      @Override
      public T decode(JsonLfReader r, UnknownTrailingFieldPolicy policy) throws Error {
        return function.apply(r, policy);
      }
    };
  }

  public static class Error extends IOException {
    public final JsonLfReader.Location location;
    private final String msg;

    public Error(String message, JsonLfReader.Location loc) {
      this(message, loc, null);
    }

    public Error(String message, JsonLfReader.Location loc, Throwable cause) {
      super(String.format("%s at line: %d, column: %d", message, loc.line, loc.column), cause);
      this.msg = message;
      this.location = loc;
    }

    public Error fromStartLocation(JsonLfReader.Location start) {
      return new Error(this.msg, start.advance(this.location), this.getCause());
    }
  }
}
