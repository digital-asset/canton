// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data.codegen;

import com.daml.ledger.javaapi.data.Value;

import java.util.function.BiFunction;

/**
 * A converter from the encoded form of a Daml value, represented by {@link Value}, to the
 * codegen-decoded form, represented by {@code Data}.
 *
 * <p>Every codegen class for a template, record, or variant includes a {@code valueDecoder} method
 * that produces one of these. If the data type has type parameters, {@code valueDecoder} has
 * arguments that correspond to {@link ValueDecoder}s for those type arguments. For primitive types
 * that are not code-generated, see {@link PrimitiveValueDecoders}.
 *
 * <pre>
 * // given template 'Foo', and encoded payload 'Value fooValue'
 * Foo foo = Foo.valueDecoder().decode(fooValue);
 *
 * // given Daml datatypes 'Bar a b' and 'Baz',
 * // and encoded 'Bar' 'Value barValue'
 * Bar&lt;Baz, Long> bar = Bar.valueDecoder(
 *     Baz.valueDecoder(), PrimitiveValueDecoders.fromInt64)
 *   .decode(barValue);
 *
 * Bar&lt;List&lt;Baz>, Map&lt;Long, String>> barWithAggregates = Bar.valueDecoder(
 *     PrimitiveValueDecoders.fromList(Baz.valueDecoder),
 *     PrimitiveValueDecoders.fromGenMap(
 *       PrimitiveValueDecoders.fromInt64,
 *       PrimitiveValueDecoders.fromText))
 *   .decode(barAggregateValue);
 * </pre>
 *
 * There is also a {@link #decode(Value, UnknownTrailingFieldPolicy)} method that takes an
 * additional {@link UnknownTrailingFieldPolicy} This method is used to handle unknown trailing
 * fields in the value. Currently, the error is thrown if there are unknown trailing fields or extra
 * fields are ignored.
 *
 * <p>In the following situation if the following DAML data object is defined in the DAML project on
 * the client side:
 *
 * <pre>
 * data Bar = Bar { bar : Int, baz: Int } deriving (Eq, Show)
 * </pre>
 *
 * And the following value is received from the ledger:
 *
 * <pre>
 * Value(1, 2, Optional(3))
 * </pre>
 *
 * if the following code is executed on the client side, the IllegalArgumentException is thrown:
 *
 * <pre>
 *     Bar bar = Bar.valueDecoder().decode(value, UnknownTrailingFieldPolicy.STRICT);
 * </pre>
 *
 * and if the following code is executed on the client side, the Bar object is created with bar = 1
 * and baz = 2, while the extra field 3 is ignored:
 *
 * <pre>
 *     Bar bar = Bar.valueDecoder().decode(value, UnknownTrailingFieldPolicy.IGNORE);
 * </pre>
 *
 * @param <Data> The codegen or primitive type that this decodes a {@link Value} to.
 */
@FunctionalInterface
public interface ValueDecoder<Data> {
  /**
   * Method used to decode value with default policy (STRICT) to handle unknown trailing fields
   *
   * @param value Value to decode.
   * @return Decoded value.
   * @see ValueDecoder
   */
  Data decode(Value value);

  /**
   * <strong>INTERNAL API</strong>: this is meant for use by <a
   * href="https://docs.daml.com/app-dev/bindings-java/codegen.html">the Java code generator</a>,
   * and <em>should not be referenced directly</em>. Applications should pass this {@link
   * ValueDecoder} as an argument to a code-generated {@code valueDecoder} method instead.
   *
   * @hidden
   */
  default ContractId<Data> fromContractId(String contractId) {
    throw new IllegalArgumentException("Cannot create contract id for this data type");
  }

  /**
   * @see ValueDecoder Method used to decode value with additional policy to handle unknown trailing
   *     fields.
   * @param value Value to decode.
   * @param policy Policy to handle unknown trailing fields. One of the following could be provided:
   *     <ul>
   *       <li><b>STRICT</b> - the error is thrown if there are unknown trailing fields in value
   *       <li><b>IGNORE</b> - the unknown fields are ignored and are not added to decoded value
   *     </ul>
   *
   * @return Decoded value.
   */
  default Data decode(Value value, UnknownTrailingFieldPolicy policy) {
    if (policy == UnknownTrailingFieldPolicy.STRICT) {
      return decode(value);
    }
    throw new UnsupportedOperationException(
        String.format("This decoder does not support UnknownTrailingFieldPolicy.%s. ", policy)
            + "Please regenerate your code using codegen 3.5.0 or greater.");
  }

  /**
   * <strong>INTERNAL API</strong>: this is meant for use by <a
   * href="https://docs.daml.com/app-dev/bindings-java/codegen.html">the Java code generator</a>,
   * and <em>should not be referenced directly</em>. Applications should pass this {@link
   * ValueDecoder} as an argument to a code-generated {@code valueDecoder} method instead.
   *
   * @hidden
   */
  static <Data> ValueDecoder<Data> create(
      BiFunction<Value, UnknownTrailingFieldPolicy, Data> function) {
    return new ValueDecoder<Data>() {
      @Override
      public Data decode(Value value) {
        return decode(value, UnknownTrailingFieldPolicy.STRICT);
      }

      @Override
      public Data decode(Value value, UnknownTrailingFieldPolicy policy) {
        return function.apply(value, policy);
      }
    };
  }
}
