// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.javaapi.data.codegen.json;

import com.daml.ledger.javaapi.data.Unit;
import com.daml.ledger.javaapi.data.codegen.ContractId;
import com.daml.ledger.javaapi.data.codegen.UnknownTrailingFieldPolicy;
import com.fasterxml.jackson.core.JsonToken;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

/// Decoders for built-in LF types. ///
public class JsonLfDecoders {

  public static final JsonLfDecoder<Unit> unit =
      JsonLfDecoder.create(
          (r, policy) -> {
            r.readStartObject();
            return Unit.getInstance();
          });

  public static final JsonLfDecoder<Boolean> bool =
      JsonLfDecoder.create(
          (r, policy) -> {
            r.expectIsAt("boolean", JsonToken.VALUE_TRUE, JsonToken.VALUE_FALSE);
            Boolean value = null;
            try {
              value = Boolean.parseBoolean(r.currentText("boolean"));
            } catch (IOException e) {
              r.parseExpected("true or false", e);
            }
            r.moveNext();
            return value;
          });

  public static final JsonLfDecoder<Long> int64 =
      JsonLfDecoder.create(
          (r, policy) -> {
            r.expectIsAt("int64", JsonToken.VALUE_NUMBER_INT, JsonToken.VALUE_STRING);
            Long value = null;
            try {
              value = Long.parseLong(r.currentText("int64"));
            } catch (NumberFormatException e) {
              r.parseExpected("int64", e);
            }
            r.moveNext();
            return value;
          });

  public static JsonLfDecoder<BigDecimal> numeric(int scale) {
    assert scale >= 0 : "negative numeric scale " + scale;
    return JsonLfDecoder.create(
        (r, policy) -> {
          r.expectIsAt(
              "numeric",
              JsonToken.VALUE_NUMBER_INT,
              JsonToken.VALUE_NUMBER_FLOAT,
              JsonToken.VALUE_STRING);
          BigDecimal value = null;
          try {
            value = new BigDecimal(r.currentText("numeric"));
          } catch (NumberFormatException e) {
            r.parseExpected("numeric", e);
          }
          int orderMag = MAX_NUMERIC_PRECISION - scale; // Available digits on lhs of decimal point
          if (value.precision() - value.scale() > orderMag) {
            r.parseExpected(String.format("numeric in range (-10^%s, 10^%s)", orderMag, orderMag));
          }
          if (value.scale() > scale) value = value.setScale(scale, RoundingMode.HALF_EVEN);
          r.moveNext();
          return value;
        });
  }

  public static final JsonLfDecoder<Instant> timestamp =
      JsonLfDecoder.create(
          (r, policy) -> {
            r.expectIsAt("timestamp", JsonToken.VALUE_STRING);
            Instant value = null;
            try {
              value = Instant.parse(r.currentText("timestamp")).truncatedTo(ChronoUnit.MICROS);
            } catch (DateTimeParseException e) {
              r.parseExpected("valid ISO 8601 date and time in UTC", e);
            }
            r.moveNext();
            return value;
          });

  public static final JsonLfDecoder<LocalDate> date =
      JsonLfDecoder.create(
          (r, policy) -> {
            r.expectIsAt("date", JsonToken.VALUE_STRING);
            LocalDate value = null;
            try {
              value = LocalDate.parse(r.currentText("date"));
            } catch (DateTimeParseException e) {
              r.parseExpected("valid ISO 8601 date", e);
            }
            r.moveNext();
            return value;
          });

  public static final JsonLfDecoder<String> text =
      JsonLfDecoder.create(
          (r, policy) -> {
            r.expectIsAt("text", JsonToken.VALUE_STRING);
            String value = r.currentText("valid textual value");
            r.moveNext();
            return value;
          });

  public static final JsonLfDecoder<String> party = text;

  public static <C extends ContractId<?>> JsonLfDecoder<C> contractId(Function<String, C> constr) {
    return JsonLfDecoder.create(
        (r, policy) -> {
          String id = text.decode(r, policy);
          return constr.apply(id);
        });
  }

  // Read an list with an unknown number of items of the same type.
  public static <T> JsonLfDecoder<List<T>> list(JsonLfDecoder<T> decodeItem) {
    return JsonLfDecoder.create(
        (r, policy) -> {
          List<T> list = new ArrayList<>();
          r.readStartArray();
          while (r.notEndArray()) {
            T item = decodeItem.decode(r, policy);
            list.add(item);
          }
          r.readEndArray();
          return list;
        });
  }

  // Read a map with textual keys, and unknown number of items of the same type.
  public static <V> JsonLfDecoder<Map<String, V>> textMap(JsonLfDecoder<V> decodeValue) {
    return JsonLfDecoder.create(
        (r, policy) -> {
          Map<String, V> map = new LinkedHashMap<>();
          r.readStartObject();
          while (r.notEndObject()) {
            String key = r.readFieldName().name;
            V val = decodeValue.decode(r, policy);
            map.put(key, val);
          }
          r.readEndObject();
          return map;
        });
  }

  // Read a map with unknown number of items of the same types.
  public static <K, V> JsonLfDecoder<Map<K, V>> genMap(
      JsonLfDecoder<K> decodeKey, JsonLfDecoder<V> decodeVal) {
    return JsonLfDecoder.create(
        (r, policy) -> {
          Map<K, V> map = new LinkedHashMap<>();
          // Maps are represented as an array of 2-element arrays.
          r.readStartArray();
          while (r.notEndArray()) {
            r.readStartArray();
            K key = decodeKey.decode(r, policy);
            V val = decodeVal.decode(r, policy);
            r.readEndArray();
            map.put(key, val);
          }
          r.readEndArray();
          return map;
        });
  }

  // The T type should not itself be Optional<?>. In that case use OptionalNested below.
  public static <T> JsonLfDecoder<Optional<T>> optional(JsonLfDecoder<T> decodeVal) {
    return JsonLfDecoder.create(
        (r, policy) -> {
          if (r.isNull()) {
            r.moveNext();
            return Optional.empty();
          } else {
            T some = decodeVal.decode(r, policy);
            assert (!(some instanceof Optional))
                : "Used `optional` to decode a "
                    + some.getClass()
                    + " but `optionalNested` must be used for the outer decoders of nested"
                    + " Optional";
            return Optional.of(some);
          }
        });
  }

  public static <T> JsonLfDecoder<Optional<Optional<T>>> optionalNested(
      JsonLfDecoder<Optional<T>> decodeVal) {
    return JsonLfDecoder.create(
        (r, policy) -> {
          if (r.isNull()) {
            r.moveNext();
            return Optional.empty();
          } else {
            r.readStartArray();
            if (r.isNull()) r.parseExpected("] or item");
            Optional<T> val = r.notEndArray() ? decodeVal.decode(r, policy) : Optional.empty();
            r.readEndArray();
            return Optional.of(val);
          }
        });
  }

  public static <E extends Enum<E>> JsonLfDecoder<E> enumeration(Map<String, E> damlNameToEnum) {
    return JsonLfDecoder.create(
        (r, policy) ->
            r.readFromText(damlNameToEnum::get, new ArrayList<>(damlNameToEnum.keySet())));
  }

  // Provides a generic way to read a variant type, by specifying each tag.
  public static <T> JsonLfDecoder<T> variant(
      List<String> tagNames, Function<String, JsonLfDecoder<? extends T>> decoderByName) {
    return JsonLfDecoder.create(
        (r, policy) -> {
          r.readStartObject();
          T result = null;
          JsonLfReader.FieldName field = r.readFieldName();
          switch (field.name) {
            case "tag":
              {
                var decoder = r.readFromText(decoderByName, tagNames);
                JsonLfReader.FieldName valueField = r.readFieldName();
                if (!valueField.name.equals("value")) {
                  r.parseExpected("field value", null, valueField.name, valueField.loc);
                }
                result = decoder.decode(r, policy);
                break;
              }
            case "value":
              {
                // Can't decode until we know the tag.
                JsonLfReader.UnknownValue unknown = JsonLfReader.UnknownValue.read(r);
                JsonLfReader.FieldName tagField = r.readFieldName();
                if (!tagField.name.equals("tag")) {
                  r.parseExpected("field tag", null, tagField.name, tagField.loc);
                }
                var decoder = r.readFromText(decoderByName, tagNames);
                result = unknown.decodeWith(decoder, policy);
                break;
              }
            default:
              r.parseExpected("field tag or value", null, field.name, field.loc);
          }
          r.readEndObject();
          return result;
        });
  }

  // Provides a generic way to read a record type, with a constructor arg for each field.
  // This is a little fragile, so is better used by code-gen. Specifically:
  // - The constructor must cast the elements and pass them to the T's constructor appropriately.
  // - The elements of argNames should all evaluate to non non-null when applied to
  // argsByName.
  // - The index field values should correspond to the args passed to the constructor.
  //
  // e.g.
  //     r.record(
  //        asList("i", "b"),
  //        name -> {
  //          switch (name) {
  //            case "i":
  //              return JsonLfReader.JavaArg.at(0, r.list(r.int64()));
  //            case "b":
  //              return JsonLfReader.JavaArg.at(1, r.bool(), false);
  //            default:
  //              return null;
  //          }
  //        },
  //        args -> new Foo((List<Long>) args[0], (Boolean) args[1]))
  //     )
  public static <T> JsonLfDecoder<T> record(
      List<String> argNames,
      Function<String, JavaArg<? extends Object>> argsByName,
      Function<Object[], T> constr) {
    return JsonLfDecoder.create(
        (r, policy) -> {
          Object[] args = new Object[argNames.size()];
          if (r.isStartObject()) {
            r.readStartObject();
            while (r.notEndObject()) {
              JsonLfReader.FieldName field = r.readFieldName();
              var constrArg = argsByName.apply(field.name);
              if (constrArg == null && policy == UnknownTrailingFieldPolicy.STRICT) {
                r.unknownField(field.name, argNames, field.loc);
              } else if (constrArg == null) {
                skipValue(r);
              } else args[constrArg.index] = constrArg.decode.decode(r, policy);
            }
            r.readEndObject();
          } else if (r.isStartArray()) {
            r.readStartArray();
            for (String fieldName : argNames) {
              var field = argsByName.apply(fieldName);
              args[field.index] = field.decode.decode(r, policy);
            }
            if (r.notEndArray() && policy == UnknownTrailingFieldPolicy.STRICT) {
              r.parseExpected("end of array");
            } else {
              while (r.notEndArray()) {
                skipValue(r);
              }
            }
            r.readEndArray();
          } else {
            r.parseExpected("object or array");
          }

          // Handle missing fields.
          for (String argName : argNames) {
            JavaArg<? extends Object> arg = argsByName.apply(argName);
            if (args[arg.index] != null) continue;
            if (arg.defaultVal == null) r.missingField(argName);
            args[arg.index] = arg.defaultVal;
          }

          return constr.apply(args);
        });
  }

  private static void skipValue(JsonLfReader r) throws JsonLfDecoder.Error {
    if (r.isStartArray()) {
      skipArray(r);
    } else if (r.isStartObject()) {
      skipObject(r);
    } else {
      r.moveNext();
    }
  }

  private static void skipArray(JsonLfReader r) throws JsonLfDecoder.Error {
    r.readStartArray();
    while (r.notEndArray()) {
      skipValue(r);
    }
    r.readEndArray();
  }

  private static void skipObject(JsonLfReader r) throws JsonLfDecoder.Error {
    r.readStartObject();
    while (r.notEndObject()) {
      r.readFieldName();
      skipValue(r);
    }
    r.readEndObject();
  }

  // Represents an argument to the constructor of the code-gen class.
  public static class JavaArg<T> {
    final int index;
    final JsonLfDecoder<T> decode;
    final T defaultVal; // If non-null, used to populate value of missing fields.

    private JavaArg(int index, JsonLfDecoder<T> decode, T defaultVal) {
      this.index = index;
      this.decode = decode;
      this.defaultVal = defaultVal;
    }

    public static <T> JavaArg<T> at(int index, JsonLfDecoder<T> decode, T defaultVal) {
      return new JavaArg<T>(index, decode, defaultVal);
    }

    public static <T> JavaArg<T> at(int index, JsonLfDecoder<T> decode) {
      return new JavaArg<T>(index, decode, null);
    }
  }

  @SuppressWarnings("unchecked")
  // Can be used within the `constr` arg to `record`, to allow casting without producing warnings.
  public static <T> T cast(Object o) {
    return (T) o;
  }

  private static final int MAX_NUMERIC_PRECISION = 38;
}
