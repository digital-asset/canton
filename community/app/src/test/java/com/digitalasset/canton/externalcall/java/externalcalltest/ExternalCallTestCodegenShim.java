package com.digitalasset.canton.externalcall.java.externalcalltest;

import com.daml.ledger.javaapi.data.Value;
import com.daml.ledger.javaapi.data.codegen.ContractTypeCompanion;
import com.daml.ledger.javaapi.data.codegen.ValueDecoder;
import com.daml.ledger.javaapi.data.codegen.json.JsonLfDecoder;
import com.daml.ledger.javaapi.data.codegen.json.JsonLfEncoder;

import java.util.function.Function;

final class ExternalCallTestCodegenShim {
  static final ContractTypeCompanion.Package PACKAGE =
      new ContractTypeCompanion.Package(
          ExternalCallInterface.PACKAGE_ID,
          ExternalCallInterface.PACKAGE_NAME,
          ExternalCallInterface.PACKAGE_VERSION);

  private ExternalCallTestCodegenShim() {}

  static ValueDecoder<Value> rawValueDecoder() {
    return ValueDecoder.create((value, policy) -> value);
  }

  static JsonLfDecoder<Value> unsupportedJsonDecoder(String label) {
    return JsonLfDecoder.create(
        (reader, policy) -> {
          throw new UnsupportedOperationException(
              "JSON decoding is unsupported for " + label + " in ExternalCallTest shim wrappers");
        });
  }

  static <T> Function<T, JsonLfEncoder> unsupportedJsonEncoder(String label) {
    return ignored ->
        writer -> {
          throw new UnsupportedOperationException(
              "JSON encoding is unsupported for " + label + " in ExternalCallTest shim wrappers");
        };
  }
}
