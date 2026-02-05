# External Call Feature - Implementation Plan

**Last Updated:** 2026-02-04
**Status:** P1 (serialization) ✅, P2 (execution) ✅ - Ready for testing

## Overview

The external call feature allows Daml contracts to make deterministic HTTP calls to extension services. The key design principle is that external calls are made during transaction **submission** and the results are stored in the transaction. During **validation**, the stored results are replayed rather than making new calls, ensuring determinism across all participants.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        SUBMISSION PATH                               │
│  Daml Contract → Interpreter → NeedExternalCall → HTTP Call         │
│                                      ↓                               │
│                              ExternalCallResult stored in            │
│                              Node.Exercise.externalCallResults       │
└─────────────────────────────────────────────────────────────────────┘
                                      ↓
                            Transaction persisted
                                      ↓
┌─────────────────────────────────────────────────────────────────────┐
│                        VALIDATION PATH                               │
│  Transaction → DAMLe reinterpret → NeedExternalCall → Replay        │
│                                           ↓                          │
│                              Read stored result from                 │
│                              ExerciseActionDescription               │
└─────────────────────────────────────────────────────────────────────┘
```

## Current State (2026-02-04)

### Completed ✅
- [x] `ExternalCallResult` data type in daml-lf-transaction
- [x] `externalCallResults` field on `Node.Exercise`
- [x] `BExternalCall` builtin in interpreter AST
- [x] `NeedExternalCall` question type in interpreter
- [x] `ResultNeedExternalCall` in engine result types
- [x] `ExtensionServiceConfig` configuration types
- [x] `HttpExtensionServiceClient` with retry logic
- [x] `ExtensionValidator` for DAR requirement validation
- [x] PureConfig readers/writers
- [x] All pattern matches updated for exhaustiveness
- [x] Build passes with `-Werror`
- [x] All existing tests pass (7,000+)
- [x] **Protobuf schema** for `ExternalCallResult` in `transaction.proto`
- [x] **Protobuf encoding/decoding** in `TransactionCoder.scala`
- [x] **Test generators** for `ExternalCallResult` in `ValueGenerators.scala`
- [x] **Test normalization** for version-dependent external call results
- [x] **P2.1: Submission path wiring** - ExtensionServiceManager → LedgerApiServer
- [x] **P2.2: Validation replay** - DAMLe replays stored results

### Verified ✅
- [x] `TransactionCoderSpec` tests pass (25 tests)
- [x] `ModelConformanceChecker` tests pass (22 tests)
- [x] `ExampleTransaction` tests pass (120 tests)

### Remaining ❌
- [ ] Extension client unit tests (P3.2)
- [ ] Integration tests (P4.1)
- [ ] Operator documentation (P5.1)

---

## P1: Critical - Data Persistence ✅ COMPLETE

External call results are now properly serialized to/from protobuf.

### Task 1.1: Add Protobuf Schema for ExternalCallResult ✅

**Status:** ✅ Complete

**File modified:**
- `community/daml-lf/transaction/src/main/protobuf/com/digitalasset/daml/lf/transaction.proto`

**Schema added:**
```protobuf
// Result of an external call made during transaction execution.
// External calls are deterministic HTTP calls to extension services.
// Results are stored in the transaction and replayed during validation.
// *since dev*
message ExternalCallResult {
  string extension_id = 1;    // Extension service identifier (e.g., "oracle-price-feed")
  string function_id = 2;     // Function identifier within the extension
  string config_hash = 3;     // Hash of the extension configuration for version validation
  string input_hex = 4;       // Hex-encoded input data sent to the extension
  string output_hex = 5;      // Hex-encoded output data returned by the extension
  int32 call_index = 6;       // Index of this call within the exercise node (for ordering)
}

// In Exercise message:
repeated ExternalCallResult external_call_results = 1002; // *since version dev*
```

---

### Task 1.2: Implement Protobuf Encoding in TransactionCoder ✅

**Status:** ✅ Complete

**File modified:**
- `community/daml-lf/transaction/src/main/scala/com/digitalasset/daml/lf/transaction/TransactionCoder.scala`

---

### Task 1.3: Test Infrastructure ✅

**Status:** ✅ Complete

**Files modified:**
- `community/daml-lf/transaction/src/test/scala/com/digitalasset/daml/lf/value/test/ValueGenerators.scala`
- `community/daml-lf/transaction/src/test/scala/com/digitalasset/daml/lf/transaction/TransactionCoderSpec.scala`

---

## P2: Core Functionality - Execution Path ✅ COMPLETE

### Task 2.1: Wire Extension Service to Submission Path ✅

**Status:** ✅ Complete

**Files modified:**
- `community/participant/src/main/scala/com/digitalasset/canton/participant/ledger/api/LedgerApiServer.scala`
- `community/ledger/ledger-api-core/src/main/scala/com/digitalasset/canton/platform/apiserver/ApiServiceOwner.scala`
- `community/ledger/ledger-api-core/src/main/scala/com/digitalasset/canton/platform/apiserver/ApiServices.scala`

**Implementation:**
1. `LedgerApiServer` creates `ExtensionServiceManager` from config
2. `ExtensionServiceExternalCallHandler.create()` wraps the manager
3. Handler passed through `ApiServiceOwner` → `ApiServices` → `StoreBackedCommandInterpreter`
4. When engine yields `ResultNeedExternalCall`, handler makes HTTP call via `HttpExtensionServiceClient`

---

### Task 2.2: Implement External Call Replay During Validation ✅

**Status:** ✅ Complete

**Files modified:**
- `community/participant/src/main/scala/com/digitalasset/canton/participant/util/DAMLe.scala`
- `community/base/src/main/scala/com/digitalasset/canton/data/ActionDescription.scala`
- `community/base/src/main/scala/com/digitalasset/canton/data/ViewParticipantData.scala`
- `community/base/src/main/protobuf/com/digitalasset/canton/protocol/v30/participant_transaction.proto`
- `community/participant/src/main/scala/com/digitalasset/canton/participant/protocol/validation/ModelConformanceChecker.scala`

**Implementation:**

1. **DAMLe.scala** - Added `StoredExternalCallResults` type and replay logic:
```scala
type StoredExternalCallResults = Map[(String, String, Int), (String, String, String)]

object StoredExternalCallResults {
  val empty: StoredExternalCallResults = Map.empty
  def fromResults(results: ImmArray[ExternalCallResult]): StoredExternalCallResults = ...
}
```

When `ResultNeedExternalCall` is encountered in `handleResultInternal`:
- First checks if engine provides `storedResult` directly
- Falls back to lookup in `storedExternalCallResults` map by (extensionId, functionId, inputHex)
- Calls `resume(Right(outputHex))` to replay
- Returns error if no matching result found

2. **ActionDescription.scala** - Added `externalCallResults` field to `ExerciseActionDescription`:
```scala
final case class ExerciseActionDescription private (
    ...
    externalCallResults: ImmArray[ExternalCallResult],
)
```

3. **participant_transaction.proto** - Added `ExternalCallResultProto` message:
```protobuf
message ExternalCallResultProto {
  string extension_id = 1;
  string function_id = 2;
  string config_hash = 3;
  string input_hex = 4;
  string output_hex = 5;
  int32 call_index = 6;
}
```

4. **ModelConformanceChecker.scala** - Wired replay in `reInterpret`:
```scala
val storedExternalCallResults: StoredExternalCallResults =
  viewParticipantData.actionDescription match {
    case exercise: ExerciseActionDescription =>
      StoredExternalCallResults.fromResults(exercise.externalCallResults)
    case _ => StoredExternalCallResults.empty
  }
```

---

## P3: Unit Tests

### Task 3.1: ExternalCallResult Serialization Tests ✅

**Status:** ✅ Complete (via existing property-based tests)

### Task 3.2: Extension Service Client Tests

**Status:** ⬜ Not Started

**File to create:**
- `community/participant/src/test/scala/com/digitalasset/canton/participant/extension/HttpExtensionServiceClientSpec.scala`

---

## P4: Integration Test

### Task 4.1: End-to-End External Call Test

**Status:** ⬜ Not Started

---

## P5: Documentation

### Task 5.1: Operator Configuration Guide

**Status:** ⬜ Not Started

**Content to document:**
```hocon
canton.participants.participant1.parameters.engine {
  extensions {
    "my-extension" {
      name = "My Extension Service"
      host = "extension.example.com"
      port = 8443
      use-tls = true
      jwt-file = "/path/to/token"
      connect-timeout = 500ms
      request-timeout = 8s
      max-total-timeout = 25s
      max-retries = 3
    }
  }
  extension-settings {
    validate-extensions-on-startup = true
    fail-on-extension-validation-error = true
  }
}
```

---

## Open Design Questions

1. **Hash computation:** External call results are currently excluded from node hashes. Is this correct? What are the implications?

2. **Call index tracking:** How do we handle nested exercises that both make external calls? Is `callIndex` per-exercise or per-transaction?

3. **Error semantics:** What happens if an extension service is unavailable during submission? Fail the transaction? Retry indefinitely?

4. **Validation mismatch:** If validation finds a `NeedExternalCall` but no stored result exists, is this always an error? Or could it indicate version skew?

---

## Session Log

### Session 1 (2026-02-04 AM)
- Ported all external call changes from daml repo to canton
- Fixed all compilation errors
- Fixed all pattern match exhaustiveness issues
- All existing tests pass
- Created this implementation plan

### Session 2 (2026-02-04 PM)
- **P1.1 Complete:** Added `ExternalCallResult` message to `transaction.proto`
- **P1.2 Complete:** Implemented `encodeExternalCallResult` and `decodeExternalCallResult` in `TransactionCoder.scala`
- **P3.1 Complete:** Added test generators in `ValueGenerators.scala`
- Fixed test normalization in `TransactionCoderSpec.normalizeExe`

### Session 3 (2026-02-04 PM continued)
- **P2.1 Complete:** Wired extension service to submission path
  - ExtensionServiceManager created in LedgerApiServer
  - ExternalCallHandler passed through API layers
- **P2.2 Complete:** Implemented validation replay
  - Added StoredExternalCallResults type to DAMLe
  - Added externalCallResults field to ExerciseActionDescription
  - Added ExternalCallResultProto to participant_transaction.proto
  - Wired ModelConformanceChecker to pass stored results to reinterpret
- All tests pass:
  - TransactionCoderSpec: 25 tests
  - ModelConformanceChecker: 22 tests
  - ExampleTransaction: 120 tests

### Next Steps
1. Add extension client unit tests (P3.2)
2. Create integration test with mock service (P4.1)
3. Add operator documentation (P5.1)

---

## File Reference

Key files for this feature:

| File | Purpose | Status |
|------|---------|--------|
| `daml-lf/transaction/Node.scala` | ExternalCallResult data type, Exercise node | ✅ |
| `daml-lf/transaction/transaction.proto` | Protobuf schema | ✅ |
| `daml-lf/transaction/TransactionCoder.scala` | Protobuf serialization | ✅ |
| `daml-lf/transaction/SerializationVersion.scala` | Version constants | ✅ |
| `daml-lf/engine/Engine.scala` | ResultNeedExternalCall type | ✅ |
| `daml-lf/interpreter/Speedy.scala` | NeedExternalCall question | ✅ |
| `participant/extension/HttpExtensionServiceClient.scala` | HTTP client | ✅ |
| `participant/extension/ExtensionServiceManager.scala` | Extension routing | ✅ |
| `participant/extension/ExtensionServiceExternalCallHandler.scala` | Handler bridge | ✅ |
| `participant/config/ExtensionServiceConfig.scala` | Configuration | ✅ |
| `participant/ledger/api/LedgerApiServer.scala` | Creates manager | ✅ |
| `participant/util/DAMLe.scala` | Validation replay | ✅ |
| `base/data/ActionDescription.scala` | ExerciseActionDescription with results | ✅ |
| `protocol/v30/participant_transaction.proto` | ExternalCallResultProto | ✅ |
| `participant/protocol/validation/ModelConformanceChecker.scala` | Wires replay | ✅ |

### Test Files

| File | Purpose | Status |
|------|---------|--------|
| `daml-lf/value/test/ValueGenerators.scala` | Test generators | ✅ |
| `daml-lf/transaction/TransactionCoderSpec.scala` | Serialization tests | ✅ |
| `participant/protocol/validation/ExampleTransactionConformanceTest.scala` | Validation tests | ✅ |
