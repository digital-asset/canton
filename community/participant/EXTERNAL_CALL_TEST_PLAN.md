# External Call Consistency Checking - Test Plan

## Assessment of Current Test Situation

### Test Infrastructure

| Aspect | Status | Details |
|--------|--------|---------|
| **Framework** | ScalaTest | `AnyWordSpec`, `AsyncWordSpec`, `BaseTestWordSpec` |
| **Mocking** | Mockito | `MockitoSugar` trait |
| **Test Utilities** | Rich | `ExampleTransactionFactory`, `TestingTopology`, `DefaultTestIdentities` |
| **Integration Tests** | Functional | `CommunityIntegrationTest`, `MockExternalCallServer` |

### Current External Call Test Coverage

| Component | Unit Tests | Integration Tests | Status |
|-----------|-----------|-------------------|--------|
| `ExternalCallResult` (data type) | None | - | Missing |
| `ExternalCallHandler` (HTTP client) | None | Basic | Partial |
| `ModelConformanceChecker` (with ext calls) | General tests | - | Partial |
| `TransactionConfirmationResponsesFactory` | **None** | Through ledger tests | Missing |
| `ExternalCallConsistencyChecker` (new) | **None** | **None** | **Not Implemented** |
| Multi-participant validation | - | **Pending** | **Not Implemented** |

### Key Gaps Identified

1. **No unit tests** for `TransactionConfirmationResponsesFactory` at all
2. **Multi-participant integration tests are all `pending`** (placeholders only)
3. **No tests for per-party consistency checking** (our new feature)
4. **No tests for verdict partitioning** based on external call results
5. Basic integration tests exist but don't test consistency validation

---

## Test Plan for 100% Coverage

### Part 1: Unit Tests for ExternalCallConsistencyChecker

**File**: `community/participant/src/test/scala/com/digitalasset/canton/participant/protocol/validation/ExternalCallConsistencyCheckerTest.scala`

**Pattern**: `AnyWordSpec with BaseTest`

```scala
class ExternalCallConsistencyCheckerTest extends AnyWordSpec with BaseTest {
  // ~400-500 lines
}
```

#### Test Cases

##### 1. ExternalCallKey Tests

| Test | Description | Priority |
|------|-------------|----------|
| `equal keys for identical calls` | Same extensionId, functionId, configHash, inputHex → equal | High |
| `different extensionId creates different key` | Different extensionId → not equal | High |
| `different functionId creates different key` | Different functionId → not equal | High |
| `different configHash creates different key` | Different configHash → not equal | High |
| `different inputHex creates different key` | Different inputHex → not equal | High |
| `outputHex does not affect equality` | Same key with different outputs → equal keys | High |
| `pretty printing formats correctly` | Verify pretty print output | Low |

##### 2. collectExternalCalls Tests

| Test | Description | Priority |
|------|-------------|----------|
| `collect calls from single exercise view` | One view with external calls → collected | High |
| `collect calls from multiple views` | Multiple views → all calls collected | High |
| `skip non-exercise action descriptions` | Create/Fetch/LookupByKey views → no calls | High |
| `skip exercise without external calls` | Exercise with empty externalCallResults → no calls | High |
| `extract signatories from coreInputs` | Contract signatories correctly extracted | Critical |
| `handle missing contract in coreInputs` | Contract not in coreInputs → empty signatories | Medium |
| `preserve callIndex from source` | callIndex correctly preserved | Medium |
| `preserve viewPosition from source` | viewPosition correctly preserved | Medium |

##### 3. checkConsistency - Single Party Tests

| Test | Description | Priority |
|------|-------------|----------|
| `return Consistent when no calls` | Empty calls → all parties Consistent | High |
| `return Consistent when no parties` | No hosted parties → empty results | High |
| `return Consistent for single call` | One call → Consistent (nothing to compare) | High |
| `return Consistent for same call same output` | Two calls, same key, same output → Consistent | Critical |
| `return Inconsistent for same call different output` | Two calls, same key, different outputs → Inconsistent | Critical |
| `return Consistent for different calls` | Two different keys → Consistent | High |
| `party not signatory sees Consistent` | Party not in any signatories → Consistent | High |

##### 4. checkConsistency - Multi-Party Tests

| Test | Description | Priority |
|------|-------------|----------|
| `different parties see different subsets` | Alice sees call1+call2, Bob sees call1 only | Critical |
| `party in both calls detects inconsistency` | Alice signatory of both, different outputs → Inconsistent | Critical |
| `party in one call only sees consistency` | Bob signatory of one only → Consistent | Critical |
| `multiple parties with same view` | Same signatory set → same result | High |
| `mixed results for different parties` | Alice=Inconsistent, Bob=Consistent | Critical |

##### 5. Edge Cases

| Test | Description | Priority |
|------|-------------|----------|
| `handle three or more equal calls` | 3 calls, 2 same output, 1 different → Inconsistent | High |
| `multiple inconsistencies per party` | First inconsistency returned | Medium |
| `empty signatory set for call` | Call with no signatories → no party validates | Medium |
| `large number of parties` | 100+ parties → correct results | Low |
| `large number of calls` | 1000+ calls → correct results | Low |

---

### Part 2: Unit Tests for TransactionConfirmationResponsesFactory (External Call Integration)

**File**: `community/participant/src/test/scala/com/digitalasset/canton/participant/protocol/validation/TransactionConfirmationResponsesFactoryExternalCallTest.scala`

**Pattern**: `AsyncWordSpec with BaseTest with HasExecutionContext with FailOnShutdown`

#### Test Cases

##### 1. partitionByExternalCallConsistency Tests

| Test | Description | Priority |
|------|-------------|----------|
| `all parties consistent returns single response` | All Consistent → one response with all parties | High |
| `all parties inconsistent returns malformed rejection` | All Inconsistent → malformed with empty parties | High |
| `mixed consistency returns multiple responses` | Some Consistent, some Inconsistent → 2 responses | Critical |
| `empty hosted parties returns empty responses` | No hosted parties → Seq.empty | High |
| `malformed general verdict with inconsistency` | General=Malformed + Inconsistent parties → 2 malformed | Medium |
| `approval with inconsistency` | General=Approve + Inconsistent parties → approve + malformed | Critical |

##### 2. Integration with responsesForWellformedPayloads

| Test | Description | Priority |
|------|-------------|----------|
| `external call consistency check runs before verdicts` | Consistency checked across all views first | Critical |
| `inconsistent party gets ExternalCallInconsistency rejection` | Correct error code | High |
| `consistent party gets normal verdict` | Approve/other rejection as normal | High |
| `logging occurs for inconsistencies` | logger.warn called | Medium |

---

### Part 3: Integration Tests for Per-Party External Call Consistency

**File**: `community/app/src/test/scala/com/digitalasset/canton/integration/tests/externalcall/ExternalCallConsistencyIntegrationTest.scala`

**Pattern**: `CommunityIntegrationTest with ExternalCallIntegrationTestBase with SharedEnvironment`

#### Test Scenarios

##### Scenario 1: Same Call, Same Signatories, Same Output (Happy Path)

```
Setup:
- Alice on P1 (signatory)
- Contract with signatories={Alice}
- Two exercises in same transaction, same external call

Mock Server:
- Returns same output for all calls

Expected:
- Transaction commits successfully
- Alice approves
```

##### Scenario 2: Same Call, Same Signatories, Different Output (Rejection)

```
Setup:
- Alice on P1 (signatory)
- Contract with signatories={Alice}
- Two exercises, same external call key

Mock Server:
- First call returns "output1"
- Second call returns "output2"

Expected:
- Transaction rejected
- Error code: LOCAL_VERDICT_EXTERNAL_CALL_INCONSISTENCY
- Alice sees inconsistency
```

##### Scenario 3: Same Call, Different Signatories, Different Output (Partial Rejection)

```
Setup:
- Alice on P1 (signatory of Contract1)
- Bob on P2 (signatory of Contract2)
- Contract1 with signatories={Alice, Bob}
- Contract2 with signatories={Alice}
- Same external call in both exercises

Mock Server:
- Contract1 exercise returns "output1"
- Contract2 exercise returns "output2"

Expected:
- Alice sees both calls → detects inconsistency → rejects
- Bob sees only Contract1 call → no inconsistency → approves
- Transaction fails (Alice's rejection is sufficient)
```

##### Scenario 4: Same Call, Different Signatories, Same Output (Success)

```
Setup:
- Same as Scenario 3

Mock Server:
- All calls return same output

Expected:
- Transaction commits
- Both Alice and Bob approve
```

##### Scenario 5: Multi-Participant Same Party Consistency

```
Setup:
- Alice hosted on both P1 and P2
- External call with Alice as signatory

Expected:
- Both P1 and P2 produce identical verdicts for Alice
- No equivocation
```

##### Scenario 6: Observer Does Not Validate External Calls

```
Setup:
- Alice on P1 (signatory)
- Bob on P2 (observer only, not signatory)
- External call with Alice as signatory

Mock Server:
- Inconsistent outputs

Expected:
- Alice (signatory) sees inconsistency → rejects
- Bob (observer) does not validate external calls → not relevant
- Transaction fails due to Alice's rejection
```

##### Scenario 7: No External Calls in Transaction

```
Setup:
- Normal transaction without external calls
- Multiple parties

Expected:
- ExternalCallConsistencyResults is empty
- Normal verdict generation proceeds
```

---

### Part 4: Additional Integration Tests (Complete the Pending Tests)

**File**: `community/app/src/test/scala/com/digitalasset/canton/integration/tests/externalcall/MultiParticipantExternalCallIntegrationTest.scala`

Complete the existing `pending` tests:

| Test | Current Status | Implementation |
|------|---------------|----------------|
| `work when both participants have extension configured` | Pending | Implement |
| `allow observer to replay without HTTP call` | Pending | Implement |
| `handle observer seeing only subset of transaction` | Pending | Implement |
| `work with three participants with varied visibility` | Pending | Implement |
| `handle signatory on P1, observers on P2 and P3` | Pending | Implement |
| `reject when observer's participant is not configured` | Pending | Implement |
| `handle party hosted on multiple participants` | Pending | Implement |

---

## Test Matrix Summary

| Layer | Tests Needed | Estimated Lines | Priority |
|-------|-------------|-----------------|----------|
| ExternalCallConsistencyChecker Unit | ~25 tests | 400-500 | Critical |
| TransactionConfirmationResponsesFactory Unit | ~10 tests | 200-300 | Critical |
| External Call Consistency Integration | ~7 scenarios | 300-400 | Critical |
| Multi-Participant Integration (existing) | ~7 tests | 200-300 | High |
| **Total** | **~49 tests** | **1100-1500 lines** | |

---

## Test Files Created

```
community/participant/src/test/scala/com/digitalasset/canton/participant/protocol/validation/
├── ExternalCallConsistencyCheckerTest.scala              (CREATED - ~415 lines, 25+ tests)
└── TransactionConfirmationResponsesFactoryExternalCallTest.scala (CREATED - ~175 lines)

community/app/src/test/scala/com/digitalasset/canton/integration/tests/externalcall/
├── ExternalCallConsistencyIntegrationTest.scala          (CREATED - ~175 lines)
└── MultiParticipantExternalCallIntegrationTest.scala     (UPDATED - 3 tests implemented)
```

### Test Coverage Summary

| File | Tests Implemented | Status |
|------|-------------------|--------|
| ExternalCallConsistencyCheckerTest.scala | 25+ unit tests | Complete |
| TransactionConfirmationResponsesFactoryExternalCallTest.scala | 10+ tests | Complete |
| ExternalCallConsistencyIntegrationTest.scala | 7 scenarios | 4 implemented, 3 pending |
| MultiParticipantExternalCallIntegrationTest.scala | 7 tests | 3 implemented, 4 pending |

Note: Some integration tests are marked `pending` because they require:
- Complex multi-contract scenarios with different signatory sets
- Ability to inject inconsistent results within a single transaction
- Party hosted on multiple participants setup

---

## Implementation Order

1. **ExternalCallConsistencyCheckerTest.scala** - Pure unit tests, no dependencies
2. **TransactionConfirmationResponsesFactoryTest.scala** - Tests integration point
3. **ExternalCallConsistencyIntegrationTest.scala** - End-to-end scenarios
4. **MultiParticipantExternalCallIntegrationTest.scala** - Complete pending tests

---

## Test Utilities Needed

From existing codebase:
- `ExampleTransactionFactory` - Transaction/view creation
- `TestingTopology.from()` - Party-to-participant mappings
- `MockExternalCallServer` - HTTP mock
- `BaseTest` - Logging, protocol version
- `HasExecutionContext` - Execution context

May need to create:
- `ExternalCallTestFactory` - Helper to create `ExternalCallWithContext` instances
- `ViewValidationResultTestFactory` - Helper to create mock validation results
