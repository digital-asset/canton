# Per-Party External Call Consistency Checking

## Overview

This document describes the design and implementation of per-party external call consistency checking in Canton's transaction validation pipeline.

## Problem Statement

Canton's external call feature allows Daml contracts to call external services during transaction execution. A critical consistency requirement arises because:

1. **The same external call can appear multiple times** in a single transaction
2. **Each occurrence can have different signatory sets** based on the contract being exercised
3. **Each party must independently verify consistency** of all calls they are responsible for validating

### Example Scenario

Consider a transaction with two exercises that both call `externalService.getPrice(ETH)`:

```
Exercise 1 (Contract with signatories {Alice, Bob}):
  → externalService.getPrice("ETH") returns "3000"

Exercise 2 (Contract with signatories {Alice}):
  → externalService.getPrice("ETH") returns "3001"
```

In this scenario:
- **Alice** sees both calls and detects the inconsistency → must reject
- **Bob** sees only the first call and has nothing to compare → can approve

This per-party validation ensures that:
1. A faulty external service cannot corrupt the ledger if at least one honest participant validates
2. Two honest participants hosting the same party always produce identical verdicts

## Design Decisions

### 1. External Call Equality

Two external calls are considered "equal" (i.e., the same call) if they have identical:
- `extensionId` - identifies the external service
- `functionId` - identifies the function being called
- `config` - configuration parameters (bytes)
- `input` - the input arguments (bytes)

**Rationale**: This treats external services as pure functions - same inputs must produce same outputs. The `output` is intentionally excluded from the equality definition.

### 2. Validating Parties

For each external call, the **signatories of the contract being exercised** are the validating parties.

**Rationale**: This follows the Canton principle that signatories are responsible for authorizing and validating actions on their contracts. It matches the phrase from the design discussion: "signatories of the input contract of the enclosing Exercise scope."

**Alternative considered**: Using `actors` (the exercising parties) was rejected because actors may not have the same stake in the contract's integrity as signatories.

### 3. Rejection Type

External call inconsistency produces a **malformed rejection** (`isMalformed = true`).

**Rationale**: Inconsistent external call results indicate either:
- A faulty/non-deterministic external service
- A malicious submitter manipulating results
- A Byzantine participant fabricating results

All of these warrant the stronger "malformed" classification, which signals potential security issues.

### 4. Per-Party Verdict Generation

When parties have different consistency results, the participant generates **multiple ConfirmationResponse entries** for the same view:
- Parties with inconsistent results → malformed rejection
- Parties with consistent results → general verdict (approve or other rejection)

**Rationale**: This ensures accurate per-party verdicts while maintaining the existing response structure.

## Implementation Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                  TransactionConfirmationResponsesFactory            │
│                                                                     │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │ Step 1: Collect hosted confirming parties for all views     │   │
│  └─────────────────────────────────────────────────────────────┘   │
│                              ↓                                      │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │ Step 2: Collect external calls with signatory context       │   │
│  │         (ExternalCallConsistencyChecker.collectExternalCalls)│   │
│  └─────────────────────────────────────────────────────────────┘   │
│                              ↓                                      │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │ Step 3: Check consistency per party                         │   │
│  │         (ExternalCallConsistencyChecker.checkConsistency)   │   │
│  └─────────────────────────────────────────────────────────────┘   │
│                              ↓                                      │
│  ┌─────────────────────────────────────────────────────────────┐   │
│  │ Step 4: Generate verdicts, partitioning parties by          │   │
│  │         consistency results                                 │   │
│  │         (partitionByExternalCallConsistency)                │   │
│  └─────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────┘
```

## Key Components

### ExternalCallConsistencyChecker

Location: `community/participant/src/main/scala/com/digitalasset/canton/participant/protocol/validation/ExternalCallConsistencyChecker.scala`

Responsibilities:
- Define `ExternalCallKey` for equality comparison
- Collect external calls from all views with signatory context
- Check consistency per party

### LocalRejectError.MalformedRejects.ExternalCallInconsistency

Location: `community/base/src/main/scala/com/digitalasset/canton/protocol/LocalRejectError.scala`

Error code: `LOCAL_VERDICT_EXTERNAL_CALL_INCONSISTENCY`

### TransactionConfirmationResponsesFactory modifications

Location: `community/participant/src/main/scala/com/digitalasset/canton/participant/protocol/validation/TransactionConfirmationResponsesFactory.scala`

Changes:
- Added `externalCallConsistencyChecker` dependency
- Modified `responsesForWellformedPayloads` to perform consistency check before verdict generation
- Added `partitionByExternalCallConsistency` to handle per-party verdict splitting

## Security Invariant

**For every external call, the signatory set S must contain at least one party hosted on an honest participant to guarantee deterministic consistency.**

If all validating participants for a particular call instance are faulty, the system cannot guarantee safety. This is consistent with existing Daml/Canton assumptions: if all responsible validators are faulty, incorrect transactions may commit.

## Test Scenarios

1. **Same call, same signatories, same output**: All parties approve
2. **Same call, same signatories, different outputs**: All parties reject
3. **Same call, different signatory sets, same output**: All parties approve
4. **Same call, different signatory sets, different outputs**:
   - Party in both sets sees inconsistency → rejects
   - Party in only one set sees consistency → approves
5. **Two participants hosting same party**: Both must send identical verdicts

## References

- Original design discussion with Andreas L (Canton team)
- Canton Protocol documentation: `community/participant/PROTOCOL.md`
- External call implementation: `ActionDescription.ExerciseActionDescription.externalCallResults`
