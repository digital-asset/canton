# External Call — Remaining Tasks

## 1. Canton Runtime: Rollback Support (10 tests)
`PartialTransaction.recordExternalCallResult` only works in `ExercisesContextInfo`.
Need to handle `TryContextInfo` so external calls work inside try/catch blocks.

- [ ] Fix `PartialTransaction.recordExternalCallResult` to walk up context stack to find enclosing exercise
- [ ] Handle rollback semantics: discard recorded results when try block rolls back
- [ ] Tests to unblock:
  - [ ] discard external call result when made inside rolled-back scope
  - [ ] preserve external call result when inside try block that succeeds
  - [ ] handle multiple rollback scopes with external calls
  - [ ] handle nested rollback scopes with external calls
  - [ ] handle multiple external calls with partial rollback
  - [ ] maintain rollback scope in deep transaction with external calls
  - [ ] handle external call result in transaction with multiple rollbacks
  - [ ] correctly order external call results with rollbacks
  - [ ] correctly handle external call followed by exception in same scope
  - [ ] handle exception thrown by external call itself

## 2. Daml Contract Redesign: Multi-View Authorization (2 tests)
`DelegatedExternalCall` uses `controller actor` and `BobExternalCall` uses `controller bob`.
Bob is on participant2, so participant1 can't authorize these choices.

- [ ] Redesign choices to use `signatory` authorization or flexible controllers
- [ ] Tests to unblock:
  - [ ] handle external calls when nested exercises have different informees
  - [ ] handle multiple views each with their own external calls

## 3. Test Infrastructure: Error Handling (15 tests)
Just need mock server error handlers. No Canton changes required.

- [ ] Implement error handling tests:
  - [ ] handle HTTP 400 Bad Request
  - [ ] handle HTTP 401 Unauthorized
  - [ ] handle HTTP 403 Forbidden
  - [ ] handle HTTP 404 Not Found
  - [ ] handle HTTP 500 Internal Server Error
  - [ ] handle HTTP 502 Bad Gateway
  - [ ] handle HTTP 503 Service Unavailable
  - [ ] handle HTTP 504 Gateway Timeout
  - [ ] handle request timeout
  - [ ] handle connection timeout
  - [ ] handle connection refused
  - [ ] handle unknown extension ID
  - [ ] handle unknown function ID
  - [ ] propagate error message from external service
  - [ ] handle empty error response body
  - [ ] handle very large error response body
  - [ ] allow subsequent calls after error

## 4. Test Infrastructure: Retry Logic (7 tests)
Mock server handlers for transient failures and retry verification.

- [ ] Implement retry tests:
  - [ ] succeed after one transient failure
  - [ ] succeed after multiple transient failures
  - [ ] fail when max retries exhausted
  - [ ] retry on connection reset
  - [ ] use exponential backoff (fix timing to exclude confirmation calls)
  - [ ] handle retry with different result (needs deterministic mock)
  - [ ] maintain idempotency across retries

## 5. Test Infrastructure: Interface Tests (7 tests)
Need ExternalCallInterface codegen and Daml interface implementation.

- [ ] Wire up interface-based external call tests:
  - [ ] execute external call via interface choice
  - [ ] work with different implementations of same interface
  - [ ] handle interface exercise in nested transaction
  - [ ] work with observer on interface exercise
  - [ ] handle interface exercise with multiple stakeholders
  - [ ] correctly identify template ID in external call from interface
  - [ ] handle view decomposition correctly for interface exercises
  - [ ] work when interface is from different package than template

## 6. Test Infrastructure: Consensus & Multi-Participant (8 tests)
Complex multi-participant scenarios with participant-specific mock responses.

- [ ] Implement consensus tests:
  - [ ] reject transaction when confirming participant receives different result
  - [ ] reject transaction when multiple confirmers disagree with preparer
  - [ ] succeed when all participants receive identical results
  - [ ] handle partial mismatch in multi-view transaction
  - [ ] allow observer to validate using stored results without HTTP call
  - [ ] reject when observer's local recomputation doesn't match stored result
  - [ ] handle signatory on P1, observers on P2 and P3

## 7. Edge Cases & Misc (5 tests)
- [ ] correctly replay two identical calls with different results via sequential index
- [ ] handle external call that takes exactly timeout duration
- [ ] handle clock skew between participants
- [ ] allow different parties to see different consistency results
- [ ] handle multiple different external calls in same transaction
- [ ] track call count for consistency verification
