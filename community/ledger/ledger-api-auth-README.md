# Ledger API authorization

Ledger API request authorization is enforced at the gRPC layer. The JSON API acts as a thin wrapper that delegates
authorization directly to the gRPC layer by forwarding tokens, performing only minimal authentication checks to mitigate
DoS risks.

## Authorization Flow

1. **JWT token extraction and validation**:
   The `GrpcAuthInterceptor` intercepts incoming calls in the gRPC layer, extracts the JWT from gRPC metadata and
   validates it (such as against a JWKS) via one of the configured `AuthService`
   (see `canton.participants.participant.ledger-api.auth-services` configuration)
2. **Claims resolution**:
   The `AuthService` provides `Claims` for the token (resolving configuration-based claims if applicable). The
   `AuthInterceptor` resolves the claims further where needed (e.g. for user-management-based claims, such as provided
   by `AuthServiceJWT`) and then attaches these claims to the request's thread-local gRPC `Context`.
3. **Request authorization**:
   The Ledger API service authorization layer (e.g. `UpdateServiceAuthorization` for `UpdateService`)
   constructs a number of `RequiredClaim`s, that are then processed by the `Authorizer` together
   with the `Context` claims into a final decision.
