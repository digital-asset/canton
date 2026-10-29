# HTTP JSON Service v2

For the legacy json api service read [LEGACY.md](LEGACY.md)

JSON API V2 mirrors gRPC ledger API, with some limitations/changes. It uses HTTP/HTTPS and Websocket (WS/WSS) protocol.

# Configuration

Relevant (minimal) config fragment that enables JSON API:
```
canton {
  participants {
    participant1 {
      storage {
        type = memory
      }
      admin-api {
        port = 14012
      }
      http-ledger-api {
        port = 8080
     }
   }
  }
}
```
(ports are arbitrary)

This enables both `legacy` and the new (`v2`) JSON api.

For a complete reference of config parameters see `JsonApiConfig` class.

# Usage

For a reference of available endpoints there is an openapi and an asyncapi documentation provided.
Yaml files can be found in artifactory:
- [canton-json-apidocs](https://digitalasset.jfrog.io/ui/repos/tree/General/canton-internal/com/digitalasset/canton/canton-json-apidocs_2.13)
- alternatively when json api is started, canton exposes `/v2/doc/openapi` and `v2/doc/asyncapi` endpoints

Swagger editor (https://editor.swagger.io) can be used to explore documentation.

For more details please read corresponding `Ledger API gRPC` documentation.

# Websockets

Websockets are expecting and returning messages in JSON Format.
After opening a websocket, typically a JSON message must be sent by the client in order to get (more) answers from server.
Example:
after opening a websocket connection to `v2/commands/completions`, there must be `CompletionStreamRequest` sent over websocket.

# Security

JSON API uses same tokens format and structure as gRPC API.

For regular http/https calls put JWT Token using  Authorization request header.

`Authorization: Bearer <<jwttoken>>`

For websocket (ws/wss) calls we use `Sec-WebSocket-Protocol`. There should be a token provided (with a `jwt.token.` prefix), as well
as additional protocol header `daml.ws.auth` (after comma).

Example:
```
jwt.token.eyJhbGciOiAiSFMyNTYiLCAidHlwIjogIkpXVCJ9.eyJhdWQiOm51bGwsImV4cCI6bnVsbCwiaXNzIjpudWxsLCJzY29wZSI6ImRhbWxfbGVkZ2VyX2FwaSIsInN1YiI6InRlc3RfNDAyYTNhYmYtMGQwNC00Yzg4LWE3ZjEtNDQ2NGFiYmFjMzZjIn0.wNx9G2u-N2R1KZYaJTS5n9lTyhL9GwsgQYQHrUDIL2c, daml.ws.auth)
```

For websocket endpoints only, the token can also be passed in a cookie named `jwt.token`, as an alternative to
passing it in `Sec-WebSocket-Protocol` for browser clients that cannot set the `Authorization` header.
The `Sec-WebSocket-Protocol` header has a risk of being exposed in logs, because it is not redacted by default by
e.g. network proxies. The token is looked up in this order: `Authorization` header, `Sec-WebSocket-Protocol`
header, `jwt.token` cookie. Regular HTTP endpoints ignore the cookie.

Cookie authentication is disabled unless the participant operator lists the allowed web application origins in
`websocket-config.cookie-auth-allowed-origins`. A cookie-authenticated handshake whose `Origin` header is not in
the list (or is missing / `null`) is refused with `403 Forbidden`, which prevents cross-site websocket hijacking.

The server always selects the `daml.ws.auth` subprotocol in its handshake response, so the client must still offer it in
`Sec-WebSocket-Protocol` (without the `jwt.token.` element); browsers otherwise reject the handshake because the server
selected a subprotocol that was not offered.

Example:
```
Sec-WebSocket-Protocol: daml.ws.auth
Cookie: jwt.token=eyJhbGciOiAiSFMyNTYiLCAidHlwIjogIkpXVCJ9.eyJhdWQiOm51bGwsImV4cCI6bnVsbCwiaXNzIjpudWxsLCJzY29wZSI6ImRhbWxfbGVkZ2VyX2FwaSIsInN1YiI6InRlc3RfNDAyYTNhYmYtMGQwNC00Yzg4LWE3ZjEtNDQ2NGFiYmFjMzZjIn0.wNx9G2u-N2R1KZYaJTS5n9lTyhL9GwsgQYQHrUDIL2c
```

or, from a browser:
```javascript
new WebSocket("wss://<host>/v2/state/active-contracts", ["daml.ws.auth"])
```

# Errors

When applicable, the endpoint returns an error in JSON format. The error is also reflected in status of a response (4xx, 5xx).

Example:
```json
{
  "cause":"The submitted request has invalid arguments: idp-id-#:-_ 92d97389-4bf2-4850-85ba-be51a1fabd04 does not match idp in body: Some(IdentityProviderConfig(idp-id-#:-_ 92d97389-4bf2-4850-85ba-be51a1fabd047,true,,,))",
  "code":"INVALID_ARGUMENT",
  "context":{},
  "correlationId":null,
  "definiteAnswer":false,
  "errorCategory":8,
  "grpcCodeValue":3,
  "resources":[],
  "retryInfo":null,
  "traceId":null
}
```

Websockets can also in some cases return error frame (json structure same as above) instead of normal message.
