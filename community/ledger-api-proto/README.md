# Ledger-API

This project contains the Protobuf definitions for the Ledger API.
These definitions serve as:
- the source for generating gRPC server stubs of the Ledger API
- documentation of the Ledger API

# Documentation

The [Ledger API Introduction](https://docs.daml.com/app-dev/grpc/index.html) contains introductory material as well as links to the protodocs reference documentation.

The reference documentation is auto-generated from the protoc comments in the *.proto files.
The comments must be [compatible with reStructuredText](https://www.sphinx-doc.org/en/master/usage/restructuredtext/basics.html). For example:

```
Long paragraphs can be wrapped
across multiple lines.

Place blank lines between paragraphs.

1. Lists require a blank line before and after the list.
2. Multiline list items
   must have consistent
   indentation before each line.
3. Numbered lists cannot be defined with (1), (2), etc.

Comments that are not RST-compatible will fail the automated build process.
```

## Documentation guideline
Each service, rpc method and message defined in the public-facing API protobuf should have brief documentation attached as comments.
The comments should be clean and concise and should provide enough information (or links to external documentation) for developers to understand how to use the service, method or message effectively.
This generally applies to the Ledger API gRPC services.

Additionally to serving as the reference documentation, these comments are also used in the OpenAPI schema descriptions for the Ledger JSON API
(see `community/ledger/ledger-json-api/src/test/resources/json-api-docs/openapi.yaml`)

### Required/Optional markers
In our Ledger API Protobuf documentation, for historical reasons, we generally do not use the typed proto3 designation for optional fields (`optional`).
For this reason, we uniformly mark the field's requiredness by using an `// Optional` or `// Required` comment for each field defined in the messages used in the Ledger API.
These requiredness markers are highlighting what the Ledger API business logic validation does. For example:

```
message DeleteUserRequest {
  // The user to delete.
  //
  // Required
  string user_id = 1;
```

This means, that a Ledger API request can be constructed as a valid PB message with a default value (`DeleteUserRequest.user_id` left unpopulated).
But if marked as `// Required` in the comment, server-side validation will reject the request.

OpenAPI generator uses the requiredness markers to determine which property should be declared as required
in all schemas defining request or JSON response bodies.

**Note**: The Ledger API JSON serializer populates all possible keys in responses,
thus, in practice, treating, all fields in responses as required.
However, when a field is marked as `// Optional` in the comments indicates that the server may legitimately omit the key from the response,
for example, when the field is a message field (e.g. `CreatedEvent.contract_key`) or a `bytes` field (e.g. `CreatedEvent.created_event_blob`).

### How to assign `// Required` and `// Optional` markers

`// Required` and `// Optional` should be placed on the last line of the field's comment, and should be separated from the rest of the comment by a blank line,
as such
```
message DeleteUserRequest {
  // The user to delete.
  //
  // Required
  string user_id = 1;
```

For fields where optionality is ambiguous, such as collections (`repeated` and `map`)
or fields with default values (e.g. `string` fields, numeric fields, enums),
the comment can be enriched with additional information to clarify the requiredness of the field, for example:

```
message GetCommandStatusResponse {
  // Optional: can be empty
  repeated CommandStatus command_status = 1;
}

message ListIdentityProviderConfigsResponse {
  // The list of identity provider configs
  //
  // Required: must be non-empty
  repeated IdentityProviderConfig identity_provider_configs = 1;
}
```

#### Requests

By default, assign `// Optional` for any request field.
Use `// Required` when the request is invalid unless the field is populated with a meaningful/non-default value,
in any of the following cases:

- `string`: must be non-empty for business logic (e.g. party ID, contract ID) and the Ledger API cannot infer it.
- Numeric (e.g. `int64`): the default (`0`) is not acceptable
- `enum`: the default / unspecified value is not acceptable and the Ledger API cannot infer a meaningful value.
- Message field: business logic requires the object to be present (i.e. the request is invalid if it is absent or semantically empty) and the Ledger API cannot infer it.
- `repeated`: business logic requires a non-empty list
- `map`: business logic requires at least one entry
- `oneof`: at least one branch must be set for the request to be valid (unless an “empty” / unset value has a well-defined meaning and is accepted).
- `bytes`: must be non-empty to be valid (e.g. `prepared_transaction_hash`).

#### Responses

By default, assign `// Required` for any response field.
Use `// Optional` only when the server may legitimately omit the key or populate it with default values that can be ignored
(i.e. can be completely left out of the JSON response), for example:

- `string` fields (e.g. `Completion.update_id`)
- Numeric (e.g. `int64`): if the default return value (`0`) can be ignored or treated as absent.
- `enum` fields, when an unspecified value may be returned and should be ignored by the client
- Message fields (e.g. `CreatedEvent.contract_key`)
- `repeated` fields (e.g. `CreatedEvent.interface_views`)
- `map` fields (e.g. `ObjectMeta.annotations`)
- `oneof` fields (e.g. `Completion.deduplication_period`)
- `bytes` fields (e.g. `CreatedEvent.created_event_blob`)

**Note**: The idiomatic `optional` protobuf qualifier should be used whenever possible for new fields in messages.
The `optional` qualifier allows distinguishing between an explicitly set default value and an unset field, which is particularly useful for numeric fields, enums, and message fields.
When used, it should be marked as `// Optional` in the comments, and the field should be treated as optional for the purposes of the documentation guidelines above.

# Dependencies

The definitions in this package depend on Protobuf's ["well known types"](https://developers.google.com/protocol-buffers/docs/reference/google.protobuf), as well as:

- https://raw.githubusercontent.com/grpc/grpc/v1.18.0/src/proto/grpc/status/status.proto
- https://raw.githubusercontent.com/grpc/grpc/v1.18.0/src/proto/grpc/health/v1/health.proto

# Terminology

## The participant's offset

Describes a specific point in the stream of updates observed by this participant.
A participant offset is meaningful only in the context of its participant. Different
participants may associate different offsets to the same change synchronized over a synchronizer,
and conversely, the same literal participant offset may refer to different changes on
different participants.

This is also a unique index of the changes which happened on the virtual shared ledger.
The order of participant offsets is reflected in the order the updates that are
visible when subscribing to the `UpdateService`. This ordering is also a fully causal
ordering for any specific synchronizer: for two updates synchronized by the same synchronizer, the
one with a bigger participant offset happened after than the one with a smaller participant
offset. Please note this is not true for updates synchronized by different synchronizers.
Accordingly, the participant offset order may deviate from the order of the changes
on the virtual shared ledger.

The Ledger API endpoints that take offsets allow to specify portions
of the participant data that is relevant for the client to read.

Offsets returned by the Ledger API can be used as-is (e.g.
to keep track of processed transactions and provide a restart
point to use in case of failure).
