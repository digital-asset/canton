Protobuf & gRPC Guidelines
==========================

We use ProtoBuf and gRPC to implement data serialization and remote procedure calls with efficient and backwards compatible message encodings.
This guide explains how to write protobuf definitions and use them in Scala.

We use [Buf](https://docs.buf.build/) to enforce consistent styling.
Some guidelines enforced by Buf are not listed below.

# General

* All Protobuf definitions should be using [`proto3`](https://developers.google.com/protocol-buffers/docs/proto3)
* Ensure that the generated stubs will be in their own package separate from any implementations
* Place `.proto` files in `src/main/protobuf`
* Avoid wrapping primitive types in a message structure unless future extensibility will likely be required
* Avoid using `google.protobuf.Empty` for requests or responses.
  Instead, create an empty request or response message.
  This allows for adding message fields later-on.
* Enum types must have a case with value 0 that indicates a missing value.
* Ensure that message names and field names are descriptive because the proto definition is also a public API.
  For example, comment message fields, be specific for the field name
  (if the field stores a `SynchronizerId` then its name should be `synchronizer_id` rather than `synchronizer`).
* Use `optional` instead of Google's wrapper types for optional primitive fields.
  E.g., use `optional string` instead of `StringValue`.

# Naming

* `.proto` file names should use `snake_casing.proto`
* Prefer having a single `.proto` definition per service.
  Append `_service` to the filename (e.g. `package_service.proto`) to enable finding service entrypoints easily.
  Typically, layout this file in the order: `service` -> request and response `message` -> `message`
* Avoid using primitive values alone as `service` requests or responses to support future extensions.
  Define a dedicated Request and Response message for each remote procedure call.
  Suffix such messages with `Request` and `Response` as appropriate.
* Keep all Request and Response messages in the same `.proto` as the service definition
    * unless messages are shared between many services, then place in a separate `.proto`
* `service`, `message` and `rpc` definitions should use upper camel case naming, e.g. `MyService`.
* `rpc` names should prefer using request verbs as a prefix (e.g. `List`, `Get`, `Create`, `Update`, `Delete`)
* `message` field names should be named using snake case, e.g. `my_field`.
* `repeated` fields should use a plural name

# Commentary

* Use commentary to indicate when optional fields are defined.
* Indicate error gRPC Statuses on service calls where appropriate

# Usage in Scala

Generated Protobuf classes should always be referred to with a package prefix, e.g., `v1.MyMessage` instead of `MyMessage`.
This avoids name conflicts with our handwritten classes.
Packages with generated Protobuf classes should be listed as to be imported only with prefix in IntelliJ's Scala code style settings.
The settings file `.idea/codeStyles/Project.xml` is therefore under version control.

# Further reading

If you need further guidelines, read [gRPC and Protobuf definitions using Google's guidelines](https://cloud.google.com/apis/design/naming_convention).
