Backwards incompatible changes
==============================

Any changes included in a minor or patch release are forbidden to break
backwards-compatibility of our public APIs. The only exception on a case-by-case
basis are console commands that change in minor with a very clear migration
path, see [Changing Console Commands](#changing-console-commands).

For background on our public APIs and Canton's versioning guarantees, please read [the public documentation](https://docs.daml.com/canton/usermanual/versioning.html).
Changes that are only active for the `unstable` protocol version should be documented in the [UNACTIVATED.md](./UNACTIVATED.md)

We try to automatically catch breaking changes through various tests and/or checks:
- `compatible_with_latest_release`: runs a ping over synchronizer of latest release against participant using current state, and vice-versa.
- `FlywayMigrationChecksumsTest`: checks via checksums that our released Flyway migration files haven't changed.
- 'Protobuf continuity tests': checks that Protobuf continuity (wire compatibility, forward/backward-compatibility for parsing) is not broken.
- 'Data continuity tests': integration tests that try to load and use data from previous Canton releases.
- TODO(i7509): Add line about cross-version tests

However, despite these tests there are ways in which we can accidentally break backwards-compatibility.
For example, if you change the return type of the `prune` console command, this would currently not be caught by the above tests.
Therefore, please proceed with care if making any change that affects our public APIs.
As it isn't always obvious whether a given change is backwards-compatible and if not, how it should best be implemented then,
common past examples of either case follow.

## Changing Console Commands

If you need to change console commands in a breaking way between minor versions, e.g., because a
non-breaking approach would be too expensive, you must:

- Clearly mark the console command changes in the release notes as `BREAKING:`
- Provide a clear migration step for the user that minimizes the impact on the
  user.

## Examples of changes that weren't implemented in a backwards-compatible manner
### Changing the gRPC status of `EnterpriseSequencerAdministrationService.Prune` for errors from `INVALID_ARGUMENT` to `FAILED_PRECONDITION`
Classified as bug fix, especially as this endpoint is part of the Admin API. Thus, no extra backwards-compatibility logic was added.


## Examples of changes implemented in a backwards-compatible manner

### Configuration changes
#### Deprecating a configuration option

Make the configuration optional and mark it as `@Deprecated` but continue
parsing it until removed. See for example
[#9025](https://github.com/DACH-NY/canton/pull/9025/files#diff-a5c891cfb6ef4e48cf4806e41e5ba5ea65796ffb39db4f841f5d3990f9e657e8R127).

The class `DeprecatedConfigUtils` can be used to move around configuration keys within
the configuration without breaking backwards compatibility and with minimal boilerplate.

To move a config key, go through the following steps:

- Identify the config case class containing the field to be moved. If the desired new location is higher in the config hierarchy, find the first parent class where the new location can be accessed from.
- Create a `trait` in the companion object of that class (let's call it `C`), and in that trait implement an implicit function with the following signature:
    ```
  implicit def deprecatedParticipantInitConfig[X <: C]: DeprecatedFieldsFor[X]
  ```

For example:

    ```
    trait LocalNodeConfigDeprecationImplicits {
        implicit def deprecatedLocalNodeConfig[X <: LocalNodeConfig]: DeprecatedFieldsFor[X] =
          new DeprecatedFieldsFor[LocalNodeConfig] {
            override def movedFields: List[DeprecatedConfigUtils.MovedConfigPath] = List(
              DeprecatedConfigUtils.MovedConfigPath(
                "init.startup-fail-fast",
                "storage.parameters.fail-fast-on-startup",
              )
            )
          }
    }
    ```
- (Optional) If `C` is a subclass of a class (say `B`) having itself such a trait, make sure to mix the deprecation trait for `B` in, and add `B`'s `movedFields` lists to `C`'s
- Find the `ConfigReader[C]` derived instance, and with `DeprecatedConfigUtils._` imported, add `.applyDeprecations` to its definition.
- Make sure your implementation from step 2 is in scope, either by creating an object from the trait and importing it, or by mixing it in a pre-existing deprecation object if there's already one in scope.

To find examples, look for implementations of the `DeprecatedFieldsFor` in the code.

### Protobuf changes
#### Move non-Admin API gRPC endpoint from one service to another service
Depending on the protocol version, we either query the old or new endpoint.
See #9216, in particular following [block](https://github.com/DACH-NY/canton/pull/9216/files#diff-1c87fec017887a9cc7863e48c086e9a90e9c5c8203f8fcc3db223c314bc18996R75).


## Data continuity tests
The data continuity tests detect whenever the latest changes break data continuity with main.
Data continuity data dumps are stored in the public AWS S3 bucket
under `s3://canton-public-releases/data-continuity-dumps/`.
This will be downloaded during the test run, so running the tests locally requires your machine having Internet access.

To list the dumps, run `aws s3 ls s3://canton-public-releases/data-continuity-dumps/ --no-sign-request`.

If the data continuity tests fail, it means that we have introduced a breaking change within the same protocol version,
which would require a new protocol version and therefore new dumps.

Data continuity dumps for regular Canton releases are published during the "tag" build of a release commit in CI.

Data continuity dumps for `release-line-3\..+` are published by approving the manual CI workflow
`manual_generate_data_continuity_dumps` on the relevant commit.

Should there arise a need to manipulate the data continuity dumps or releases under `s3://canton-public-releases/`,
Canton team leads have "delete" permissions on the bucket. In order to do this:
1. Use the 3x3 dots menu of your Google account and select "AWS Web Services".
2. Type "S3" in the search and select it.
3. Search for the bucket `canton-public-releases`.
4. Navigate and select what you need to delete, then click on the "Delete" button, then confirm the deletion.

### Data continuity test and CI
**Postgres**

Creating and restoring dumps uses standard tools that are available on most of the images.

### Protobuf continuity tests
As part of our static tests, we run a check for Protobuf continuity breaking changes in the Canton protobuf definitions.
Concretely, this is done via `sbt protobufContinuityCheck`.

#### Protobuf continuity
Fundamentally, our notion of Protobuf continuity is based on [wire-compatibility](https://docs.buf.build/breaking/rules).
However, due to our security-sensitive context, we enforce that changes are both forwards-
and-backwards compatible wrt to parsing to not open up parsing attacks as demonstrated in the `ProtobufParsingAttackTest`.
For example, this means that we don't allow adding a new field to an existing Protobuf message.

#### Implementation
The Protobuf continuity checks use the CLI tool [Buf](https://docs.buf.build/) and a custom configuration of
its Protobuf breaking change detection.

Buf's breaking change detection doesn't support all checks we need for our particular notion of Protobuf continuity.
For example, detecting the addition of a new field to a message, is not ordinarily possible.
We use the trick of running the breaking change detection 'backwards' and instead looking for the deletion of a field
to a message to work around this limitation.
Apart from that, fundamentally the tests use a Protobuf snapshot along with ~[golden-master-testing](https://stevenschwenke.de/whatIsTheGoldenMasterTechnique).


#### If the tests fail
Generally, Protobuf continuity isn't allowed to break.
Thus, if you made changes Protobuf continuity-incompatible changes (such that the tests failed),
you will need to revert or adapt your changes.
Often, this means introducing a new message or adding a new `v{n+1}` Protobuf package.
Please see the section on "Backwards incompatible Protobuf changes" for more guidance.

The only exception is if you are adapting a Protobuf message or service that hasn't
been part of any release yet. In that case, generate a new snapshot (`sbt generateNewProtobufSnapshot`)
and checking it in. In all other cases, you need to adapt your changes such that the tests no longer fail.

Note that you need to have installed [Buf](https://docs.buf.build/) (version >= 1.x) and
added it to your PATH for `sbt generateNewProtobufSnapshot` and `sbt protobufContinuityCheck` to work locally.

If you have extended an admin-api endpoint message in a backwards compatible way, you can turn off the strict
check by editing the file `backwards-buf.yaml` and adding an explicit exception.

Backwards incompatible Protobuf changes
=======================================
The following documentation describes how we support backwards-incompatible changes of Protobuf message definitions between versions.

We will consider two separate categories of Protobuf messages:
1. Those that are part of the consensus protocol (type 1).
2. The others (admin API, sequencer API) (type 2).

### Messages that are part of the consensus protocol
Messages that are part of the consensus protocol are more difficult to handle because we need to make sure that each
entity will reach the same conclusion when reading such a message. In that context, even though some changes to the Protobuf
files would be considered backward compatible from a Protobuf point of view,
they can lead to a behaviour which depends on the version of the Profobuf parser (see `ProtobufParsingAttackTest`).
Hence, a change that is Protobuf backward compatible is not necessarily backward compatible from a protocol version
point of view.
In particular, adding a field in a Protobuf message should be done by adding a new version of the message.

In general, we distinguish between two kinds of Protobuf messages
1. Those Protobuf messages which are never serialized into an 'anonymous' binary format (like `ByteString` or `Array[Byte]`).
2. Those Protobuf messages which are serialized into an 'anonymous' binary format.

### Messages that are not part of the consensus protocol
For Protobuf messages of type 2 (and Protobuf service definitions) that are not part of the consensus protocol (see above),
we generally follow the standard best practices as laid out in
[Microsoft's gRPC versioning guidelines](https://docs.microsoft.com/en-us/aspnet/core/grpc/versioning?view=aspnetcore-5.0).
In short, they can be summarized as:
- Avoid breaking changes if reasonable.
- Don't update the version number unless making breaking changes.
- Do update the version number when you make breaking changes.

Moreover, we try to adopt the following additional guidelines:
- Wrap empty parameters in a message (e.g., `SequencerConnect.GetSynchronizerParametersRequest`). This allows to keep the
  same type when a parameter needs to be added to a request.
- Similarly, wrap responses in dedicated object (see `SequencerConnect.GetSynchronizerParametersResponse`).
- Use `oneof`'s to version message attributes.

Please see the section and examples under Serialization for what exactly this means in our code base.

For Protobuf messages of type 1., we use a more complex solution because we need to encode at least what Canton version an anonymous binary blob comes from
(Consider the scenario where you are given a binary blob and know that it contains either v0 or v1 of a Protobuf message. What deserializer do you use?).

The rest of this document focuses exclusively on messages of type 1.

# Some generic considerations

## Messages that are persisted should be versioned
All messages that are persisted in the database should be versioned using the tools presented below.
This is to avoid future issues when we want to update a data structure and need to support multiple versions of it.

# Messages that are serialized to anonymous binary format
We focus now on the messages that are serialized/deserialized to/from anonymous binary format (`ByteString` or `Array[Byte]`).

## Context
The Protobuf message for versioned data structures is the following:
```
message UntypedVersionedMessage {
  oneof wrapper {
    bytes data = 1;
  }
  int32 version = 2;
}
```
The `version` integer corresponds to the version number of the Protobuf while `data` contains the serialized instance.
For more safety, there exists a typed alias `VersionedMessage[T]`, where `T` denotes the corresponding (case) class (e.g.,
`SequencedEvent`).

Note: the `bytes data` field is wrapped into a oneof for backward compatibility for previously generated messages (this
prevents the message to be empty if bytes is empty and version is 0).

### Deserialization
With the above notations, deserialization is a method
```scala
fromProtoVersioned[T]: VersionedMessage[T] => Either[ProtoDeserializationError, T]=ParsingResult[T]
```
or, equivalently,
```scala
fromByteString[T]: ByteString => Either[ProtoDeserializationError, T]=ParsingResult[T]
```
In this second case, the bytes are first parsed into a `UntypedVersionMessage` and then `fromProtoVersioned` is used.

The deserialization of `T` is completely determined by a list of deserializers, one for each Proto version.
For example, if `SequencedEvent` has Proto versions `v0.SequencedEvent` and `v1.SequencedEvent`,
then two serializers need to be implemented in order to be able to define `fromProtoVersioned[SequencedEvent]`
```scala
v0.SequencedEvent => ParsingResult[SequencedEvent]
v1.SequencedEvent => ParsingResult[SequencedEvent]
```

### Serialization and representative protocol version
Since the protocol version of a synchronizer impacts serialization, then the serialization of an element of type `T`
can be thought as a mapping
```scala
(T, ProtocolVersion) => VersionedMessage[T]
```

Now, since each protocol version does not necessarily yield to a new Proto version for type `T`, we don't need
a map `T => VersionedMessage[T]` for each protocol version. Rather, we need a new serializer every time a new protobuf
version for `T` is introduced.

As an example, let's consider the following versioning scheme:

| Proto version | Introduced with protocol version |
|---------------|----------------------------------|
| v0            | 2.0.0                            |
| v1            | 5.0.0                            |

Then, we need one serializer for protocol version `2.0.0`, `3.0.0`, `4.0.0` and one for versions `5.0.0` and above.
In this setting, we will call protocol versions `2.0.0` and `5.0.0` *representative protocol versions* for the type `T`.
The representative protocol version corresponds to the protocol version when the new Proto version was introduced.

**Important notes**

1. In general, the representative protocol version of a message shipped over the synchronizer is different
   from the synchronizer protocol version.

2. Except in some very specific cases, we should never convert a representative protocol version for a type to the one
   of another type (e.g., do not convert a `RepresentativeProtocolVersion[StaticSynchronizerParameters]` to a
   `RepresentativeProtocolVersion[ConfirmationResponse]`). See comment in `RepresentativeProtocolVersion.representative`
   for an explanation.

The reason we use the word *representative* is that the list of Proto versions of a given message induces
an equivalence relation on the list of protocol versions. Hence, the representative protocol version is just
a representative of the corresponding equivalence class.

Note that the decomposition of the list of protocol versions into different equivalence classes depend on the data
structure. Hence, the representative protocol version is a parametrized class
```scala
sealed abstract case class RepresentativeProtocolVersion[+ValueClass](
    private val v: ProtocolVersion
)
```

## Helpers for classes

In our code base, classes that are serialized to bytes implement the trait `HasProtocolVersionedWrapper` or `HasVersionedWrapper`.
The trait `HasProtocolVersionedWrapper` is used when the class contains the protocol version information (i.e., its
representative protocol version), which means that serialization is completely determined by the class instance.

In such a case, serialization methods are:
```scala
def toProtoVersioned: Proto
def toByteString: ByteString
```
Among others, this is the case for protocol messages (since they are tied to a synchronizer, the protocol version is prescribed and so is the
serializer version) and data structures that have memoized evidence (because serialization should yield the original byte string).

On the other hand, a class that implements `HasVersionedWrapper` has the following methods:
```scala
def toProtoVersioned(version: ProtocolVersion): Proto
def toByteString(version: ProtocolVersion): ByteString
```
In this case, serialization requires the protocol version to be passed explicitly.

As examples, you can have a look at the following classes:

- for `HasProtocolVersionWrapper`: `ConfirmationResponse`,
- for `HasVersionedWrapper`: `SerializableContract`.

## Helpers for companion objects

Helpers for companion objects of classes that are deserialized can be found in two traits (and their subtraits):

- Trait `HasVersionWrapperCompanion` for classes that implement `HasVersionedWrapper` (such classes do not embed a
  representative protocol version).
- Trait `BaseVersioningCompanion` for classes that implement `HasProtocolVersionedWrapper` (which embed
  a representative protocol version).

### HasVersionWrapperCompanion
In its basic version, one needs to provide the list of deserializers (one for each Proto version) and the name of the
object (for logging purposes). One example is the following:
```scala
object EncryptionPrivateKey extends HasVersionedMessageCompanion[EncryptionPrivateKey] {
  val supportedProtoVersions: Map[Int, Parser] = Map(
    0 -> supportedProtoVersion(v0.EncryptionPrivateKey)(fromProtoV0)
  )

  override def name: String = "encryption private key"
  // ...
```

Then, different methods will be available:
```scala
def fromProtoVersioned(proto: VersionedMessage[ValueClass]): ParsingResult[ValueClass]
def fromByteString(bytes: ByteString): ParsingResult[ValueClass]
// ...
```

A similar trait, `HasVersionedMessageWithContextCompanion` allows to pass some additional context for the deserialization.
Object `AssignmentViewTree` is one example of object that implements it, with context `HashOps`.
```scala
object AssignmentViewTree
    extends HasVersionedMessageWithContextCompanion[AssignmentViewTree, HashOps] {
  override val name: String = "AssignmentViewTree"

  val supportedProtoVersions: Map[Int, Parser] = Map(
    0 -> supportedProtoVersion(v0.ReassignmentViewTree)(fromProtoV0)
  )

  def fromProtoV0(
    hashOps: HashOps,
    assignmentViewTreeP: v0.ReassignmentViewTree,
  ): ParsingResult[AssignmentViewTree] = ???
```

Again, one can then invoke the helpers to parse serialized data:
```scala
  def fromProtoVersioned(ctx: Ctx)(proto: VersionedMessage[ValueClass]): ParsingResult[ValueClass]
  def fromByteString(ctx: Ctx)(bytes: ByteString): ParsingResult[ValueClass]
```

### BaseVersioningCompanion
If you don't know which sub-trait of BaseVersioningCompanion to pick, refer to [How to: choose sub-trait of BaseVersioningCompanion](how-to-choose-BaseVersioningCompanion.md).

The trait `BaseVersioningCompanion` implements helpers for classes that contain a representative protocol
version. Because they include this additional piece of information, it allows to derive some additional methods
for serialization. Similarly to `HasVersionWrapperCompanion`, several versions exists (with and without context,
with and without memoization). We present here a one example (with context, without memoization):
```scala
object ViewCommonData
  extends VersioningCompanionContextMemoizationNoDependency[
    ViewCommonData,
    HashOps,
  ] {
  override val name: String = "ViewCommonData"

  val versioningTable: VersioningTable = VersioningTable(
    ProtoVersion(30) -> VersionedProtoCodec(ProtocolVersion.v34)(v30.ViewCommonData)(
      supportedProtoVersionMemoized(_)(fromProtoV30),
      _.toProtoV30,
    )
  )
```
With each Proto version, one specifies:

- the representative protocol version (when the Proto version was introduced),
- the deserializer,
- the serializer.

Then, in addition to the deserialization helpers on the companion objects, the case class can extend
`HasProtocolVersionedWrapper[ViewCommonData]` and benefits from serialization helpers.

## Container classes
Some Scala classes are generic containers that abstracts over common functionality.
For example, `SignedContent` adds a signature with a timestamp for the signing key to different kinds of messages.
In Scala, `SignedContent` takes a covariant type argument `+A` with a suitable type bound.
The Protobuf message represents the corresponding field as `bytes`.
(Other use cases may use an `UntypedVersionedMessage` directly when the exact serialization does not need to be known.)

Such type arguments do not work nicely with the above helper classes,
because it is difficult to pass in the deserializer for the content as a parameter to the deserialization methods.
Instead, deserialization for such containers should leave the contents as bytestrings
(using a suitable wrapper class to meet the type bounds, e.g., `ByteStringWithCryptographicEvidence`),
so that the caller can then subsequently deserialize the contents.
Typically, the content deserializer can be applied using a `traverse` on the container.

The helper classes for the companion object therefore come in another variant:
`HasProtocolVersionedCompanion2` and `HasMemoizedProtocolVersionedWrapperCompanion2` take two type arguments,
one for the container types to be serialized and one for the type of the deserialized container with serialized content.
For example, `SignedContent` uses `SignedContent[HasCryptographicEvidence]` for the value type
and `SignedContent[BytestringWithCryptographicEvidence]` for the deserialized type.

## Other generic considerations
In this section, the examples we will use are based on having a first version of a `SequencedEvent` (further down written `v0.SequencedEvent`)
that looks like this:
```
message SequencedEvent {
    int64 counter = 1;
    Batch batch = 2;
}
```

We also have a second version of a `SequencedEvent` (further down written `v1.SequencedEvent`) that looks like this:
```
message SequencedEvent {
    int64 counter = 1;
    int64 counter2 = 2;
    Batch batch = 3;
}
```
While `v0.SequencedEvent` was introduced with Canton v2.0.0 and PV=2, `v1.SequencedEvent` is the new version introduced
in Canton v2.3.0 and is used when PV=3.

### Protobuf files and new versions
Backwards-incompatible Protobuf message definitions are located in the same parent package as their predecessors, but
with a bumped version number.

E.g. if the previous definition of Protobuf message `SequencedEvent` is in `community/common/src/main/protobuf/com/digitalasset/canton/protocol/v0/sequencing.proto`,
the new definition will be in `community/common/src/main/protobuf/com/digitalasset/canton/protocol/v1/sequencing.proto`.

As far as possible, we version Protobuf messages separately, and only add new versions when needed.

E.g. Introducing `v1.SequencedEvent` in addition to `v0.SequencedEvent`, does not have the effect that we automatically
also add version `v1` for Protobuf message `SynchronizerParameters` (which is also defined in `sequencing.proto`) or `Batch`
(which is part of `SequencedEvent` but didn't change between `v0.SequencedEvent` and `v1.SequencedEvent`).
However, note, that the introduction of a `v1.Batch` in addition to `v0.Batch`, __would__ require the addition of
`v1.SequencedEvent` (because a `SequencedEvent` defines whether it uses `v0.Batch` or `v1.Batch`).

### Serialization
All Scala data classes belonging to Protobuf message definitions which need a `toByteString` or `toByteArray` method have
a common wrapper Protobuf message `UntypedVersionedMessage`.

E.g. the Scala data class `SequencedEvent` needs a `toByteString` method because we will write it as ByteString to `SequencedEventStore`.

We try our best to make a given Scala data class convertible to every single corresponding Protobuf message definition
supported in the current Canton version. The goal is to avoid exception being thrown during the serialization process.

E.g., if Canton v2.0 supports `v0.SequencedEvent` and `v1.SequencedEvent`, then `SequencedEvent` will expose the methods
`toProtoV0: v0.SequencedEvent`, `toProtoV1: v1.SequencedEvent`, and `toProtoVersioned(version: ProtocolVersion): VersionedMessage[SequencedEvent]`, where
`VersionedMessage[SequencedEvent]` is just a typed alias for `UntypedVersionedMessage`.

### Deserialization
The companion object of a Scala data class will support `fromProto...` methods for every Protobuf message definition we
support in the current Canton version as well as for `UntypedVersionedMessage`.

E.g. on Canton v2.1, Scala class `SequencedEvent` will expose the methods `fromProtoV0(arg: v0.SequencedEvent)`,
`fromProtoV1(arg: v1.SequencedEvent)` and `fromProtoVersioned(arg: UntypedVersionedMessage)`.

Note that the boilerplate code is inherited from traits `HasVersionWrapperCompanion` and `HasProtocolVersionedWrapperCompanion`
and their subtraits (see section [Helpers for companion objects](##helpers-for-companion-objects)).

To support `fromProto...` methods for every Protobuf message definition we support in the current Canton version, we
will use sensible default arguments if we can and optional arguments if we must.

#### Necessity of version data
This is an historical note. We had some messages that were previously persisted without versioning information.
This causes an issue because we then need to wire the protocol version to the place where the deserializer is created.
Moreover, it is sometimes difficult to ensure that the version you pass is the one that was used when you serialized
the message.

### Single Scala class
Despite possibly having multiple Protobuf message definitions for a single 'entity', we try to have only one corresponding
Scala data class. This is to simplify the code and keep versioning considerations as localized as possible.

E.g. the 'entity' `SequencedEvent` may have a corresponding `v0.SequencedEvent` and a `v1.SequencedEvent`, but in our
Scala code base we will only have one case class/trait called `SequencedEvent`.

In some cases, having a single internal class for different versions of the Protobuf message is not possible. This can
happen when the behavior changes significantly between the two versions (see, e.g., `EncryptedViewMessageV0` and
`EncryptedViewMessageV1`). In such a situation, we recommend having a trait that exposes the common behavior and
multiple sub-classes.

### Inclusion of the protocol version in Protobuf messages
In most of the cases, there is no need to include the protocol version in the Protobuf messages: upon deserialization,
the representative protocol version of the corresponding data structures is automatically computed and can be re-used
for subsequent serializaation.

However, in some cases, embedding the protocol version can be useful. One can cite the following cases:

- Reassignments
  Since we have two synchronizers in scope and a mixture of versions (e.g., the `AssignmentView` contains the `UnassignmentResultEvent`),
  then we need to store protocol version of the source and/or target synchronizer.
- Nested data structures (e.g., signed topology transactions) for which you need the protocol version to perform
  some operations.
- Some protocol messages
  E.g., the `InformeeMessage` whose protocol version is passed to `ConfirmationResultMessage`.

### Stable and unstable protobuf messages

Every protobuf message definition should be marked as stable or unstable using the message-level options
```protobuf
option (scalapb.message).companion_extends = "com.digitalasset.canton.version.StableProtoVersion";
```
or
```protobuf
option (scalapb.message).companion_extends = "com.digitalasset.canton.version.AlphaProtoVersion";
```
with the import `import "scalapb/scalapb.proto";`.
Protocol message definitions tagged as unstable may be modified freely, whereas stable ones must not.
Accordingly, alpha Protobuf messages only be used in alpha protocol versions,
while stable ones can be used in both stable and alpha protocol versions.
If you try to use an alpha protocol message in a stable protocol version,
the Scala compiler fails with a corresponding type error when you constructed the appropriate `ProtoCodec`.

Accordingly, to mark a new protocol version as stable, you must change the annotations on all releveant Protobuf messages and make them stable.
