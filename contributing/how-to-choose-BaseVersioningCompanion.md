How to: choose sub-trait of BaseVersioningCompanion
===================================================

The goal of this page is to help you pick the correct sub-trait of `BaseVersioningCompanion` for your use case.
The different sub-traits expose different behavior and signature of the `fromByteString` and
`fromProtoVxy` methods.

The section [naming scheme](#the-naming-scheme) explains how the different traits are named, which will allow
you to pick the correct one.
If you don't understand some of the concepts, refer to section [concepts](#concepts-around-versioning).

# The naming scheme

## Basic use cases
We present first the basic use cases.
The name of each trait starts with prefix `VersioningCompanion`  and indicates what features it supports:

- with or without context
- with or without memoization

If you don't know what are [context](#context) or [memoization](#memoization), read [below](#concepts-around-versioning).

For example, if you need context but no memoization, you should use `VersioningCompanionContext`, while
if you need both, then you should use `VersioningCompanionContextMemoization`.
These aliases are defined [here](https://github.com/DACH-NY/canton/blob/main/community/base/src/main/scala/com/digitalasset/canton/version/version.scala).

## Advanced use cases

### Deserialized class has different type from the initial class
In this case, use suffix `2` to indicates that you have two value classes in scope.
For example, if you need context but no memoization, you should use `VersioningCompanionContext2`.

### Dependency on the protocol version
Some deserialization methods need the protocol version to be passed explicitly.
One example is the `EnvelopeContent` for which we have
```scala
def fromProtoV30(
      context: (HashOps, ProtocolVersion),
      contentP: v30.EnvelopeContent,
  ): ParsingResult[EnvelopeContent]
```
Because the protocol version is added to the context, that lead to the following cumbersome syntax:
```scala
EnvelopeContent.fromByteString(pv)((pv, hashOps))(bytes)
```
In that case, using the trait `VersioningCompanionContextPVValidation2` will allow you
to use the nicer syntax
```scala
EnvelopeContent.fromByteString(hashOps, pv)(bytes)
```
where the protocol version is not passed twice.

### Dependency
It can happen that you need to tie two versioning schemes together.
This is the case for the `SubmissionRequest`: its versioning scheme needs to be aligned with the
one of `Recipients` (we say the `SubmissionRequest` versioning depends on `Recipients` versioning).
In that case, reach out to Raf or Andreas to discuss.

# Concepts around versioning

The simplest version of the deserialization method is
```scala
def fromByteString(data: ByteString): ParsingResult[ValueClass]
```
which will call the correct `fromProto` method, e.g.,
```scala
def fromProtoV30(proto: v30.AggregationRule): ParsingResult[AggregationRule]
```

However, in order to prevent against downgrading attack, we also pass the protocol version to the
`fromByteString` method: it allows to check that the version of the message is compatible with the
protocol version of the synchronizer.

Hence, the simplest versions are:
```scala
def fromByteString(pv: ProtocolVersion)(data: ByteString): ParsingResult[MyClass]
def fromProtoV30(proto: ProtoMessage): ParsingResult[MyClass]
```

## Context
In this simplest version presented above, the only input is the bytestring or the protobuf message (of type `scalapb.GeneratedMessage`).
However, it can happen that additional data is required for the deserialization. We refer to the type of this additional
data as the `Context`.

In most of the cases, the `Context` consists of `HashOps` or `ProtocolVersion`.
In that case, the signatures of deserialization methods will look like
```scala
def fromByteString(pv: ProtocolVersion)(context: Context)(bytes: OriginalByteString): ParsingResult[MyClass]
def fromProtoV30(context: Context)(proto: v30.ProtoMessage): ParsingResult[MyClass]
```
For example, in the case where the context is the protocol version, we have:
```scala
def fromProtoV30(
      expectedProtocolVersion: ProtocolVersion,
      message: v30.TopologyTransactionsBroadcast,
  ): ParsingResult[TopologyTransactionsBroadcast]
```

## Memoization
In some cases, we want to guarantee that the re-serialization of a message will be the same as the initial serialization.
For example, consider the following scenario:

- start with a `c1: ConfirmationResponse` that is serialized to a bytestring `bs1` and signed
- bytestring `bs1` is sent and deserialized to a confirmation response `c2`
- `c2` is serialized again to a bytestring `bs2`
We want to guarantee that the two bytestrings `bs1` and `bs2` are the same, so that the initial signature still apply.

In that context, we want **memoization**. This is done by storing the bytestring in the class instance:
```scala
case class ConfirmationResponse private (
    requestId: RequestId,
    sender: ParticipantId,
  ...
)(
    override val representativeProtocolVersion: RepresentativeProtocolVersion[
      ConfirmationResponse.type
    ],
    override val deserializedFrom: Option[ByteString],
) extends  HasProtocolVersionedWrapper[ConfirmationResponse]
```

In that case, the signatures of deserialization methods will look like
```scala
def fromByteString(pv: ProtocolVersion)(bytes: OriginalByteString): ParsingResult[MyClass]
def fromProtoV30(proto: v30.ProtoMessage)(bytes: ByteString): ParsingResult[MyClass]
```
For example, in the case of the confirmation response:
```scala
def fromProtoV30(proto: v30.ConfirmationResponse)(bytes: ByteString): ParsingResult[ConfirmationResponse]
```

## Deserialized class has different type from the initial class
In most of the cases, starting with an instance `c: MyClass`, serializing to a bytestring and deserializing
the bytestring gives you an instance of `MyClass`.

However, there can be situations where the instance that you get after deserialization has a different type.
This is the case with batches:
```scala
final case class Batch[+Env <: Envelope[?]]
```
After deserialization, the result is a batch of closed envelopes, which is encoded as follows
```scala
object Batch
    extends VersioningCompanion2[
      Batch[Envelope[?]], // initial type
      Batch[ClosedEnvelope] // type after deserialization
    ]
```

The suffix `2` at the end of the name indicates that you have two types in scope: the one from before the serialization
and the one from after the serialization.

# Internal concepts
In this section, we present some concepts that are used in the tooling itself.

## Type parameters
The versioning tooling (e.g., `ProtoCodec`, `VersionedProtoCodec`) uses many type parameters:

- `ValueClass`: the internal class
- `Context`: the additional input for the `fromByteString` method.
  If there is no context, we use `Context=Unit`
- `DeserializedValueClass`: the type after deserialization
  It is usually equal to `ValueClass` (see [above](#deserialized-class-has-different-type-from-the-initial-class) for an example)
- `Comp`: Companion object (`this.type` in `BaseVersioningCompanion` and descendants)
- `Dependency`: Other class the versioning scheme depends on.
  In most of the cases, there is not dependency which means `Dependency=Unit`.
  See [above](#dependency) for an example
