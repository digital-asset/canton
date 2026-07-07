// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton

import cats.Id
import com.digitalasset.canton.util.ReassignmentTag
import com.google.protobuf.ByteString

package object version {
  type VersionedMessage[+M] = VersionedMessageImpl.Instance.VersionedMessage[M]
  type VersionedJsonMessage[+M] = VersionedJsonMessageImpl.Instance.VersionedJsonMessage[M]
  type OriginalByteString = ByteString // What is passed to the fromByteString method
  type DataByteString = ByteString // What is inside the parsed UntypedVersionedMessage message

  // Aliases for F=Id (when serialization never fails)
  type HasToByteString = HasToByteStringF[Id]

  type HasVersionedMessageCompanion[ValueClass] = HasVersionedMessageCompanionF[Id, ValueClass]

  type BaseVersioningCompanion[
      ValueClass <: HasRepresentativeProtocolVersion,
      Context,
      DeserializedValueClass <: HasRepresentativeProtocolVersion,
      Dependency,
  ] = BaseVersioningCompanionF[Id, ValueClass, Context, DeserializedValueClass, Dependency]

  type VersioningCompanion2[
      ValueClass <: HasRepresentativeProtocolVersion,
      DeserializedValueClass <: HasRepresentativeProtocolVersion,
  ] = VersioningCompanion2F[Id, ValueClass, DeserializedValueClass]

  type VersioningCompanionContextTaggedPVValidation2[
      ValueClass <: HasRepresentativeProtocolVersion,
      T[X] <: ReassignmentTag[X],
      RawContext,
  ] = VersioningCompanionContextTaggedPVValidation2F[Id, ValueClass, T, RawContext]

  type VersioningCompanionContext2[
      ValueClass <: HasRepresentativeProtocolVersion,
      DeserializedValueClass <: HasRepresentativeProtocolVersion,
      Context,
  ] = VersioningCompanionContext2F[Id, ValueClass, DeserializedValueClass, Context]

  type VersioningCompanionMemoization2[
      ValueClass <: HasRepresentativeProtocolVersion,
      DeserializedValueClass <: HasRepresentativeProtocolVersion,
  ] = VersioningCompanionMemoization2F[Id, ValueClass, DeserializedValueClass]

  type VersioningCompanionContextMemoization2[
      ValueClass <: HasRepresentativeProtocolVersion,
      Context,
      DeserializedValueClass <: HasRepresentativeProtocolVersion,
      Dependency,
  ] = VersioningCompanionContextMemoization2F[
    Id,
    ValueClass,
    Context,
    DeserializedValueClass,
    Dependency,
  ]

  type VersioningCompanionContextPVValidation2[
      ValueClass <: HasRepresentativeProtocolVersion,
      RawContext,
  ] = VersioningCompanionContextPVValidation2F[Id, ValueClass, RawContext]

  type HasVersionedMessageCompanionCommon[ValueClass] =
    HasVersionedMessageCompanionCommonF[Id, ValueClass]

  // Aliases for F=Either[String, *] (when serialization van fail)
  type HasVersionedMessageCompanionE[ValueClass] =
    HasVersionedMessageCompanionF[Either[String, *], ValueClass]

  type BaseVersioningCompanionE[
      ValueClass <: HasRepresentativeProtocolVersion,
      Context,
      DeserializedValueClass <: HasRepresentativeProtocolVersion,
      Dependency,
  ] = BaseVersioningCompanionF[
    Either[String, *],
    ValueClass,
    Context,
    DeserializedValueClass,
    Dependency,
  ]

  type VersioningCompanion2E[
      ValueClass <: HasRepresentativeProtocolVersion,
      DeserializedValueClass <: HasRepresentativeProtocolVersion,
  ] = VersioningCompanion2F[Either[String, *], ValueClass, DeserializedValueClass]

  type VersioningCompanionContextTaggedPVValidation2E[
      ValueClass <: HasRepresentativeProtocolVersion,
      T[X] <: ReassignmentTag[X],
      RawContext,
  ] = VersioningCompanionContextTaggedPVValidation2F[Either[String, *], ValueClass, T, RawContext]

  type VersioningCompanionContext2E[
      ValueClass <: HasRepresentativeProtocolVersion,
      DeserializedValueClass <: HasRepresentativeProtocolVersion,
      Context,
  ] = VersioningCompanionContext2F[Either[String, *], ValueClass, DeserializedValueClass, Context]

  type VersioningCompanionMemoizationE[
      ValueClass <: HasRepresentativeProtocolVersion
  ] = VersioningCompanionMemoization2F[Either[String, *], ValueClass, ValueClass]

  type VersioningCompanionMemoization2E[
      ValueClass <: HasRepresentativeProtocolVersion,
      DeserializedValueClass <: HasRepresentativeProtocolVersion,
  ] = VersioningCompanionMemoization2F[Either[String, *], ValueClass, DeserializedValueClass]

  type VersioningCompanionContextMemoization2E[
      ValueClass <: HasRepresentativeProtocolVersion,
      Context,
      DeserializedValueClass <: HasRepresentativeProtocolVersion,
      Dependency,
  ] = VersioningCompanionContextMemoization2F[
    Id,
    ValueClass,
    Context,
    DeserializedValueClass,
    Dependency,
  ]

  type VersioningCompanionContextPVValidation2E[
      ValueClass <: HasRepresentativeProtocolVersion,
      RawContext,
  ] = VersioningCompanionContextPVValidation2F[Either[String, *], ValueClass, RawContext]

  type HasVersionedMessageCompanionCommonE[ValueClass] =
    HasVersionedMessageCompanionCommonF[Either[String, *], ValueClass]

  // Main use cases
  type VersioningCompanionContextMemoization[
      ValueClass <: HasRepresentativeProtocolVersion,
      Context,
  ] = VersioningCompanionContextMemoization2F[Id, ValueClass, Context, ValueClass, Unit]

  type VersioningCompanionContext[
      ValueClass <: HasRepresentativeProtocolVersion,
      Context,
  ] = VersioningCompanionContext2[ValueClass, ValueClass, Context]

  type VersioningCompanionMemoization[
      ValueClass <: HasRepresentativeProtocolVersion
  ] = VersioningCompanionMemoization2F[Id, ValueClass, ValueClass]

  type VersioningCompanion[ValueClass <: HasRepresentativeProtocolVersion] =
    VersioningCompanion2F[Id, ValueClass, ValueClass]

  // Dependency
  type VersioningCompanionContextMemoizationWithDependency[
      ValueClass <: HasRepresentativeProtocolVersion,
      Context,
      Dependency,
  ] = VersioningCompanionContextMemoization2F[Id, ValueClass, Context, ValueClass, Dependency]
}
