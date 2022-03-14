// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.db

import com.digitalasset.canton.participant.LedgerSyncEvent
import com.digitalasset.canton.participant.protocol.version.VersionedLedgerSyncEvent
import com.digitalasset.canton.participant.store.SerializableLedgerSyncEvent
import com.digitalasset.canton.serialization.ProtoConverter
import com.digitalasset.canton.store.db.DbDeserializationException
import com.digitalasset.canton.tracing.{TraceContext, Traced}
import com.digitalasset.canton.version.ProtocolVersion
import slick.jdbc.{GetResult, SetParameter}

object ParticipantStorageImplicits {

  // the reader and setting for the LedgerSyncEvent can throw DbSerializationException and DbDeserializationExceptions
  // which is currently permitted for storage operations as we have no practical alternative with the slick api
  private[db] implicit def getLedgerSyncEvent(implicit
      getResultByteArray: GetResult[Array[Byte]]
  ): GetResult[LedgerSyncEvent] = {
    GetResult(r => bytesToEvent(r.<<[Array[Byte]]))
  }

  private def bytesToEvent(bytes: Array[Byte]): LedgerSyncEvent = {
    ProtoConverter
      .protoParserArray(VersionedLedgerSyncEvent.parseFrom)(bytes)
      .flatMap(SerializableLedgerSyncEvent.fromProtoVersioned)
      .fold(
        err =>
          throw new DbDeserializationException(
            s"LedgerSyncEvent protobuf deserialization error: $err"
          ),
        identity,
      )
  }

  private def eventToBytes(event: LedgerSyncEvent): Array[Byte] =
    SerializableLedgerSyncEvent(event).toByteArray(ProtocolVersion.v2_0_0_Todo_i8793)

  private[db] implicit def setLedgerSyncEvent(implicit
      setParameterByteArray: SetParameter[Array[Byte]]
  ): SetParameter[LedgerSyncEvent] = (v, pp) => pp >> eventToBytes(v)

  private[participant] implicit def getOptionLedgerSyncEvent(implicit
      getResultByteArrayO: GetResult[Option[Array[Byte]]]
  ): GetResult[Option[LedgerSyncEvent]] =
    _.<<[Option[Array[Byte]]].map(bytesToEvent)

  private[participant] implicit def setOptionLedgerSyncEvent(implicit
      setParameterByteArrayO: SetParameter[Option[Array[Byte]]]
  ): SetParameter[Option[LedgerSyncEvent]] = (v, pp) => pp >> v.map(eventToBytes)

  private[db] implicit def getTracedLedgerSyncEvent(implicit
      getResultByteArray: GetResult[Array[Byte]]
  ): GetResult[Traced[LedgerSyncEvent]] =
    GetResult { r =>
      import TraceContext.readTraceContextFromDb

      val event = GetResult[LedgerSyncEvent].apply(r)
      implicit val traceContext: TraceContext = GetResult[TraceContext].apply(r)

      Traced(event)
    }
}
