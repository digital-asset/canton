// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.participant.store.data

import com.digitalasset.canton.InternedPartyId
import com.digitalasset.canton.participant.store.AcsDigestStore.{
  HashedDigest,
  InternedParticipantId,
  PartyAndOrder,
  RawDigest,
}
import com.digitalasset.canton.resource.DbStorage.Implicits.setParameterArrayFromToDbPrimitive
import com.digitalasset.canton.resource.{DbStorage, ToDbPrimitive}
import com.digitalasset.canton.store.db.DbDeserializationException
import com.google.protobuf.ByteString
import slick.jdbc.{GetResult, SetParameter}

import scala.reflect.ClassTag

sealed trait DbAcsDigestJournalImplicits[K, V] {
  implicit def classTag: ClassTag[K]
  implicit def getResultKey: GetResult[K]
  implicit def getResultVal: GetResult[Option[V]]
  implicit def setParamKey: SetParameter[K]
  implicit def setParamVal: SetParameter[Option[V]]
  implicit def setParamArrayKey: SetParameter[Array[K]]

  def toKeysArray(iterableK: Iterable[K]): Array[K] =
    iterableK.toArray
}

object DbAcsDigestJournalImplicits {
  final case class PartyJournalImplicits(storage: DbStorage)
      extends DbAcsDigestJournalImplicits[PartyAndOrder[InternedPartyId], RawDigest] {

    override implicit val classTag: ClassTag[PartyAndOrder[InternedPartyId]] =
      ClassTag(classOf[PartyAndOrder[InternedPartyId]])

    override implicit val getResultKey: GetResult[PartyAndOrder[InternedPartyId]] =
      GetResult[PartyAndOrder[InternedPartyId]] { pr =>
        val encoded = pr.nextInt()
        PartyAndOrder.decodePartyAndOrder(encoded)
      }

    // Note about underlying DB type:
    // in H2 we use `VARBINARY` data type hence we use custom GetResult implicits
    // see the relevant sql migration file community/common/src/main/resources/db/migration/canton/h2
    // for more details
    override implicit val getResultVal: GetResult[Option[RawDigest]] =
      GetResult[Option[RawDigest]] { pr =>
        val rawBytesO = pr.nextBytesOption()
        rawBytesO.map(ByteString.copyFrom)
      }

    override implicit val setParamKey: SetParameter[PartyAndOrder[InternedPartyId]] =
      SetParameter[PartyAndOrder[InternedPartyId]] { (key, pp) =>
        val encoded = PartyAndOrder.encodePartyAndOrder(key)
        pp.setInt(encoded)
      }

    @SuppressWarnings(Array("org.wartremover.warts.Null"))
    override implicit val setParamVal: SetParameter[Option[RawDigest]] =
      SetParameter[Option[RawDigest]] { (pd, pp) =>
        pp.setBytes(pd.map(_.toByteArray).orNull)
      }

    override implicit val setParamArrayKey: SetParameter[Array[PartyAndOrder[InternedPartyId]]] =
      setParameterArrayFromToDbPrimitive(
        toDbPrimitive = ToDbPrimitive(v => PartyAndOrder.encodePartyAndOrder(v)),
        setArrayParameter = storage.DbStorageConverters.setParameterArrayInt,
        ct = implicitly[ClassTag[Int]],
      )
  }

  final case class ParticipantJournalImplicits(storage: DbStorage)
      extends DbAcsDigestJournalImplicits[InternedParticipantId, (RawDigest, HashedDigest)] {

    override implicit val classTag: ClassTag[InternedParticipantId] =
      ClassTag(classOf[InternedParticipantId])

    override implicit val getResultKey: GetResult[InternedParticipantId] =
      GetResult[InternedParticipantId] { pr =>
        pr.nextInt()
      }

    // in H2 we use `VARBINARY` data type hence we use custom GetResult implicits
    // see the relevant sql migration file community/common/src/main/resources/db/migration/canton/h2
    // for more details
    override implicit val getResultVal: GetResult[Option[(RawDigest, HashedDigest)]] =
      GetResult[Option[(RawDigest, HashedDigest)]] { pr =>
        val rawBytesO = pr.nextBytesOption()
        val hashedBytesO = pr
          .nextBytesOption()
        (rawBytesO, hashedBytesO) match {
          case (Some(rd), Some(hd)) => Some((ByteString.copyFrom(rd), ByteString.copyFrom(hd)))
          case (None, None) => None
          case (rawDigest, hashedDigest) =>
            throw new DbDeserializationException(
              s"Acs running Digest participant journal encountered an invalid database digest state! " +
                s"Raw digest presence: ${rawDigest.isDefined}, Hashed digest presence: ${hashedDigest.isDefined}. " +
                s"Both columns should be populated or NULL."
            )
        }
      }

    override implicit val setParamKey: SetParameter[InternedParticipantId] =
      SetParameter[InternedParticipantId] { (key, pp) =>
        pp.setInt(key)
      }

    override implicit val setParamVal: SetParameter[Option[(RawDigest, HashedDigest)]] =
      SetParameter[Option[(RawDigest, HashedDigest)]] { (pd, pp) =>
        pd match {
          case Some((rawDigest, hashedDigest)) =>
            pp.setBytes(rawDigest.toByteArray)
            pp.setBytes(hashedDigest.toByteArray)
          // VARBINARY is bytea in PG, varbinary in H2
          case None =>
            pp.setNull(java.sql.Types.VARBINARY)
            pp.setNull(java.sql.Types.VARBINARY)
        }
      }

    override implicit val setParamArrayKey: SetParameter[Array[InternedParticipantId]] =
      storage.converters.setParameterArrayInt
  }
}
